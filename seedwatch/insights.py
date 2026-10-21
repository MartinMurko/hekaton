"""Reproducible numbers behind "what we learned" – the problem-understanding part of the pitch.

* how much of WARN/ERROR/FATAL is noise, and which "errors" tick like a clock
* how predictable failures are at all: system-wide precursor lift and rack-local
  activity before quiet-preceded incidents (labelled data)
* whether START/FINISH-style invariants break before incidents (own environments)
"""
import bisect
import collections
import random
from pathlib import Path

import orjson

from .labels import find_incidents, find_labeled_incidents
from .profile import build_profile
from .series import Series

PRE_WINDOW = 60     # minutes looked at before an incident
ONSET_GAP = 5       # ...excluding the last minutes, which belong to the onset itself
QUIET = 360         # an incident counts as "isolated" after 6 h without alerts


def noise(series, profile):
    lines = collections.Counter()
    for p in profile["tpl"].values():
        lines[p["class"]] += p["n"]
    total = sum(lines.values()) or 1
    loud = [p for p in profile["tpl"].values()
            if p["class"] == "weed" and p["level"] in ("ERROR", "CRITICAL")
            and (p["reason"].startswith("metronom") or p["reason"].startswith("cron"))]
    loud.sort(key=lambda p: -p["n"])
    return {"warn_error_lines": total, "noise_share": round(1 - lines["seed"] / total, 3),
            "by_class": dict(lines),
            "clockwork_errors": [{"level": p["level"], "logger": p["logger"], "n": p["n"],
                                  "template": p["template"][:80], "reason": p["reason"]} for p in loud[:4]]}


def precursor_lift(series, incidents, samples=1500, seed=0):
    """Which streams/templates show up more often in the hour before isolated incidents?"""
    iso = [i for i in incidents if (i.get("quiet_before_min") or 0) >= QUIET]
    if not iso:
        return None

    def keys(i):
        ks = set()
        for j in range(max(0, i - PRE_WINDOW), i - ONSET_GAP):
            r = series.op[j]
            ks.update("lg:" + k for k in r["lg"])
            ks.update("tpl:" + k for k in r["tpl"])
        return ks

    pre = collections.Counter()
    for x in iso:
        pre.update(keys(series.index[x["start"]]))
    busy = set()
    for x in incidents:
        i = series.index[x["start"]]
        busy.update(range(i - QUIET, i + 60))
    rng = random.Random(seed)
    cands = [i for i in range(QUIET + PRE_WINDOW, len(series)) if i not in busy]
    rnd = collections.Counter()
    picks = rng.sample(cands, min(samples, len(cands)))
    for i in picks:
        rnd.update(keys(i))
    n, r = len(iso), len(picks)
    rows = sorted(((c / n) / ((rnd[k] + 0.5) / (r + 1)), c, k) for k, c in pre.items() if c >= 8)
    best = rows[-1] if rows else (0, 0, "")
    return {"isolated": n, "max_lift": round(best[0], 1), "max_lift_support": best[1], "key": best[2][:80],
            "keys_with_lift_ge_3": sum(1 for x in rows if x[0] >= 3)}


def local_precursors(raw_log, incidents, factor=3):
    """Did the rack of the first alert line log unusually much in the hour before?"""
    iso = sorted(i["start"] for i in incidents if (i.get("quiet_before_min") or 0) >= QUIET)
    racks = collections.defaultdict(list)
    first_rack = {}
    fatal = fatal_alert = 0
    with open(raw_log, "rb") as f:
        for line in f:
            p = line.split(None, 9)
            if len(p) < 9 or not p[1].isdigit():
                continue
            ts, rack, alert = int(p[1]), p[3][:6], p[0] != b"-"
            racks[rack].append(ts)
            if p[8] == b"FATAL":
                fatal += 1
                fatal_alert += alert
            if alert:
                m = ts - ts % 60
                k = bisect.bisect_right(iso, m) - 1
                if k >= 0 and iso[k] == m and m not in first_rack:
                    first_rack[m] = rack

    def cnt(rack, t0, t1):
        xs = racks[rack]
        return bisect.bisect_left(xs, t1) - bisect.bisect_left(xs, t0)

    hits = 0
    for m, rack in first_rack.items():
        before = cnt(rack, m - PRE_WINDOW * 60, m - ONSET_GAP * 60)
        base = [cnt(rack, m - h * 3600 - PRE_WINDOW * 60, m - h * 3600 - ONSET_GAP * 60) for h in range(6, 150, 3)]
        if before > factor * sum(base) / len(base) + 3:
            hits += 1
    return {"isolated": len(first_rack), "rack_local_precursor": hits,
            "fatal_lines": fatal, "fatal_not_alert": fatal - fatal_alert}


def invariants(series):
    """START/FINISH and Active/Inactive pairs: do they ever fall out of balance (10-min window)?"""
    tot = collections.Counter()
    for r in series.op:
        tot.update(r["lg"])
    pairs = []
    for k in tot:
        if ".TASK_STARTED." in k:
            head, name = k.split("TASK_STARTED.")
            fin = [x for x in tot if x.startswith(head + "TASK_FINISHED." + name + ".")]
            if fin:
                pairs.append((k, fin))
    if "Multiplexer.channelActive" in tot:
        pairs.append(("Multiplexer.channelActive", ["Multiplexer.channelInactive"]))
    breaks = 0
    for a, bs in pairs:
        da = [r["lg"].get(a, 0) for r in series.op]
        db = [sum(r["lg"].get(b, 0) for b in bs) for r in series.op]
        run = 0
        for i in range(10, len(da)):
            A, B = sum(da[i - 10:i]), sum(db[i - 10:i])
            run = run + 1 if (A >= 4 and B < 0.5 * A) else 0
            breaks += run == 3
    return {"pairs": len(pairs), "breaks": breaks}


def collect(out, raw_dir=None, log=print):
    doc = {}
    for env in sorted(p.parent.name for p in Path(out).glob("*/oplog.minutes.jsonl")):
        log(f"  insights {env} …")
        s = Series(out, env)
        prof = build_profile(s)
        e = {"noise": noise(s, prof)}
        if s.labeled:
            inc = find_labeled_incidents(s)
            e["precursor_lift"] = precursor_lift(s, inc)
            raw = next(Path(raw_dir).glob("*BGL*.log"), None) if raw_dir else None
            if raw:
                e["local"] = local_precursors(raw, inc)
        else:
            e["invariants"] = invariants(s)
            e["incidents"] = len(find_incidents(s, prof))
        doc[env] = e
    (Path(out) / "insights.json").write_bytes(orjson.dumps(doc, option=orjson.OPT_INDENT_2))
    return doc
