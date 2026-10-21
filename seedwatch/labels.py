"""Automatic ground truth: what counts as "SYSTEM DOWN" in these logs.

There is no CRITICAL level in the data, so incidents are derived from observable
damage, never from the predictor's own features:
  silence    – log volume collapses (< 20 % of the last hour's median)      sev 3
  failure    – a component is declared FAILED / offline                      sev 3
  link_down  – echo (heartbeat) errors to host links >= 10/min for 3+ min,
               or > 30 % of outgoing host messages unanswered for 3+ min      sev 2
  avalanche  – one seed template explodes (>= 100/min and >= 20x its usual)  sev 2
  hb_lost    – a steady INFO stream (>= 5/min for 2 h) stops for 15+ min while
               the rest of the system keeps logging: silent component death  sev 2
  restart    – a new pod appears (crash or deploy)                           sev 1
Triggers closer than 20 min are merged into one incident.
"""
import re
from statistics import median

FAILURE_RX = re.compile(r"to FAILED|farm offline|OutOfMemory|SYSTEM DOWN|unavailable", re.I)
SEV = {"silence": 3, "failure": 3, "link_down": 2, "avalanche": 2, "hb_lost": 2, "restart": 1}
MERGE_GAP_MIN = 20


def _runs(flags, min_len):
    """Indices that belong to runs of True of length >= min_len."""
    out, run = [], []
    for i, f in enumerate(flags + [False]):
        if f:
            run.append(i)
        else:
            if len(run) >= min_len:
                out.extend(run)
            run = []
    return out


def _rolling_median(xs, window=60):
    return [median(xs[max(0, i - window):i]) if i else xs[0] for i in range(len(xs))]


def find_triggers(series, profile):
    n = len(series)
    trig = []
    tp = profile["tpl"]

    vol = [r["n"] for r in series.op]
    base = _rolling_median(vol)
    silent = [i >= 30 and base[i] >= 50 and vol[i] < 0.2 * base[i] for i in range(n)]
    for i in _runs(silent, 2):
        trig.append((i, "silence", f"log volume {vol[i]}/min vs usual {base[i]:.0f}/min"))

    echo = [0] * n
    for i, row in enumerate(series.op):
        for tid, c in row["tpl"].items():
            p = tp.get(tid)
            if not p or p["class"] != "seed":
                continue
            if p["logger"].endswith("errorWhileEcho"):
                echo[i] += c
            if FAILURE_RX.search(p["template"]):
                trig.append((i, "failure", f"{p['logger']}: {p['template'][:80]}"))
    for i in _runs([c >= 10 for c in echo], 3):
        trig.append((i, "link_down", f"{echo[i]} echo errors/min"))

    unanswered = [False] * n
    ratio = [0.0] * n
    for i, row in enumerate(series.ct):
        if not row:
            continue
        out = sum(v[0] for k, v in row["ct"].items() if k.startswith("EIS_"))
        inn = sum(v[1] for k, v in row["ct"].items() if k.startswith("EIS_"))
        if out >= 50:
            ratio[i] = (out - inn) / out
            unanswered[i] = ratio[i] > 0.3
    for i in _runs(unanswered, 3):
        trig.append((i, "link_down", f"{ratio[i]:.0%} of host messages unanswered"))

    per_tpl = {}
    for i, row in enumerate(series.op):
        for tid, c in row["tpl"].items():
            per_tpl.setdefault(tid, []).append((i, c))
    for tid, occ in per_tpl.items():
        p = tp.get(tid)
        if not p or p["class"] != "seed":
            continue
        usual = median(c for _, c in occ)
        for i, c in occ:
            if c >= 100 and c >= 20 * usual:
                trig.append((i, "avalanche", f"{c}x {p['logger']} in one minute (usual {usual:.0f})"))

    loggers = {}
    for i, row in enumerate(series.op):
        for lg, c in row["lg"].items():
            loggers.setdefault(lg, [0] * n)[i] = c
    for lg, xs in loggers.items():
        i = 120
        while i < n:
            if xs[i] == 0 and not silent[i]:
                prev = xs[i - 120:i]
                steady = sum(prev) / 120 >= 5 and sum(1 for k in range(0, 120, 3) if sum(prev[k:k + 3])) >= 38
                gap = 0
                while i + gap < n and xs[i + gap] == 0:
                    gap += 1
                if steady and gap >= 15:
                    trig.append((i, "hb_lost", f"{lg} stopped (was {sum(prev) / 120:.0f}/min)"))
                i += max(gap, 1)
            else:
                i += 1

    for pod, t in profile["pod_births"].items():
        trig.append((series.index[t], "restart", f"new pod {pod}"))
    return sorted(trig)


def find_incidents(series, profile):
    incidents = []
    for i, kind, why in find_triggers(series, profile):
        cur = incidents[-1] if incidents else None
        if cur is None or i - cur["_last"] > MERGE_GAP_MIN:
            cur = {"_first": i, "_last": i, "kinds": {}, "evidence": []}
            incidents.append(cur)
        cur["_last"] = max(cur["_last"], i)
        cur["kinds"][kind] = cur["kinds"].get(kind, 0) + 1
        if len(cur["evidence"]) < 8 and why not in cur["evidence"]:
            cur["evidence"].append(why)
    out = []
    for k, inc in enumerate(incidents):
        sev = max(SEV[x] for x in inc["kinds"])
        out.append({"id": f"{series.env}-{k + 1}", "env": series.env,
                    "start": series.t[inc["_first"]], "end": series.t[inc["_last"]] + 60,
                    "severity": sev, "kinds": inc["kinds"], "evidence": inc["evidence"]})
    return out


def find_labeled_incidents(series, gap_min=30):
    """Incidents from a dataset's own alert tags (e.g. BGL): alert lines closer than
    ``gap_min`` form one incident.  ``quiet_before_min`` lets the evaluation separate
    fresh incidents from aftershocks of the previous one."""
    incidents, cur, prev_end = [], None, None
    for i, row in enumerate(series.op):
        al = row.get("al")
        if not al:
            continue
        if cur is None or i - cur["_last"] > gap_min:
            if cur is not None:
                prev_end = cur["_last"]
            cur = {"_first": i, "_last": i, "tags": {}, "quiet": None if prev_end is None else i - prev_end}
            incidents.append(cur)
        cur["_last"] = i
        for tag, c in al.items():
            cur["tags"][tag] = cur["tags"].get(tag, 0) + c
    out = []
    for k, inc in enumerate(incidents):
        tags = sorted(inc["tags"].items(), key=lambda x: -x[1])
        out.append({"id": f"{series.env}-{k + 1}", "env": series.env,
                    "start": series.t[inc["_first"]], "end": series.t[inc["_last"]] + 60,
                    "severity": 3, "kinds": {"alert": sum(inc["tags"].values())},
                    "evidence": [f"{tag}: {c} alert lines" for tag, c in tags[:5]],
                    "quiet_before_min": inc["quiet"]})
    return out
