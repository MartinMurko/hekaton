"""Chaos injection: slow-burn failures replayed on top of real background traffic.

The real logs contain only a handful of outages, too few to measure lead time
honestly.  Here we take quiet stretches of the real minute series, inject a gradual
degradation over RAMP minutes (the "subtle patterns" from the brief) and then a hard
outage (total silence) at T0, and measure when the unchanged predictor first alerts.
"""
import math
import random
from statistics import median

from . import predict

RAMP = 30
WARM = 360
SCENARIOS = {
    "warn_ramp": "nov WARN vzorec narašča z 0,1/min na 8/min",
    "trickle": "redka obstoječa napaka postaja vse pogostejša",
    "latency_ramp": "odziv 4 bančnih hostov se postopno upočasni do 6×",
    "unanswered_ramp": "vse več sporočil hostom ostane brez odgovora (do 60 %)",
    "heartbeat_thinning": "najmočnejši utrip (npr. echo) postopno upade na 20 %",
}


def _poisson(rng, lam):
    if lam <= 0:
        return 0
    k, p, l = 0, 1.0, math.exp(-lam)
    while True:
        p *= rng.random()
        if p <= l:
            return k
        k += 1


def _copy_op(row):
    return dict(row, lg=dict(row["lg"]), tpl=dict(row["tpl"]), lv=dict(row.get("lv", {})))


def _copy_ct(row):
    return dict(row, ct={k: list(v) for k, v in row["ct"].items()})


def inject(series, profile, i0, kind, rng):
    """Return (op, ct) lists for indices [i0-WARM, i0+15) with the scenario applied."""
    lo, hi = i0 - WARM, i0 + 15
    op = list(series.op[lo:hi])
    ct = list(series.ct[lo:hi])
    r0 = WARM - RAMP
    seeds = [tid for tid, p in profile["tpl"].items() if p["class"] == "seed" and p["n"] >= 5]
    trickle_tid = rng.choice(seeds) if seeds else "chaos-trickle"
    hb = max(profile["heartbeats"].items(), key=lambda x: x[1]["rate_per_min"])[0]
    links = None
    for k in range(RAMP):
        j = r0 + k
        x = k / RAMP
        if kind in ("warn_ramp", "trickle", "heartbeat_thinning"):
            row = op[j] = _copy_op(op[j])
            if kind == "warn_ramp":
                c = _poisson(rng, 0.1 * 80 ** x)
                tid = "chaos-warn"
            elif kind == "trickle":
                c = _poisson(rng, 0.05 * 60 ** x)
                tid = trickle_tid
            else:
                before = row["lg"].get(hb, 0)
                after = round(before * (1 - 0.8 * x))
                row["lg"][hb] = after
                row["n"] -= before - after
                continue
            if c:
                row["tpl"][tid] = row["tpl"].get(tid, 0) + c
                row["lv"]["WARN"] = row["lv"].get("WARN", 0) + c
                row["n"] += c
        else:
            if ct[j] is None:
                return None
            row = ct[j] = _copy_ct(ct[j])
            if links is None:
                links = sorted((k for k in row["ct"] if k.startswith("EIS_")),
                               key=lambda k: -row["ct"][k][2])[:4]
            for name, v in row["ct"].items():
                if not name.startswith("EIS_"):
                    continue
                if kind == "latency_ramp" and name in links:
                    f = 6 ** x
                    v[3] = round(v[3] * f)
                    v[4] = round(v[4] * f)
                elif kind == "unanswered_ramp":
                    keep = 1 - 0.6 * x
                    v[1] = round(v[1] * keep)
                    v[2] = round(v[2] * keep)
    for j in range(WARM, WARM + 10):   # the outage itself: everything goes dark
        op[j] = {"n": 0, "lv": {}, "lg": {}, "pod": {}, "tpl": {}}
        if ct[j] is not None:
            ct[j] = {"n": 0, "pod": {}, "ct": {}}
    return op, ct


def run_one(series, profile, i0, kind, rng):
    inj = inject(series, profile, i0, kind, rng)
    if inj is None:
        return None
    op, ct = inj
    pred = predict.Predictor(profile)
    t0 = series.t[i0]
    first = first_watch = None
    for j, (o, c) in enumerate(zip(op, ct)):
        t = series.t[i0 - WARM + j]
        st = pred.step(t, o, c)
        if t >= t0 - (RAMP + 5) * 60:
            if st["alert"] and first is None:
                first = st["alert"]
            if st["watch"] and first_watch is None:
                first_watch = st["watch"]
    watch_lead = (t0 - first_watch["t"]) / 60 if first_watch else None
    if first is None:
        return {"kind": kind, "t0": t0, "outcome": "missed", "lead_min": None, "watch_lead_min": watch_lead}
    lead = (t0 - first["t"]) / 60
    return {"kind": kind, "t0": t0, "outcome": "predicted" if lead > 0 else "detected", "lead_min": lead,
            "watch_lead_min": watch_lead, "cause": first.get("cause")}


def quiet_minutes(series, incidents, alerts):
    """Candidate T0 indices: >= 4 h from incidents, no natural alert near the ramp."""
    bad = set()
    for inc in incidents:
        for t in range(inc["start"] - 4 * 3600, inc["end"] + 4 * 3600, 60):
            bad.add(t)
    for a in alerts:
        for t in range(a["t"] - 20 * 60, a["t"] + (RAMP + 30) * 60, 60):
            bad.add(t)
    return [i for i in range(WARM + 1, len(series) - 20) if series.t[i] not in bad]


def run_all(series, profile, incidents, alerts, per_kind=12, seed=7):
    rng = random.Random(seed)
    cands = quiet_minutes(series, incidents, alerts)
    results = []
    for kind in SCENARIOS:
        picks = rng.sample(cands, min(len(cands), per_kind * 2))
        n = 0
        for i0 in picks:
            r = run_one(series, profile, i0, kind, rng)
            if r is None:
                continue
            results.append(r)
            n += 1
            if n >= per_kind:
                break
    return results


def summarize(results):
    out = {}
    for kind, desc in SCENARIOS.items():
        rs = [r for r in results if r["kind"] == kind]
        if not rs:
            continue
        leads = [r["lead_min"] for r in rs if r["lead_min"] is not None]
        out[kind] = {
            "desc": desc, "runs": len(rs),
            "caught": sum(1 for r in rs if r["outcome"] != "missed"),
            "predicted": sum(1 for r in rs if r["outcome"] == "predicted"),
            "lead_ge_10": sum(1 for x in leads if x >= 10),
            "median_lead_min": median(leads) if leads else None,
            "watch_lead_ge_10": sum(1 for r in rs if (r["watch_lead_min"] or 0) >= 10),
            "median_watch_lead_min": median(wl) if (wl := [r["watch_lead_min"] for r in rs
                                                         if r["watch_lead_min"] is not None]) else None,
        }
    return out
