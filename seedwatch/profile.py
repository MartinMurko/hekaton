"""Learn what is noise from *behaviour*, not from the level or the word "error".

Every template gets a rhythm profile over the whole history:
  hours_present  – share of hours in which it occurs at all
  cv             – coefficient of variation of hourly counts (metronome ~ 0)
  startup_share  – share of occurrences within minutes after a new pod appeared
and is classified as
  weed   – steady background (fires like a clock, or a known-benign notice)
  marker – start-up / deploy fingerprint (not a problem itself, tells us a pod restarted)
  seed   – everything else: rare, bursty, or new  -> candidate signal
INFO loggers with a steady rhythm become *heartbeats*: we keep their expected rate so a
missing beat (silence) is itself a signal.
"""
import math
import re
from statistics import mean, pstdev

TX_REASON = "težava ene transakcije (eno sporočilo / ena kartica), ne zdravje sistema"
MAINTENANCE_REASON = "servisni poseg (načrtovano vzdrževanje): sam ni težava, napake med njim so pričakovane"
KNOWN = [
    (re.compile(r"is not needed|only considered for container|only used for text types|"
                r"only evaluated for container", re.I),
     "marker", "opozorila formata sporočil, ki jih pod izpiše ob nalaganju ISO formatov"),
    (re.compile(r"without menu configuration", re.I), "weed", "obvestilo o konfiguraciji menija v admin UI"),
    (re.compile(r"encryption.*disabled|disabled.*will not be used|producer on connector .* is disabled", re.I),
     "weed", "statično obvestilo o konfiguraciji (za varnostni pregled, ne za razpoložljivost)"),
    (re.compile(r"PIN block does not contain valid|No format found for subfield|"
                r"Content conversion warning|Inbound request .* not allowed for not signed-on", re.I),
     "weed", TX_REASON),
    (re.compile(r"PrepareForService|EndServiceAction|as part of Service Action"),
     "marker", MAINTENANCE_REASON),
    (re.compile(r"\bExpected error\b|\bsuccessful(ly)?\b|\bcompleted\b|\bOKAY\b|no ALERTs are active", re.I),
     "weed", "pozitiven ali pričakovan izid"),
]
NEGATIVE = re.compile(r"fail|exception|timeout|refused|unable|lost|down|offline|wrong", re.I)


def classify_text(logger, tpl):
    text = f"{logger} {tpl}"
    for rx, cls, reason in KNOWN:
        if rx.search(text) and not (cls == "weed" and NEGATIVE.search(tpl) and "Expected" not in tpl):
            return cls, reason
    return None


def _rhythm(counts, hours):
    hourly = [0] * hours
    for i, c in counts.items():
        h = i // 60
        if h < hours:
            hourly[h] += c
    present = sum(1 for x in hourly if x) / max(hours, 1)
    m = mean(hourly) if hourly else 0
    cv = pstdev(hourly) / m if m else math.inf
    return present, cv


CRON_PERIODS = (1440, 720, 360, 240, 180, 120, 60)  # minutes
CRON_TOL = 3


def _cron(counts, startup):
    """Detect a job that fires at a fixed phase of some period (cron-like).

    Occurrences right after pod starts are ignored (start-up runs the job once too).
    Returns (period_min, phase_index, share) or None.
    """
    occ = [(i, c) for i, c in counts.items() if i not in startup]
    total = sum(c for _, c in occ)
    if total < 3:
        return None
    for period in CRON_PERIODS:
        phase = {}
        for i, c in occ:
            phase[i % period] = phase.get(i % period, 0) + c
        best, best_n = 0, 0
        for p in phase:
            n = sum(phase.get((p + d) % period, 0) for d in range(-CRON_TOL, CRON_TOL + 1))
            if n > best_n:
                best, best_n = p, n
        cycles = {i // period for i, _ in occ
                  if abs((i - best + period // 2) % period - period // 2) <= CRON_TOL}
        if best_n / total >= 0.8 and len(cycles) >= 3:
            return period, best, best_n / total
    return None


def _fmt_period(period, phase_idx, t0):
    minute = (t0 // 60 + phase_idx) % 1440
    clock = f"{minute // 60:02d}:{minute % 60:02d} UTC"
    if period == 1440:
        return f"dnevno opravilo ob {clock}"
    return f"opravilo vsakih {period // 60} h (npr. {clock})"


def cascade_groups(catalog, min_share=0.8):
    """Union templates that (almost) always share a correlationId -> one root cause."""
    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x
    tpl = catalog["tpl"]
    for key, n in catalog["pairs"].items():
        a, b = key.split("|")
        ca, cb = tpl[a].get("corr_n") or tpl[a]["count"], tpl[b].get("corr_n") or tpl[b]["count"]
        if n >= min_share * min(ca, cb):
            parent[find(a)] = find(b)
    return {t: find(t) for t in tpl}


def build_profile(series):
    n = len(series)
    hours = n // 60
    births = {} if series.labeled else series.pod_births()
    startup = set()
    for b in births.values():
        startup.update(range(b - 2, b + 11))

    per_tpl = {}
    per_lg = {}
    for i, row in enumerate(series.op):
        for tid, c in row["tpl"].items():
            per_tpl.setdefault(tid, {})[i] = c
        for lg, c in row["lg"].items():
            per_lg.setdefault(lg, {})[i] = c

    tpl_prof = {}
    for tid, meta in series.catalog["tpl"].items():
        counts = per_tpl.get(tid, {})
        total = sum(counts.values())
        if not total:
            continue
        present, cv = _rhythm(counts, hours)
        st_share = sum(c for i, c in counts.items() if i in startup) / total
        active = len(counts)
        burst = max(counts.values()) / (total / active)
        p = {"logger": meta["logger"], "level": meta["level"], "template": meta["template"],
             "n": total, "hours_present": round(present, 3), "cv": round(cv, 2),
             "startup_share": round(st_share, 2), "burst": round(burst, 1),
             "rate_per_min": total / n}
        known = classify_text(meta["logger"], meta["template"])
        cron = None if known else _cron(counts, startup)
        if known:
            p["class"], p["reason"] = known
        elif cron and present < 0.8:
            extra = f", tudi ob zagonu poda ({st_share:.0%})" if st_share >= 0.1 else ""
            p["class"] = "weed"
            p["reason"] = f"cron: {_fmt_period(cron[0], cron[1], series.t[0])}{extra}"
            p["cron"] = {"period": cron[0], "phase": cron[1], "share": round(cron[2], 2)}
        elif births and st_share >= 0.7 and total >= 3:
            p["class"], p["reason"] = "marker", f"{st_share:.0%} pojavitev takoj po zagonu poda"
        elif present >= 0.8 and cv <= 0.75:
            p["class"], p["reason"] = "weed", f"metronom: prisoten v {present:.0%} ur, CV {cv:.2f}"
        else:
            p["class"], p["reason"] = "seed", f"nereden: prisoten v {present:.0%} ur, izbruh ×{burst:.0f}"
        tpl_prof[tid] = p

    # a seed that is only ever the echo of a known-benign template inherits its class
    groups = cascade_groups(series.catalog)
    members = {}
    for tid, g in groups.items():
        members.setdefault(g, []).append(tid)
    for g, tids in members.items():
        benign = [t for t in tids if t in tpl_prof and tpl_prof[t]["class"] == "weed"
                  and tpl_prof[t]["reason"] == TX_REASON]
        for t in tids:
            if benign and t in tpl_prof and tpl_prof[t]["class"] == "seed":
                tpl_prof[t]["class"] = "weed"
                tpl_prof[t]["reason"] = f"vedno del kaskade {tpl_prof[benign[0]]['logger']}"

    heartbeats = {}
    windows = max(n // 10, 1)
    for lg, counts in per_lg.items():
        total = sum(counts.values())
        rate = total / n
        if rate < 0.5:
            continue
        win = {}
        for i, c in counts.items():
            win[i // 10] = win.get(i // 10, 0) + c
        presence = len(win) / windows
        if presence >= 0.95:
            heartbeats[lg] = {"rate_per_min": round(rate, 3), "presence": round(presence, 3)}

    return {"env": series.env, "minutes": n, "restarts": not series.labeled,
            "pod_births": {p: series.t[i] for p, i in births.items()},
            "tpl": tpl_prof, "groups": groups, "heartbeats": heartbeats}
