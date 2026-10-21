"""Score alerts against the automatically labelled incidents.

Only alerts whose evidence is *about* the incident count (same component as the
incident evidence, or the matching signal family) – an unrelated alert that happens to
fire just before does not earn a prediction.

predicted  – first related alert in [start - 60 min, start)   -> lead time = start - alert
detected   – first related alert in [start, start + 10 min]
missed     – otherwise
false alarm – an alert outside every incident's span that is not a related early warning
"""
FAMILY = {"silence": {"volume", "heartbeat"}, "hb_lost": {"volume", "heartbeat"},
          "link_down": {"unanswered", "latency"}}


def related(alert, inc, profile):
    if "alert" in inc["kinds"]:
        return True   # ground-truth tags name a failure class, not a logger: any alert counts
    if "restart" in inc["kinds"] and len(inc["kinds"]) == 1:
        return abs(alert["t"] - inc["start"]) <= 10 * 60   # a restart has no long precursor
    dom = max(alert["comps"], key=alert["comps"].get)
    if any(dom in FAMILY.get(k, ()) for k in inc["kinds"]):
        return True
    text = " ".join(inc["evidence"])
    for _, tid in alert["top_tpl"][:3]:
        logger = profile["tpl"].get(tid, {}).get("logger", "")
        if logger and (logger in text or logger.split(".")[0] + "." in text):
            return True
        if "link_down" in inc["kinds"] and logger.endswith("errorWhileEcho"):
            return True
    return False

EARLY_S = 60 * 60
LATE_S = 10 * 60
AFTER_S = 30 * 60


def score(incidents, alerts, minutes, profile, min_sev=2, watches=()):
    rows = []
    used = set()
    used_w = set()
    for inc in incidents:
        early = [w for w in watches if inc["start"] - EARLY_S <= w["t"] <= inc["start"] + LATE_S
                 and related(w, inc, profile)]
        watch_lead = (inc["start"] - early[0]["t"]) / 60 if early else None
        for w in watches:
            if inc["start"] - EARLY_S <= w["t"] <= inc["end"] + AFTER_S and (
                    related(w, inc, profile) or w["t"] >= inc["start"]):
                used_w.add(w["t"])
        hits = [a for a in alerts if inc["start"] - EARLY_S <= a["t"] <= inc["start"] + LATE_S
                and related(a, inc, profile)]
        first = hits[0] if hits else None
        for a in alerts:
            during = inc["start"] <= a["t"] <= inc["end"] + AFTER_S
            if during or (inc["start"] - EARLY_S <= a["t"] < inc["start"] and related(a, inc, profile)):
                used.add(a["t"])
        if first is None:
            outcome, lead = "missed", None
        elif first["t"] < inc["start"]:
            outcome, lead = "predicted", (inc["start"] - first["t"]) / 60
        else:
            outcome, lead = "detected", -(first["t"] - inc["start"]) / 60
        rows.append({"id": inc["id"], "start": inc["start"], "severity": inc["severity"],
                     "kinds": list(inc["kinds"]), "outcome": outcome, "lead_min": lead,
                     "alert_t": first["t"] if first else None, "watch_lead_min": watch_lead,
                     "quiet_before_min": inc.get("quiet_before_min")})
    false_alarms = [a for a in alerts if a["t"] not in used]
    days = minutes / 1440
    graded = [r for r in rows if r["severity"] >= min_sev]
    caught = [r for r in graded if r["outcome"] != "missed"]
    predicted = [r for r in graded if r["outcome"] == "predicted"]
    return {
        "incidents": rows,
        "false_alarms": false_alarms,
        "summary": {
            "outages": len(graded),
            "caught": len(caught),
            "predicted": len(predicted),
            "recall": round(len(caught) / len(graded), 2) if graded else None,
            "median_lead_min": (sorted(r["lead_min"] for r in caught)[len(caught) // 2] if caught else None),
            "alerts": len(alerts),
            "false_alarms": len(false_alarms),
            "fa_per_day": round(len(false_alarms) / days, 2),
            "watch_early": sum(1 for r in graded if (r["watch_lead_min"] or 0) > 0),
            "watch_per_day": round(sum(1 for w in watches if w["t"] not in used_w) / days, 2),
            "days": round(days, 2),
            "isolated": _isolated(rows),
        },
    }


def _isolated(rows, quiet_min=360):
    """Incidents preceded by >= 6 h without alerts: the clean test of *prediction*
    (no aftershock of the previous incident can count as an early warning)."""
    iso = [r for r in rows if (r.get("quiet_before_min") or 0) >= quiet_min]
    if not iso:
        return None
    leads = sorted(r["lead_min"] for r in iso if r["lead_min"] is not None)
    wleads = sorted(r["watch_lead_min"] for r in iso if r["watch_lead_min"] is not None)
    return {"n": len(iso),
            "caught": len(leads),
            "predicted": sum(1 for x in leads if x > 0),
            "lead_ge_10": sum(1 for x in leads if x >= 10),
            "median_lead_min": leads[len(leads) // 2] if leads else None,
            "watch_early": sum(1 for x in wleads if x > 0),
            "watch_lead_ge_10": sum(1 for x in wleads if x >= 10),
            "median_watch_lead_min": wleads[len(wleads) // 2] if wleads else None}


def naive_baseline(series, incidents, quantile=0.995, cooldown_min=30):
    """What a classic level filter + static threshold would do: alert when WARN+ERROR
    lines per minute exceed the 99.5th percentile of the whole history."""
    counts = [r["lv"].get("WARN", 0) + r["lv"].get("ERROR", 0) + r["lv"].get("CRITICAL", 0) for r in series.op]
    th = sorted(counts)[int(len(counts) * quantile)]
    alerts, last = [], -10 ** 12
    for t, c in zip(series.t, counts):
        if c > th and t - last > cooldown_min * 60:
            alerts.append({"t": t})
        if c > th:
            last = t
    caught = fa = 0
    graded = [i for i in incidents if i["severity"] >= 2]
    for inc in graded:
        if any(inc["start"] - EARLY_S <= a["t"] <= inc["start"] + LATE_S for a in alerts):
            caught += 1
    for a in alerts:
        if not any(i["start"] - EARLY_S <= a["t"] <= i["end"] + AFTER_S for i in incidents):
            fa += 1
    return {"threshold_lines_per_min": th, "alerts": len(alerts), "caught": caught, "outages": len(graded),
            "false_alarms": fa, "fa_per_day": round(fa / (len(series) / 1440), 2),
            "warn_error_lines": sum(counts)}
