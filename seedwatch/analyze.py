"""Second stage: minute rows -> noise profile, incidents, risk curve, alert cards, evaluation."""
from pathlib import Path

import orjson

from . import chaos, evaluate, predict
from .explain import Explainer
from .labels import find_incidents, find_labeled_incidents
from .profile import build_profile
from .series import Series


def _dump(path, obj):
    Path(path).write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS))


def load_events(out, env):
    p = Path(out) / env / "events.jsonl"
    with open(p, "rb") as f:
        return [orjson.loads(line) for line in f]


def analyze_env(out, env, threshold=predict.THRESHOLD):
    d = Path(out) / env
    s = Series(out, env)
    prof = build_profile(s)
    incidents = find_labeled_incidents(s) if s.labeled else find_incidents(s, prof)
    states, alerts = predict.run(s, prof, threshold=threshold)
    ex = Explainer(env, prof, s.catalog, load_events(out, env))
    cards = [ex.card(a, history=alerts) for a in alerts]
    result = evaluate.score(incidents, alerts, len(s), prof, watches=predict.watches(states))
    by_t = {c["t"]: c for c in cards}
    for r in result["incidents"]:
        r["alert_title"] = by_t[r["alert_t"]]["title"] if r["alert_t"] in by_t else None
    fa_t = {a["t"] for a in result["false_alarms"]}
    for c in cards:
        c["false_alarm"] = c["t"] in fa_t

    _dump(d / "profile.json", prof)
    _dump(d / "incidents.json", incidents)
    _dump(d / "alerts.json", cards)
    _dump(d / "eval.json", {"summary": result["summary"], "incidents": result["incidents"]})
    with open(d / "risk.jsonl", "wb") as f:
        for st in states:
            f.write(orjson.dumps({"t": st["t"], "risk": st["risk"], "comps": st["comps"],
                                  "n": s.op[s.index[st["t"]]]["n"]}))
            f.write(b"\n")
    result["summary"]["naive"] = evaluate.naive_baseline(s, incidents)
    # synthetic slow burns need real heartbeats / host traffic; labelled data is measured directly
    synthetic = [] if s.labeled else chaos.run_all(s, prof, incidents, alerts, per_kind=12)
    for r in synthetic:
        r["env"] = env
    result["summary"]["labeled"] = s.labeled
    return {"env": env, "summary": result["summary"], "incidents": result["incidents"],
            "cards": cards, "profile": prof, "chaos": synthetic}


def analyze_all(out, threshold=predict.THRESHOLD):
    envs = sorted(p.name for p in Path(out).iterdir() if (p / "oplog.minutes.jsonl").exists())
    all_results = [analyze_env(out, env, threshold) for env in envs]
    # headline numbers: own environments; public labelled datasets are reported per env
    results = [r for r in all_results if not r["summary"]["labeled"]]
    total = {"outages": 0, "caught": 0, "predicted": 0, "false_alarms": 0, "days": 0.0, "watch_early": 0}
    for r in results:
        for k in total:
            total[k] += r["summary"][k]
    total["fa_per_day"] = round(total["false_alarms"] / total["days"], 2)
    naive = [r["summary"]["naive"] for r in results]
    total["naive"] = {"caught": sum(n["caught"] for n in naive), "outages": sum(n["outages"] for n in naive),
                      "fa_per_day": round(sum(n["false_alarms"] for n in naive) / total["days"], 2),
                      "warn_error_lines": sum(n["warn_error_lines"] for n in naive)}
    total["watch_per_day"] = round(sum(r["summary"]["watch_per_day"] * r["summary"]["days"]
                                       for r in results) / total["days"], 2)
    total["recall"] = round(total["caught"] / total["outages"], 2) if total["outages"] else None
    total["threshold"] = threshold
    runs = [x for r in results for x in r["chaos"]]
    total["chaos"] = chaos.summarize(runs)
    _dump(Path(out) / "chaos.json", runs)
    _dump(Path(out) / "summary.json", {"total": total, "per_env": {r["env"]: r["summary"] for r in all_results}})
    return total, all_results
