"""Ablation: how much does each idea add?  Same incidents, same scoring, one pillar at a time.

A  naive static threshold on WARN+ERROR lines per minute
B  model, but every template is treated as a signal (no noise learning), no silence, no rarity
C  + noise learned from behaviour (weed / marker / maintenance)
D  + noise as a sensor: missing heartbeats and log volume (silence)
E  + level x rarity: a rare severe template alerts on its first line  (= full model)
"""
import copy
from pathlib import Path

import orjson

from . import evaluate, predict
from .labels import find_incidents, find_labeled_incidents
from .profile import build_profile
from .series import Series

CONFIGS = [
    ("A", "naiven prag WARN+ERROR na minuto"),
    ("B", "model brez učenja šuma (vse napake = signal)"),
    ("C", "+ šum iz ritma (plevel, marker, servisni poseg)"),
    ("D", "+ šum kot senzor (tišina, manjkajoči utripi)"),
    ("E", "+ nivo × redkost (redek resen dogodek) = celoten model"),
]
NO_SILENCE = dict(predict.WEIGHTS, volume=0.0, heartbeat=0.0)


def _no_noise_learning(profile):
    p = copy.deepcopy(profile)
    for t in p["tpl"].values():
        t["class"], t["reason"] = "seed", ""
    p["heartbeats"] = {}
    return p


def _run(series, profile, incidents, **kw):
    states, alerts = predict.run(series, profile, **kw)
    sm = evaluate.score(incidents, alerts, len(series), profile)["summary"]
    return {"caught": sm["caught"], "outages": sm["outages"], "predicted": sm["predicted"],
            "fa_per_day": sm["fa_per_day"]}


def ablate_env(out, env):
    s = Series(out, env)
    prof = build_profile(s)
    incidents = find_labeled_incidents(s) if s.labeled else find_incidents(s, prof)
    naive = evaluate.naive_baseline(s, incidents)
    return {
        "A": {k: naive[k] for k in ("caught", "outages", "fa_per_day")} | {"predicted": None},
        "B": _run(s, _no_noise_learning(prof), incidents, weights=NO_SILENCE, rare=False),
        "C": _run(s, prof, incidents, weights=NO_SILENCE, rare=False),
        "D": _run(s, prof, incidents, rare=False),
        "E": _run(s, prof, incidents),
    }


def ablate_all(out, log=print):
    envs = sorted(p.parent.name for p in Path(out).glob("*/oplog.minutes.jsonl"))
    results = {}
    for env in envs:
        log(f"  ablation {env} …")
        results[env] = ablate_env(out, env)
    doc = {"configs": [{"id": c, "label": label} for c, label in CONFIGS], "results": results}
    (Path(out) / "ablation.json").write_bytes(orjson.dumps(doc, option=orjson.OPT_INDENT_2))
    return doc
