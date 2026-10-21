"""Live replay: raw log files -> aggregator -> predictor -> alert cards, paced in time.

The same MinuteAggregator and Predictor as the batch path, fed line by line from the
raw files at ``speed`` x real time.  Minutes are closed by a *clock* watermark, not by
the arrival of newer data – otherwise a silent system would never close a minute and
silence (the strongest outage signal) would only be noticed after logs resume.
"""
import heapq
import os
import time
from pathlib import Path

import orjson

from . import predict
from .aggregate import LAG_MIN, MinuteAggregator
from .explain import Explainer
from .fields import classify_file, create_time
from .series import EMPTY_CT, EMPTY_OP, Series


def seek_create_time(path, ts_ms):
    """Byte offset of the first line whose Kafka CreateTime is >= ts_ms (approximately)."""
    size = os.path.getsize(path)
    lo, hi = 0, size
    with open(path, "rb") as f:
        while hi - lo > 1 << 16:
            mid = (lo + hi) // 2
            f.seek(mid)
            f.readline()
            ct = create_time(f.readline())
            if ct is None or ct < ts_ms:
                lo = mid
            else:
                hi = mid
        f.seek(lo)
        if lo:
            f.readline()
        return f.tell()


def _lines(path, kind, offset):
    with open(path, "rb", buffering=1 << 20) as f:
        f.seek(offset)
        for line in f:
            ct = create_time(line)
            if ct is not None:
                yield ct, kind, line.rstrip(b"\n")


def rss_mb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    return None


class Replay:
    def __init__(self, out, env, raw_files, start_ts, speed=60.0, threshold=predict.THRESHOLD):
        self.out = Path(out)
        self.env = env
        self.start = start_ts - start_ts % 60
        self.speed = speed
        self.files = {}
        for p in raw_files:
            e, kind = classify_file(p)
            if e == env:
                self.files[kind] = p
        d = self.out / env
        self.profile = orjson.loads((d / "profile.json").read_bytes())
        self.catalog = orjson.loads((d / "catalog.json").read_bytes())
        self.incidents = orjson.loads((d / "incidents.json").read_bytes())
        self.cls = {tid: p["class"] for tid, p in self.profile["tpl"].items()}
        self.pred = predict.Predictor(self.profile, threshold=threshold)
        self.explainer = Explainer(env, self.profile, self.catalog, [])
        self.alert_history = []
        self.funnel = {"lines": 0, "warn_err": 0, "seed": 0, "events": 0, "alerts": 0}
        self.op_rows, self.ct_rows, self.events = {}, {}, {}

    def warmup(self):
        """Learn baselines from the minutes before the replay window (fast, from batch rows)."""
        s = Series(self.out, self.env)
        n = 0
        for i, t in enumerate(s.t):
            if t >= self.start:
                break
            st = self.pred.step(t, s.op[i], s.ct[i])
            if st["alert"]:
                self.alert_history.append(st["alert"])
            n += 1
        return n

    def _emit(self, kind):
        def emit(row, events):
            if row["t"] < self.start:   # pre-roll minutes were already learned in warmup
                return
            if kind == "oplog":
                self.op_rows[row["t"]] = row
                self.events[row["t"]] = events
            else:
                self.ct_rows[row["t"]] = row
        return emit

    def _minute_payload(self, t):
        op = self.op_rows.pop(t, None) or dict(EMPTY_OP, t=t)
        ct = self.ct_rows.pop(t, None)
        if ct is None and "commtrace" in self.files:
            ct = dict(EMPTY_CT, t=t)
        events = self.events.pop(t, [])
        self.explainer.by_minute[t] = events
        self.explainer.by_minute.pop(t - 30 * 60, None)
        st = self.pred.step(t, op, ct)

        cls = {"weed": 0, "marker": 0, "seed": 0}
        for tid, c in op["tpl"].items():
            cls[self.cls.get(tid, "seed")] += c
        self.funnel["warn_err"] += sum(cls.values())
        self.funnel["seed"] += cls["seed"]
        seed_events = [e for e in events if self.cls.get(e["tid"], "seed") == "seed"]
        self.funnel["events"] += len(seed_events)
        card = None
        if st["alert"]:
            card = self.explainer.card(st["alert"], history=self.alert_history)
            card["level"] = "alarm"
            self.alert_history.append(st["alert"])
            self.funnel["alerts"] += 1
        elif st.get("watch"):
            card = self.explainer.card(st["watch"], history=self.alert_history)
            card["level"] = "watch"
        lv = op.get("lv", {})
        return {
            "type": "minute", "t": t, "n": op["n"], "ct_n": ct["n"] if ct else None,
            "warn": lv.get("WARN", 0), "error": lv.get("ERROR", 0) + lv.get("CRITICAL", 0),
            "cls": cls, "risk": st["risk"], "comps": st["comps"],
            "threshold": self.pred.threshold, "level": st["level"],
            "funnel": dict(self.funnel), "rss_mb": rss_mb(),
            "events": [{"count": e["count"], "level": e["level"], "logger": e["logger"],
                        "pod": e["pod"], "msg": e["msg"][:160], "ents": e["ents"][:2]}
                       for e in sorted(seed_events, key=lambda e: -e["count"])[:6]],
            "card": card,
        }

    def run(self, send, minutes=90, stop=lambda: False):
        warm = self.warmup()
        end = self.start + minutes * 60
        send({"type": "hello", "env": self.env, "start": self.start, "end": end, "speed": self.speed,
              "warmup_minutes": warm, "threshold": self.pred.threshold,
              "watch_threshold": predict.WATCH_RATIO * self.pred.threshold,
              "incidents": [i for i in self.incidents if self.start - 3600 <= i["start"] <= end]})
        aggs = {k: MinuteAggregator(self.env, k, self._emit(k)) for k in self.files}
        pre_ms = (self.start - LAG_MIN * 60 - 120) * 1000
        streams = [_lines(p, k, seek_create_time(p, pre_ms)) for k, p in self.files.items()]
        merged = heapq.merge(*streams, key=lambda x: x[0])
        wall0 = time.monotonic()
        next_t = self.start
        start_ms = self.start * 1000

        def clock_ms():
            return start_ms + (time.monotonic() - wall0) * self.speed * 1000

        def close_minutes(now_ms):
            nonlocal next_t
            upto = int(now_ms // 60000) - LAG_MIN
            for a in aggs.values():
                a.flush(upto)
            while next_t < upto * 60 and next_t < end:
                send(self._minute_payload(next_t))
                next_t += 60

        for ct, kind, line in merged:
            if stop() or next_t >= end:
                break
            while ct > clock_ms():
                close_minutes(clock_ms())
                if stop() or next_t >= end:
                    break
                time.sleep(max(0.0, min(0.05, (ct - clock_ms()) / self.speed / 1000 + 0.001)))
            aggs[kind].add(line)
            self.funnel["lines"] += 1
        while next_t < end and not stop():
            close_minutes(clock_ms())
            time.sleep(0.05)
        send({"type": "done", "funnel": self.funnel})
