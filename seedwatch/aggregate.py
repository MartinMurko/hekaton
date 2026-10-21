"""Online per-minute aggregation with bounded memory.

Every raw line updates the bucket of its event-time minute.  Buckets older than
``max_minute - LAG_MIN`` are flushed (emitted) and forgotten, so memory depends on
the lag window, not on the input size.  INFO lines only touch a few counters; WARN+
lines are fully parsed, templated and deduplicated into one event per
(minute, template, pod) with a repeat counter.
"""
from bisect import bisect_right
from collections import Counter, OrderedDict

from . import fields as F
from .template import entities, template, template_id

LAG_MIN = 3
CORR_CAP = 20000
DUR_BOUNDS = [5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]  # ms, 11 histogram buckets
CT_LEN = 5 + len(DUR_BOUNDS) + 1  # out, in, resp, dur_sum, dur_max, hist...
QUIET_LEVELS = (b"INFO", b"DEBUG", b"TRACE")  # META = records without a level


class Catalog:
    """Template catalog plus template co-occurrence by correlationId (cascades)."""

    def __init__(self):
        self.tpl = {}
        self.pairs = Counter()
        self._corr = OrderedDict()

    def add(self, tid, logger, level, tpl, msg, ts, ents, corr):
        e = self.tpl.get(tid)
        if e is None:
            e = self.tpl[tid] = {"logger": logger, "level": level, "template": tpl,
                                 "sample": msg[:400], "count": 0, "corr_n": 0, "first": ts,
                                 "last": ts, "ents": Counter()}
        e["count"] += 1
        e["first"] = min(e["first"], ts)
        e["last"] = max(e["last"], ts)
        for x in ents[:5]:
            e["ents"][x] += 1
        if corr:
            seen = self._corr.get(corr)
            if seen is None:
                seen = self._corr[corr] = set()
                if len(self._corr) > CORR_CAP:
                    self._corr.popitem(last=False)
            if tid not in seen:
                e["corr_n"] += 1
                for other in seen:
                    self.pairs["|".join(sorted((tid, other)))] += 1
                seen.add(tid)

    def to_dict(self):
        tpl = {}
        for tid, e in self.tpl.items():
            tpl[tid] = dict(e, ents=dict(e["ents"].most_common(20)))
        return {"tpl": tpl, "pairs": dict(self.pairs)}

    @staticmethod
    def merge(a, b):
        """Merge two ``to_dict()`` results."""
        for tid, e in b["tpl"].items():
            x = a["tpl"].get(tid)
            if x is None:
                a["tpl"][tid] = e
                continue
            x["count"] += e["count"]
            x["corr_n"] = x.get("corr_n", 0) + e.get("corr_n", 0)
            x["first"] = min(x["first"], e["first"])
            x["last"] = max(x["last"], e["last"])
            ents = Counter(x["ents"])
            ents.update(e["ents"])
            x["ents"] = dict(ents.most_common(20))
        pairs = Counter(a["pairs"])
        pairs.update(b["pairs"])
        a["pairs"] = dict(pairs)
        return a


class _Bucket:
    __slots__ = ("n", "lv", "lg", "pod", "tpl", "ev", "ct", "al")

    def __init__(self):
        self.n = 0
        self.lv = {}
        self.lg = {}
        self.pod = {}
        self.tpl = {}
        self.ev = {}
        self.ct = {}
        self.al = {}   # ground-truth alert tags (labelled datasets only; never fed to the predictor)


def _inc(d, k):
    d[k] = d.get(k, 0) + 1


class MinuteAggregator:
    """Aggregates one (env, kind) stream; calls ``emit(row, events)`` per flushed minute."""

    def __init__(self, env, kind, emit, catalog=None, on_interesting=None, fmt="kafka-json"):
        self.env = env
        self.kind = kind
        self.emit = emit
        self.on_interesting = on_interesting
        self.catalog = catalog if catalog is not None else Catalog()
        self.buckets = {}
        self.max_m = -1
        self.flushed_upto = -1  # minutes < this are already emitted
        self.late = {}
        self.lines = 0
        self.bad = 0
        self.interesting = 0
        if fmt == "bgl":
            self.add = self._add_bgl
        else:
            self.add = self._add_commtrace if kind == "commtrace" else self._add_oplog
        self._pods = {}

    # ---- per-line ---------------------------------------------------------
    def _bucket(self, ts):
        m = ts // 60000
        b = self.buckets.get(m)
        if b is not None:
            return b, m
        if m < self.flushed_upto:
            b = self.late.get(m)
            if b is None:
                b = self.late[m] = _Bucket()
            return b, m
        b = self.buckets[m] = _Bucket()
        if m > self.max_m:
            self.max_m = m
            self.flush(m - LAG_MIN)
        return b, m

    def _pod(self, server):
        p = self._pods.get(server)
        if p is None:
            p = self._pods[server] = F.pod(server.decode())
        return p

    def _add_oplog(self, line):
        self.lines += 1
        ts, lvl, logger, server = F.oplog_head(line)
        if ts is None:
            self.bad += 1
            return
        b, m = self._bucket(ts)
        b.n += 1
        _inc(b.lv, lvl)
        _inc(b.lg, logger)
        _inc(b.pod, server)
        if lvl not in QUIET_LEVELS and lvl != b"META":
            self._interesting(line, ts, b, server)

    def _interesting(self, line, ts, b, server):
        try:
            d = F.full(line)
        except ValueError:
            self.bad += 1
            return
        self.interesting += 1
        level = F.LEVELS.get((d.get("level") or "").encode(), d.get("level") or "?")
        logger = d.get("logger") or "?"
        msg = d["message"]
        tpl = template(msg)
        tid = template_id(logger, tpl)
        ents = entities(msg)
        self.catalog.add(tid, logger, level, tpl, msg, ts, ents, d.get("correlationId"))
        _inc(b.tpl, tid)
        if self.on_interesting is not None:
            self.on_interesting({"ts": ts, "env": self.env, "level": level, "logger": logger,
                                 "pod": self._pod(server) if server else "?", "tid": tid,
                                 "corr": d.get("correlationId"), "msg": msg[:500]})
        key = (tid, server)
        ev = b.ev.get(key)
        if ev is None:
            b.ev[key] = [1, ts, ts, Counter(ents[:5]), msg[:300], level, logger]
        else:
            ev[0] += 1
            ev[1] = min(ev[1], ts)
            ev[2] = max(ev[2], ts)
            ev[3].update(ents[:5])

    def _add_bgl(self, line):
        self.lines += 1
        head = F.bgl_head(line)
        if head is None:
            self.bad += 1
            return
        label, ts, node, comp, lvl, msg = head
        b, m = self._bucket(ts)
        b.n += 1
        _inc(b.lv, lvl)
        rack = node[:6]   # R02-M1: rack + midplane, the "pod" of this machine
        if rack not in self._pods:
            self._pods[rack] = rack.decode(errors="replace")
        _inc(b.pod, rack)
        if label != b"-":
            _inc(b.al, label)
        if lvl == b"INFO":
            _inc(b.lg, F.bgl_key(comp, msg))
            return
        self.interesting += 1
        logger = comp.decode(errors="replace")
        text = msg.decode(errors="replace")
        tpl = template(text)
        tid = template_id(logger, tpl)
        ents = [node.decode(errors="replace")]
        level = F.LEVELS.get(lvl, "?")
        _inc(b.lg, logger + "." + level)
        self.catalog.add(tid, logger, level, tpl, text, ts, ents, None)
        _inc(b.tpl, tid)
        if self.on_interesting is not None:
            self.on_interesting({"ts": ts, "env": self.env, "level": level, "logger": logger,
                                 "pod": self._pods[rack], "tid": tid, "corr": None, "msg": text[:500]})
        key = (tid, rack)
        ev = b.ev.get(key)
        if ev is None:
            b.ev[key] = [1, ts, ts, Counter(ents), text[:300], level, logger]
        else:
            ev[0] += 1
            ev[2] = max(ev[2], ts)
            ev[3].update(ents)

    def _add_commtrace(self, line):
        self.lines += 1
        ts = F.ftime(line)
        if ts is None:
            self.bad += 1
            return
        b, _ = self._bucket(ts)
        b.n += 1
        _inc(b.pod, F.fstr(line, F.K_SERVER))
        ct = F.fstr(line, F.K_COMMTYPE)
        arr = b.ct.get(ct)
        if arr is None:
            arr = b.ct[ct] = [0] * CT_LEN
        direction = F.fstr(line, F.K_DIRECTION)
        if direction == b"Out":
            arr[0] += 1
        elif direction == b"In":
            arr[1] += 1
            dm = F.K_DURATION.search(line)
            if dm:
                dur = int(dm.group(1))
                arr[2] += 1
                arr[3] += dur
                if dur > arr[4]:
                    arr[4] = dur
                arr[5 + bisect_right(DUR_BOUNDS, dur)] += 1

    # ---- flushing ---------------------------------------------------------
    def flush(self, upto=None):
        """Emit all buckets with minute < upto (all if None)."""
        ready = sorted(m for m in self.buckets if upto is None or m < upto)
        for m in ready:
            self._emit(m, self.buckets.pop(m))
        if upto is not None and upto > self.flushed_upto:
            self.flushed_upto = upto
        if upto is None:
            for m in sorted(self.late):
                self._emit(m, self.late.pop(m))

    def _emit(self, m, b):
        dec = lambda d: {(k.decode() if isinstance(k, bytes) else str(k)): v for k, v in d.items()}
        pods = Counter()
        for srv, n in b.pod.items():
            pods[self._pod(srv) if srv else "?"] += n
        row = {"t": m * 60, "env": self.env, "k": self.kind, "n": b.n, "pod": dict(pods)}
        events = []
        if self.kind == "oplog":
            row["lv"] = dec(b.lv)
            row["lg"] = dec(b.lg)
            row["tpl"] = b.tpl
            if b.al:
                row["al"] = dec(b.al)
            for (tid, srv), ev in b.ev.items():
                events.append({"t": m * 60, "env": self.env, "tid": tid, "level": ev[5],
                               "logger": ev[6], "pod": self._pod(srv) if srv else "?",
                               "count": ev[0], "first": ev[1], "last": ev[2],
                               "ents": [e for e, _ in ev[3].most_common(5)], "msg": ev[4]})
        else:
            row["ct"] = dec(b.ct)
        self.emit(row, events)


def merge_rows(a, b):
    """Merge two rows of the same (env, kind, minute) – used at shard boundaries."""
    a["n"] += b["n"]
    for key in ("pod", "lv", "lg", "tpl", "al"):
        if key in b:
            d = a.setdefault(key, {})
            for k, v in b[key].items():
                d[k] = d.get(k, 0) + v
    if "ct" in b:
        d = a.setdefault("ct", {})
        for k, v in b["ct"].items():
            x = d.get(k)
            if x is None:
                d[k] = v
            else:
                for i in range(CT_LEN):
                    x[i] = max(x[i], v[i]) if i == 4 else x[i] + v[i]
    return a
