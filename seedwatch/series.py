"""Load per-minute rows of one environment into a dense, gap-filled minute series."""
from pathlib import Path

import orjson

EMPTY_OP = {"n": 0, "lv": {}, "lg": {}, "pod": {}, "tpl": {}}
EMPTY_CT = {"n": 0, "pod": {}, "ct": {}}


def _read(path):
    rows = {}
    if not path.exists():
        return rows
    with open(path, "rb") as f:
        for line in f:
            r = orjson.loads(line)
            rows[r["t"]] = r
    return rows


class Series:
    """Minute grid over the oplog coverage.

    ``op[i]`` is always a row (zero-filled for silent minutes – silence is data).
    ``ct[i]`` is a row inside commtrace coverage, ``None`` outside it.
    """

    def __init__(self, out, env):
        d = Path(out) / env
        self.env = env
        op = _read(d / "oplog.minutes.jsonl")
        ct = _read(d / "commtrace.minutes.jsonl")
        t0, t1 = min(op) + 60, max(op) - 60   # first/last minute of a dump are partial
        self.t = list(range(t0, t1 + 60, 60))
        self.op = [op.get(t, EMPTY_OP) for t in self.t]
        if ct:
            c0, c1 = min(ct) + 60, max(ct) - 60
            self.ct = [ct.get(t, EMPTY_CT) if c0 <= t <= c1 else None for t in self.t]
        else:
            self.ct = [None] * len(self.t)
        self.index = {t: i for i, t in enumerate(self.t)}
        # public benchmark datasets carry ground-truth alert tags; their "pods" are hardware
        # locations, so a location appearing is not a process restart
        self.labeled = any("al" in r for r in op.values())
        self.catalog = orjson.loads((d / "catalog.json").read_bytes())

    def __len__(self):
        return len(self.t)

    def pod_births(self, warmup_min=60):
        """{pod: minute index of first appearance}, ignoring pods present at the start."""
        first = {}
        for i, row in enumerate(self.op):
            for p in row["pod"]:
                if p not in first and p != "?":
                    first[p] = i
        return {p: i for p, i in first.items() if i >= warmup_min}
