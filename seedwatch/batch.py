"""Parallel batch pass: raw log files -> per-minute rows, deduplicated events, catalog.

Files are split into byte-range shards aligned to line starts.  Each worker streams
its shard in fixed-size blocks (memory ~ block size), aggregates per minute and
returns compact results.  The parent merges minute rows that straddle shard
boundaries and writes everything out in time order.
"""
import gzip
import os
import resource
import time
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path

import orjson

from .aggregate import Catalog, MinuteAggregator, merge_rows
from .fields import classify_file, line_format

BLOCK = 2 << 20
FLUSH_MARGIN_S = 15 * 60


def aligned_shards(path, shard_bytes):
    size = os.path.getsize(path)
    bounds = [0]
    with open(path, "rb") as f:
        for b in range(shard_bytes, size, shard_bytes):
            f.seek(b)
            f.readline()
            pos = f.tell()
            if bounds[-1] < pos < size:
                bounds.append(pos)
    bounds.append(size)
    return list(zip(bounds[:-1], bounds[1:]))


def iter_lines(path, start=0, end=None):
    """Yield lines (without newline) of path[start:end], reading in BLOCK chunks."""
    if str(path).endswith(".gz"):
        with gzip.open(path, "rb") as f:
            for line in f:
                yield line.rstrip(b"\n")
        return
    with open(path, "rb", buffering=0) as f:
        f.seek(start)
        remaining = (end if end is not None else os.path.getsize(path)) - start
        carry = b""
        while remaining > 0:
            buf = f.read(min(BLOCK, remaining))
            if not buf:
                break
            remaining -= len(buf)
            lines = (carry + buf).split(b"\n")
            carry = lines.pop()
            yield from lines
        if carry:
            yield carry


def run_task(task):
    path, env, kind, start, end, part = task
    t0 = time.perf_counter()
    rows = []
    Path(part).parent.mkdir(parents=True, exist_ok=True)
    with open(part, "wb") as raw:   # filtered lines go straight to disk: memory stays flat
        agg = MinuteAggregator(env, kind, lambda r, e: rows.append((r, e)),
                               on_interesting=lambda r: raw.write(orjson.dumps(r) + b"\n"),
                               fmt=line_format(path))
        add = agg.add
        for line in iter_lines(path, start, end):
            if line:
                add(line)
        agg.flush()
    return {"task": task, "rows": rows, "catalog": agg.catalog.to_dict(),
            "lines": agg.lines, "bad": agg.bad, "interesting": agg.interesting,
            "bytes": (end - start) if end else os.path.getsize(path),
            "secs": time.perf_counter() - t0,
            "maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}


def plan_tasks(paths, shard_bytes, out):
    tasks = []
    for p in paths:
        env, kind = classify_file(p)
        ranges = [(0, None)] if str(p).endswith(".gz") else aligned_shards(p, shard_bytes)
        for s, e in ranges:
            part = str(Path(out) / env / f".filtered.{len(tasks):05d}.part")
            tasks.append((str(p), env, kind, s, e, part))
    return tasks


def _merge_events(events):
    merged = {}
    for ev in events:
        key = (ev["tid"], ev["pod"])
        x = merged.get(key)
        if x is None:
            merged[key] = dict(ev)
        else:
            x["count"] += ev["count"]
            x["first"] = min(x["first"], ev["first"])
            x["last"] = max(x["last"], ev["last"])
            x["ents"] = list(dict.fromkeys(x["ents"] + ev["ents"]))[:5]
    return sorted(merged.values(), key=lambda e: (-e["count"], e["tid"]))


class _Writer:
    def __init__(self, out):
        self.out = Path(out)
        self.files = {}

    def write(self, env, name, obj):
        f = self.files.get((env, name))
        if f is None:
            d = self.out / env
            d.mkdir(parents=True, exist_ok=True)
            f = self.files[(env, name)] = open(d / name, "wb")
        f.write(orjson.dumps(obj))
        f.write(b"\n")

    def append_file(self, env, name, src):
        f = self.files.get((env, name))
        if f is None:
            d = self.out / env
            d.mkdir(parents=True, exist_ok=True)
            f = self.files[(env, name)] = open(d / name, "wb")
        with open(src, "rb") as g:
            while chunk := g.read(1 << 20):
                f.write(chunk)

    def close(self):
        for f in self.files.values():
            f.close()


def run_batch(paths, out, workers=6, shard_mb=256, log=print, stats_name="batch_stats.json"):
    t0 = time.perf_counter()
    tasks = plan_tasks(paths, shard_mb << 20, out)
    log(f"{len(paths)} files, {len(tasks)} shards, {workers} workers")
    w = _Writer(out)
    pending = defaultdict(dict)  # (env, kind) -> {t: [row, events]}
    catalogs = {}
    totals = defaultdict(int)
    worker_peak = 0
    counts = defaultdict(lambda: defaultdict(int))

    def flush(key, horizon=None):
        env, kind = key
        pend = pending[key]
        for t in sorted(x for x in pend if horizon is None or x < horizon):
            row, events = pend.pop(t)
            w.write(env, f"{kind}.minutes.jsonl", row)
            for ev in _merge_events(events):
                w.write(env, "events.jsonl", ev)
                counts[env]["events"] += 1

    with Pool(workers, maxtasksperchild=8) as pool:
        for res in pool.imap(run_task, tasks):
            path, env, kind, *_ = res["task"]
            key = (env, kind)
            pend = pending[key]
            for row, events in res["rows"]:
                x = pend.get(row["t"])
                if x is None:
                    pend[row["t"]] = [row, list(events)]
                else:
                    merge_rows(x[0], row)
                    x[1].extend(events)
            part = Path(res["task"][5])
            w.append_file(env, "filtered.jsonl", part)
            part.unlink()
            counts[env]["filtered"] += res["interesting"]
            counts[env]["lines"] += res["lines"]
            if not path.endswith(".gz") and pend:
                flush(key, max(pend) - FLUSH_MARGIN_S)
            cat = catalogs.get(env)
            catalogs[env] = res["catalog"] if cat is None else Catalog.merge(cat, res["catalog"])
            for k in ("lines", "bad", "interesting", "bytes"):
                totals[k] += res[k]
            worker_peak = max(worker_peak, res["maxrss_kb"])
            log(f"  {Path(path).name[:48]:48s} [{res['task'][3] >> 20:>6} MB] "
                f"{res['lines']:>8} lines {res['bytes'] / res['secs'] / 1e6:6.0f} MB/s")
    for key in list(pending):
        flush(key)
    w.close()
    for env, cat in catalogs.items():
        (Path(out) / env / "catalog.json").write_bytes(orjson.dumps(cat, option=orjson.OPT_INDENT_2))
    secs = time.perf_counter() - t0
    stats = dict(totals, secs=round(secs, 2), workers=workers,
                 mb_per_s=round(totals["bytes"] / secs / 1e6, 1),
                 lines_per_s=round(totals["lines"] / secs),
                 parent_peak_mb=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
                 worker_peak_mb=round(worker_peak / 1024, 1),
                 per_env={e: dict(c) for e, c in counts.items()})
    stats["est_total_peak_mb"] = round(stats["parent_peak_mb"] + workers * stats["worker_peak_mb"], 1)
    (Path(out) / stats_name).write_bytes(orjson.dumps(stats, option=orjson.OPT_INDENT_2))
    return stats
