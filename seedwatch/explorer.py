"""Read-only, bounded log investigation. Independent of detection and profiling.

A small file-offset index stores event-time bounds for line-aligned blocks. It
handles out-of-order records without assuming Kafka time == application time.
The index contains no message text and is invalidated when the source changes.
"""
from collections import Counter, OrderedDict
import hashlib
import heapq
import os
from pathlib import Path
import re
import threading
import secrets
from time import monotonic

import orjson

from . import fields
from .predict import THRESHOLD
from .template import template, entities
from .grouping import GroupingRules, clean_rule, matches

BLOCK_BYTES = 4 * 1024 * 1024
MAX_SCAN = 8 * 1024 * 1024
MAX_RANGE = 366 * 86400
MAX_GROUPS = 20000
MAX_RETAINED_CHARS = 16 * 1024 * 1024
MAX_MESSAGE = 24000
SEVERITY_RANK = {"TRACE": 10, "DEBUG": 20, "INFO": 30, "WARN": 40,
                 "ERROR": 50, "CRITICAL": 60, "FATAL": 60}
LEVEL_ALIASES = {"WARNING": "WARN", "SEVERE": "ERROR"}
LABELS = {
    "volume": "Log volume dropped",
    "heartbeat": "Recurring log activity dropped",
    "latency": "Response times increased",
    "unanswered": "Outgoing / incoming message imbalance",
    "templates": "Log pattern activity changed",
}
# Presentation templates are independent of detector templates. Mask volatile
# values but explicitly retain outcomes so "Failed=0" and "Failed=1" differ.
_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b")
_TIME = re.compile(r"\b\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\b")
_RESOURCE = re.compile(r"\b[\w./-]+\.(?:bpmn|jar|class|xml)\b", re.I)
_LIFECYCLE = re.compile(r"^(Starting|Complete|Shutdown):\s*[^,\n]+")
_OUTCOME = re.compile(
    r"(?i)[\"']?\b(response(?:\s+code)?|extended\s+code|return\s+code|error\s+code|"
    r"status(?:\s+code)?|code|state|(?:ex[ce]+ution\s+)?result|RC|ERC|success|successful|failed|failure)"
    r"[\"']?(?:\s*[:=]\s*|\s+is\s*:?\s*|\s+)[\"']?([A-Za-z0-9_+.-]+)")
_SKIP_OUTCOME = {"code", "by", "for", "while", "to", "in", "of", "is", "the", "with", "at"}


def outcome_suffix(msg):
    outcomes = sorted({(k.casefold(), v.rstrip('.')) for k, v in _OUTCOME.findall(msg)
                       if v.casefold() not in _SKIP_OUTCOME})
    return " | " + ", ".join(k + "=" + v for k, v in outcomes) if outcomes else ""


def pattern(msg, custom=None):
    if custom is None:
        text = _TIME.sub("<time>", _TIMESTAMP.sub("<timestamp>", msg))
        text = _RESOURCE.sub("<resource>", _LIFECYCLE.sub(r"\1: <component>", text))
        normalized = template(text, max_len=1600)
    else:
        normalized = custom
    return normalized + outcome_suffix(msg)


def rows(path):
    if path.exists():
        with path.open("rb") as f:
            for line in f:
                yield orjson.loads(line)


def event_time(line):
    return fields.ftime(line) or fields.ftime(line, fields.E_TIME) or fields.create_time(line)


class Explorer:
    severity_ranks = SEVERITY_RANK

    def __init__(self, out, raw_files, scan_limit=MAX_SCAN, indexed=False):
        self.out = Path(out)
        self.raw = {}
        for name in raw_files:
            try:
                env, kind = fields.classify_file(name)
                if kind == "oplog":
                    self.raw.setdefault(env, []).append(Path(name).resolve())
            except ValueError:
                continue
        self.envs = sorted(p.parent.name for p in self.out.glob("*/risk.jsonl"))
        self.scan_limit = scan_limit
        self.lock = threading.RLock()
        self.indexes = {}
        self.workspaces = {}
        self.jobs = OrderedDict()
        self.query_keys = {}
        self.rules = GroupingRules(self.out)
        self.indexed, self.stores = indexed, {}

    def store(self, env):
        if not self.indexed or not self.raw.get(env):
            return None
        if env not in self.stores:
            from .logstore import LogStore
            self.stores[env] = LogStore(self.out, env, self.raw[env], pattern, outcome_suffix,
                                        SEVERITY_RANK, LEVEL_ALIASES)
        return self.stores[env]

    def close(self):
        for job in self.jobs.values():
            job.iterator.close()
        for store in self.stores.values():
            store.close()

    def workspace(self, env):
        if env not in self.envs:
            raise ValueError("Unknown environment")
        with self.lock:
            if env in self.workspaces:
                return self.workspaces[env]
            d = self.out / env
            risk = list(rows(d / "risk.jsonl"))
            alerts = []
            src = d / "alerts.json"
            for i, a in enumerate(orjson.loads(src.read_bytes()) if src.exists() else []):
                logger = a.get("title", "").split(": ", 1)[-1] if a.get("dominant") == "templates" else None
                alerts.append({"id": str(i), "t": a["t"], "risk": a["risk"], "logger": logger,
                               "label": LABELS.get(a.get("dominant"), "Log activity changed"),
                               "dominant": a.get("dominant"), "components": a.get("components", {}),
                               "evidence": a.get("why", [])})
            services, levels = set(), Counter()
            for row in rows(d / "oplog.minutes.jsonl"):
                services.update(p.split("/", 1)[0] for p in row.get("pod", {}))
                levels.update(row.get("lv", {}))
            stats = self.out / "batch_stats.json"
            result = {"env": env, "envs": self.envs, "risk": risk, "alerts": alerts,
                      "threshold": THRESHOLD, "services": sorted(services),
                      "levels": sorted((lv for lv, n in levels.items() if n and lv in SEVERITY_RANK),
                                       key=SEVERITY_RANK.get),
                      "coverage": "original" if self.raw.get(env) else "prepared-warn-only",
                      "stats": orjson.loads(stats.read_bytes()) if stats.exists() else {}}
            self.workspaces[env] = result
            return result

    def _index(self, path):
        st = path.stat()
        fingerprint = [st.st_size, st.st_mtime_ns]
        key = str(path)
        if key in self.indexes and self.indexes[key][0] == fingerprint:
            return self.indexes[key][1]
        directory = self.out / "explorer-index"
        directory.mkdir(exist_ok=True)
        target = directory / (hashlib.sha256(key.encode()).hexdigest()[:16] + ".json")
        if target.exists():
            saved = orjson.loads(target.read_bytes())
            if saved.get("version") == 1 and saved.get("fingerprint") == fingerprint:
                self.indexes[key] = (fingerprint, saved["blocks"])
                return saved["blocks"]
        blocks = []
        with path.open("rb") as f:
            start, low, high = 0, None, None
            while True:
                line = f.readline()
                if not line:
                    break
                ts = event_time(line)
                if ts is not None:
                    low = ts if low is None else min(low, ts)
                    high = ts if high is None else max(high, ts)
                end = f.tell()
                if end - start >= BLOCK_BYTES:
                    blocks.append([start, end, low, high])
                    start, low, high = end, None, None
            if f.tell() > start:
                blocks.append([start, f.tell(), low, high])
        tmp = target.with_suffix(".tmp")
        tmp.write_bytes(orjson.dumps({"version": 1, "fingerprint": fingerprint, "blocks": blocks}))
        os.replace(tmp, target)
        self.indexes[key] = (fingerprint, blocks)
        return blocks

    def query(self, q):
        offset = max(0, int(q.get("offset", 0)))
        if offset > 5000:
            raise ValueError("Narrow the filters to browse beyond 5,000 rows")
        limit = max(1, min(int(q.get("limit", 50)), 100))
        excluded = tuple(sorted(set(q.get("exclude", "").split(",")) - {""}))
        only = q.get("only", "")
        if len(excluded) > 100:
            raise ValueError("At most 100 patterns can be hidden")
        with self.lock:
            token = q.get("job")
            env = self.jobs[token].env if token in self.jobs else q.get('env', 'vit-3')
            store = self.store(env)
            storage = store.ensure() if store else None
            if token:
                if token not in self.jobs:
                    raise ValueError("This query expired. Refresh the logs to start it again.")
                job = self.jobs[token]
                if storage and job.spec[-1] != storage['generation']:
                    job.iterator.close()
                    del self.jobs[token]
                    self.query_keys.pop(job.spec, None)
                    raise ValueError('Sources changed. Refresh the logs for the current files.')
                if "asof" in q and job.end > int(q["asof"]):
                    raise ValueError("Query is beyond the replay cursor")
                if "extend_to" in q:
                    end = min(int(q["extend_to"]), int(q.get("asof", q["extend_to"])))
                    if end - job.start > MAX_RANGE:
                        raise ValueError("Select a positive interval within the recorded data")
                    if end > job.end and job.done and not job.notice:
                        if self.query_keys.get(job.spec) == token:
                            self.query_keys.pop(job.spec, None)
                        job.extend(end)
                        self.query_keys[job.spec] = token
            else:
                env = q.get("env", "vit-3")
                if env not in self.envs:
                    raise ValueError("Unknown environment")
                start, end = int(q["start"]), int(q["end"])
                if "asof" in q:
                    end = min(end, int(q["asof"]))
                if end <= start or end - start > MAX_RANGE:
                    raise ValueError("Select a positive interval within the recorded data")
                minimum = q.get("min_level", "").upper()
                if minimum and minimum not in SEVERITY_RANK:
                    raise ValueError("Unknown minimum log level")
                spec = (env, start, end, q.get("mode", "grouped"), q.get("text", "").casefold()[:300],
                        q.get("level", ""), q.get("service", ""),
                        (), "", minimum, self.rules.list(env)["revision"])
                if storage:
                    spec += (storage['generation'],)
                token = self.query_keys.get(spec)
                if token in self.jobs:
                    job = self.jobs[token]
                else:
                    token = secrets.token_hex(12)
                    job = LogQuery(self, spec, token)
                    self.jobs[token] = job
                    self.query_keys[spec] = token
            if storage and storage['ready'] and not getattr(job, 'indexed', False):
                from .logstore import IndexedQuery
                job.iterator.close()
                job = self.jobs[token] = IndexedQuery(self, job.spec, token, store)
            self.jobs.move_to_end(token)
            # Every request advances a bounded chunk; the client can cancel or
            # pause without a worker continuing a multi-GB scan in the background.
            if q.get("advance", "1") != "0" or (getattr(job, 'indexed', False) and not job.done):
                job.advance(self.scan_limit)
            while len(self.jobs) > 3:
                _, old = self.jobs.popitem(last=False)
                if self.query_keys.get(old.spec) == old.token:
                    self.query_keys.pop(old.spec, None)
                old.iterator.close()
            result = job.snapshot(offset, limit, excluded, only)
            if storage:
                result['storage'] = store.status()
            return result

    def list_rules(self, env):
        if env not in self.envs:
            raise ValueError("Unknown environment")
        with self.lock:
            return self.rules.list(env)

    def update_rules(self, request):
        env = request.get("env", "vit-3")
        if env not in self.envs:
            raise ValueError("Unknown environment")
        with self.lock:
            result = self.rules.update(env, request)
            # Rule changes require regrouping. Old tokens must not silently keep
            # returning results produced under a different rule revision.
            for token, job in list(self.jobs.items()):
                if job.env == env:
                    job.iterator.close()
                    del self.jobs[token]
                    if self.query_keys.get(job.spec) == token:
                        del self.query_keys[job.spec]
            return result

    def preview_rule(self, request):
        rule = clean_rule(request.get("rule"))
        with self.lock:
            job = self.jobs.get(request.get("job"))
            if job is None or job.env != request.get("env"):
                raise ValueError("Load some logs in this environment before previewing a rule")
            if "asof" in request and job.end > int(request["asof"]):
                raise ValueError("Query is beyond the replay cursor")
            excluded, only = set(request.get("exclude", [])), request.get("only", "")
            candidates = (job.preview_candidates(rule, excluded, only) if getattr(job, 'indexed', False) else
                          job.groups.values() if job.mode == "grouped" else
                          (r for _, _, r in sorted(job.latest, reverse=True)))
            seen, tested, matched, affected = set(), 0, 0, set()
            examples, nonmatches, earlier = [], [], set()
            for group in candidates:
                if group["pattern_id"] in excluded or (only and group["pattern_id"] != only):
                    continue
                if any(rule.get(k) and rule[k] != group[k] for k in ("logger", "service", "level")):
                    continue
                for record in [group, *group.get("samples", [])]:
                    example = (record["logger"], record["service"], record["level"], record["message"])
                    if example in seen:
                        continue
                    seen.add(example)
                    if len(seen) > 500:
                        break
                    tested += 1
                    sample = {k: record[k] for k in ("t", "message", "logger", "service", "level", "source")}
                    if matches(rule, record["message"], record["logger"], record["service"], record["level"]):
                        matched += 1
                        affected.add(record["pattern_id"])
                        if len(examples) < 6:
                            examples.append(sample)
                        for old in job.rules:
                            if old["id"] == request.get("id"):
                                break
                            if matches(old, record["message"], record["logger"], record["service"], record["level"]):
                                earlier.add(old["name"])
                                break
                    elif len(nonmatches) < 3:
                        nonmatches.append(sample)
                if len(seen) > 500:
                    break
            return {"tested": tested, "matched": matched, "patterns": len(affected),
                    "sampled": min(len(seen), 500), "sample_limit": 500,
                    "examples": examples, "nonmatches": nonmatches,
                    "earlier_rules": sorted(earlier), "partial_query": not job.done,
                    "start": job.start, "end": job.end, "rule": rule}

    def source(self, q):
        env = q.get("env", "vit-3")
        path = next((p for p in self.raw.get(env, []) if p.name == q.get("file")), None)
        if path is None:
            raise ValueError("Source is unavailable")
        offset = int(q["offset"])
        if not 0 <= offset < path.stat().st_size:
            raise ValueError("Invalid source offset")
        with path.open("rb") as f:
            if offset:
                f.seek(offset - 1)
                if f.read(1) != b"\n":
                    raise ValueError("Offset must identify a record boundary")
            f.seek(offset)
            raw = f.readline(128000)
        ts = event_time(raw)
        if "asof" in q and (ts is None or ts >= int(q["asof"]) * 1000):
            raise ValueError("Record is beyond the replay cursor")
        return {"raw": raw.decode(errors="replace"), "truncated": len(raw) == 128000}


class LogQuery:
    """Bounded progressive scan, with stable pattern identities across ranges."""
    def __init__(self, explorer, spec, token):
        self.explorer, self.spec, self.token = explorer, spec, token
        (self.env, self.start, self.end, self.mode, self.text, self.level,
         self.service, self.excluded, self.only, self.minimum, self.rule_revision) = spec[:11]
        self.rules = explorer.rules.list(self.env)["rules"]
        self.groups, self.latest = {}, []
        self.pattern_counts = Counter()
        self.total = self.in_range = self.hidden = self.scanned = self.retained = 0
        self.done, self.notice = False, None
        self.files = explorer.raw.get(self.env, [])
        self.planned = 0
        self.iterator = self.records(self.start, self.end)

    def extend(self, end):
        """Append a disjoint event-time interval; never re-count prior records."""
        start, self.end = self.end, end
        self.spec = self.spec[:2] + (end,) + self.spec[3:]
        self.done = False
        self.iterator = self.records(start, end)

    def records(self, start, end):
        blocks = []
        for path in self.files:
            for a, b, lo, hi in self.explorer._index(path):
                if lo is not None and hi >= start * 1000 and lo < end * 1000:
                    blocks.append((path, a, b, hi))
        # Newest blocks first: useful recent records appear before scanning days.
        blocks.sort(key=lambda b: (-b[3], str(b[0]), -b[1]))
        self.planned += sum(b-a for _, a, b, _ in blocks)
        if self.files:
            for path, a, b, _ in blocks:
                with path.open("rb") as f:
                    f.seek(a)
                    while f.tell() < b:
                        at = f.tell()
                        raw = f.readline()
                        self.scanned += len(raw)
                        ts, lvl, logger, server = fields.oplog_head(raw)
                        if ts is None or not start * 1000 <= ts < end * 1000:
                            continue
                        raw_level = (lvl or b"OTHER").decode(errors="replace").upper()
                        lv = LEVEL_ALIASES.get(raw_level, raw_level)
                        instance = fields.pod((server or b"").decode(errors="replace"))
                        yield ts, lv, instance, (logger or b"?").decode(errors="replace"), raw, {"file": path.name, "offset": at}
                yield None  # progress even if a block has no exact matches
        else:
            for i, r in enumerate(rows(self.explorer.out / self.env / "filtered.jsonl")):
                self.scanned += 1
                if start * 1000 <= r["ts"] < end * 1000:
                    yield r["ts"], r["level"], r.get("pod", "?"), r.get("logger", "?"), r, {"prepared": True, "record": i}

    def advance(self, budget):
        if self.done:
            return
        until, deadline = self.scanned + budget, monotonic() + .65
        while True:
            try:
                item = next(self.iterator)
            except StopIteration:
                self.done = True
                return
            if item is not None:
                self.add(item)
            if self.notice:
                self.done = True
                self.iterator.close()
                return
            if self.scanned >= until or monotonic() >= deadline:
                return

    def add(self, item):
        ts, lv, instance, logger, raw, source = item
        svc = instance.split("/", 1)[0]
        self.in_range += 1
        if self.minimum and SEVERITY_RANK.get(lv, -1) < SEVERITY_RANK[self.minimum]:
            return
        if self.level and (SEVERITY_RANK.get(lv, -1) < SEVERITY_RANK["WARN"] if self.level == "WARN+" else lv != self.level):
            return
        if self.service and svc != self.service:
            return
        if isinstance(raw, bytes):
            try:
                parsed = fields.full(raw)
                msg = parsed["message"]
                if parsed.get("throwable") and str(parsed["throwable"]) not in msg:
                    msg += "\n" + str(parsed["throwable"])
            except (ValueError, TypeError):
                msg = raw.decode(errors="replace")
        else:
            msg = raw.get("msg", "")
        if self.text and self.text not in (logger + " " + msg + " " + instance).casefold():
            return
        custom = next((r for r in self.rules if matches(r, msg, logger, svc, lv)), None)
        normalized = pattern(msg, custom["template"] if custom else None)
        identity = (custom["id"]+"|" if custom else "") + lv + "|" + logger + "|" + normalized
        pid = hashlib.sha256(identity.encode()).hexdigest()[:20]
        if pid not in self.pattern_counts and len(self.pattern_counts) >= MAX_GROUPS:
            self.notice = "Distinct-pattern limit reached. Results so far remain visible; narrow the filters to finish."
            return
        self.total += 1
        self.pattern_counts[pid] += 1
        record = {"t": ts, "level": lv, "service": svc, "instance": instance,
                  "logger": logger, "message": msg[:MAX_MESSAGE], "message_truncated": len(msg) > MAX_MESSAGE,
                  "source": source, "pattern_id": pid, "pattern": normalized,
                  "rule_id": custom["id"] if custom else None,
                  "rule_name": custom["name"] if custom else None}
        if self.mode == "records":
            heapq.heappush(self.latest, (ts, self.total, record))
            self.retained += len(record["message"])
            # Keep a bounded, consistent newest-record window for pagination.
            if len(self.latest) > 5100:
                self.retained -= len(heapq.heappop(self.latest)[2]["message"])
        else:
            signature = svc + "|" + pid
            if signature not in self.groups:
                if len(self.groups) >= MAX_GROUPS:
                    self.notice = "Distinct-pattern limit reached. Results so far remain visible; narrow the filters to finish."
                    return
                self.groups[signature] = {"id": hashlib.sha256(signature.encode()).hexdigest()[:16],
                    **record, "first": ts, "last": ts, "count": 0, "instances": set(), "entities": {}, "samples": []}
            g = self.groups[signature]
            before = len(g["message"]) + sum(len(r["message"]) for r in g["samples"])
            g["count"] += 1
            g["first"], g["last"] = min(g["first"], ts), max(g["last"], ts)
            g["instances"].add(instance)
            for entity in entities(msg):
                if entity in g["entities"] or len(g["entities"]) < 256:
                    g["entities"][entity] = g["entities"].get(entity, 0) + 1
            # Distinct original examples reveal the variable values collapsed by
            # grouping; representative/latest source is kept separately.
            if not any(r["message"] == record["message"] for r in g["samples"]):
                g["samples"] = sorted(g["samples"] + [record], key=lambda r: -r["t"])[:3]
            if ts >= g["t"]:
                g.update(record)
            self.retained += len(g["message"]) + sum(len(r["message"]) for r in g["samples"]) - before
        if self.retained > MAX_RETAINED_CHARS:
            self.notice = "Message buffer limit reached. Results so far remain visible; narrow the filters to finish."

    def snapshot(self, offset, limit, excluded=(), only=""):
        def visible(pid):
            return pid not in excluded and (not only or pid == only)
        hidden = sum(n for pid, n in self.pattern_counts.items() if not visible(pid))
        if self.mode == "records":
            ordered = [r for _, _, r in sorted(self.latest, key=lambda r: (-r[0], -r[1])) if visible(r['pattern_id'])]
            size = self.total - hidden
        else:
            ordered = sorted((r for r in self.groups.values() if visible(r['pattern_id'])), key=lambda r: (-r["last"], r["id"]))
            size = len(ordered)
        page = []
        for item in ordered[offset:offset+limit]:
            row = dict(item)
            if "instances" in row:
                row["instances"] = sorted(row["instances"])
                row["entities"] = [{"name": k, "count": v} for k, v in sorted(row["entities"].items(), key=lambda x: -x[1])]
            page.append(row)
        return {"job": self.token, "start": self.start, "end": self.end, "rows": page,
                "total_records": self.total-hidden, "interval_records": self.in_range, "hidden_records": hidden,
                "total_rows": size, "offset": offset, "limit": limit, "pending": not self.done,
                "complete": self.done and self.notice is None, "notice": self.notice,
                "progress": min(1, self.scanned / max(1, self.planned)) if self.files else (1 if self.done else 0),
                "scanned_bytes": self.scanned, "planned_bytes": self.planned, "rule_revision": self.rule_revision,
                "coverage": "original" if self.files else "prepared-warn-only"}
