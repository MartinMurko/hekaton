"""Persistent parsed operational logs for the local explorer.

Source offsets remain available, but browsing uses compressed columnar data.
Each committed batch includes its resume position. A source fingerprint change
rebuilds the store; grouping-rule changes only change SQL expressions.
"""
from collections import Counter
from functools import lru_cache
import hashlib
from pathlib import Path
import threading
from time import monotonic

import orjson

from . import fields
from .template import entities

VERSION = 1
BATCH_ROWS = 8192
COLUMNS = ['uid', 'ts', 'level', 'rank', 'logger', 'service', 'instance', 'message',
           'search_text', 'clean_message', 'pattern', 'pid', 'outcomes', 'entities',
           'source_file', 'byte_offset']
SCHEMA = """CREATE TABLE logs (
    uid BIGINT, ts BIGINT, level VARCHAR, rank SMALLINT, logger VARCHAR,
    service VARCHAR, instance VARCHAR, message VARCHAR, search_text VARCHAR,
    clean_message VARCHAR, pattern VARCHAR, pid VARCHAR, outcomes VARCHAR,
    entities VARCHAR[], source_file VARCHAR, byte_offset BIGINT)"""


class LogStore:
    def __init__(self, out, env, files, normalize, suffix, ranks, aliases):
        self.files = sorted(Path(p) for p in files)
        self.path = Path(out)/'explorer-store'/(env+'.duckdb')
        self.normalize, self.suffix = normalize, suffix
        self.ranks, self.aliases = ranks, aliases
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.worker = None
        self.connection = None
        self.state = {'ready': False, 'pending': False, 'progress': 0,
                      'processed_bytes': 0, 'total_bytes': 0, 'records': 0,
                      'generation': '', 'error': None}

    def fingerprint(self):
        sources = [[str(p.resolve()), p.stat().st_size, p.stat().st_mtime_ns] for p in self.files]
        key = hashlib.sha256(orjson.dumps([VERSION, sources])).hexdigest()
        return key, sum(s[1] for s in sources)

    def ensure(self):
        generation, size = self.fingerprint()
        with self.lock:
            if self.state['generation'] != generation:
                if self.worker and self.worker.is_alive():
                    return {**self.state, 'ready': False, 'generation': generation}
                self.state = {'ready': False, 'pending': True, 'progress': 0,
                              'processed_bytes': 0, 'total_bytes': size, 'records': 0,
                              'generation': generation, 'error': None}
                self.worker = threading.Thread(target=self._prepare, args=(generation,), daemon=True,
                                               name='log-store-'+self.path.stem)
                self.worker.start()
            return dict(self.state)

    def status(self):
        with self.lock:
            return dict(self.state)

    def cursor(self):
        return self.connection.cursor()

    def _prepare(self, generation):
        started = monotonic()
        db = None
        try:
            import duckdb
            import pyarrow as pa
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.connection is None:
                self.connection = duckdb.connect(str(self.path), config={
                    'memory_limit': '512MB', 'threads': 2,
                    'temp_directory': str(self.path.with_suffix('.tmp'))})
            db = self.cursor()
            db.execute('CREATE TABLE IF NOT EXISTS metadata (value VARCHAR)')
            saved = db.execute('SELECT value FROM metadata').fetchone()
            meta = orjson.loads(saved[0]) if saved else {}
            if meta.get('generation') != generation:
                db.execute('BEGIN')
                db.execute('DROP TABLE IF EXISTS logs')
                db.execute(SCHEMA)
                meta = {'generation': generation, 'complete': False, 'file': 0,
                        'offset': 0, 'bytes': 0, 'records': 0}
                self._save_meta(db, meta)
                db.execute('COMMIT')
            with self.lock:
                self.state.update(processed_bytes=meta['bytes'], records=meta['records'],
                                  progress=meta['bytes']/max(1, self.state['total_bytes']))
            if not meta['complete']:
                @lru_cache(maxsize=4096)
                def metadata(message):
                    return self.normalize(message), self.suffix(message), entities(message)

                # Explicit Arrow types avoid type changes when a batch contains
                # no entities or only unclassified records.
                schema = pa.schema([(name, pa.int64() if name in ('uid', 'ts', 'byte_offset') else
                                     pa.int16() if name == 'rank' else
                                     pa.list_(pa.string()) if name == 'entities' else pa.string())
                                    for name in COLUMNS])
                for file_id in range(meta['file'], len(self.files)):
                    path = self.files[file_id]
                    with path.open('rb') as source:
                        source.seek(meta['offset'] if file_id == meta['file'] else 0)
                        batch = {name: [] for name in COLUMNS}
                        read_bytes = 0
                        while not self.stop.is_set():
                            at = source.tell()
                            raw = source.readline()
                            if raw:
                                read_bytes += len(raw)
                                ts, lv, lg, server = fields.oplog_head(raw)
                                if ts is not None:
                                    level = (lv or b'OTHER').decode(errors='replace').upper()
                                    level = self.aliases.get(level, level)
                                    logger = (lg or b'?').decode(errors='replace')
                                    instance = fields.pod((server or b'').decode(errors='replace'))
                                    try:
                                        parsed = fields.full(raw)
                                        message = parsed['message']
                                        if parsed.get('throwable') and str(parsed['throwable']) not in message:
                                            message += '\n'+str(parsed['throwable'])
                                    except (ValueError, TypeError):
                                        message = raw.decode(errors='replace')
                                    norm, outcomes, mentioned = (metadata(message) if len(message) <= 2000 else
                                                               (self.normalize(message), self.suffix(message), entities(message)))
                                    pid = hashlib.sha256((level+'|'+logger+'|'+norm).encode()).hexdigest()[:20]
                                    values = [(file_id << 48)+at, ts, level, self.ranks.get(level, -1), logger,
                                              instance.split('/', 1)[0], instance, message,
                                              (logger+' '+message+' '+instance).casefold(), ' '.join(message.split()),
                                              norm, pid, outcomes, mentioned, path.name, at]
                                    for name, value in zip(COLUMNS, values):
                                        batch[name].append(value)
                            if len(batch['uid']) >= BATCH_ROWS or not raw:
                                db.execute('BEGIN')
                                try:
                                    if batch['uid']:
                                        db.register('incoming', pa.Table.from_pydict(batch, schema=schema))
                                        db.execute('INSERT INTO logs SELECT * FROM incoming ORDER BY ts, uid')
                                        db.unregister('incoming')
                                    meta.update(file=file_id+(not raw), offset=source.tell() if raw else 0,
                                                bytes=meta['bytes']+read_bytes,
                                                records=meta['records']+len(batch['uid']))
                                    self._save_meta(db, meta)
                                    db.execute('COMMIT')
                                except BaseException:
                                    db.execute('ROLLBACK')
                                    raise
                                with self.lock:
                                    self.state.update(processed_bytes=meta['bytes'], records=meta['records'],
                                                      progress=min(1, meta['bytes']/max(1, self.state['total_bytes'])))
                                batch = {name: [] for name in COLUMNS}
                                read_bytes = 0
                            if not raw:
                                break
                    if self.stop.is_set():
                        return
                if self.fingerprint()[0] != generation:
                    raise ValueError('Sources changed during preparation; restart preparation for the new files')
                meta['complete'] = True
                self._save_meta(db, meta)
                db.execute('CHECKPOINT')
            with self.lock:
                self.state.update(ready=True, pending=False, progress=1,
                                  elapsed_seconds=monotonic()-started, error=None)
        except Exception as error:
            with self.lock:
                self.state.update(ready=False, pending=False, error=str(error))
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _save_meta(db, value):
        db.execute('DELETE FROM metadata')
        db.execute('INSERT INTO metadata VALUES (?)', [orjson.dumps(value).decode()])

    def close(self):
        self.stop.set()
        if self.worker:
            self.worker.join(timeout=10)
        if self.connection and not (self.worker and self.worker.is_alive()):
            self.connection.close()
            self.connection = None


def like_pattern(template):
    return '%'.join(part.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
                    for part in template.split('<*>'))


class IndexedQuery:
    """SQL aggregation and pagination over prepared data, with legacy API shape."""
    indexed = True

    def __init__(self, explorer, spec, token, store):
        self.explorer, self.spec, self.token, self.store = explorer, spec, token, store
        (self.env, self.start, self.end, self.mode, self.text, self.level,
         self.service, _, _, self.minimum, self.rule_revision) = spec[:11]
        self.rules = explorer.rules.list(self.env)['rules']
        self.groups, self.pattern_counts = {}, Counter()
        self.total = self.in_range = 0
        self.done, self.notice = False, None
        self.iterator = (row for row in ())
        self.hydrated = {}

    def extend(self, end):
        self.end = end
        self.spec = self.spec[:2]+(end,)+self.spec[3:]
        self.done = False
        self.hydrated.clear()

    def selection(self, *, extra='', extra_params=()):
        conditions, values = ['ts >= ?', 'ts < ?'], [self.start*1000, self.end*1000]
        if self.minimum:
            conditions.append('rank >= ?');values.append(self.explorer.severity_ranks[self.minimum])
        if self.level:
            conditions.append('rank >= 40' if self.level == 'WARN+' else 'level = ?')
            if self.level != 'WARN+':
                values.append(self.level)
        if self.service:
            conditions.append('service = ?');values.append(self.service)
        if self.text:
            conditions.append('contains(search_text, ?)');values.append(self.text)
        if extra:
            conditions.append(extra);values.extend(extra_params)
        expression = "struct_pack(pid := pid, pattern := pattern, rule_id := NULL::VARCHAR, rule_name := NULL::VARCHAR)"
        if self.rules:
            cases, params = [], []
            for rule in self.rules:
                scope = []
                for key in ('logger', 'service', 'level'):
                    if rule[key]:
                        scope.append(key+' = ?');params.append(rule[key])
                scope.append("clean_message LIKE ? ESCAPE '\\'");params.append(like_pattern(rule['template']))
                cases.append('WHEN '+' AND '.join(scope)+" THEN struct_pack(pid := substr(sha256(? || '|' || level || '|' || logger || '|' || ? || outcomes),1,20), pattern := ? || outcomes, rule_id := ?, rule_name := ?)")
                params.extend([rule['id'], rule['template'], rule['template'], rule['id'], rule['name']])
            expression = 'CASE '+' '.join(cases)+' ELSE '+expression+' END'
            values.extend(params)
        sql = ('WITH base AS (SELECT * FROM logs WHERE '+' AND '.join(conditions)+'), '
               'selected AS (SELECT *, '+expression+' AS g FROM base) ')
        return sql, values

    def advance(self, budget=None):
        if self.done:
            return
        sql, values = self.selection()
        with self.store.cursor() as db:
            self.in_range = db.execute('SELECT count(*) FROM logs WHERE ts>=? AND ts<?',
                                       [self.start*1000, self.end*1000]).fetchone()[0]
            if self.mode == 'grouped':
                data = db.execute(sql+"""SELECT service, g.pid, any_value(level), any_value(logger),
                    any_value(g.pattern), any_value(g.rule_id), any_value(g.rule_name), count(*),
                    min(ts), max(ts), list(DISTINCT instance),
                    arg_max(uid, struct_pack(t := ts, id := uid), 8)
                    FROM selected GROUP BY service, g.pid ORDER BY max(ts) DESC, service, g.pid LIMIT 20001""", values).fetchall()
                self.total = 0
                if len(data) > 20000:
                    self.notice = 'Distinct-pattern limit reached. Narrow the filters for all groups.'
                    data = data[:20000]
                self.groups.clear();self.pattern_counts.clear()
                for svc, pid, lv, logger, norm, rid, name, count, first, last, instances, uids in data:
                    key = svc+'|'+pid
                    self.groups[key] = {'id': hashlib.sha256(key.encode()).hexdigest()[:16],
                        'pattern_id': pid, 'service': svc, 'level': lv, 'logger': logger,
                        'pattern': norm, 'rule_id': rid, 'rule_name': name, 'count': count,
                        'first': first, 'last': last, 't': last, 'instances': sorted(instances), '_uids': uids}
                    self.pattern_counts[pid] += count;self.total += count
            else:
                self.total = db.execute(sql+'SELECT count(*) FROM selected', values).fetchone()[0]
        self.done = True

    @staticmethod
    def record(value):
        from .explorer import MAX_MESSAGE
        uid, ts, lv, svc, instance, logger, message, norm, pid, rid, name, mentioned, file, offset = value
        return {'t': ts, 'level': lv, 'service': svc, 'instance': instance, 'logger': logger,
                'message': message[:MAX_MESSAGE], 'message_truncated': len(message)>MAX_MESSAGE,
                'pattern': norm, 'pattern_id': pid, 'rule_id': rid, 'rule_name': name,
                'source': {'file': file, 'offset': offset}, '_uid': uid, '_entities': mentioned}

    @staticmethod
    def record_columns():
        return 'uid,ts,level,service,instance,logger,message,g.pattern,g.pid,g.rule_id,g.rule_name,entities,source_file,byte_offset'

    def hydrate(self, page):
        missing = [g for g in page if g['id'] not in self.hydrated]
        if not missing:
            return
        ids = {uid for g in missing for uid in g['_uids']}
        sql, values = self.selection(extra='uid IN ('+','.join('?' for _ in ids)+')', extra_params=list(ids))
        with self.store.cursor() as db:
            records = {r['_uid']: r for r in (self.record(v) for v in db.execute(sql+'SELECT '+self.record_columns()+' FROM selected', values).fetchall())}
            pids = list({g['pattern_id'] for g in missing})
            sql, values = self.selection()
            counts = db.execute(sql+""", mentioned AS (
                SELECT service,g.pid AS pid,unnest(entities) AS entity FROM selected
                WHERE g.pid IN ("""+','.join('?' for _ in pids)+"""))
                SELECT service,pid,entity,count(*) AS n FROM mentioned
                GROUP BY service,pid,entity QUALIFY row_number() OVER
                (PARTITION BY service,pid ORDER BY n DESC,entity)<=256""", values+pids).fetchall()
        mentioned = {}
        for svc, pid, entity, count in counts:
            mentioned.setdefault(svc+'|'+pid, []).append({'name': entity, 'count': count})
        for group in missing:
            examples, seen = [], set()
            for uid in group['_uids']:
                record = records[uid]
                if record['message'] not in seen:
                    examples.append({k: v for k, v in record.items() if not k.startswith('_')})
                    seen.add(record['message'])
                if len(examples) == 3:
                    break
            latest = records[group['_uids'][0]]
            self.hydrated[group['id']] = {**{k: v for k, v in latest.items() if not k.startswith('_')},
                **{k: v for k, v in group.items() if not k.startswith('_')}, 'samples': examples,
                'entities': sorted(mentioned.get(group['service']+'|'+group['pattern_id'], []), key=lambda r: (-r['count'],r['name']))}

    def snapshot(self, offset, limit, excluded=(), only=''):
        visible = lambda pid: pid not in excluded and (not only or pid == only)
        hidden = 0
        if self.mode == 'grouped':
            hidden = sum(n for pid, n in self.pattern_counts.items() if not visible(pid))
            ordered = sorted((g for g in self.groups.values() if visible(g['pattern_id'])), key=lambda g: (-g['last'],g['id']))
            page = ordered[offset:offset+limit]
            self.hydrate(page)
            result = [self.hydrated[g['id']] for g in page]
            size = len(ordered)
        else:
            sql, values = self.selection()
            clauses = []
            if excluded:
                clauses.append('g.pid NOT IN ('+','.join('?' for _ in excluded)+')');values.extend(excluded)
            if only:
                clauses.append('g.pid=?');values.append(only)
            where = ' WHERE '+' AND '.join(clauses) if clauses else ''
            with self.store.cursor() as db:
                size = db.execute(sql+'SELECT count(*) FROM selected'+where, values).fetchone()[0]
                data = db.execute(sql+'SELECT '+self.record_columns()+' FROM selected'+where+
                                  ' ORDER BY ts DESC,uid DESC LIMIT ? OFFSET ?', values+[limit,offset]).fetchall()
            hidden = self.total-size
            result = [{k: v for k, v in self.record(row).items() if not k.startswith('_')} for row in data]
        return {'job': self.token, 'start': self.start, 'end': self.end, 'rows': result,
                'total_records': self.total-hidden, 'interval_records': self.in_range, 'hidden_records': hidden,
                'total_rows': size, 'offset': offset, 'limit': limit, 'pending': False,
                'complete': self.notice is None, 'notice': self.notice, 'progress': 1,
                'scanned_bytes': 0, 'planned_bytes': 0, 'rule_revision': self.rule_revision,
                'coverage': 'original', 'engine': 'duckdb'}

    def preview_candidates(self, rule, excluded, only):
        conditions, params = [], []
        for key in ('logger', 'service', 'level'):
            if rule.get(key):
                conditions.append(key+'=?');params.append(rule[key])
        sql, values = self.selection(extra=' AND '.join(conditions), extra_params=params)
        clauses = []
        if excluded:
            clauses.append('g.pid NOT IN ('+','.join('?' for _ in excluded)+')');values.extend(excluded)
        if only:
            clauses.append('g.pid=?');values.append(only)
        where = ' WHERE '+' AND '.join(clauses) if clauses else ''
        with self.store.cursor() as db:
            rows = db.execute(sql+'SELECT '+self.record_columns()+' FROM selected'+where+
                              ' ORDER BY ts DESC,uid DESC LIMIT 4096', values).fetchall()
        return (self.record(row) for row in rows)
