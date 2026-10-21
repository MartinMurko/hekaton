"""Fast field extraction from raw log lines.

Lines look like ``CreateTime:<ms>\\tnull\\t{filebeat json}`` (Kafka dump) or plain
``{json}`` (gz archive).  Nested records are JSON-escaped (``\\"level\\":``), so a plain
``bytes.find`` for ``"level":"`` only ever hits the top-level key.  This lets us skip
full JSON parsing for the ~99.5 % INFO lines.
"""
import re
from pathlib import Path

try:
    import orjson as _json
    loads = _json.loads
except ImportError:  # pragma: no cover
    import json as _json
    loads = _json.loads

K_LEVEL = b'"level":"'
K_LOGGER = b'"logger":"'
K_SERVER = b'"server":"'
K_FARM = b'"farm":"'
K_TIME = b'"timeMillis":'
K_COMMTYPE = b'"commType":"'
K_DIRECTION = b'"direction":"'
K_DURATION = re.compile(rb'"duration":(\d+)')
# same keys inside a JSON-escaped nested record (filebeat failed to decode it)
E_LEVEL = b'\\"level\\":\\"'
E_LOGGER = b'\\"logger\\":\\"'
E_SERVER = b'\\"server\\":\\"'
E_TIME = b'\\"timeMillis\\":'
K_JARS = b'"jars":"'

LEVELS = {
    b"INFO": "INFO", b"DEBUG": "DEBUG", b"TRACE": "DEBUG",
    b"WARN": "WARN", b"WARNING": "WARN",
    b"ERROR": "ERROR", b"SEVERE": "ERROR",
    b"CRITICAL": "CRITICAL", b"FATAL": "CRITICAL",
}
INTERESTING = {"WARN", "ERROR", "CRITICAL"}


def fstr(line, key):
    i = line.find(key)
    if i < 0:
        return None
    i += len(key)
    return line[i:line.find(b'"', i)]


def fstr_esc(line, key):
    i = line.find(key)
    if i < 0:
        return None
    i += len(key)
    return line[i:line.find(b'\\"', i)]


def ftime(line, key=K_TIME):
    """Event time in ms (13-digit epoch millis)."""
    i = line.find(key)
    if i < 0:
        return None
    i += len(key)
    try:
        return int(line[i:i + 13])
    except ValueError:
        return None


def oplog_head(line):
    """(ts, level, logger, server) as bytes, salvaging records filebeat failed to decode.

    About 0.2 % of lines carry the real record only as a truncated, escaped string
    (long stack traces) - a naive top-level level filter silently drops those ERRORs.
    """
    ts = ftime(line)
    lvl = fstr(line, K_LEVEL)
    if ts is not None and lvl is not None:
        return ts, lvl, fstr(line, K_LOGGER), fstr(line, K_SERVER)
    if ts is None:
        ts = ftime(line, E_TIME) or ((create_time(line) or 0) or None)
    if lvl is None:
        lvl = fstr_esc(line, E_LEVEL)
        if lvl is not None:
            return ts, lvl, fstr_esc(line, E_LOGGER), fstr_esc(line, E_SERVER) or fstr(line, K_SERVER)
        if K_JARS in line:  # classpath dump written once at pod start-up
            return ts, b"META", b"startup.jars", fstr(line, K_SERVER)
        return ts, b"META", b"?", fstr(line, K_SERVER)
    return ts, lvl, fstr(line, K_LOGGER), fstr(line, K_SERVER)


def create_time(line):
    """Kafka CreateTime (ms) if the line has the dump prefix, else None."""
    if line.startswith(b"CreateTime:"):
        return int(line[11:24])
    return None


def full(line):
    """Fully parse a line and unwrap the nested message record if present."""
    d = loads(line[line.find(b"{"):])
    msg = d.get("message") or ""
    if msg.startswith("{") and '"message"' in msg:
        try:
            inner = loads(msg)
            msg = inner.get("message", msg)
            for k in ("logger", "level", "correlationId", "callingLocation", "thread"):
                d.setdefault(k, inner.get(k))
        except ValueError:
            msg = _salvage(d, msg)
    d["message"] = msg if isinstance(msg, str) else str(msg)
    return d


_SALVAGE = {k: re.compile(rf'"{k}":"((?:[^"\\\\]|\\\\.)*)') for k in
            ("level", "logger", "message", "server", "correlationId", "throwable")}


def _salvage(d, raw):
    """Pull fields out of a truncated nested JSON record."""
    d.pop("message", None)
    for k, rx in _SALVAGE.items():
        m = rx.search(raw)
        if m and not d.get(k):
            d[k] = m.group(1)
    msg = d.get("message") or "<unparsed record>"
    if d.get("throwable"):
        msg = f"{msg} | {d['throwable'].split(chr(92))[0][:200]}"
    return msg


def pod(server):
    """'Alpha.ROUTING.ALL.vp-routing-blue-deploy-d5dcffb77-fnmbl' -> 'ROUTING/fnmbl'."""
    if not server:
        return "?"
    parts = server.split(".")
    farm = parts[1] if len(parts) > 1 else parts[0]
    return f"{farm}/{server.rsplit('-', 1)[-1]}"


FILE_RE = re.compile(r"(vit-\d+)-(errorlog|commtracelog)")

# BGL (Blue Gene/L, loghub): "<label> <unix ts> <date> <node> <time> <node> RAS <component> <level> <message>"
BGL_LEVELS = {b"INFO": b"INFO", b"WARNING": b"WARN", b"SEVERE": b"ERROR", b"ERROR": b"ERROR",
              b"FATAL": b"CRITICAL", b"FAILURE": b"CRITICAL"}
_WORD = re.compile(rb"[A-Za-z_]{3,}")


def bgl_head(line):
    """(label, ts_ms, node, component, level, message) or None for a malformed line."""
    parts = line.split(None, 9)
    if len(parts) == 9:
        parts.append(b"")   # FATAL lines with an empty message exist
    if len(parts) < 10 or not parts[1].isdigit():
        return None
    label, ts, _, node, _, _, _, comp, level, msg = parts
    return label, int(ts) * 1000, node, comp, BGL_LEVELS.get(level, b"INFO"), msg


def bgl_key(comp, msg):
    """Cheap stream key for INFO lines: component + first three words (no full templating)."""
    return comp + b"." + b"_".join(_WORD.findall(msg[:80])[:3])


def line_format(path):
    return "bgl" if "bgl" in Path(path).name.lower() else "kafka-json"


def classify_file(path):
    """Return (env, kind) where kind is 'oplog' or 'commtrace'."""
    if line_format(path) == "bgl":
        return "bgl", "oplog"
    m = FILE_RE.search(str(path))
    if not m:
        raise ValueError(f"cannot infer env/kind from {path}")
    return m.group(1), ("commtrace" if m.group(2) == "commtracelog" else "oplog")
