"""Demo server (stdlib only): dashboard, overview JSON and a Server-Sent-Events replay."""
import datetime as dt
import glob
import json
import mimetypes
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import orjson

from .llm import Summarizer
from .explorer import Explorer
from .stream import Replay

ROOT = Path(__file__).resolve().parent.parent / "dashboard"


def _ts(s):
    return int(dt.datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=dt.UTC).timestamp())


SCENARIOS = [
    {"id": "i1", "env": "vit-3", "start": _ts("2026-10-14 08:22"), "minutes": 55,
     "label": "14.10. vit-3 · Crypt farma offline → 27 min tišine"},
    {"id": "i3", "env": "vit-3", "start": _ts("2026-10-19 05:58"), "minutes": 35,
     "label": "19.10. vit-3 · bančni hosti ne odgovarjajo na echo"},
    {"id": "i4", "env": "vit-2", "start": _ts("2026-10-16 06:15"), "minutes": 35,
     "label": "16.10. vit-2 · curljanje → plaz napak uvoza kartic"},
    {"id": "prm", "env": "vit-2", "start": _ts("2026-10-15 06:48"), "minutes": 25,
     "label": "15.10. vit-2 · PRM storitev FAILED (hkrati v obeh okoljih)"},
]


def overview(out, env):
    d = Path(out) / env
    load = lambda name: orjson.loads((d / name).read_bytes())
    prof = load("profile.json")
    risk = []
    with open(d / "risk.jsonl", "rb") as f:
        for line in f:
            r = orjson.loads(line)
            risk.append([r["t"], r["risk"], r["n"]])
    classes = {}
    for p in prof["tpl"].values():
        c = classes.setdefault(p["class"], {"lines": 0, "templates": 0})
        c["lines"] += p["n"]
        c["templates"] += 1
    examples = sorted(prof["tpl"].values(), key=lambda p: -p["n"])
    pick = []
    for cls, k in (("weed", 4), ("marker", 2), ("seed", 4)):
        seen = set()
        for p in examples:
            if p["class"] == cls and p["logger"] not in seen and len(seen) < k:
                seen.add(p["logger"])
                pick.append({"class": cls, "logger": p["logger"], "level": p["level"], "n": p["n"],
                             "template": p["template"][:120], "reason": p["reason"]})
    summary = orjson.loads((Path(out) / "summary.json").read_bytes())
    own = Path(out) / f"batch_stats_{env}.json"
    batch = orjson.loads((own if own.exists() else Path(out) / "batch_stats.json").read_bytes())
    if len(risk) > 20000:   # months of minutes: keep the peak of each bucket so spikes survive
        k = -(-len(risk) // 15000)
        risk = [max(risk[i:i + k], key=lambda r: r[1]) for i in range(0, len(risk), k)]
    envs = sorted(p.parent.name for p in Path(out).glob("*/risk.jsonl"))
    return {"env": env, "envs": envs, "risk": risk, "incidents": load("incidents.json"),
            "alerts": load("alerts.json"), "eval": load("eval.json"), "classes": classes, "examples": pick,
            "summary": summary, "env_summary": summary["per_env"].get(env, {}), "batch": batch,
            "scenarios": SCENARIOS}


def make_handler(out, raw_files):
    summarizer = Summarizer(Path(out) / "llm_cache.json")
    explorer = Explorer(out, raw_files, indexed=True)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if u.path in ("/api/workspace", "/api/logs", "/api/source", "/api/pattern-rules"):
                try:
                    if u.path == "/api/workspace":
                        result = explorer.workspace(q.get("env", "vit-3"))
                    elif u.path == "/api/logs":
                        result = explorer.query(q)
                    elif u.path == "/api/pattern-rules":
                        result = explorer.list_rules(q.get("env", "vit-3"))
                    else:
                        result = explorer.source(q)
                    return self._send(200, orjson.dumps(result), "application/json")
                except (ValueError, KeyError) as e:
                    return self._send(400, orjson.dumps({"error": str(e)}), "application/json")
            if u.path == "/api/overview":
                return self._send(200, orjson.dumps(overview(out, q.get("env", "vit-3"))), "application/json")
            if u.path == "/api/replay":
                return self._replay(q)
            rel = "index.html" if u.path in ("/", "") else u.path.lstrip("/")
            f = (ROOT / rel).resolve()
            if ROOT not in f.parents and f != ROOT / "index.html" or not f.is_file():
                return self._send(404, b"not found", "text/plain")
            ctype = mimetypes.guess_type(str(f))[0] or "application/octet-stream"
            return self._send(200, f.read_bytes(), ctype)

        def do_POST(self):
            path = urlparse(self.path).path
            if path in ("/api/pattern-rules", "/api/pattern-rules/preview"):
                try:
                    n = int(self.headers.get("Content-Length", 0))
                    if not 0 < n <= 64000 or self.headers.get_content_type() != "application/json":
                        self.close_connection = True
                        raise ValueError("Send a JSON grouping rule up to 64 KB")
                    origin = self.headers.get("Origin")
                    if origin and origin != "http://"+self.headers.get("Host", ""):
                        self.close_connection = True
                        raise ValueError("Open the dashboard on this server to change grouping rules")
                    request = orjson.loads(self.rfile.read(n))
                    if not isinstance(request, dict):
                        raise ValueError("Provide a JSON object")
                    result = explorer.preview_rule(request) if path.endswith("/preview") else explorer.update_rules(request)
                    return self._send(200, orjson.dumps(result), "application/json")
                except (ValueError, TypeError, KeyError) as e:
                    return self._send(400, orjson.dumps({"error": str(e)}), "application/json")
            if urlparse(self.path).path != "/api/explain":
                return self._send(404, b"not found", "text/plain")
            n = int(self.headers.get("Content-Length", 0))
            card = orjson.loads(self.rfile.read(n))
            return self._send(200, orjson.dumps(summarizer.summarize(card)), "application/json")

        def _replay(self, q):
            env = q.get("env", "vit-3")
            start = int(q.get("start", SCENARIOS[0]["start"]))
            speed = float(q.get("speed") or 30)
            minutes = int(q.get("minutes", 60))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            closed = threading.Event()

            def send(msg):
                try:
                    self.wfile.write(b"data: " + orjson.dumps(msg) + b"\n\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    closed.set()

            try:
                Replay(out, env, raw_files, start, speed=speed).run(send, minutes=minutes, stop=closed.is_set)
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True

    Handler.explorer = explorer
    return Handler


def serve(out, raw_dir, host="127.0.0.1", port=8765):
    raw_files = sorted(glob.glob(str(Path(raw_dir) / "vp-logs-*.txt")))
    try:
        httpd = ThreadingHTTPServer((host, port), make_handler(out, raw_files))
    except OSError as e:
        raise SystemExit(f"Port {port} is not available ({e.strerror}). Another seedwatch server is "
                         f"probably running: stop it (pkill -f 'seedwatch.cli serve') or use --port {port + 1}.")
    print(json.dumps({"url": f"http://{host}:{port}/", "raw_files": [Path(p).name for p in raw_files]}))
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        httpd.RequestHandlerClass.explorer.close()
