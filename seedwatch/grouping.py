"""Persisted viewer grouping rules. Literal text plus <*>; no user regex/code."""
import os
from pathlib import Path
import secrets

import orjson

MAX_RULES = 50
MAX_TEMPLATE = 2000


def clean_rule(value):
    if not isinstance(value, dict):
        raise ValueError("Provide a grouping rule")
    limits = {"name": 80, "template": MAX_TEMPLATE, "logger": 300,
              "service": 100, "level": 16}
    result = {}
    for key, limit in limits.items():
        text = value.get(key, "")
        if not isinstance(text, str) or len(text) > limit:
            raise ValueError(f"{key.capitalize()} must be text up to {limit} characters")
        result[key] = " ".join(text.split())
    if not result["name"] or not result["template"]:
        raise ValueError("Give the rule a name and message pattern")
    if not result["template"].replace("<*>", "").strip():
        raise ValueError("Keep some fixed message text around the changing values")
    if result["template"].count("<*>") > 20:
        raise ValueError("Use at most 20 changing values")
    result["level"] = result["level"].upper()
    if result["level"] not in ("", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "FATAL"):
        raise ValueError("Choose a recognized severity or all levels")
    return result


def matches(rule, message, logger, service, level):
    if any(rule.get(k) and rule[k] != v for k, v in (
            ("logger", logger), ("service", service), ("level", level))):
        return False
    text = " ".join(message.split())
    parts = rule["template"].split("<*>")
    if len(parts) == 1:
        return text == parts[0]
    if not text.startswith(parts[0]) or not text.endswith(parts[-1]):
        return False
    pos, end = len(parts[0]), len(text)-len(parts[-1])
    # Ordered literal searches avoid regex backtracking on large/unusual messages.
    for literal in parts[1:-1]:
        found = text.find(literal, pos, end)
        if found < 0:
            return False
        pos = found + len(literal)
    return pos <= end


class GroupingRules:
    def __init__(self, out):
        self.path = Path(out) / "grouping-rules.json"

    def _state(self):
        if not self.path.exists():
            return {"version": 1, "environments": {}}
        state = orjson.loads(self.path.read_bytes())
        if state.get("version") != 1:
            raise ValueError("Unsupported grouping-rules file version")
        return state

    def list(self, env):
        entry = self._state()["environments"].get(env, {"revision": 0, "rules": [], "removed": []})
        return {"env": env, **entry}

    def update(self, env, request):
        state = self._state()
        entry = state["environments"].setdefault(env, {"revision": 0, "rules": [], "removed": []})
        entry.setdefault("removed", [])
        if request.get("revision") != entry["revision"]:
            raise ValueError("Grouping rules changed. Reopen the rules panel and try again.")
        rule_id = request.get("id")
        index = next((i for i, r in enumerate(entry["rules"]) if r["id"] == rule_id), None)
        action = request.get("action")
        if action == "delete":
            if index is None:
                raise ValueError("Grouping rule was not found")
            entry["removed"].append({"rule": entry["rules"].pop(index), "position": index})
        elif action == "restore":
            old = next((r for r in entry["removed"] if r["rule"]["id"] == rule_id), None)
            if old is None:
                raise ValueError("Removed grouping rule was not found")
            if len(entry["rules"]) >= MAX_RULES:
                raise ValueError(f"At most {MAX_RULES} grouping rules per environment")
            entry["rules"].insert(min(old["position"], len(entry["rules"])), old["rule"])
            entry["removed"].remove(old)
        elif action == "save":
            rule = clean_rule(request.get("rule"))
            if rule_id and index is None:
                raise ValueError("Grouping rule was not found")
            rule["id"] = rule_id or secrets.token_hex(8)
            if index is not None:
                entry["rules"][index] = rule
            else:
                if len(entry["rules"]) >= MAX_RULES:
                    raise ValueError(f"At most {MAX_RULES} grouping rules per environment")
                entry["rules"].append(rule)
        else:
            raise ValueError("Choose save, delete or restore")
        entry["revision"] += 1
        self.path.parent.mkdir(parents=True, exist_ok=True)
        target = self.path.with_suffix(".tmp")
        target.write_bytes(orjson.dumps(state, option=orjson.OPT_INDENT_2))
        os.replace(target, self.path)
        return {"env": env, **entry}
