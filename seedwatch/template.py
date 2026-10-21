"""Turn a raw message into a stable template + the entities it mentions.

Entities (host links, HSMs, EPPs, pods) are pulled out before masking, so
"Echo failed for EIS_MASTERCARD_DMS.BNET4" and "... for EIS_BICISO.BMKS" share one
template while the alert can still say *which* links are affected.
"""
import hashlib
import re

ENTITY_RES = [
    re.compile(r"\b(?:EIS_[A-Z0-9_]+|PRM[A-Z_]*|HSM)\.[A-Za-z0-9_]+(?:\.(?:PRIMARY|BACKUP))?"),
    re.compile(r"\bPRM(?:_[A-Z]+)?\.(?:PRIMARY|BACKUP)\b"),
    re.compile(r"\b[A-Z0-9]+_EPP\b"),
    re.compile(r"\bvp-[a-z]+-(?:blue|green)-deploy-[a-z0-9]+-[a-z0-9]+\b"),
    re.compile(r"\bzone [A-Z0-9_]+\b"),
    re.compile(r"(?<=file\.name:)[A-Za-z0-9_.\-]+"),
]

MASKS = [
    (re.compile(r"Message headers:.*", re.S), "Message headers: <headers>"),
    (re.compile(r"\{.*\}", re.S), "<json>"),
    (re.compile(r"\b(?:[0-9A-Fa-f]{2}:){5,}[0-9A-Fa-f]{2}\b"), "<mac>"),
    (re.compile(r"\bR\d\d-M\d(?:-[A-Z0-9]+)?(?:-[A-Z]:J\d\d-U\d\d)?\b"), "<loc>"),   # BGL machine location
    (re.compile(r"\b[0-9a-fA-F]{8}\b"), "<hex>"),
    (re.compile(r"\b[a-z]+\d{2,}\b"), "<host>"),   # bglio184, node names
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<uuid>"),
    (re.compile(r"/?\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"), "<ip>"),
    (re.compile(r"\b0x[0-9a-fA-F]+\b"), "<hex>"),
    (re.compile(r"\b[0-9A-Fa-f]{16,}\b"), "<hex>"),
    (re.compile(r"\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{5,}\b"), "<hex>"),   # register dumps
    (re.compile(r"(?<![\w<])/[\w.\-]+(?:/[\w.\-]*)+"), "<path>"),
    (re.compile(r"\b[A-Z0-9_]*_\d{8,}\b"), "<file>"),
    (re.compile(r"\bfield [A-Za-z0-9_\-]+ /"), "field <f> /"),
    (re.compile(r"'[^']*'"), "'<s>'"),
    (re.compile(r"\b\d+\b"), "<n>"),
]


def entities(msg):
    found = []
    for rx in ENTITY_RES:
        for m in rx.findall(msg):
            if m not in found:
                found.append(m)
    return found


def template(msg, max_len=240):
    t = msg
    for rx in ENTITY_RES:
        t = rx.sub("<E>", t)
    for rx, rep in MASKS:
        t = rx.sub(rep, t)
    t = " ".join(t.split())
    return t[:max_len]


def template_id(logger, tpl):
    return hashlib.blake2b(f"{logger}|{tpl}".encode(), digest_size=5).hexdigest()
