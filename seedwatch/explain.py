"""Turn an alert's evidence into a card an on-call engineer can act on: what, where, why."""
import datetime as _dt
import re
from collections import Counter

COMPONENTS = [
    (re.compile(r"^EIS_BICISO"), "bančni host (BICISO) – avtorizacije ATM/POS te banke"),
    (re.compile(r"^EIS_MASTERCARD"), "Mastercard omrežje – kartične avtorizacije"),
    (re.compile(r"^EIS_VISA"), "Visa omrežje – kartične avtorizacije"),
    (re.compile(r"^EIS_"), "zunanji host / omrežje kartic"),
    (re.compile(r"^HSM"), "HSM – PIN in kriptografske operacije"),
    (re.compile(r"^PRM"), "PRM storitev (spremljanje tveganj transakcij)"),
    (re.compile(r"_EPP$"), "EPP tipkovnica bankomata (remote key loading)"),
    (re.compile(r"^PBF_"), "uvozna datoteka kartic/računov (Bankart)"),
]
FARMS = {
    "ROUTING": "usmerjanje transakcij", "CRYPT": "kriptografija / ključi", "TOKEN": "tokenizacija kartic",
    "DEVICEHANDLER": "upravljanje ATM/POS naprav",
}
ACTIONS = [
    (re.compile(r"to FAILED|offline|Acquiring channel .* failed", re.I), "Preveri dosegljivost {ent} (omrežje, firewall, stanje storitve); pripravi preklop na BACKUP."),
    (re.compile(r"HOST_TIMEOUT|HOST_COMMUNICATION_ERROR|errorWhileEcho", re.I), "Preveri povezavo do hosta {ent}: ali je v vzdrževanju, ali TCP seja visi; po potrebi ponovni sign-on."),
    (re.compile(r"KAFKA_IMPORT_FAILED|PAN_PARTITION", re.I), "Preveri zadnjo uvozno datoteko{ent_sfx} (format, enumeracije); popravi in ponovno uvozi, preden se zaostanek poveča."),
    (re.compile(r"HSM|TMCrypt", re.I), "Preveri HSM povezavo in obremenitev Crypt farme."),
    (re.compile(r"lease|FarmResource", re.I), "Farm resource leasing je zmeden: preveri pode in Kubernetes dogodke (restart, OOM)."),
]


def _fmt(t):
    return _dt.datetime.fromtimestamp(t, _dt.UTC).strftime("%Y-%m-%d %H:%M UTC")


def _ago(minutes):
    if minutes < 120:
        return f"{minutes} min"
    if minutes < 2 * 1440:
        return f"{minutes / 60:.0f} h"
    return f"{minutes / 1440:.0f} dni"


def describe_entity(ent):
    for rx, desc in COMPONENTS:
        if rx.search(ent):
            return desc
    return None


class Explainer:
    def __init__(self, env, profile, catalog, events):
        self.env = env
        self.profile = profile
        self.catalog = catalog
        self.by_minute = {}
        for ev in events:
            self.by_minute.setdefault(ev["t"], []).append(ev)

    def _recent(self, t, tids, minutes=15):
        evs = []
        for m in range(t - minutes * 60, t + 60, 60):
            evs.extend(e for e in self.by_minute.get(m, []) if e["tid"] in tids)
        return evs

    def card(self, alert, history=()):
        t = alert["t"]
        comps = alert["comps"]
        cause = alert.get("cause", "")
        dominant = "templates" if cause.startswith("tpl:") else (cause or max(comps, key=comps.get))
        if dominant not in comps:
            dominant = max(comps, key=comps.get)
        if dominant == "volume" and comps.get("heartbeat", 0) > comps.get("volume", 0):
            dominant = "heartbeat"
        tpl_meta = self.profile["tpl"]
        top = [tid for _, tid in alert["top_tpl"] if tid in tpl_meta]
        evs = self._recent(t, set(top))
        pods = Counter()
        ents = Counter()
        for e in evs:
            pods[e["pod"]] += e["count"]
            for x in e["ents"]:
                ents[x] += e["count"]
        why = []
        rare = {r["tid"]: r for r in alert.get("rare") or []}
        for tid, r in rare.items():
            m = tpl_meta.get(tid)
            if m:
                when = f"zadnjič pred {_ago(r['quiet_min'])}" if r["quiet_min"] else "prvič v zgodovini"
                why.append(f"{m['logger']}: „{m['template'][:90]}“ – nivo {m['level']}, "
                           f"redkejše kot 1× na 2 dni ({when})")
        for score, tid in alert["top_tpl"][:3]:
            if tid in rare:
                continue
            m = tpl_meta.get(tid)
            if not m:
                continue
            n15 = sum(e["count"] for e in evs if e["tid"] == tid)
            usual = m["rate_per_min"] * 15
            novelty = "nov vzorec" if m["n"] == n15 else f"običajno ~{usual:.1f}× v 15 min"
            why.append(f"{m['logger']}: „{m['template'][:90]}“ – {n15}× v zadnjih 15 min ({novelty})")
        vol = alert.get("volume")
        if comps.get("volume", 0) > 1 and vol and vol[1]:
            why.append(f"volumen logov {vol[0]}/min namesto ~{vol[1]:.0f}/min ({(vol[0] / vol[1] - 1) * 100:+.0f} %)")
        for s, lg in alert.get("missing_hb", [])[:3]:
            why.append(f"manjka utrip: {lg}")
        for s, name, now, usual in alert.get("slow", [])[:3]:
            why.append(f"odziv {name}: {now} ms namesto ~{usual} ms")
        if alert.get("unanswered") and comps.get("unanswered", 0) > 1:
            why.append(f"{alert['unanswered']:.0%} sporočil hostom brez odgovora")

        lead_ents = Counter()
        for e in evs:
            if top and e["tid"] == top[0]:
                for x in e["ents"]:
                    lead_ents[x] += e["count"]
        main_ent = (lead_ents or ents).most_common(1)[0][0] if (lead_ents or ents) else None
        if dominant in ("volume", "heartbeat"):
            title = "Sistem utiša – izginja običajni utrip logov"
        elif dominant == "latency":
            title = "Odzivi hostov se upočasnjujejo"
        elif dominant == "unanswered":
            title = "Hosti ne odgovarjajo na sporočila"
        elif top and top[0] in rare:
            title = f"Redek kritičen dogodek: {tpl_meta[top[0]]['logger']}"
        else:
            m = tpl_meta.get(top[0]) if top else None
            title = f"Nenavaden vzorec napak: {m['logger']}" if m else "Nenavaden vzorec napak"

        where = []
        if pods:
            where.append("podi: " + ", ".join(f"{p} ({FARMS.get(p.split('/')[0], '?')})" for p, _ in pods.most_common(3)))
        for ent, _ in ents.most_common(4):
            d = describe_entity(ent)
            where.append(f"{ent}" + (f" – {d}" if d else ""))
        for s, name, *_ in alert.get("slow", [])[:3]:
            d = describe_entity(name)
            where.append(f"{name}" + (f" – {d}" if d else ""))

        action = None
        for tid in top[:2]:
            text = tpl_meta[tid]["logger"] + " " + tpl_meta[tid]["template"]
            for rx, a in ACTIONS:
                if rx.search(text):
                    action = a.format(ent=main_ent or "prizadeto komponento",
                                      ent_sfx=f" {main_ent}" if main_ent else "")
                    break
            if action:
                break
        if action is None:
            action = {
                "volume": "Preveri pode (restart, OOM, CPU throttling) in log pipeline (filebeat → Kafka).",
                "heartbeat": "Preveri, katera storitev je nehala delati periodična opravila (scheduler, echo).",
                "latency": "Preveri obremenitev in omrežno pot do počasnih hostov.",
                "unanswered": "Preveri TCP seje in sign-on stanje do hostov brez odgovora.",
            }.get(dominant, "Preglej dogodke v časovnici okoli alarma.")

        similar = []
        for h in history:
            if h["t"] < t and set(x for _, x in h["top_tpl"][:2]) & set(top[:2]):
                similar.append(_fmt(h["t"]))
        return {
            "t": t, "time": _fmt(t), "env": self.env, "risk": alert["risk"],
            "confidence": round(1 - 2.718281828 ** (-alert["risk"] / 12), 2),
            "title": title, "what": title, "where": where[:6], "why": why[:6], "action": action,
            "dominant": dominant, "similar": similar[-3:],
            "components": comps,
        }


def card_text(c):
    lines = [f"⚠ {c['title']}   [{c['env']} · {c['time']} · tveganje {c['risk']:.0f} · zaupanje {c['confidence']:.0%}]"]
    if c["where"]:
        lines.append("KJE:    " + "; ".join(c["where"]))
    for i, w in enumerate(c["why"]):
        lines.append(("ZAKAJ:  " if i == 0 else "        ") + w)
    lines.append("UKREP:  " + c["action"])
    if c["similar"]:
        lines.append("PODOBNO: " + ", ".join(c["similar"]))
    return "\n".join(lines)
