"""Optional two-sentence summary of an alert card by Claude, with an offline fallback.

The card's structured evidence (what / where / why / action) is the source of truth;
the model only rewrites it for a human.  Results are cached on disk so a demo replay
never waits twice for the same alert, and without credentials or network we fall back
to a deterministic template.
"""
import hashlib
import json
import threading
from pathlib import Path

MODEL = "claude-opus-5-5"
SYSTEM = (
    "Si dežurni SRE inženir za plačilni sistem (Diebold Nixdorf Vynamic Payments: ATM/POS "
    "avtorizacije, bančni hosti, HSM). Iz strukturiranih dokazov alarma napiši natanko dva "
    "kratka stavka v slovenščini: kaj se dogaja in kje, ter kaj naj dežurni najprej preveri. "
    "Uporabi samo dejstva iz dokazov, brez ugibanja vzrokov, ki jih dokazi ne podpirajo."
)
_lock = threading.Lock()


def fallback_summary(card):
    why = card["why"][0] if card.get("why") else ""
    where = card["where"][0] if card.get("where") else card.get("env", "")
    return f"{card['title']} ({where}). {why}. Najprej: {card['action']}".replace("..", ".")


class Summarizer:
    def __init__(self, cache_path):
        self.cache_path = Path(cache_path)
        self.cache = json.loads(self.cache_path.read_text()) if self.cache_path.exists() else {}
        self._client = None
        self._disabled = False

    def _key(self, card):
        evidence = {k: card.get(k) for k in ("env", "title", "where", "why", "action")}
        return hashlib.sha256(json.dumps(evidence, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]

    def _call(self, card):
        import anthropic

        if self._client is None:
            self._client = anthropic.Anthropic(timeout=30.0, max_retries=1)
        evidence = json.dumps({k: card.get(k) for k in ("env", "time", "title", "where", "why", "action")},
                              ensure_ascii=False, indent=1)
        try:
            response = self._client.beta.messages.create(
                model=MODEL,
                max_tokens=4000,
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
                output_config={"effort": "low"},
                system=SYSTEM,
                messages=[{"role": "user", "content": f"Dokazi alarma:\n{evidence}"}],
            )
        except anthropic.AuthenticationError:
            self._disabled = True   # no usable credentials: stay offline for this run
            return None
        except (anthropic.RateLimitError, anthropic.APIStatusError, anthropic.APIConnectionError):
            return None
        if response.stop_reason == "refusal":
            return None
        text = "".join(b.text for b in response.content if b.type == "text").strip()
        return text or None

    def summarize(self, card):
        key = self._key(card)
        if key in self.cache:
            return {"summary": self.cache[key], "source": "claude"}
        text = None
        if not self._disabled:
            try:
                text = self._call(card)
            except ImportError:
                self._disabled = True
            except Exception:   # e.g. no credentials configured at all
                self._disabled = True
        if text:
            with _lock:
                self.cache[key] = text
                self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=1))
            return {"summary": text, "source": "claude"}
        return {"summary": fallback_summary(card), "source": "predloga"}
