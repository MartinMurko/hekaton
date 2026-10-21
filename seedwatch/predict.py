"""Online risk scoring, one minute at a time, using only the past.

Each signal is scored as *surprise* against a baseline the model learns on the fly
(EWMA mean + over-dispersion), so nothing about the incidents is hard-coded:

  templates  – a template fires more than its own rhythm predicts (5 and 30 min windows);
               brand-new templates start from a near-zero prior
  volume     – total log volume drops below what this minute should have
  heartbeat  – steady INFO "noise" (echoes, scheduler ticks) goes missing  <- noise as a sensor
  latency    – host response time rises above its usual band
  unanswered – outgoing host messages stop getting answers

Surprise = Poisson KL divergence c*ln(c/mu) - c + mu, divided by the learned dispersion.
"""
import math
import re
from collections import deque

from .profile import MAINTENANCE_REASON

ALPHA = 1 / 120        # ~2 h memory for volume / heartbeat baselines
ALPHA_TPL = 1 / 720    # ~12 h memory for template rates (a trickle must not become normal fast)
NEW_TPL_PRESENCE = 1 / 2880   # prior: an unseen template is active once in two days
WARMUP_MIN = 90
THRESHOLD = 14.0      # ALARM: pages on-call
WATCH_RATIO = 0.5     # WATCH: early heads-up on the dashboard only, at half the alarm threshold
LAT_STREAK = 6        # minutes a single link must stay slow (natural spikes are shorter)
TREND_MIN_EVENTS = 4  # events in the last 10 min before a rise counts as a trend
TREND_DIV = 2.0
RARE_P = 1 / 2880     # a CRITICAL seed rarer than once in two days alerts on its first line
SEVERE_RX = re.compile(r"FAILED|offline|time ?out|refused|unavailable|\bdown\b|\blost\b", re.I)


def binom_kl(k, n, p):
    """n * KL(k/n || p), only for excess (k/n > p)."""
    if n <= 0:
        return 0.0
    q = k / n
    p = min(max(p, 1e-6), 1 - 1e-6)
    if q <= p:
        return 0.0
    a = q * math.log(q / p) if q > 0 else 0.0
    b = (1 - q) * math.log((1 - q) / (1 - p)) if q < 1 else 0.0
    return n * (a + b)


def kl(c, mu):
    mu = max(mu, 1e-6)
    if c <= 0:
        return mu
    return c * math.log(c / mu) - c + mu


SHIFT_MIN = 45  # anomalous this long -> accept the new level as normal (alert already fired)


class Rate:
    """EWMA rate with over-dispersion estimate; frozen while the series is anomalous."""

    __slots__ = ("mu", "phi", "n", "streak", "recent")

    def __init__(self, mu=None):
        self.mu = mu
        self.phi = 1.0
        self.n = 0
        self.streak = 0
        self.recent = deque(maxlen=15)

    def surprise(self, c, scale=1):
        if self.mu is None:
            return 0.0, 0
        mu = self.mu * scale
        s = kl(c, mu) / max(self.phi, 1.0)
        return s, (1 if c > mu else -1)

    def update(self, c, alpha, anomalous=False):
        self.n += 1
        self.recent.append(c)
        if self.mu is None:
            self.mu = float(c)
            return
        self.streak = self.streak + 1 if anomalous else 0
        if self.streak >= SHIFT_MIN:
            self.mu = sum(self.recent) / len(self.recent)
            self.streak = 0
            return
        a = alpha * (0.1 if anomalous else 1.0)
        if self.n < WARMUP_MIN:
            a = max(a, 1 / self.n)
        r = (c - self.mu) ** 2 / max(self.mu, 0.5)
        self.phi = (1 - a) * self.phi + a * min(r, 50)
        self.mu = (1 - a) * self.mu + a * c


class Gauss:
    """EWMA mean/variance of a real-valued signal (log latency, unanswered ratio)."""

    __slots__ = ("m", "v", "n")

    def __init__(self):
        self.m = None
        self.v = 0.0
        self.n = 0

    def z(self, x, floor):
        if self.m is None or self.n < 30:
            return 0.0
        return (x - self.m) / math.sqrt(self.v + floor * floor)

    def update(self, x, alpha, anomalous=False):
        self.n += 1
        if self.m is None:
            self.m = x
            return
        a = alpha * (0.1 if anomalous else 1.0)
        if self.n < WARMUP_MIN:
            a = max(a, 1 / self.n)
        d = x - self.m
        self.m += a * d
        self.v = (1 - a) * (self.v + a * d * d)


WEIGHTS = {"templates": 1.0, "volume": 1.0, "heartbeat": 1.0, "latency": 1.0, "unanswered": 1.0}


class Predictor:
    def __init__(self, profile, threshold=THRESHOLD, consecutive=2, cooldown_min=30, weights=None, rare=True):
        self.cls = {tid: p["class"] for tid, p in profile["tpl"].items()}
        self.weights = weights or WEIGHTS
        # level alone lies (most FATAL lines are noise); level x rarity does not
        self.rare_on = rare
        self.severe_level = {tid for tid, p in profile["tpl"].items()
                             if p["class"] == "seed" and p["level"] == "CRITICAL"}
        self._rare = []
        self.last_fire = {}   # tid -> minute index of its last occurrence
        self.group = profile["groups"]
        # semantic prior: an already-anomalous seed that talks about failure weighs more
        self.severe = {tid: 1.5 for tid, p in profile["tpl"].items() if SEVERE_RX.search(p["template"])}
        self.meta = profile["tpl"]
        self.heartbeats = set(profile["heartbeats"])
        self.threshold = threshold
        self.consecutive = consecutive
        self.cooldown = cooldown_min * 60
        self.tpl_hist = {}           # tid -> [presence(30), rate, usual count, age, streak, counts(60), last minute]
        self.active = {}             # tid -> last minute it fired; only these are scored each minute
        self.vol = Rate()
        self.hb = {lg: Rate() for lg in self.heartbeats}
        self.hb_hist = {lg: deque(maxlen=3) for lg in self.heartbeats}
        self.lat = {}
        self.lat_hist = {}
        self.lat_streak = {}
        self.unans = Gauss()
        self.ua_hist = deque(maxlen=3)
        self.pods_seen = set()
        self.track_restarts = profile.get("restarts", True)
        self.maintenance = {tid for tid, p in profile["tpl"].items() if p["reason"] == MAINTENANCE_REASON}
        self.maintenance_until = -1
        self.restart_until = -1
        self.minute = 0
        self.above = 0
        self.above_watch = 0
        self.last_alert = {}   # cause -> last minute the cause was above threshold
        self.last_watch = {}

    # ---- components ------------------------------------------------------
    def _templates(self, row, t):
        """Seed templates showing up more often (active minutes) or louder than usual.

        Presence is scored per *active minute* (binomial KL), so 4 identical lines are one
        event; loudness is scored separately as a jump over the usual count per active
        minute.  Templates that always co-occur (same correlationId cascade) form one
        group and count once.
        """
        restarting = t <= self.restart_until
        if any(tid in self.maintenance for tid in row["tpl"]):
            self.maintenance_until = t + 30 * 60
        in_maintenance = t <= self.maintenance_until
        group_score = {}
        self._rare = []
        m = self.minute
        for tid in row["tpl"]:
            self.active[tid] = m
        for tid in [x for x, last in self.active.items() if m - last > 60]:
            del self.active[tid]   # an hour of zeros: its windows are empty, park it until it returns
        for tid in list(self.active):
            cls = self.cls.get(tid, "seed")
            if cls == "marker":
                continue
            c = row["tpl"].get(tid, 0)
            st = self.tpl_hist.get(tid)
            if st is None:
                st = self.tpl_hist[tid] = [deque(maxlen=30), NEW_TPL_PRESENCE, None, 0, 0,
                                           deque([0] * 59, maxlen=60), m - 1]   # unseen so far = zero history
            gap = m - st[6] - 1
            if gap > 0:   # catch up on the parked minutes in one step
                st[1] *= (1 - ALPHA_TPL) ** gap
                st[3] += gap
                st[4] = 0
            st[6] = m
            hist, p, usual, age, streak, counts, _ = st
            if (c and self.rare_on and tid in self.severe_level and p <= RARE_P and not in_maintenance
                    and not (restarting and age < 15)):   # never-seen counts too; start-up chatter does not
                last = self.last_fire.get(tid)
                self._rare.append({"tid": tid, "quiet_min": None if last is None else m - last})
            if c:
                self.last_fire[tid] = m
            hist.append(1 if c else 0)
            counts.append(c)
            k30 = sum(hist)
            k5 = sum(list(hist)[-5:])
            pres = max(binom_kl(k5, min(5, len(hist)), p), binom_kl(k30, len(hist), p))
            loud = 0.0
            if c and usual:
                loud = min(max(0.0, math.log2(c / usual) - 2) * 5, 30)
            if cls == "seed":
                sc = (min(pres, 12) + loud + self._trend(counts, usual)) * self.severe.get(tid, 1.0)
            else:   # known noise only counts when it badly breaks its own rhythm
                sc = min(0.2 * (pres + loud), 4)
            if restarting and age < 15:
                sc *= 0.2   # templates born during a pod start are start-up chatter
            if in_maintenance:
                sc *= 0.3   # errors while hardware is being serviced are expected
            anomalous = sc > 4
            a = max(ALPHA_TPL * (0.1 if anomalous else 1.0), 1 / (age + 20))
            st[4] = streak + 1 if anomalous else 0
            if st[4] >= SHIFT_MIN:   # persistently "anomalous" -> this is the new normal
                a, st[4] = 1.0, 0
                st[1] = sum(hist) / len(hist)
            else:
                st[1] = (1 - a) * p + a * (1 if c else 0)
            if c:
                st[2] = c if usual is None else (1 - 0.05) * usual + 0.05 * min(c, 8 * usual)
            st[3] = age + 1
            if sc > 0.5:
                g = self.group.get(tid, tid)
                if sc > group_score.get(g, (0, None))[0]:
                    group_score[g] = (sc, tid)
        top = sorted(group_score.values(), reverse=True)
        total = sum(x for x, _ in top)
        if self._rare:   # rare + severe: alert on the first line, do not wait for a second minute
            bonus = 2 * self.threshold
            total += bonus
            top = sorted([(bonus, r["tid"]) for r in self._rare] + top, reverse=True)
        return total, top[:5]

    @staticmethod
    def _trend(counts, usual):
        """Slow burn: the last 10 min vs a *lagged* reference (60..20 min ago), rising only.

        The reference window cannot be contaminated by a ramp that started < 20 min
        ago, unlike an adaptive baseline, which quietly learns a gradual rise as normal.
        """
        if len(counts) < 40:
            return 0.0
        xs = list(counts)
        burst = max(usual or 1.0, 1.0)            # lines per event: count events, not lines
        recent = sum(xs[-10:]) / burst
        middle = sum(xs[-20:-10]) / burst
        ref = sum(xs[:-20]) / burst / (len(xs) - 20) * 10
        if not (recent > middle >= 0 and recent >= TREND_MIN_EVENTS):
            return 0.0
        return min(kl(recent, max(ref, 0.3)) / TREND_DIV, 15)

    def _volume(self, row):
        c = row["n"]
        raw, d = self.vol.surprise(c)
        s = raw if d < 0 else 0.0
        self.vol.update(c, ALPHA, anomalous=raw > 8)  # bursts must not inflate the baseline
        return min(s, 60), (c, self.vol.mu)

    def _heartbeat(self, row):
        total, missing = 0.0, []
        for lg in self.heartbeats:
            c = row["lg"].get(lg, 0)
            h = self.hb_hist[lg]
            h.append(c)
            r = self.hb[lg]
            raw, d = r.surprise(sum(h), len(h))
            cap = min(15.0, 3 + 2 * math.log2(1 + (r.mu or 0)))   # a loud stream going quiet matters more
            s = min(raw, cap) if d < 0 else 0.0
            r.update(c, ALPHA, anomalous=raw > 3)
            if s > 1:
                missing.append((round(s, 1), lg))
            total += s
        missing.sort(reverse=True)
        return total, missing[:5]

    def _latency(self, ct):
        """Host response time: fleet-wide slowdown or one link persistently slow.

        Single slow echoes are common (3-4 samples per link per minute), so a link only
        counts after 3 slow minutes in a row; the fleet signal uses the median z over
        all links, which a single outlier cannot move.
        """
        if ct is None:
            return 0.0, []
        zs, slow = [], []
        for name, v in ct["ct"].items():
            h = self.lat_hist.get(name)
            if h is None:
                h = self.lat_hist[name] = deque(maxlen=3)
                self.lat[name] = Gauss()
                self.lat_streak[name] = 0
            h.append((v[2], v[3]))
            resp = sum(r for r, _ in h)
            if resp < 5:
                continue
            x = math.log(1 + sum(d for _, d in h) / resp)
            g = self.lat[name]
            z = g.z(x, 0.2)
            g.update(x, ALPHA, anomalous=z > 3)
            zs.append(z)
            self.lat_streak[name] = self.lat_streak[name] + 1 if z > 3 else 0
            if self.lat_streak[name] >= LAT_STREAK:
                slow.append((round(min((z - 3) * 2, 10), 1), name, round(sum(d for _, d in h) / resp),
                             round(math.exp(g.m) - 1)))
        fleet = 0.0
        if len(zs) >= 5:
            zs.sort()
            fleet = min(max(0.0, zs[len(zs) // 2] - 1.5) * 4, 30)
        slow.sort(reverse=True)
        return fleet + sum(s for s, *_ in slow[:4]), slow[:5]

    def _unanswered(self, ct):
        if ct is None:
            return 0.0, None
        out = sum(v[0] for k, v in ct["ct"].items() if k.startswith("EIS_"))
        inn = sum(v[1] for k, v in ct["ct"].items() if k.startswith("EIS_"))
        self.ua_hist.append((out, inn))
        o = sum(x for x, _ in self.ua_hist)
        i = sum(y for _, y in self.ua_hist)
        if o < 30:
            return 0.0, None
        r = max(0.0, (o - i) / o)
        z = self.unans.z(r, 0.03)
        s = min(max(0.0, z - 3) * 2, 30)
        self.unans.update(r, ALPHA, anomalous=s > 2)
        return s, round(r, 3)

    def _causes(self, comps, top_tpl, threshold=None):
        """Alerts are de-duplicated per cause: each strong template group or signal family.

        A second, unrelated problem during an open alert still gets its own alert.
        """
        th = threshold or self.threshold
        out = []
        for s, tid in top_tpl:
            if s >= 0.5 * th or (s == top_tpl[0][0] and max(comps, key=comps.get) == "templates"):
                out.append((s, "tpl:" + self.group.get(tid, tid), tid))
        silence = comps["volume"] + comps["heartbeat"]
        if silence >= 0.5 * th:
            out.append((silence, "volume", None))
        for k in ("latency", "unanswered"):
            if comps[k] >= 0.5 * th:
                out.append((comps[k], k, None))
        out.sort(key=lambda x: -x[0])   # the strongest signal names the alert
        return [(c, tid) for _, c, tid in out] or [(max(comps, key=comps.get), None)]

    # ---- main step -------------------------------------------------------
    def step(self, t, op_row, ct_row):
        self.minute += 1
        new_pods = [p for p in op_row["pod"] if p not in self.pods_seen and p != "?"]
        if self.track_restarts and self.pods_seen and new_pods:
            self.restart_until = t + 10 * 60
        self.pods_seen.update(op_row["pod"])

        comps = {}
        comps["templates"], top_tpl = self._templates(op_row, t)
        comps["volume"], vol = self._volume(op_row)
        comps["heartbeat"], missing = self._heartbeat(op_row)
        comps["latency"], slow = self._latency(ct_row)
        comps["unanswered"], ua = self._unanswered(ct_row)
        if self.minute < WARMUP_MIN:
            comps = {k: 0.0 for k in comps}
        risk = sum(self.weights[k] * v for k, v in comps.items())
        rare = self._rare if self.minute >= WARMUP_MIN else []

        state = {"t": t, "risk": round(risk, 2), "comps": {k: round(v, 2) for k, v in comps.items()},
                 "top_tpl": [(round(s, 1), tid) for s, tid in top_tpl], "volume": vol,
                 "missing_hb": missing, "slow": slow, "unanswered": ua,
                 "new_pods": new_pods if len(self.pods_seen) > len(new_pods) else [], "rare": rare}
        self.above = self.above + 1 if risk >= self.threshold else 0
        alert = None
        if self.above >= self.consecutive or risk >= 2 * self.threshold:
            for cause, lead_tid in self._causes(comps, top_tpl):
                fresh = t - self.last_alert.get(cause, -10 ** 12) > self.cooldown
                self.last_alert[cause] = t
                if fresh and alert is None:
                    ordered = sorted(state["top_tpl"], key=lambda x: x[1] != lead_tid)
                    alert = {"t": t, "risk": state["risk"], "comps": state["comps"], "cause": cause,
                             "top_tpl": ordered, "missing_hb": missing, "slow": slow,
                             "unanswered": ua, "volume": vol, "rare": rare}
        state["alert"] = alert
        watch = None
        wth = WATCH_RATIO * self.threshold
        self.above_watch = self.above_watch + 1 if risk >= wth else 0
        if self.above_watch >= self.consecutive or risk >= 2 * wth:
            for cause, lead_tid in self._causes(comps, top_tpl, wth):
                fresh = t - self.last_watch.get(cause, -10 ** 12) > self.cooldown
                self.last_watch[cause] = t
                if fresh and watch is None:
                    ordered = sorted(state["top_tpl"], key=lambda x: x[1] != lead_tid)
                    watch = {"t": t, "risk": state["risk"], "comps": state["comps"], "cause": cause,
                             "top_tpl": ordered, "missing_hb": missing, "slow": slow,
                             "unanswered": ua, "volume": vol, "rare": rare}
        state["watch"] = watch
        state["level"] = "alarm" if risk >= self.threshold else ("watch" if risk >= wth else "ok")
        return state


def run(series, profile, **kw):
    pred = Predictor(profile, **kw)
    states, alerts = [], []
    for i, t in enumerate(series.t):
        st = pred.step(t, series.op[i], series.ct[i])
        states.append(st)
        if st["alert"]:
            alerts.append(st["alert"])
    return states, alerts


def watches(states):
    return [st["watch"] for st in states if st.get("watch")]
