# SeedWatch – plevel ali seme?

Pretočni pipeline, ki 25 GB logov Vynamic Payments (vit-2, vit-3) prebere v ~25 s pod 512 MB RAM,
loči šum od signala po **obnašanju** (ne po nivoju ali besedi „error“) in opozori na izpad z razlago
*kaj, kje, zakaj in kaj narediti*.

## Hiter začetek

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 1) surovi logi -> minute, deduplicirani dogodki, katalog predlog   (~25 s za 25 GB)
.venv/bin/python -m seedwatch.cli batch ../vp-logs-*.txt --out out

# 2) profil šuma, samodejne oznake incidentov, napoved, kartice, evalvacija, chaos   (~10 s)
.venv/bin/python -m seedwatch.cli analyze --out out [--cards]

# 3) demo: http://127.0.0.1:8765/   (živi replay iz surovih datotek + pregled 5 dni)
.venv/bin/python -m seedwatch.cli serve --out out --raw-dir ..

# testi
.venv/bin/python -m unittest discover -s tests -t .
```

**Javni nabor BGL** (Blue Gene/L, [loghub](https://github.com/logpai/loghub), prost prenos z Zenoda):

```bash
mkdir -p ../data/bgl && curl -L -o ../data/bgl/BGL.zip "https://zenodo.org/records/8196385/files/BGL.zip?download=1"
unzip ../data/bgl/BGL.zip -d ../data/bgl
.venv/bin/python -m seedwatch.cli batch ../data/bgl/BGL.log --out out --shard-mb 64 --stats-name batch_stats_bgl.json
.venv/bin/python -m seedwatch.cli analyze --out out     # oceni vsa okolja v out/, tudi bgl
```
V nadzorni plošči: *Pregled 5 dni* → izbira okolja **BGL** ali `/?tab=overview&env=bgl`.

Neposredne povezave za demo: `/?autoplay=i1&speed=40` (Crypt farma → 27 min tišine),
`/?autoplay=i4&speed=40` (curljanje → plaz), `/?autoplay=i3`, `/?autoplay=prm`, `/?tab=overview&env=vit-2`.

**Dokaz za 512 MB** (trda omejitev, brez swapa):

```bash
docker build -t seedwatch .
docker run --rm --user $(id -u):$(id -g) --memory=512m --memory-swap=512m \
  -v /pot/do/logov:/data:ro -v $PWD/out_docker:/out --entrypoint sh seedwatch \
  -c 'python -m seedwatch.cli batch /data/vp-logs-*.txt --out /out --workers 4'
```

Izmerjeno: 25,5 GB v 27 s (945 MB/s), 0 OOM, vrh procesnega pomnilnika (cgroup `anon`) **261 MB**.

AI povzetek alarma (opcijsko): nastavi `ANTHROPIC_API_KEY` in pred zagonom `serve` namesti paket `anthropic`.
Brez ključa se uporabi deterministična predloga, demo zato deluje tudi brez povezave.

## Kaj smo odkrili v podatkih

| | |
|---|---|
| Obseg | 4 datoteke, 25,5 GB, 12,18 M vrstic; 99,7 % INFO, **ni nivoja CRITICAL** |
| Najglasnejši ERROR | `TMCrypt.RKL_CHECK_KEY_CONTINUE_WITH`, 14.502×, tiktaka vsakih ~32 s (CV 0,05). To je plevel. |
| „Stop processing.“ (ERROR) | dnevni cron ob 00:08 UTC na vseh podih in ob vsakem zagonu poda |
| `fieldDefinitionOptimization` (WARN, 6.179×) | izpiše se samo ob zagonu poda, zato je **marker** restarta |
| Kaskade | 3 različni ERROR-ji z istim `correlationId` = en vzrok (Kafka import → PAN → service) |
| Skrite napake | 1.387 ERROR-jev s stack trace-om filebeat ni razčlenil, naivni filter po `level` bi jih izgubil |
| Najhujši izpad | vit-3, 14.10. 08:43–09:10: **27 min popolne tišine**, nato restart vseh podov; predznaki ob 08:37 (−22 % volumna, echo timeouti, „Crypt farm offline“) |
| Skupna odvisnost | 15.10. 07:02 gre PRM storitev v `FAILED` **hkrati v obeh okoljih** |

V vit-3 je **97 % vseh WARN/ERROR vrstic šum** (plevel ali marker).

## Kako deluje

```
surovi Kafka dump ─► bytes predfilter (INFO se nikoli ne razčleni v celoti) ─► agregat na minuto
   ├─ WARN+: predloga (maskiranje + entitete) ─► dedup (predloga × minuta × pod, s števcem) ─► events.jsonl
   └─ vse vrstice: volumen, utripi po loggerjih, latenca in Out/In po hostu ─► minutes.jsonl
profil šuma (ritem, cron, zagon poda, baza znanja) ─► napovednik (online, samo preteklost) ─► OPAZUJ / ALARM + kartica
```

* **Plevel je senzor.** Šuma ne zavržemo. Ritmični zapisi postanejo pričakovani „utripi“, zato je njihovo izginotje (tišina) najmočnejši signal izpada.
* **Razred določa obnašanje:** metronom, cron, zagon poda ali transakcijska napaka je plevel/marker; vse nepravilno ali novo je seme.
* **Napovednik** sešteje presenečenja (Poisson/binomski KL glede na sproti naučen baseline z disperzijo): nove ali pogostejše predloge, plaz, **trend glede na zamaknjeno referenco** (počasna rast), primanjkljaj volumna in utripov, latenco hostov, neodgovorjena sporočila.
* **Brez dvojnih alarmov:** kaskade se štejejo enkrat, ohlajanje velja na vzrok, OPAZUJ preide v ALARM.
* **Zaprtje minute po uri, ne po podatkih:** tišina je vidna takoj, ne šele ko se logi vrnejo.

## Rezultati (prag ALARM 14, OPAZUJ 7)

| | SeedWatch | naiven prag WARN+ERROR/min |
|---|---|---|
| Ujeti izpadi (7, samodejne oznake) | **7/7**, vsi z vsebinsko pravim vzrokom | 5/7 |
| Lažni alarmi | **1,25 / dan** (OPAZUJ: 5,3 / dan, samo nadzorna plošča) | 1,83 / dan, brez razlage |
| Hitrost / RAM | 1,0 GB/s, < 512 MB (Docker: 261 MB) | – |

Chaos injection: 120 sintetičnih 30-minutnih postopnih degradacij na resničnem ozadju, nato izpad.

| Scenarij | ALARM pred izpadom | OPAZUJ ≥ 10 min prej |
|---|---|---|
| nov WARN vzorec narašča | 24/24 (mediana 9 min) | 21/24 |
| redka napaka postaja pogostejša | 23/24 (6 min) | 15/24 |
| latenca 4 hostov do 6× | 21/24 (3,5 min) | 3/24 |
| neodgovorjena sporočila do 60 % | 24/24 (8 min) | 21/24 |
| utrip upade na 20 % | 24/24 (17,5 min) | 24/24 |

**Iskreno:** v resničnih logih večina incidentov nima opaznih predznakov ≥ 10 min (1–6 min).
Postopni vzorci, ki jih naloga omenja, pa se zaznajo zgodaj. Najšibkejša točka je počasna rast latence.

## Preverba na neodvisnem javnem naboru: BGL

Isti model in isti pragovi brez prilagajanja, na 7 mesecih logov superračunalnika Blue Gene/L
(4,7 M vrstic, 15 s, 610 incidentov iz **pravih oznak** skrbnikov; oznake model ne vidi, služijo le za oceno).

| | SeedWatch | naiven prag WARN/ERROR |
|---|---|---|
| Ujeti incidenti | **423 / 610 (69 %)** | 227 / 610 (37 %) |
| Lažni alarmi | 0,8 / dan | 0,4 / dan |
| „Čisti“ incidenti (≥ 6 h miru prej): ujeti | 175 / 248 (71 %) | – |
| „Čisti“ incidenti: OPAZUJ ≥ 10 min prej | 15 / 248 | – |

* **Nivo laže tudi tam:** 507.000 vrstic FATAL ni alarm; nekateri FATAL/ERROR se ponavljajo kot cron (npr. „Ido packet timeout“ vsake 4 h).
* **Napoved ≥ 10 min je redka (~6 %) tudi na BGL.** Večina okvar nastopi nenadoma, enako kot v plačilnem sistemu. Vrednost je v zgodnjem in razložljivem zaznavanju, ne v čarobni napovedi.
* Edino pravilo, dodano po ogledu BGL: servisni posegi (`PrepareForService`, `EndServiceAction`) so marker
  in med njimi se napake štejejo le s 30 %, enako kot deploy pri plačilnem sistemu. Zmanjša lažne alarme z 1,7 na 0,8 na dan.

## Datoteke

`seedwatch/fields.py` hitro branje polj · `template.py` predloge in entitete · `aggregate.py` agregat na minuto in dedup ·
`batch.py` vzporedni shardi · `profile.py` plevel/marker/seme · `labels.py` definicija „SYSTEM DOWN“ ·
`predict.py` napovednik · `explain.py` kartice · `evaluate.py`, `chaos.py` evalvacija · `stream.py`, `server.py`, `dashboard/` demo · `llm.py` AI povzetek
# hekaton

## Log investigation dashboard

One UTC time range drives the charts, alerts and log table. Open the range button
for absolute dates or quick ranges. Quick ranges end at the latest recorded time
(or replay cursor), since these exports are historical. Drag a chart to zoom and
filter logs. **Previous range** restores the last selection; **Zoom out** doubles
the visible span. **Earlier / Later** inside the time picker shift it by half its
duration. Previous range appears only after a range change. The
range, environment, minimum severity, search, service and presentation mode are
preserved in the URL. Alert selection opens the ten
minutes leading up to that alert.

**Minimum level** is a severity threshold, initially **INFO**. INFO
includes INFO, WARN, ERROR and CRITICAL/FATAL; WARN includes WARN and higher, and
ERROR includes ERROR and higher. Choices are colored and populated from severities
present in the environment's minute aggregates; these exports offer INFO/WARN/ERROR.
**All levels** also includes entries without a recognized severity. Service and
text filters narrow the evidence further. **Grouped** groups
the same logger, service, severity and normalized message across the entire
selected interval. Embedded timestamps, task IDs, durations, file/resource names
and host/entity parameters are variables; explicit status, failure and response
code outcomes remain distinct. This is presentation grouping, not a detector change.

**Grouping rules** beside the Logs heading manages saved custom message patterns.
Expand a row and choose **Create grouping rule** to start with its message pattern
and service/logger/severity scope. Replace changing text with `<*>`; all remaining
characters are literal, and the entire whitespace-normalized message must match.
For example: `Continue last incomplete run after EPP <*> with a timeout of <*> ms.`
Preview shows up to 500 distinct original message samples from the loaded query,
with matching/non-matching examples. It reads no additional source bytes and does
not represent a full-range occurrence count. Saving requires a preview; a zero-match
rule can still be saved for messages outside those samples.

Rules persist per environment on the server in `out/grouping-rules.json`. They can
be edited, removed and restored from **Removed rules**. The first matching saved
rule wins; the automatic grouping remains the fallback. Status/response-code
differences still produce separate groups, as do different services, loggers and
severities. Rows identify the custom rule applied. Saving/removing/restoring a rule
invalidates cached queries for that environment and regroups the selected range;
the original logs, learned detection profile, scores and alerts remain unchanged.
Pattern isolation is cleared on a rule change. Saved hidden-pattern IDs remain,
but a group whose template changes can acquire a new ID and need hiding again.
The rules API is `GET /api/pattern-rules?env=...` and JSON `POST /api/pattern-rules`
with `env`, the current `revision`, and `action` (`save`, `delete`, `restore`). Save
includes `rule` and optionally an existing `id`; delete/restore use `id`.
`POST /api/pattern-rules/preview` takes `env`, the current log-query `job`, and a
draft `rule`; preview requests can also include replay `asof` and pattern filters.

Expand a pattern to see original messages, first/last occurrence, instances,
affected hosts/entities and source file/byte locations. **Individual** shows individual
occurrences. **Hide pattern** suppresses that pattern across services; **Only this pattern**
is available in the expanded row. Hidden patterns persist in this browser per
environment. Restore individual patterns or use **Restore all**. **Reset filters** clears
search, service and pattern isolation and restores the default minimum severity;
it preserves deliberately hidden patterns. Sources and detector inputs are never
deleted or changed.

Log retrieval is progressive: even the entire recorded period produces initial
results while source scanning continues. Counts are labeled "so far" until complete.
Expanding a row pauses scanning and replay so evidence stays still; **Resume scan**
explicitly resumes. The progress label shows reading/processing and bytes scanned:
new queries parse and normalize messages as well as reading them. Each request
advances at most about 8 MB / 650 ms of
work (a source block boundary may add overhead). Cancelling or pausing stops further
requests. Three query states, a 20,000-pattern limit and a bounded message buffer
protect memory. Resource-limited partial results are explicit. Pages contain up to
100 rows; individual-record browsing retains the newest 5,100 records, so narrow the
range or filters to browse older occurrences.

Original INFO messages require the raw operational TXT exports in `--raw-dir`.
Without them, the viewer explicitly falls back to prepared WARN+ samples. A small
event-time block index under `out/explorer-index/` makes time lookup independent
of arrival order. It is reused until the source size/mtime changes. No message text
is stored in this index. Source previews cap messages at 24 KB and raw records at
128 KB. Endpoints are `/api/workspace`, `/api/logs`, `/api/source`; progressive
responses include a `job` token, `pending`, `progress`, and `complete`. A subsequent
`/api/logs?job=...` advances the scan; `advance=0` reads a snapshot without scanning.
`exclude` and `only` are snapshot filters on the existing query, so hiding,
restoring or isolating patterns never restarts its source scan. Include them on
each job request. Queries share the underlying scan regardless of these filters.
Intervals use `[start, end)` epoch seconds. `min_level` selects the inclusive
minimum severity. The older `level` parameter retains exact-level filtering for
benchmark scripts (`WARN+` remains supported). Unknown severities are included
only when no minimum is set. Viewer filtering does not change detector levels.

**Replay range** plays recorded minute scores and alerts. Inspection pauses replay;
resuming deliberately restores its original range and cursor. Future logs and alerts
are hidden until reached. There are no automatic AI calls. This remains a local
hackathon viewer, with the original ingestion, profiles, scoring and alert policy.
Replay extends its query with `job`, `extend_to` and `asof`, processing disjoint
new time intervals instead of rescanning the whole accumulated range. Sparse
source blocks may overlap between intervals, but previous records are not parsed,
normalized or counted again. Existing table rows update in place; playback holds
the viewport still and Hide pattern preserves the current reading position.
The source index contains time bounds, not a fully parsed message database, so
first-time queries over several days still require a substantial scan.

Run checks: `python -m unittest discover -s tests -v` (37 tests).
