# SeedWatch – plevel ali seme?

Pretočni pipeline, ki 25 GB logov Vynamic Payments (vit-2, vit-3) prebere v ~25 s pod 512 MB RAM,
loči šum od signala po **obnašanju** (ne po nivoju ali besedi „error“) in opozori na izpad z razlago
*kaj, kje, zakaj in kaj narediti*. Preverjeno tudi na javnem naboru superračunalnika BGL.

Scenarij 5-minutne predstavitve in odgovori na vprašanja žirije: [PITCH.md](PITCH.md).

## Hiter začetek

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 1) surovi logi -> minute, deduplicirani dogodki, katalog predlog   (~25 s za 25 GB)
.venv/bin/python -m seedwatch.cli batch ../vp-logs-*.txt --out out

# 2) profil šuma, samodejne oznake incidentov, napoved, kartice, evalvacija, chaos   (~10 s)
.venv/bin/python -m seedwatch.cli analyze --out out [--cards]

# 3) demo: http://127.0.0.1:8765/   (živi replay iz surovih datotek + pregled 5 dni)
.venv/bin/python -m seedwatch.cli serve --out out --raw-dir ..

# 4) številke za predstavitev: koliko doda vsak steber, kaj smo izvedeli
.venv/bin/python -m seedwatch.cli ablation --out out
.venv/bin/python -m seedwatch.cli insights --out out --raw-dir ../data/bgl

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
| Lažni alarmi | **1,25 / dan** (OPAZUJ: 5,1 / dan, samo nadzorna plošča) | 1,83 / dan, brez razlage |
| Hitrost / RAM | 1,0 GB/s, < 512 MB (Docker: 261 MB) | – |

Chaos injection: 120 sintetičnih 30-minutnih postopnih degradacij na resničnem ozadju, nato izpad.

| Scenarij | ALARM pred izpadom | OPAZUJ ≥ 10 min prej |
|---|---|---|
| nov WARN vzorec narašča | 24/24 (mediana 9 min) | 21/24 |
| redka napaka postaja pogostejša | 23/24 (5,5 min) | 14/24 |
| latenca 4 hostov do 6× | 21/24 (3,5 min) | 3/24 |
| neodgovorjena sporočila do 60 % | 24/24 (8 min) | 21/24 |
| utrip upade na 20 % | 24/24 (17,5 min) | 24/24 |

**Iskreno:** v resničnih logih večina incidentov nima opaznih predznakov ≥ 10 min (1–6 min).
Postopni vzorci, ki jih naloga omenja, pa se zaznajo zgodaj. Najšibkejša točka je počasna rast latence.

## Preverba na neodvisnem javnem naboru: BGL

Isti model in isti pragovi kot za plačilni sistem, na 7 mesecih logov superračunalnika Blue Gene/L
(4,7 M vrstic, 15 s, 610 incidentov iz **pravih oznak** skrbnikov; oznake model ne vidi, služijo le za oceno).

| | SeedWatch | naiven prag WARN/ERROR |
|---|---|---|
| Ujeti incidenti | **547 / 610 (90 %)** | 227 / 610 (37 %) |
| Lažni alarmi | 1,16 / dan | 0,39 / dan |
| Samo druga časovna polovica (pri izbiri pravil je nismo gledali) | **274 / 288 (95 %)**, 0,75 / dan | – |
| „Čisti“ incidenti (≥ 6 h miru prej): ujeti | 233 / 248 (94 %) | – |
| „Čisti“ incidenti: OPAZUJ ≥ 10 min prej | 15 / 248 | – |

* **Nivo laže tudi tam:** 507.000 vrstic FATAL ni alarm; nekateri FATAL/ERROR se ponavljajo kot cron (npr. „Ido packet timeout“ vsake 4 h).
* **Napoved ≥ 10 min je redka (~6 %) tudi na BGL.** Večina okvar nastopi nenadoma, enako kot v plačilnem sistemu. Vrednost je v zgodnjem in razložljivem zaznavanju, ne v čarobni napovedi.
* **Nivo × redkost:** kritična predloga (FATAL/FAILURE), ki se pojavi redkeje kot 1× na 2 dni, sproži alarm takoj ob prvi vrstici
  („kernel panic“, „Power deactivated“, „mmcs_server exited abnormally“). Pogost FATAL (šum) tega nikoli ne stori.
  Dvigne zaznavo na BGL s 423 na 547; plačilnega sistema ne spremeni (tam ni nivoja CRITICAL).
* Pravila, dodana po ogledu BGL (zato tudi ocena samo na drugi polovici): servisni posegi (`PrepareForService`, `EndServiceAction`) so marker
  in med njimi se napake štejejo le s 30 %, enako kot deploy pri plačilnem sistemu; prag redkosti 1× na 2 dni.

## Koliko doda vsak steber (ablacija)

Isti incidenti in isto ocenjevanje, vsaka vrstica doda eno idejo (ujeti / vsi · lažni alarmi na dan):

| | BGL | vit-2 | vit-3 |
|---|---|---|---|
| A naiven prag WARN+ERROR na minuto | 227/610 · 0,39 | 3/4 · 1,80 | 2/3 · 1,86 |
| B model brez učenja šuma (vse napake = signal) | 459/610 · 1,62 | 4/4 · 2,40 | 3/3 · 3,35 |
| C + šum iz ritma (plevel, marker, servisni poseg) | 419/610 · 0,67 | 4/4 · 2,00 | 3/3 · 0,56 |
| D + šum kot senzor (tišina, utripi) | 423/610 · 0,80 | 4/4 · 2,00 | 3/3 · 0,56 |
| E + nivo × redkost = celoten model | **547/610 · 1,16** | **4/4 · 2,00** | **3/3 · 0,56** |

* **Učenje šuma (B → C)** zmanjša lažne alarme 2,4× (BGL) do 6× (vit-3).
* **Nivo × redkost (D → E)** doda 124 ujetih incidentov na BGL.
* **Tišina (C → D)** na teh incidentih skoraj ne doda – njena vrednost je pri postopnem upadu utripa (chaos: alarm mediano 17,5 min pred izpadom).

## Kaj smo preverili in ne deluje (iskreno)

* **Predznaki pred okvaro:** v uri pred „čistimi“ BGL incidenti je največji lift predloge 4,2 (pri 8/248 incidentih);
  lokalna aktivnost iste omare je povišana le pri 16/248. Večina okvar je nenadnih – napovedati jih ne more nihče, ujamemo jih v prvi minuti.
* **Invariante START↔FINISH, Active↔Inactive:** 86 parov, 0 zlomov – ob izpadu utihne vse hkrati, zato ne dajo signala.

## Datoteke

`seedwatch/fields.py` hitro branje polj · `template.py` predloge in entitete · `aggregate.py` agregat na minuto in dedup ·
`batch.py` vzporedni shardi · `profile.py` plevel/marker/seme · `labels.py` definicija „SYSTEM DOWN“ ·
`predict.py` napovednik · `explain.py` kartice · `evaluate.py`, `chaos.py`, `ablation.py` evalvacija · `insights.py` ugotovitve ·
`stream.py`, `server.py`, `dashboard/` demo · `llm.py` AI povzetek
# hekaton
