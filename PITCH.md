# SeedWatch – scenarij govora (5 min)

Pred nastopom: `.venv/bin/python -m seedwatch.cli serve --out out --raw-dir ..`, v brskalniku odprti dve zavihki:
`/?tab=overview&env=vit-3` in `/?autoplay=i1&speed=40` (drugega osveži tik pred demom). Demo 2× preizkusi vnaprej,
da so datoteke v predpomnilniku; imej rezervni posnetek zaslona.

---

## 0:00 – 0:40 · Problem
**Zaslon:** *Pregled 5 dni*, vit-3 – ploščice zgoraj in kartica „Kaj smo izvedeli“.

> „Dobili smo 25 GB logov plačilnega sistema – 12 milijonov vrstic. 99,7 % je INFO, nivoja CRITICAL sploh ni.
> Najglasnejši ERROR se je ponovil 14.500-krat – vsakih 32 sekund, kot metronom – in je popolnoma nenevaren.
> ‚Stop processing.‘ zveni grozljivo, a je dnevni cron ob 00:08. Kdor filtrira po besedi ERROR, se utopi v šumu.“

**Številke:** 97 % WARN/ERROR v vit-3 je šum · ERROR metronom 14.500× (CV 0,05) · cron ob 00:08.

## 0:40 – 1:20 · Kaj smo izvedeli
**Zaslon:** ista kartica „Kaj smo izvedeli“.

> „Preden smo gradili model, smo vprašali podatke, kako napovedljive so okvare sploh.
> Na plačilnem sistemu in na javnem naboru superračunalnika BGL je odgovor enak: **večina okvar je nenadnih** –
> lokalni predznaki so le pri 16 od 248 incidentov. Kjer pa se težava gradi postopoma, je signal v **ritmu**:
> šum utihne, se pospeši ali se razredči.“

**Številke:** BGL 506.797 od 855.195 vrstic FATAL ni alarm · predznaki 16/248 · invariante 86 parov, 0 zlomov.

## 1:20 – 1:50 · Ideja v treh stavkih
> „Prvič: **šum je senzor** – plevela ne zavržemo, iz njega naredimo utrip sistema; njegova tišina je najmočnejši signal.
> Drugič: **o tem, kaj je šum, odloča obnašanje**, ne nivo – metronom, cron, zagon poda in servisni poseg se naučimo sami.
> Tretjič: **nivo sam laže, nivo × redkost ne** – redek kritičen dogodek sproži alarm takoj, pogost FATAL nikoli.“

## 1:50 – 3:20 · Živi demo
**Zaslon:** *Živo*, scenarij „14.10. vit-3 · Crypt farma offline“, 40×.

1. Pokaži ploščice: vrstice tečejo, RAM ostaja pod 512 MB, „94 % WARN/ERROR je šum“; sivi stolpci = plevel.
2. **08:37** – krivulja skoči, rdeča kartica. Preberi naglas: *Sistem utiša – volumen −22 %, manjkajo utripi povezav,
   echo napake na bančnih hostih; ukrep: preveri povezavo do hosta NLBLUKA.*
3. **08:43–09:10** – volumen pade na nič: „To je bil dejanski izpad, 27 minut tišine. Opozorili smo, preden je utihnil.“
4. **09:12** – modri stolpec markerjev: „Strežniki se ponovno zaganjajo – to ni nova težava, zato ne kličemo.“
5. Če je čas: scenarij „16.10. vit-2 · curljanje → plaz“ – rumeni **OPAZUJ 06:34**, rdeči **ALARM 06:39**,
   834 enakih napak v minuti stisnjenih v en zapis „×834“.

## 3:20 – 4:20 · Dokaz
**Zaslon:** *Pregled*, kartica „Koliko doda vsak steber (ablacija)“, nato izbira okolja **BGL**.

> „Vsaka vrstica doda eno idejo. Ko se model nauči šuma iz ritma, lažni alarmi na plačilnem sistemu padejo s 3,4 na 0,6 na dan.
> Ko dodamo nivo × redkost, na neodvisnem superračunalniku ujamemo 547 od 610 incidentov – naiven prag jih 227.
> Model za BGL nismo predelali: isti pragovi, edino novo pravilo je ‚servisni poseg je načrtovan‘.
> Na drugi polovici podatkov, ki je pri izbiri pravil nismo gledali, ujamemo 274 od 288.“

**Številke:**

| | naiven prag | SeedWatch |
|---|---|---|
| plačilni sistem – izpadi (7) | 5/7, 1,8 lažnih/dan | **7/7, 1,25 lažnih/dan** |
| BGL – incidenti (610) | 227 (37 %) | **547 (90 %)**, 1,16 lažnih/dan |
| BGL – druga polovica | – | **274/288 (95 %)**, 0,75 lažnih/dan |
| chaos: 30-min postopna degradacija | – | ALARM pred izpadom 116/120 |

Ablacija (ujeti/vsi · lažni na dan): B brez učenja šuma vit-3 3/3 · 3,35 → C s šumom iz ritma 3/3 · 0,56;
BGL D 423/610 · 0,80 → E + nivo × redkost 547/610 · 1,16.

## 4:20 – 5:00 · Vrednost in iskrenost
> „Dežurni namesto 34.000 vrstic dobi nekaj kartic, ki povedo kaj, kje, zakaj in kaj narediti. Vse teče pretočno:
> 25 GB v 25 sekundah, pod 512 MB pomnilnika. Iskreno: ‚10 minut vnaprej‘ je možno tam, kjer se težava gradi –
> upad utripa ujamemo mediano 17 minut prej. Nenadnih okvar ne napove nihče – ujamemo pa jih v prvi minuti in jih razložimo.“

---

## Priprava na vprašanja žirije

**Zakaj ne globoko učenje (LSTM, transformer)?**
Imamo 7 izpadov na plačilnem sistemu – premalo za učenje z oznakami. Naš pristop se uči brez oznak, je razložljiv
(vsak alarm pove zakaj) in na BGL brez prilagajanja ujame 90 % incidentov. Model z oznakami bi bil naslednji korak, ko bi imeli zgodovino incidentov.

**Ali deluje na drugem sistemu?**
Da – preverjeno na BGL (Blue Gene/L, 4,7 M vrstic, 7 mesecev, prave oznake). Dodali smo le bralnik formata in eno pravilo za servisne posege.

**Kaj pa „10 minut vnaprej“?**
Podatki kažejo, da je večina okvar nenadnih (predznaki pri 6,5 %). Kjer se težava gradi (upad utripa, naraščanje opozoril,
odgovori brez odziva), OPAZUJ pride ≥ 10 min prej v 83 od 120 sintetičnih scenarijev. Raje povemo resnico kot lepo številko.

**Koliko je lažnih alarmov?**
1,25 na dan na plačilnem sistemu (naiven prag 1,8 in brez razlage); OPAZUJ je samo na zaslonu, ne kliče dežurnega.

**Kaj če se šum spremeni (nov deploy, nova verzija)?**
Baseline se uči sproti: anomalija, ki traja 45 min, postane nova normala (alarm se je že sprožil); nova predloga ob zagonu poda se ne šteje.

**Kako v produkcijo?**
Isti agregator bere Kafka topic namesto datoteke; stanje je omejeno (okno 3 min + profil), zato pomnilnik ne raste s prometom.
Kartice gredo v Teams/PagerDuty; AI povzetek (Claude) je opcijski.

**Kako dokažete 512 MB?**
Docker z `--memory=512m --memory-swap=512m`: 25,5 GB v 27 s, 0 OOM, vrh procesnega pomnilnika 261 MB.

**Zakaj ne invariante START/FINISH?**
Preverili smo 86 parov – nobeden se ni zlomil, ker ob izpadu utihne vse hkrati. Ideja je dobra, a v teh podatkih ne prinese signala; zato je ne prodajamo.

**Kaj je bilo najtežje?**
Da „ERROR“ ne pomeni napake: metronomi, croni, zagoni podov in transakcijske zavrnitve. Ko to ločimo po obnašanju, ostane malo – in to malo je vredno pozornosti.
