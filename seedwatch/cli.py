import argparse
import json
import os
import sys


def main(argv=None):
    ap = argparse.ArgumentParser(prog="seedwatch")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("batch", help="stream raw logs -> minute rows, events, catalog")
    b.add_argument("files", nargs="+")
    b.add_argument("--out", default="out")
    b.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 2))
    b.add_argument("--shard-mb", type=int, default=256)
    b.add_argument("--stats-name", default="batch_stats.json")

    a = sub.add_parser("analyze", help="minute rows -> noise profile, incidents, alerts, evaluation")
    a.add_argument("--out", default="out")
    a.add_argument("--threshold", type=float, default=None)
    a.add_argument("--cards", action="store_true", help="print every alert card")

    v = sub.add_parser("serve", help="demo dashboard with live replay")
    v.add_argument("--out", default="out")
    v.add_argument("--raw-dir", default="..")
    v.add_argument("--host", default="127.0.0.1")
    v.add_argument("--port", type=int, default=8765)

    args = ap.parse_args(argv)
    if args.cmd == "batch":
        from .batch import run_batch
        stats = run_batch(args.files, args.out, args.workers, args.shard_mb,
                          log=lambda s: print(s, file=sys.stderr, flush=True), stats_name=args.stats_name)
        print(json.dumps(stats, indent=2))
    elif args.cmd == "serve":
        from .server import serve
        serve(args.out, args.raw_dir, args.host, args.port)
    elif args.cmd == "analyze":
        from .analyze import analyze_all
        from .explain import card_text
        from .predict import THRESHOLD
        total, results = analyze_all(args.out, args.threshold or THRESHOLD)
        for r in results:
            prof = r["profile"]
            cls = {}
            for p in prof["tpl"].values():
                cls[p["class"]] = cls.get(p["class"], 0) + p["n"]
            print(f"== {r['env']}: WARN/ERROR lines by class {cls}")
            if r["summary"].get("labeled"):
                sm = r["summary"]
                print(f"   labelled incidents {sm['outages']}: caught {sm['caught']}, before start {sm['predicted']}, "
                      f"false alarms {sm['fa_per_day']}/day, naive {sm['naive']['caught']}/{sm['naive']['outages']} "
                      f"at {sm['naive']['fa_per_day']}/day")
                print(f"   isolated (>=6 h quiet before): {sm['isolated']}")
                continue
            for inc in r["incidents"]:
                lead = "" if inc["lead_min"] is None else f"{inc['lead_min']:+.0f} min"
                print(f"   {inc['id']:9s} sev{inc['severity']} {','.join(inc['kinds']):38s} "
                      f"{inc['outcome']:9s} {lead:8s} {inc['alert_title'] or ''}")
            if args.cards:
                for c in r["cards"]:
                    print()
                    print(("[LAŽNI ALARM] " if c["false_alarm"] else "") + card_text(c))
        print("== chaos injection (30-min slow burn, then outage):")
        for kind, c in total.pop("chaos").items():
            print(f"   {kind:18s} ALARM before outage {c['predicted']}/{c['runs']} (median lead "
                  f"{c['median_lead_min']} min) · WATCH >=10 min early {c['watch_lead_ge_10']}/{c['runs']} "
                  f"(median {c['median_watch_lead_min']} min) – {c['desc']}")
        print(json.dumps(total, indent=2))


if __name__ == "__main__":
    main()
