#!/usr/bin/env python3
"""Bundle recorded runs into a self-contained dashboard (no launcher needed).

usage: make_replay.py [--fragment] RESULTS_DIR OUT_HTML [run_name ...]
Without run names, every run directory that has a replay.json is included.
--fragment writes the page body only (for hosts that supply the document skeleton).
"""
import json
import os
import sys


def main():
    args = sys.argv[1:]
    fragment = "--fragment" in args
    args = [a for a in args if a != "--fragment"]
    if len(args) < 2:
        print(__doc__)
        return 1
    results, out = args[0], args[1]
    names = args[2:] or sorted(d for d in os.listdir(results)
                                   if os.path.isfile(os.path.join(results, d, "replay.json")))
    replays = []
    for n in names:
        with open(os.path.join(results, n, "replay.json")) as f:
            replays.append(json.load(f))
    bench_runs = None
    bpath = os.path.join(results, "benchmark.json")
    if os.path.isfile(bpath):
        with open(bpath) as f:
            bench_runs = json.load(f).get("runs")
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "..", "web", "dashboard.html")) as f:
        page = f.read()
    data = "<script>window.NEXUS_REPLAYS=" + json.dumps(replays, separators=(",", ":")) + ";"
    if bench_runs is not None:
        data += "window.NEXUS_BENCH_RUNS=" + json.dumps(bench_runs, separators=(",", ":")) + ";"
    data += "</script>\n"
    marker = "<script>\n(function(){"
    if marker not in page:
        raise SystemExit("dashboard.html layout changed: cannot find the main script")
    page = page.replace(marker, data + marker, 1)
    if not fragment:  # a standalone file needs its own document skeleton
        page = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
                '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
                '<style>[hidden]{display:none!important}</style></head><body>\n' + page + "\n</body></html>\n")
    with open(out, "w") as f:
        f.write(page)
    print(f"wrote {out}: {len(replays)} runs, {os.path.getsize(out) / 1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
