#!/usr/bin/env python3
"""Figures for the technical report from a results directory.

usage: plot_results.py RESULTS_DIR   (writes RESULTS_DIR/figures/*.png)
"""
import csv
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ORDER = ["nexus", "centralized", "compute-only", "fifo"]
COLOR = {"nexus": "#2f56d0", "centralized": "#2c8a8a", "compute-only": "#7b4fa8", "fifo": "#8a6d3b"}
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.25, "figure.dpi": 140})


def agg(bench, scenario):
    rows = {a["policy"]: a for a in bench["aggregate"] if a["scenario"] == scenario and a["probe"] == "adaptive"}
    return [rows[p] for p in ORDER if p in rows]


def bars(ax, rows, metric, title, unit, scale=1.0):
    xs = range(len(rows))
    m = [r["metrics"][metric]["mean"] * scale for r in rows]
    s = [r["metrics"][metric]["sd"] * scale for r in rows]
    ax.bar(xs, m, yerr=s, capsize=3, color=[COLOR[r["policy"]] for r in rows])
    ax.set_xticks(list(xs), [r["policy"] for r in rows])
    ax.set_title(title)
    ax.set_ylabel(unit)
    for x, v in zip(xs, m):
        ax.annotate(f"{v:.1f}" if v < 100 else f"{v:.0f}", (x, v), ha="center", va="bottom", fontsize=8,
                    xytext=(0, 3), textcoords="offset points")


def timeline(results, run):
    path = os.path.join(results, run, "timeline.csv")
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        return list(csv.DictReader(f))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    results = sys.argv[1]
    out = os.path.join(results, "figures")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(results, "benchmark.json")) as f:
        bench = json.load(f)

    base = agg(bench, "baseline")
    if base:
        fig, axs = plt.subplots(1, 4, figsize=(13, 3.2))
        bars(axs[0], base, "jobs.jct_mean_s", "Mean job completion time", "s")
        bars(axs[1], base, "jobs.jct_p95_s", "p95 job completion time", "s")
        bars(axs[2], base, "tasks.xfer_mean_s", "Mean input staging per task", "s")
        bars(axs[3], base, "tasks.network_gb", "Data moved over the network", "GB")
        fig.tight_layout()
        fig.savefig(os.path.join(out, "fig1_baseline.png"))
        plt.close(fig)

    faults = agg(bench, "faults")
    if faults:
        fig, axs = plt.subplots(1, 3, figsize=(10, 3.2))
        bars(axs[0], faults, "jobs.jct_mean_s", "Mean JCT under faults", "s")
        bars(axs[1], faults, "jobs.jct_p95_s", "p95 JCT under faults", "s")
        bars(axs[2], faults, "jobs.makespan_s", "Makespan under faults", "s")
        fig.tight_layout()
        fig.savefig(os.path.join(out, "fig2_faults.png"))
        plt.close(fig)

    fig, axs = plt.subplots(1, 2, figsize=(11, 3.4))
    for p in ORDER:
        tl = timeline(results, f"baseline_{p}_s1")
        if tl:
            axs[0].plot([float(r["t"]) for r in tl], [100 * float(r["util"]) for r in tl], color=COLOR[p], label=p, lw=1.4)
        tl = timeline(results, f"faults_{p}_s1")
        if tl:
            axs[1].plot([float(r["t"]) for r in tl], [float(r["jobs_done"]) for r in tl], color=COLOR[p], label=p, lw=1.4)
    axs[0].set_title("Core utilisation, baseline seed 1")
    axs[0].set_ylabel("% of cores allocated")
    axs[0].set_xlabel("experiment time (s)")
    axs[1].set_title("Jobs completed, fault scenario")
    axs[1].set_xlabel("experiment time (s)")
    for r in bench["runs"]:
        if r.get("scenario") == "faults" and r.get("policy") == "nexus":
            for fl in r.get("fault_log", []):
                if fl["kind"] in ("kill", "gpu-fail", "freeze"):
                    axs[1].axvline(fl["t"], color="#c53030", ls="--", lw=0.8)
                    axs[1].annotate(f'{fl["kind"]} {fl["node"]}', (fl["t"], 2), rotation=90, fontsize=7, color="#c53030",
                                    xytext=(3, 0), textcoords="offset points")
    axs[0].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "fig3_timelines.png"))
    plt.close(fig)

    # probing ablation: same seed, same workload, only the probing policy differs
    probes = [r for r in bench["runs"] if r.get("policy") == "nexus" and int(r.get("seed", 0)) == 1 and
              (r.get("scenario") == "probe-ablation" or r.get("scenario") == "baseline")]
    probes.sort(key=lambda r: ["fixed-fast", "adaptive", "fixed-slow"].index(r["probe"]))
    if len(probes) > 1:
        fig, axs = plt.subplots(1, 3, figsize=(10, 3.2))
        names = [r["probe"] for r in probes]
        for ax, metric, title, unit, sc in [
            (axs[0], "overhead.telemetry_kBps_per_node", "Telemetry traffic per node", "kB/s", 1),
            (axs[1], "overhead.lat_est_rel_err_p50", "Latency estimate error (p50)", "%", 100),
            (axs[2], "jobs.jct_mean_s", "Mean JCT", "s", 1)]:
            sect, key = metric.split(".")
            v = [r[sect][key] * sc for r in probes]
            ax.bar(names, v, color=["#2f56d0" if n == "adaptive" else "#9aa5b8" for n in names])
            ax.set_title(title)
            ax.set_ylabel(unit)
        fig.tight_layout()
        fig.savefig(os.path.join(out, "fig4_probing.png"))
        plt.close(fig)
    print("figures written to", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
