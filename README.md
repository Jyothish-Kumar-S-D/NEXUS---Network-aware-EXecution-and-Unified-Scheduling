# NEXUS — Network-aware EXecution and Unified Scheduling

A distributed, telemetry-driven HPC resource orchestration and task scheduling system in C++17.
Every HPC node runs as its own OS process; nodes discover each other through adaptive probing and
gossip, score candidate execution nodes with a workload-dependent heuristic (compute suitability,
availability, data locality, communication cost, queueing delay, reliability), reserve capacity on
the winner and delegate tasks — with no central authority. A live dashboard shows topology,
telemetry, queues, heuristic scores and decisions, and compares NEXUS with FIFO, compute-only and
centralized scheduling on identical workloads.

See `docs/TECHNICAL_REPORT.pdf` for the design and the measured results.

## Build

Requirements: Linux, a C++17 compiler (GCC ≥ 9 or Clang ≥ 10), CMake ≥ 3.16. No other dependencies.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
(cd build && ctest)             # unit tests (json, wire format, fabric, state view, cost model, workload)
```

Binaries: `build/nexus_node` (one HPC node), `build/nexus_launcher` (experiments + dashboard), `build/nexus_tests`.

## Run

```bash
# 1. host capability analysis (hardware, OS limits, timer/IPC micro-benchmarks, sizing)
./build/nexus_launcher sysinfo

# 2. one live run with the dashboard at http://localhost:8080/
./build/nexus_launcher run --policy nexus --seed 1
./build/nexus_launcher run --policy centralized --faults        # fault-injection scenario
./build/nexus_launcher run --policy nexus --probe fixed-fast     # probing ablation

# 3. the full benchmark suite (4 policies x 3 seeds, fault scenario, probing ablation; ~40 min)
./build/nexus_launcher suite --seeds 3 --out results --hold
```

Options: `--config FILE` (default `config/cluster.json`), `--out DIR` (default `results`),
`--port N` (dashboard), `--duration S` / `--rate R` (workload), `--hold` (keep the dashboard up).
Only one launcher may run at a time: it binds UDP ports 46990 and 47000–470NN.

Open the dashboard while a run is in progress; it follows the live run and fills the benchmark panel
as runs finish. To view recorded runs without the launcher:

```bash
python3 scripts/make_replay.py results results/dashboard_replay.html   # then open the HTML file
python3 scripts/plot_results.py results                                 # PNG figures into results/figures
```

## What runs where

| Process | Count | Role |
| --- | --- | --- |
| `nexus_launcher` | 1 | host analysis, spawns nodes, workload client, fault injector, cross-traffic generator, monitor, HTTP dashboard |
| `nexus_node` | 12 | telemetry agent, home scheduler, executor (worker threads), data server; the FIFO/centralized coordinator role on a0 |

Nodes talk over real UDP sockets. Their datagrams pass through an emulated network fabric that lives
in POSIX shared memory: each NIC and rack uplink is a reservation timeline updated with lock-free CAS
by every process, which yields latency, serialisation, queueing behind bulk transfers, congestion
drops and NIC timestamps. Task kernels are emulated as timed occupancy of a worker thread against a
hidden ground-truth hardware model (some nodes are slower than they advertise), because the host has
far fewer cores than the emulated cluster.

## Outputs (per run, under `results/<run>/`)

| File | Content |
| --- | --- |
| `summary.json` | JCT stats, makespan, utilisation, data movement, prediction error, telemetry overhead, scheduling counters, host cost |
| `tasks.csv` / `jobs.csv` | per-task (wait, transfer, exec, predictions, bytes, placement) and per-job records |
| `timeline.csv` | 1 Hz cluster metrics |
| `replay.json` | everything the dashboard needs to replay the run |
| `logs/<node>.log` | each node process's log, including state left open at shutdown |

`results/benchmark.json` aggregates all runs (mean ± sd per scenario/policy); `results/system_profile.json`
holds the host analysis.

## Layout

```
config/cluster.json        racks, nodes, datasets, tenants, fabric, cross traffic, interference, workload, tunables
include/nexus/             headers (protocol, fabric, transport, state view, scheduler, node, experiment …)
src/net/                   shared-memory fabric, UDP transport with delay thread
src/core/                  cost model and feasibility rules
src/node/                  node event loop, telemetry, home scheduler, executor, data server
src/launcher/              host analysis, workload generator, experiment runner, HTTP server
web/dashboard.html         live / replay dashboard (no build step)
tests/test_main.cpp        unit tests
scripts/                   replay bundler, figure plotting
docs/TECHNICAL_REPORT.pdf  technical report
```
