# Project status (snapshot, 25 Oct 2026)

The benchmark suite was stopped on request after 7 of 18 runs. Everything below is what existed at that point.

## Complete

- C++17 source: node process, shared-memory network fabric, UDP transport, telemetry, cost model,
  reservation/delegation protocol, executor, fault handling, launcher, HTTP server (`src/`, `include/`)
- Unit tests: 490 checks passing (`tests/`)
- Dashboard (`web/dashboard.html`), live and replay modes
- Scripts: `scripts/make_replay.py`, `scripts/plot_results.py`
- Report sections 2–6 and 8 (`docs/TECHNICAL_REPORT.pdf`)

## Results included (`results/`)

Baseline scenario, all jobs completed in every run:

| Run | Jobs | Mean JCT | p95 JCT | Makespan |
| --- | --- | --- | --- | --- |
| nexus, seed 1 | 121/121 | 13.32 s | 25.82 s | 90.4 s |
| fifo, seed 1 | 121/121 | 17.29 s | 35.34 s | 96.4 s |
| compute-only, seed 1 | 121/121 | 16.84 s | 35.89 s | 93.9 s |
| centralized, seed 1 | 121/121 | 12.90 s | 23.97 s | 94.6 s |
| nexus, seed 2 | 130/130 | 16.08 s | 40.32 s | 91.8 s |
| fifo, seed 2 | 130/130 | 31.66 s | 66.53 s | 132.6 s |
| compute-only, seed 2 | 130/130 | 17.57 s | 36.55 s | 96.1 s |

`results/dashboard_replay.html` opens these 7 runs in any browser without the launcher.
`results/benchmark.json` aggregates them; `results/figures/` has the plots that this subset supports.

## Not done

- Remaining runs: centralized seed 2 (stopped mid-run, discarded), all of seed 3, the fault-injection
  scenario (4 policies) and the probing ablation (2 runs). Re-run with:
  `./build/nexus_launcher suite --seeds 3 --out results`
- Report section 1 (abstract) and section 7 (results), which depend on the full suite.
