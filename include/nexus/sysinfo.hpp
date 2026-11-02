// Host capability analysis: hardware inventory, OS limits and micro-benchmarks
// that decide how many node processes the host can emulate faithfully.
#pragma once

#include <string>

#include "nexus/json.hpp"

namespace nexus {

struct SystemProfile {
    json::Value j;               // full report
    int logical_cpus = 1;
    double mem_avail_mb = 0;
    double timer_overshoot_us = 0;   // p99 extra delay of a 1 ms sleep
    double udp_rtt_us = 0;           // p50 loopback round trip between two processes
    double spawn_ms = 0;             // fork+exec latency
    int max_node_processes = 0;      // recommended upper bound for this host
    double min_task_s = 0;           // shortest task duration that timing noise does not distort (>1%)
};

SystemProfile analyze_system(bool run_benchmarks = true);
void print_profile(const SystemProfile& p);

}  // namespace nexus
