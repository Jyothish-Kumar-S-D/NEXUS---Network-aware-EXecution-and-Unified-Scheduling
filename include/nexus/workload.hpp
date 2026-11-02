// Deterministic synthetic HPC workload: Poisson job arrivals drawn from a mix of
// job templates (single tasks and DAG pipelines) with data inputs and tenants.
#pragma once

#include <vector>

#include "nexus/protocol.hpp"

namespace nexus {

struct WorkloadParams {
    double duration_s = 75;
    double rate_per_s = 1.0;
    double w_cpu = 0.3, w_gpu = 0.2, w_pipeline = 0.2, w_interactive = 0.2, w_mem = 0.1;
    double scale = 1.0;  // multiplies task work (lets the system analysis shrink the trace)
    static WorkloadParams from_json(const json::Value* j);
};

std::vector<JobSpec> generate_workload(const ClusterConfig& cfg, const WorkloadParams& wp, uint64_t seed);

// Aggregate demand of a trace, used for sizing against the host and the cluster.
struct WorkloadStats {
    int jobs = 0, tasks = 0;
    double core_seconds = 0, gpu_seconds = 0, input_gb = 0;
};
WorkloadStats workload_stats(const ClusterConfig& cfg, const std::vector<JobSpec>& jobs);

}  // namespace nexus
