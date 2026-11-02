// Placement heuristic. For a task and a candidate node the cost model predicts
//   wait  : queueing delay at the candidate (advertised backlog, aged, + our own
//           not-yet-visible delegations to it)
//   xfer  : input staging time from the best replica/parent location
//   exec  : execution time from advertised speed x learned correction factor
//   risk  : expected rework  = P(failure | health, failure history, staleness) x (wait+xfer+exec)
//   uncert: penalty for acting on old or noisy estimates
// and combines them with workload-class weights. Baseline policies reuse the same
// feasibility rules but deliberately ignore parts of the model.
#pragma once

#include <string>
#include <vector>

#include "nexus/state_view.hpp"

namespace nexus {

struct ClassWeights {
    double wq = 1, wx = 1, we = 1, wr = 1, wu = 1;
};
ClassWeights class_weights(TaskClass c);

struct PendingLoad {
    // Load this node has sent to peers that their latest advertised record does not yet reflect.
    struct Item { int64_t ts; double core_s, gpu_s; uint16_t cores; uint8_t gpus; };
    std::vector<std::vector<Item>> per_node;
    explicit PendingLoad(int n = 0) : per_node(size_t(n)) {}
    void add(NodeId n, int64_t ts, double core_s, double gpu_s, uint16_t cores, uint8_t gpus) {
        per_node[n].push_back({ts, core_s, gpu_s, cores, gpus});
    }
    void clear_before(NodeId n, int64_t origin_ns) {
        auto& v = per_node[n];
        v.erase(std::remove_if(v.begin(), v.end(), [&](const Item& i) { return i.ts <= origin_ns; }), v.end());
    }
    double core_s(NodeId n) const { double s = 0; for (auto& i : per_node[n]) s += i.core_s; return s; }
    double gpu_s(NodeId n) const { double s = 0; for (auto& i : per_node[n]) s += i.gpu_s; return s; }
    int cores(NodeId n) const { int s = 0; for (auto& i : per_node[n]) s += i.cores; return s; }
    int gpus(NodeId n) const { int s = 0; for (auto& i : per_node[n]) s += i.gpus; return s; }
};

struct CostBreakdown {
    NodeId node = 0;
    bool feasible = false;
    std::string reason;
    double wait = 0, xfer = 0, exec = 0, risk = 0, uncert = 0, total = 1e18;
    double xfer_mean = 0;
    std::vector<NodeId> srcs;  // chosen source per input
};

class CostModel {
public:
    CostModel(const ClusterConfig& cfg, Policy p) : cfg_(cfg), policy_(p) {}
    // Static + live feasibility (capacity, partition access, tenant quota, health, liveness).
    bool feasible(const TaskSpec& t, NodeId n, const StateView& v, int64_t now, std::string* why) const;
    CostBreakdown evaluate(const TaskSpec& t, NodeId n, const StateView& v, const PendingLoad& pl, int64_t now) const;
    // Rank all nodes; best first. Infeasible candidates are kept (with reason) at the end.
    std::vector<CostBreakdown> rank(const TaskSpec& t, const StateView& v, const PendingLoad& pl, int64_t now) const;

    // Predicted execution time on a node from its advertised record.
    double nominal_exec(const TaskSpec& t, const NodeRecord& r) const;
    double predicted_exec(const TaskSpec& t, const NodeRecord& r) const;
    double input_cost(const InputRef& in, NodeId cand, const StateView& v, int64_t now, NodeId* best_src, double* mean_out) const;
    Policy policy() const { return policy_; }

private:
    const ClusterConfig& cfg_;
    Policy policy_;
};

}  // namespace nexus
