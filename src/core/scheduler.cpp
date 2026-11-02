#include "nexus/scheduler.hpp"

namespace nexus {

ClassWeights class_weights(TaskClass c) {
    switch (c) {
        case TaskClass::CpuBatch: return {1.0, 1.0, 1.0, 1.0, 1.0};
        case TaskClass::GpuTrain: return {1.0, 1.0, 1.0, 1.5, 1.0};   // long & costly to redo
        case TaskClass::DataEtl: return {1.0, 1.25, 1.0, 1.0, 1.0};   // data movement dominates
        case TaskClass::Interactive: return {1.5, 1.2, 1.0, 1.0, 1.5}; // latency-sensitive
        case TaskClass::MemHeavy: return {1.0, 1.0, 1.0, 1.2, 1.0};
    }
    return {};
}

bool CostModel::feasible(const TaskSpec& t, NodeId n, const StateView& v, int64_t now, std::string* why) const {
    auto fail = [&](const char* r) { if (why) *why = r; return false; };
    const auto& e = v.entry(n);
    if (!e.known) return fail("unknown");
    if (n != v.self() && !v.usable(n, now)) return fail("suspected");
    const NodeRecord& r = e.rec;
    if (t.cores > r.cores) return fail("cores");
    if (t.mem_gb > r.mem_gb) return fail("memory");
    if (t.gpus > r.gpus) return fail("no-gpu");
    if (t.gpus > 0 && !(r.health & kGpuOk)) return fail("gpu-fault");
    if (!(r.health & kCpuOk)) return fail("cpu-fault");
    if (!cfg_.tenant_allowed(t.tenant, n)) return fail("policy");
    double q = cfg_.tenants[t.tenant].node_quota_frac;
    if (t.cores > q * r.cores + 1e-9) return fail("quota");
    // live quota state as last advertised (the executor re-checks authoritatively at reservation)
    if (t.tenant < 4 && n != v.self() && r.tenant_cores[t.tenant] + t.cores > q * r.cores + 1e-9) return fail("quota-live");
    return true;
}

double CostModel::nominal_exec(const TaskSpec& t, const NodeRecord& r) const {
    if (t.gpus > 0) return t.work / (double(t.gpus) * std::max(0.05f, r.gpu_speed));
    return t.work / (double(std::max<uint16_t>(1, t.cores)) * std::max(0.05f, r.cpu_speed));
}

double CostModel::predicted_exec(const TaskSpec& t, const NodeRecord& r) const {
    double base = nominal_exec(t, r);
    if (policy_ == Policy::Nexus || policy_ == Policy::Centralized) base *= clampv<double>(r.corr[int(t.cls)], 0.2, 5.0);
    return base;
}

double CostModel::input_cost(const InputRef& in, NodeId cand, const StateView& v, int64_t now, NodeId* best_src,
                             double* mean_out) const {
    double best = 1e18, best_mean = 1e18;
    NodeId bs = kNoNode;
    const auto& cr = v.entry(cand).rec;
    for (NodeId s : in.alts) {
        if (s != v.self() && s != cand && !v.usable(s, now)) continue;
        double c, m;
        if (s == cand) {
            c = m = double(in.bytes) / (std::max(0.1f, cr.disk_GBps) * 1e9);
        } else {
            NetEst e = v.net(s, cand, now);
            double rtx = 1.0 / std::max(0.5, 1.0 - e.loss);
            c = (e.lat_s + e.lat_sd) + double(in.bytes) / e.bw_lb_Bps * rtx;
            m = e.lat_s + double(in.bytes) / e.bw_Bps * rtx;
        }
        if (c < best) { best = c; best_mean = m; bs = s; }
    }
    if (bs == kNoNode) {  // no reachable source known: pessimistic guess
        best = best_mean = double(in.bytes) / 10e6 + 0.05;
        if (!in.alts.empty()) bs = in.alts.front();
    }
    if (best_src) *best_src = bs;
    if (mean_out) *mean_out = best_mean;
    return best;
}

CostBreakdown CostModel::evaluate(const TaskSpec& t, NodeId n, const StateView& v, const PendingLoad& pl,
                                  int64_t now) const {
    CostBreakdown c;
    c.node = n;
    if (!feasible(t, n, v, now, &c.reason)) return c;
    c.feasible = true;
    const NodeRecord& r = v.entry(n).rec;
    double age = v.age_s(n, now);
    ClassWeights w = class_weights(t.cls);

    // --- queueing delay
    int free_cores = int(r.cores_free) - pl.cores(n);
    double core_back = r.backlog_core_s + pl.core_s(n);
    double core_wait = 0;
    if (free_cores < int(t.cores) || r.queue_len > 0) core_wait = std::max(0.0, core_back / std::max<uint16_t>(1, r.cores) - age);
    double gpu_wait = 0;
    if (t.gpus > 0) {
        int free_g = int(r.gpus_free) - pl.gpus(n);
        double gb = r.backlog_gpu_s + pl.gpu_s(n);
        if (free_g < int(t.gpus) || r.queue_len > 0) gpu_wait = std::max(0.0, gb / std::max<uint8_t>(1, r.gpus) - age);
    }
    c.wait = std::max(core_wait, gpu_wait);

    // --- execution
    c.exec = predicted_exec(t, r);

    // --- data staging
    bool network_aware = policy_ == Policy::Nexus || policy_ == Policy::Centralized;
    double xm_total = 0;
    for (auto& in : t.inputs) {
        NodeId src = kNoNode;
        double mean = 0;
        if (network_aware) {
            c.xfer += input_cost(in, n, v, now, &src, &mean);
            xm_total += mean;
        } else {
            // network-oblivious: first live replica in catalogue order
            for (NodeId s : in.alts)
                if (s == n || s == v.self() || v.usable(s, now)) { src = s; break; }
            if (src == kNoNode && !in.alts.empty()) src = in.alts.front();
        }
        c.srcs.push_back(src);
    }
    c.xfer_mean = xm_total;

    // --- reliability
    const auto& P = cfg_.params;
    double ramp0 = 0.5 * P.stale_s;
    double stale_risk = clampv((age - ramp0) / std::max(0.1, P.suspect_s - ramp0), 0.0, 1.0) * 0.5;
    double pfail = clampv<double>(r.fail_rate + ((r.health & kDegraded) ? 0.1 : 0.0) + stale_risk, 0.0, 0.95);
    c.risk = pfail * (c.wait + c.xfer + c.exec);

    // --- uncertainty of acting on aged / noisy information
    c.uncert = std::max(0.0, c.xfer - c.xfer_mean) * 0.5 + 0.05 * std::min(age, 10.0) * (c.wait + c.exec);

    switch (policy_) {
        case Policy::Nexus:
        case Policy::Centralized:
            c.total = w.wq * c.wait + w.wx * c.xfer + w.we * c.exec + w.wr * c.risk + w.wu * c.uncert;
            break;
        case Policy::ComputeOnly:
            c.exec = nominal_exec(t, r);
            c.total = c.wait + c.exec;
            break;
        case Policy::Fifo:
            c.total = double(n);
            break;
    }
    return c;
}

std::vector<CostBreakdown> CostModel::rank(const TaskSpec& t, const StateView& v, const PendingLoad& pl,
                                           int64_t now) const {
    std::vector<CostBreakdown> out;
    out.reserve(size_t(v.size()));
    for (int i = 0; i < v.size(); ++i) out.push_back(evaluate(t, NodeId(i), v, pl, now));
    std::stable_sort(out.begin(), out.end(), [](const CostBreakdown& a, const CostBreakdown& b) {
        if (a.feasible != b.feasible) return a.feasible;
        return a.total < b.total;
    });
    return out;
}

}  // namespace nexus
