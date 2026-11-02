// Cluster description: racks, heterogeneous nodes, datasets, tenants, network
// fabric parameters, background traffic, and scheduler tunables.
#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "nexus/common.hpp"
#include "nexus/json.hpp"

namespace nexus {

enum class TaskClass : uint8_t { CpuBatch = 0, GpuTrain = 1, DataEtl = 2, Interactive = 3, MemHeavy = 4 };
constexpr int kNumClasses = 5;
inline const char* class_name(TaskClass c) {
    static const char* n[] = {"cpu-batch", "gpu-train", "data-etl", "interactive", "mem-heavy"};
    return n[int(c)];
}

enum class Policy : uint8_t { Nexus = 0, Fifo = 1, ComputeOnly = 2, Centralized = 3 };
inline const char* policy_name(Policy p) {
    static const char* n[] = {"nexus", "fifo", "compute-only", "centralized"};
    return n[int(p)];
}
inline Policy parse_policy(const std::string& s) {
    if (s == "fifo") return Policy::Fifo;
    if (s == "compute-only" || s == "compute") return Policy::ComputeOnly;
    if (s == "centralized" || s == "central") return Policy::Centralized;
    return Policy::Nexus;
}
// Policies where one coordinator node owns every scheduling decision.
inline bool is_coordinated(Policy p) { return p == Policy::Fifo || p == Policy::Centralized; }

enum class ProbeMode : uint8_t { Adaptive = 0, FixedFast = 1, FixedSlow = 2 };
inline const char* probe_mode_name(ProbeMode m) {
    static const char* n[] = {"adaptive", "fixed-fast", "fixed-slow"};
    return n[int(m)];
}
inline ProbeMode parse_probe_mode(const std::string& s) {
    if (s == "fixed-fast") return ProbeMode::FixedFast;
    if (s == "fixed-slow") return ProbeMode::FixedSlow;
    return ProbeMode::Adaptive;
}

struct RackSpec {
    std::string name;
    double uplink_gbps = 10;
    double intra_latency_us = 50;
};

struct NodeSpec {
    NodeId id = 0;
    std::string name;
    int rack = 0;
    int cores = 8;
    int mem_gb = 32;
    int gpus = 0;
    double cpu_speed = 1.0;  // advertised, relative to reference core
    double gpu_speed = 1.0;  // advertised, relative to reference GPU
    double storage_tb = 1;
    double nic_gbps = 1;
    double disk_gbps = 8;  // local read bandwidth (Gbit/s)
    std::string partition = "general";
    std::vector<int> datasets;
    // ---- ground truth (hidden from schedulers; used only by hardware emulation) ----
    double true_cpu_factor = 1.0;
    double true_gpu_factor = 1.0;
};

struct DatasetSpec {
    int id = 0;
    std::string name;
    double size_mb = 100;
};

struct TenantSpec {
    std::string name;
    std::vector<std::string> partitions;  // access policy
    double node_quota_frac = 1.0;         // max fraction of a node's cores one tenant may hold
};

// Periodic background (cross) traffic occupying a link: it creates busy and quiet
// network periods that the adaptive prober and the scheduler must cope with.
struct CrossTraffic {
    std::string link;  // "rack:<r>:up", "rack:<r>:down", "node:<n>:up", "node:<n>:down"
    double period_s = 30;
    double duty = 0.5;      // fraction of period that is busy
    double load = 0.7;      // fraction of capacity consumed while busy
    double phase_s = 0;
};

// Hidden slowdown of a node over a time window (co-tenant interference, thermal throttling).
struct Interference {
    int node = 0;
    double start_s = 0, end_s = 0;
    double factor = 1.0;  // execution slowdown multiplier (>1 = slower)
};

struct NexusParams {
    // telemetry
    double probe_base_ms = 400;       // adaptive base interval between probe rounds
    double probe_min_ms = 150;
    double probe_max_ms = 3000;
    double fixed_fast_ms = 100;
    double fixed_slow_ms = 2000;
    int probe_fanout = 3;             // peers probed per round
    int gossip_records = 4;           // third-party node records piggybacked per probe
    double bw_probe_every = 6;        // one packet-pair bandwidth probe per N latency probes
    int bw_probe_bytes = 32000;       // packet-pair probe size
    double telemetry_budget_kBps = 64; // token-bucket cap on telemetry bytes per node
    double busy_nic_util = 0.6;       // defer probing while local NIC utilisation exceeds this
    // state model
    double fresh_tau_s = 2.0;         // freshness decay constant
    double stale_s = 4.0;             // older than this => stale (penalised)
    double suspect_s = 6.0;           // older than this => suspected failed (excluded)
    // scheduling
    double delegate_margin = 0.10;    // keep local unless remote is >10% cheaper
    double reservation_lease_ms = 1500;
    int max_reserve_attempts = 4;
    int max_queue = 6;                // max queued tasks a node will admit
    double rebalance_ms = 1000;
    double migrate_gain = 0.35;       // migrate a queued task only if >35% cheaper elsewhere
    int max_migrations = 2;
    double central_poll_ms = 500;     // centralized baseline polling period
};

struct ClusterConfig {
    std::string name = "nexus-cluster";
    int base_port = 47000;     // node i listens on base_port + i
    int client_port = 46990;   // launcher: workload driver + monitor plane
    int http_port = 8080;
    double switch_buffer_kb = 512;
    double base_loss = 0.001;
    std::vector<RackSpec> racks;
    std::vector<std::vector<double>> rack_latency_us;  // one-way core latency between racks
    std::vector<NodeSpec> nodes;
    std::vector<DatasetSpec> datasets;
    std::vector<TenantSpec> tenants;
    std::vector<CrossTraffic> cross;
    std::vector<Interference> interference;
    NexusParams params;
    json::Value raw;

    int tenant_index(const std::string& n) const {
        for (size_t i = 0; i < tenants.size(); ++i) if (tenants[i].name == n) return int(i);
        return -1;
    }
    const DatasetSpec* dataset(int id) const {
        for (auto& d : datasets) if (d.id == id) return &d;
        return nullptr;
    }

    static ClusterConfig from_json(const json::Value& j) {
        ClusterConfig c;
        c.raw = j;
        c.name = j.get_str("name", c.name);
        c.base_port = int(j.get_num("base_port", c.base_port));
        c.client_port = int(j.get_num("client_port", c.client_port));
        c.http_port = int(j.get_num("http_port", c.http_port));
        if (auto f = j.find("fabric")) {
            c.switch_buffer_kb = f->get_num("switch_buffer_kb", c.switch_buffer_kb);
            c.base_loss = f->get_num("base_loss", c.base_loss);
        }
        for (size_t i = 0; i < j.at("racks").size(); ++i) {
            auto& r = j.at("racks")[i];
            RackSpec rs;
            rs.name = r.get_str("name", "rack" + std::to_string(i));
            rs.uplink_gbps = r.get_num("uplink_gbps", 10);
            rs.intra_latency_us = r.get_num("intra_latency_us", 50);
            c.racks.push_back(rs);
        }
        auto& rl = j.at("rack_latency_us");
        for (size_t i = 0; i < rl.size(); ++i) {
            std::vector<double> row;
            for (size_t k = 0; k < rl[i].size(); ++k) row.push_back(rl[i][k].num());
            c.rack_latency_us.push_back(row);
        }
        for (size_t i = 0; i < j.at("datasets").size(); ++i) {
            auto& d = j.at("datasets")[i];
            c.datasets.push_back({int(d.get_num("id", 0)), d.get_str("name", ""), d.get_num("size_mb", 100)});
        }
        for (size_t i = 0; i < j.at("tenants").size(); ++i) {
            auto& t = j.at("tenants")[i];
            TenantSpec ts;
            ts.name = t.get_str("name", "");
            ts.node_quota_frac = t.get_num("node_quota_frac", 1.0);
            if (auto p = t.find("partitions"))
                for (size_t k = 0; k < p->size(); ++k) ts.partitions.push_back((*p)[k].str());
            c.tenants.push_back(ts);
        }
        auto& ns = j.at("nodes");
        for (size_t i = 0; i < ns.size(); ++i) {
            auto& n = ns[i];
            NodeSpec s;
            s.id = NodeId(i);
            s.name = n.get_str("name", "n" + std::to_string(i));
            std::string rack = n.get_str("rack", "");
            for (size_t r = 0; r < c.racks.size(); ++r) if (c.racks[r].name == rack) s.rack = int(r);
            s.cores = int(n.get_num("cores", 8));
            s.mem_gb = int(n.get_num("mem_gb", 32));
            s.gpus = int(n.get_num("gpus", 0));
            s.cpu_speed = n.get_num("cpu_speed", 1.0);
            s.gpu_speed = n.get_num("gpu_speed", 1.0);
            s.storage_tb = n.get_num("storage_tb", 1);
            s.nic_gbps = n.get_num("nic_gbps", 1);
            s.disk_gbps = n.get_num("disk_gbps", 8);
            s.partition = n.get_str("partition", "general");
            if (auto d = n.find("datasets"))
                for (size_t k = 0; k < d->size(); ++k) s.datasets.push_back(int((*d)[k].num()));
            if (auto g = n.find("ground_truth")) {
                s.true_cpu_factor = g->get_num("cpu_factor", 1.0);
                s.true_gpu_factor = g->get_num("gpu_factor", 1.0);
            }
            c.nodes.push_back(s);
        }
        if (auto ct = j.find("cross_traffic"))
            for (size_t i = 0; i < ct->size(); ++i) {
                auto& x = (*ct)[i];
                CrossTraffic t;
                t.link = x.get_str("link", "");
                t.period_s = x.get_num("period_s", 30);
                t.duty = x.get_num("duty", 0.5);
                t.load = x.get_num("load", 0.7);
                t.phase_s = x.get_num("phase_s", 0);
                c.cross.push_back(t);
            }
        if (auto in = j.find("interference"))
            for (size_t i = 0; i < in->size(); ++i) {
                auto& x = (*in)[i];
                Interference t;
                std::string nn = x.get_str("node", "");
                for (auto& s : c.nodes) if (s.name == nn) t.node = s.id;
                t.start_s = x.get_num("start_s", 0);
                t.end_s = x.get_num("end_s", 0);
                t.factor = x.get_num("factor", 1.0);
                c.interference.push_back(t);
            }
        if (auto p = j.find("nexus")) {
            auto& P = c.params;
#define NXP(k) P.k = p->get_num(#k, P.k)
            NXP(probe_base_ms); NXP(probe_min_ms); NXP(probe_max_ms); NXP(fixed_fast_ms); NXP(fixed_slow_ms);
            P.probe_fanout = int(p->get_num("probe_fanout", P.probe_fanout));
            P.gossip_records = int(p->get_num("gossip_records", P.gossip_records));
            NXP(bw_probe_every);
            P.bw_probe_bytes = int(p->get_num("bw_probe_bytes", P.bw_probe_bytes));
            NXP(telemetry_budget_kBps); NXP(busy_nic_util); NXP(fresh_tau_s); NXP(stale_s); NXP(suspect_s);
            NXP(delegate_margin); NXP(reservation_lease_ms);
            P.max_reserve_attempts = int(p->get_num("max_reserve_attempts", P.max_reserve_attempts));
            P.max_queue = int(p->get_num("max_queue", P.max_queue));
            NXP(rebalance_ms); NXP(migrate_gain);
            P.max_migrations = int(p->get_num("max_migrations", P.max_migrations));
            NXP(central_poll_ms);
#undef NXP
        }
        return c;
    }

    static ClusterConfig load(const std::string& path) {
        std::ifstream f(path);
        if (!f) throw std::runtime_error("cannot open config " + path);
        std::stringstream ss;
        ss << f.rdbuf();
        return from_json(json::parse(ss.str()));
    }

    // One-way propagation latency (seconds) between two nodes on the emulated fabric.
    double base_latency_s(NodeId a, NodeId b) const {
        if (a == b) return 0;
        int ra = nodes[a].rack, rb = nodes[b].rack;
        if (ra == rb) return racks[ra].intra_latency_us * 1e-6;
        return (racks[ra].intra_latency_us + racks[rb].intra_latency_us + rack_latency_us[ra][rb]) * 1e-6;
    }
    bool tenant_allowed(int tenant, NodeId n) const {
        if (tenant < 0 || tenant >= int(tenants.size())) return false;
        for (auto& p : tenants[tenant].partitions) if (p == nodes[n].partition || p == "*") return true;
        return false;
    }
};

}  // namespace nexus
