// Unit tests for the NEXUS core (no external framework).
#include <iostream>
#include <unistd.h>

#include "nexus/fabric.hpp"
#include "nexus/scheduler.hpp"
#include "nexus/workload.hpp"

using namespace nexus;

static int g_fail = 0, g_pass = 0;
#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (cond) ++g_pass;                                                               \
        else { ++g_fail; std::cerr << __FILE__ << ":" << __LINE__ << " CHECK failed: " #cond "\n"; } \
    } while (0)
#define NEAR(a, b, tol) CHECK(std::fabs(double(a) - double(b)) <= (tol))

static ClusterConfig small_cluster() {
    const char* js = R"({
      "racks": [{"name":"R0","uplink_gbps":8,"intra_latency_us":50},{"name":"R1","uplink_gbps":1,"intra_latency_us":50}],
      "rack_latency_us": [[0,1000],[1000,0]],
      "datasets": [{"id":0,"name":"d0","size_mb":1000}],
      "tenants": [{"name":"physics","partitions":["general"],"node_quota_frac":1.0},
                  {"name":"ml","partitions":["gpu"],"node_quota_frac":1.0},
                  {"name":"bio","partitions":["general"],"node_quota_frac":0.5}],
      "nodes": [
        {"name":"n0","rack":"R0","cores":8,"mem_gb":32,"cpu_speed":1.0,"nic_gbps":8,"partition":"general","datasets":[0]},
        {"name":"n1","rack":"R0","cores":8,"mem_gb":32,"cpu_speed":2.0,"nic_gbps":8,"partition":"general"},
        {"name":"n2","rack":"R1","cores":8,"mem_gb":32,"cpu_speed":2.0,"nic_gbps":8,"partition":"general"},
        {"name":"n3","rack":"R1","cores":8,"mem_gb":32,"gpus":2,"cpu_speed":1.0,"nic_gbps":8,"partition":"gpu"}
      ]})";
    return ClusterConfig::from_json(json::parse(js));
}

static NodeRecord rec_for(const ClusterConfig& c, NodeId id, int64_t now) {
    auto& s = c.nodes[id];
    NodeRecord r;
    r.id = id;
    r.version = 1;
    r.origin_ns = now;
    r.rack = uint8_t(s.rack);
    r.cores = uint16_t(s.cores);
    r.mem_gb = uint32_t(s.mem_gb);
    r.gpus = uint8_t(s.gpus);
    r.cpu_speed = float(s.cpu_speed);
    r.gpu_speed = float(s.gpu_speed);
    r.disk_GBps = 1.0f;
    r.partition = s.partition;
    for (int d : s.datasets) r.datasets |= 1ull << d;
    r.cores_free = r.cores;
    r.mem_free = r.mem_gb;
    r.gpus_free = r.gpus;
    return r;
}

static void test_json() {
    auto v = json::parse(R"({"a":[1,2.5,"x\n"],"b":{"c":true,"d":null}})");
    CHECK(v.at("a").size() == 3);
    NEAR(v.at("a")[1].num(), 2.5, 1e-12);
    CHECK(v.at("a")[2].str() == "x\n");
    CHECK(v.at("b").get_bool("c", false));
    auto s = json::dump(v);
    auto w = json::parse(s);
    CHECK(json::dump(w) == s);
}

static void test_serialization() {
    TaskSpec t;
    t.id = make_task_id(7, 3);
    t.job = 7;
    t.index = 3;
    t.cls = TaskClass::DataEtl;
    t.cores = 4;
    t.mem_gb = 16;
    t.work = 12.5f;
    InputRef in;
    in.kind = InputKind::Parent;
    in.ref = 1;
    in.bytes = 123456789;
    in.src = 2;
    in.alts = {2, 5};
    t.inputs.push_back(in);
    t.deps = {1, 2};
    DelegateMsg d;
    d.spec = t;
    d.attempt = (2u << 16) | 3;
    d.home = 9;
    d.pred_exec = 3.5f;
    Writer w;
    d.write(w);
    Reader r(w.buf.data(), w.buf.size());
    DelegateMsg e = DelegateMsg::read(r);
    CHECK(e.spec.id == t.id);
    CHECK(e.spec.inputs.size() == 1 && e.spec.inputs[0].alts.size() == 2 && e.spec.inputs[0].bytes == 123456789);
    CHECK(e.spec.deps.size() == 2 && e.spec.deps[1] == 2);
    CHECK(e.attempt == d.attempt && e.home == 9);
    CHECK(r.remaining() == 0);

    NodeRecord nr;
    nr.id = 3;
    nr.partition = "gpu";
    nr.links.push_back({1, 0.5f, 0.1f, 100.f, 0.01f, 2.f});
    ProbeMsg pm;
    pm.self = nr;
    pm.gossip.push_back(nr);
    pm.pad = 1000;
    Writer w2;
    pm.write(w2);
    Reader r2(w2.buf.data(), w2.buf.size());
    ProbeMsg pm2 = ProbeMsg::read(r2);
    CHECK(pm2.self.partition == "gpu" && pm2.gossip.size() == 1 && pm2.self.links.size() == 1);
    CHECK(w2.size() > 1000);
    bool threw = false;
    try {
        Reader bad(w2.buf.data(), 10);
        ProbeMsg::read(bad);
    } catch (...) { threw = true; }
    CHECK(threw);
}

static void test_fabric() {
    auto cfg = small_cluster();
    std::string name = "/nexus_test_" + std::to_string(::getpid());
    Fabric f = Fabric::create(name, cfg);
    Fabric g = Fabric::attach(name);
    CHECK(g.ok() && g.n_nodes() == 4);
    CHECK(f.path(0, 1).size() == 2);
    CHECK(f.path(0, 2).size() == 4);
    int64_t now = now_ns();
    // 100 MB intra-rack at 8 Gb/s (1 GB/s) ~ 0.1 s
    int64_t a = f.bulk_chunk(0, 1, 100000000, now);
    NEAR(ns_to_s(a - now), 0.1, 0.002);
    // cross-rack is limited by the 1 Gb/s rack uplink (125 MB/s): 0.8 s + 1.1 ms
    int64_t b = f.bulk_chunk(1, 2, 100000000, now);
    NEAR(ns_to_s(b - now), 0.8 + 0.0011, 0.01);
    // a second flow over the same uplink queues behind the first (shared physical link)
    int64_t c = f.bulk_chunk(0, 3, 10000000, now);
    CHECK(ns_to_s(c - now) > 0.8);
    // a small control packet sees bounded (switch-buffer) queueing, not the full backlog
    Rng rng(1);
    SendResult s = f.control_send(0, 2, 200, now, rng);
    double buf_s = 512.0 * 1024 / 125e6;
    CHECK(s.queue_delay_s <= buf_s * 2 + 1e-9);
    CHECK(ns_to_s(s.arrival_ns - now) < 0.1);
    CHECK(f.utilisation(f.rack_up(0), now) > 0.5);
    CHECK(f.true_avail_Bps(0, 2, now) < 125e6);
    Fabric::unlink(name);
}

static void test_state_view() {
    auto cfg = small_cluster();
    NexusParams P = cfg.params;
    StateView v(0, 4, P);
    int64_t now = now_ns();
    auto r1 = rec_for(cfg, 1, now - s_to_ns(1.0));
    CHECK(v.merge(r1, now));
    auto old = r1;
    old.origin_ns -= s_to_ns(2);
    CHECK(!v.merge(old, now));  // older record ignored
    CHECK(v.status(1, now) == PeerStatus::Fresh);
    CHECK(v.status(2, now) == PeerStatus::Unknown);
    CHECK(v.status(1, now + s_to_ns(P.stale_s)) == PeerStatus::Stale);
    CHECK(v.status(1, now + s_to_ns(P.suspect_s)) == PeerStatus::Suspected);
    CHECK(v.freshness(1, now) > v.freshness(1, now + s_to_ns(3)));
    // own link estimates and uncertainty widening with age
    auto& L = v.link(1);
    for (int i = 0; i < 10; ++i) L.lat.add(0.001, now);
    for (int i = 0; i < 10; ++i) L.bw.add(100e6, now);
    L.last_lat_ns = L.last_bw_ns = now;
    NetEst e0 = v.net(0, 1, now), e1 = v.net(0, 1, now + s_to_ns(10));
    CHECK(e0.known && std::fabs(e0.lat_s - 0.001) < 1e-6);
    CHECK(e1.bw_lb_Bps < e0.bw_lb_Bps);
    // third-party link estimate comes from the gossiped record
    auto r2 = rec_for(cfg, 2, now);
    r2.links.push_back({3, 2.0f, 0.1f, 50.f, 0.f, 0.f});
    v.merge(r2, now);
    NetEst e2 = v.net(2, 3, now);
    CHECK(e2.known && std::fabs(e2.lat_s - 0.002) < 1e-6 && std::fabs(e2.bw_Bps - 50e6) < 1);
}

static void test_cost_model() {
    auto cfg = small_cluster();
    int64_t now = now_ns();
    StateView v(0, 4, cfg.params);
    v.set_self(rec_for(cfg, 0, now), now);
    for (NodeId i = 1; i < 4; ++i) v.merge(rec_for(cfg, i, now), now);
    // links as seen by node 0: fast to n1, slow to n2/n3; n1 learned links too
    for (int k = 0; k < 5; ++k) {
        v.link(1).lat.add(0.0001, now); v.link(1).bw.add(1000e6, now);
        v.link(2).lat.add(0.001, now);  v.link(2).bw.add(20e6, now);
        v.link(3).lat.add(0.001, now);  v.link(3).bw.add(20e6, now);
    }
    PendingLoad pl(4);
    TaskSpec t;
    t.id = 1;
    t.cls = TaskClass::DataEtl;
    t.cores = 4;
    t.mem_gb = 8;
    t.work = 40;  // 10 s on speed 1, 5 s on speed 2
    InputRef in;
    in.kind = InputKind::Dataset;
    in.ref = 0;
    in.bytes = 800ull << 20;  // 800 MB held by n0
    in.alts = {0};
    t.inputs.push_back(in);
    CostModel nexus(cfg, Policy::Nexus), compute(cfg, Policy::ComputeOnly);
    auto rn = nexus.rank(t, v, pl, now);
    auto rc = compute.rank(t, v, pl, now);
    // compute-only ignores the 800 MB that must cross the slow link; NEXUS does not.
    CHECK(rc.front().node == 1 || rc.front().node == 2);
    CHECK(rn.front().node == 1);  // fast CPU *and* a fast link to the data
    auto e2 = nexus.evaluate(t, 2, v, pl, now);
    CHECK(e2.xfer > 20.0);  // 800 MB at ~20 MB/s
    // feasibility: gpu partition is closed to physics; quota for bio is half a node
    std::string why;
    CHECK(!nexus.feasible(t, 3, v, now, &why) && why == "policy");
    t.tenant = 2;
    t.cores = 6;
    CHECK(!nexus.feasible(t, 1, v, now, &why) && why == "quota");
    t.cores = 4;
    t.tenant = 0;
    // learned correction factor steers away from a node that is slower than advertised
    auto r1 = rec_for(cfg, 1, now + 1);
    r1.corr[int(TaskClass::DataEtl)] = 3.0f;
    v.merge(r1, now);
    auto e1 = nexus.evaluate(t, 1, v, pl, now);
    auto e1c = compute.evaluate(t, 1, v, pl, now);
    CHECK(e1.exec > 2.9 * e1c.exec);
    // pending (not yet advertised) delegations raise the predicted wait
    PendingLoad pl2(4);
    for (int k = 0; k < 4; ++k) pl2.add(1, now, 40, 0, 4, 0);
    auto w0 = nexus.evaluate(t, 1, v, pl, now).wait, w1 = nexus.evaluate(t, 1, v, pl2, now).wait;
    CHECK(w1 > w0);
    // a stale peer carries risk and uncertainty; a suspected one is excluded
    auto st = nexus.evaluate(t, 1, v, pl, now + s_to_ns(4.5));
    CHECK(st.feasible && st.risk > 0 && st.uncert > 0);
    CHECK(!nexus.evaluate(t, 1, v, pl, now + s_to_ns(10)).feasible);
}

static void test_workload() {
    auto cfg = ClusterConfig::load(std::string(NEXUS_TEST_CONFIG));
    WorkloadParams wp = WorkloadParams::from_json(cfg.raw.find("workload"));
    auto a = generate_workload(cfg, wp, 1), b = generate_workload(cfg, wp, 1), c = generate_workload(cfg, wp, 2);
    CHECK(!a.empty() && a.size() == b.size());
    CHECK(a.size() != c.size() || a[0].submit_s != c[0].submit_s);
    bool dag = false;
    for (auto& j : a) {
        CHECK(j.submit_s < wp.duration_s);
        for (auto& t : j.tasks) {
            for (auto d : t.deps) CHECK(d < t.index);
            if (!t.deps.empty()) dag = true;
        }
    }
    CHECK(dag);
    // every job must be placeable somewhere in the cluster
    int64_t now = now_ns();
    StateView v(0, int(cfg.nodes.size()), cfg.params);
    v.set_self(rec_for(cfg, 0, now), now);
    for (NodeId i = 1; i < cfg.nodes.size(); ++i) v.merge(rec_for(cfg, i, now), now);
    CostModel cm(cfg, Policy::Nexus);
    for (auto& j : a)
        for (auto& t : j.tasks) {
            bool any = false;
            for (NodeId i = 0; i < cfg.nodes.size(); ++i) any |= cm.feasible(t, i, v, now, nullptr);
            CHECK(any);
        }
}

int main() {
    test_json();
    test_serialization();
    test_fabric();
    test_state_view();
    test_cost_model();
    test_workload();
    std::cout << g_pass << " checks passed, " << g_fail << " failed\n";
    return g_fail ? 1 : 0;
}
