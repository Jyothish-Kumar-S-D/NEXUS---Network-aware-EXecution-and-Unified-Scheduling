#include "nexus/fabric.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <new>
#include <stdexcept>

namespace nexus {

Fabric Fabric::create(const std::string& name, const ClusterConfig& cfg) {
    ::shm_unlink(name.c_str());
    int fd = ::shm_open(name.c_str(), O_CREAT | O_RDWR | O_EXCL, 0600);
    if (fd < 0) throw std::runtime_error("shm_open(create) failed for " + name);
    if (::ftruncate(fd, sizeof(FabricShm)) != 0) { ::close(fd); throw std::runtime_error("ftruncate failed"); }
    void* p = ::mmap(nullptr, sizeof(FabricShm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
    std::memset(p, 0, sizeof(FabricShm));
    auto* s = static_cast<FabricShm*>(p);
    int N = int(cfg.nodes.size()), R = int(cfg.racks.size());
    if (N > kMaxNodes || R > 16) throw std::runtime_error("cluster too large for fabric segment");
    s->n_nodes = N;
    s->n_racks = R;
    s->n_links = 2 * N + 2 * R;
    s->buffer_bytes = cfg.switch_buffer_kb * 1024.0;
    s->base_loss = cfg.base_loss;
    for (int i = 0; i < N; ++i) s->rack_of[i] = cfg.nodes[i].rack;
    for (int a = 0; a < N; ++a)
        for (int b = 0; b < N; ++b) s->prop_s[a][b] = cfg.base_latency_s(NodeId(a), NodeId(b));
    for (int i = 0; i < s->n_links; ++i) {
        auto* l = new (&s->links[i]) LinkState();
        l->bulk_busy_ns = 0;
        l->ctrl_busy_ns = 0;
        l->bytes_bulk = l->bytes_ctrl = l->bytes_cross = l->drops = l->busy_ns_total = 0;
        double gbps = 1;
        if (i < 2 * N) gbps = cfg.nodes[i / 2].nic_gbps;
        else gbps = cfg.racks[(i - 2 * N) / 2].uplink_gbps;
        l->cap_Bps = gbps * 1e9 / 8.0;
        l->prop_s = 0;
    }
    s->magic = kMagic;
    Fabric f;
    f.shm_ = s;
    return f;
}

Fabric Fabric::attach(const std::string& name) {
    int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
    if (fd < 0) throw std::runtime_error("shm_open(attach) failed for " + name);
    void* p = ::mmap(nullptr, sizeof(FabricShm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
    auto* s = static_cast<FabricShm*>(p);
    if (s->magic != kMagic) throw std::runtime_error("fabric segment not initialised");
    Fabric f;
    f.shm_ = s;
    return f;
}

void Fabric::unlink(const std::string& name) { ::shm_unlink(name.c_str()); }

int Fabric::link_index(const std::string& spec, const ClusterConfig& cfg) const {
    // "rack:<name>:up|down" or "node:<name>:up|down"
    auto p1 = spec.find(':'), p2 = spec.rfind(':');
    if (p1 == std::string::npos || p2 == p1) return -1;
    std::string kind = spec.substr(0, p1), name = spec.substr(p1 + 1, p2 - p1 - 1), dir = spec.substr(p2 + 1);
    bool isup = dir == "up";
    if (kind == "rack") {
        for (size_t r = 0; r < cfg.racks.size(); ++r)
            if (cfg.racks[r].name == name) return isup ? rack_up(int(r)) : rack_down(int(r));
    } else {
        for (auto& n : cfg.nodes)
            if (n.name == name) return isup ? up(n.id) : down(n.id);
    }
    return -1;
}

std::vector<int> Fabric::path(NodeId src, NodeId dst) const {
    std::vector<int> p;
    p.reserve(4);
    p.push_back(up(src));
    int rs = shm_->rack_of[src], rd = shm_->rack_of[dst];
    if (rs != rd) {
        p.push_back(rack_up(rs));
        p.push_back(rack_down(rd));
    }
    p.push_back(down(dst));
    return p;
}

int64_t Fabric::reserve(std::atomic<int64_t>& busy, int64_t earliest, int64_t dur, int64_t* start_out) {
    int64_t b = busy.load(std::memory_order_relaxed);
    while (true) {
        int64_t start = std::max(earliest, b);
        int64_t end = start + dur;
        if (busy.compare_exchange_weak(b, end, std::memory_order_acq_rel)) {
            if (start_out) *start_out = start;
            return end;
        }
    }
}

double Fabric::backlog_s(int li, int64_t now) const {
    int64_t b = shm_->links[li].bulk_busy_ns.load(std::memory_order_relaxed);
    return b > now ? ns_to_s(b - now) : 0.0;
}

double Fabric::utilisation(int li, int64_t now) const {
    double q = backlog_s(li, now);
    // Queue occupancy -> utilisation: 50 ms of standing queue ~ 50% utilisation.
    return std::min(0.95, q / (q + 0.05));
}

SendResult Fabric::control_send(NodeId src, NodeId dst, size_t bytes, int64_t now, Rng& rng) {
    SendResult r;
    if (src == dst) { r.arrival_ns = now; return r; }
    int64_t t = now, last_end = now;
    double qsum = 0;
    for (int li : path(src, dst)) {
        LinkState& L = shm_->links[li];
        double backlog = backlog_s(li, now);
        double buffer_s = shm_->buffer_bytes / L.cap_Bps;
        double q = std::min(backlog, buffer_s);
        double avail = L.cap_Bps * (1.0 - utilisation(li, now));
        int64_t ser = s_to_ns(double(bytes) / avail);
        int64_t start;
        int64_t end = reserve(L.ctrl_busy_ns, t, ser, &start);
        t = start;
        last_end = std::max(last_end, end);
        qsum += q;
        L.bytes_ctrl.fetch_add(bytes, std::memory_order_relaxed);
        double pdrop = shm_->base_loss;
        if (backlog > buffer_s) pdrop += 0.03 * std::min(1.0, (backlog - buffer_s) / (4 * buffer_s));
        if (!r.dropped && rng.chance(pdrop)) {
            r.dropped = true;
            L.drops.fetch_add(1, std::memory_order_relaxed);
        }
    }
    r.queue_delay_s = qsum;
    // + a few microseconds of NIC/driver timestamping jitter, so estimators face realistic noise
    r.arrival_ns = last_end + s_to_ns(qsum + shm_->prop_s[src][dst]) + int64_t(rng.uniform(0, 4000));
    return r;
}

int64_t Fabric::bulk_chunk(NodeId src, NodeId dst, uint64_t bytes, int64_t now) {
    if (src == dst) return now;
    int64_t t = now, last_end = now;
    for (int li : path(src, dst)) {
        LinkState& L = shm_->links[li];
        int64_t dur = s_to_ns(double(bytes) / L.cap_Bps);
        int64_t start;
        int64_t end = reserve(L.bulk_busy_ns, t, dur, &start);
        t = start;
        last_end = std::max(last_end, end);
        L.bytes_bulk.fetch_add(bytes, std::memory_order_relaxed);
        L.busy_ns_total.fetch_add(uint64_t(dur), std::memory_order_relaxed);
    }
    return last_end + s_to_ns(shm_->prop_s[src][dst]);
}

void Fabric::inject(int li, uint64_t bytes, int64_t now) {
    LinkState& L = shm_->links[li];
    int64_t dur = s_to_ns(double(bytes) / L.cap_Bps);
    reserve(L.bulk_busy_ns, now, dur, nullptr);
    L.bytes_cross.fetch_add(bytes, std::memory_order_relaxed);
    L.busy_ns_total.fetch_add(uint64_t(dur), std::memory_order_relaxed);
}

// What a 1500-byte packet sent right now would experience (without reserving anything).
double Fabric::true_one_way_s(NodeId a, NodeId b, int64_t now) const {
    if (a == b) return 0;
    double q = 0;
    for (int li : path(a, b)) {
        const LinkState& L = shm_->links[li];
        q += std::min(backlog_s(li, now), shm_->buffer_bytes / L.cap_Bps);
        int64_t cb = L.ctrl_busy_ns.load(std::memory_order_relaxed);
        if (cb > now) q += ns_to_s(cb - now);
        q += 1500.0 / (L.cap_Bps * (1.0 - utilisation(li, now)));
    }
    return shm_->prop_s[a][b] + q;
}

double Fabric::true_avail_Bps(NodeId a, NodeId b, int64_t now) const {
    double m = 1e18;
    for (int li : path(a, b)) m = std::min(m, shm_->links[li].cap_Bps * (1.0 - utilisation(li, now)));
    return m;
}

}  // namespace nexus
