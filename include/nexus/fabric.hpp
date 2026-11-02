// Emulated network fabric in POSIX shared memory.
//
// Every node process maps the same segment. Each physical link (node NIC up/down,
// rack uplink/downlink to the core) is a shared reservation timeline updated with
// lock-free CAS, so congestion caused by one process is felt by all others —
// exactly like flows sharing a switch port. Nodes only *read* their own NIC
// counters (as a real host reads /proc/net/dev); everything they know about remote
// links comes from active probing and passive measurement of their own transfers.
#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "nexus/common.hpp"
#include "nexus/config.hpp"

namespace nexus {

constexpr int kMaxLinks = 2 * kMaxNodes + 2 * 16;

struct alignas(64) LinkState {
    double cap_Bps;
    double prop_s;
    std::atomic<int64_t> bulk_busy_ns;   // FIFO reservation timeline for bulk chunks
    std::atomic<int64_t> ctrl_busy_ns;   // serialisation timeline for small packets
    std::atomic<uint64_t> bytes_bulk;
    std::atomic<uint64_t> bytes_ctrl;
    std::atomic<uint64_t> bytes_cross;
    std::atomic<uint64_t> drops;
    std::atomic<uint64_t> busy_ns_total; // cumulative reserved time (for true utilisation)
};

struct FabricShm {
    uint64_t magic;
    int n_nodes, n_racks, n_links;
    double buffer_bytes;
    double base_loss;
    int rack_of[kMaxNodes];
    double prop_s[kMaxNodes][kMaxNodes];
    LinkState links[kMaxLinks];
};

struct SendResult {
    bool dropped = false;
    int64_t arrival_ns = 0;     // emulated hardware RX timestamp
    double queue_delay_s = 0;
};

class Fabric {
public:
    static constexpr uint64_t kMagic = 0x4E45585553464142ULL;  // "NEXUSFAB"
    static constexpr uint64_t kChunkBytes = 2ull << 20;          // bulk transfer granularity

    // Launcher: create and initialise the segment.
    static Fabric create(const std::string& name, const ClusterConfig& cfg);
    // Node: attach to an existing segment.
    static Fabric attach(const std::string& name);
    static void unlink(const std::string& name);

    Fabric() = default;
    bool ok() const { return shm_ != nullptr; }
    int n_nodes() const { return shm_->n_nodes; }

    int up(NodeId n) const { return 2 * n; }
    int down(NodeId n) const { return 2 * n + 1; }
    int rack_up(int r) const { return 2 * shm_->n_nodes + 2 * r; }
    int rack_down(int r) const { return 2 * shm_->n_nodes + 2 * r + 1; }
    int link_index(const std::string& spec, const ClusterConfig& cfg) const;
    std::vector<int> path(NodeId src, NodeId dst) const;
    LinkState& link(int i) { return shm_->links[i]; }
    const LinkState& link(int i) const { return shm_->links[i]; }
    int n_links() const { return shm_->n_links; }

    // Small datagram (control/telemetry). Applies serialisation at the available
    // bandwidth, queueing behind bulk traffic (bounded by switch buffers), RED-like
    // congestion drops and random loss.
    SendResult control_send(NodeId src, NodeId dst, size_t bytes, int64_t now, Rng& rng);
    // Reserve one bulk chunk along the path; returns when its last byte arrives.
    int64_t bulk_chunk(NodeId src, NodeId dst, uint64_t bytes, int64_t now);
    // Reserve raw capacity on a single link (background cross traffic).
    void inject(int link, uint64_t bytes, int64_t now);

    // Instantaneous queue occupancy of a link in seconds, and the derived utilisation.
    double backlog_s(int link, int64_t now) const;
    double utilisation(int link, int64_t now) const;
    // Local NIC view (legitimately visible to the node that owns the NIC).
    double nic_utilisation(NodeId n, int64_t now) const {
        return std::max(utilisation(up(n), now), utilisation(down(n), now));
    }
    // Ground truth for evaluation only (launcher/monitor, never schedulers).
    double true_one_way_s(NodeId a, NodeId b, int64_t now) const;
    double true_avail_Bps(NodeId a, NodeId b, int64_t now) const;

private:
    FabricShm* shm_ = nullptr;
    static int64_t reserve(std::atomic<int64_t>& busy, int64_t earliest, int64_t dur, int64_t* start_out);
};

}  // namespace nexus
