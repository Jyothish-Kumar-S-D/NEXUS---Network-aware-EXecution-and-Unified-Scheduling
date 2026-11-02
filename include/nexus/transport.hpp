// UDP transport. Datagrams on the data/control plane pass through the emulated
// fabric (latency, serialisation, queueing, loss) and are held in a delay queue
// until their emulated arrival time; management-plane datagrams go direct.
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "nexus/fabric.hpp"
#include "nexus/protocol.hpp"

namespace nexus {

struct Datagram {
    MsgHeader h;
    std::vector<uint8_t> body;
    Reader reader() const { return Reader(body.data(), body.size()); }
};

struct TransportStats {
    std::atomic<uint64_t> msgs_sent{0}, bytes_sent{0}, telem_msgs{0}, telem_bytes{0}, drops{0};
    std::atomic<uint64_t> msgs_recv{0}, bytes_recv{0};
};

class Transport {
public:
    Transport(NodeId self, const ClusterConfig& cfg, Fabric* fabric, uint64_t seed);
    ~Transport();
    bool open();
    void close();
    int fd() const { return fd_; }
    NodeId self() const { return self_; }

    // Send over the emulated fabric (or directly when the mgmt flag is set or either
    // endpoint is the client). Returns false if the fabric dropped the datagram.
    bool send(NodeId dst, MsgType t, const Writer& body, uint8_t flags = 0);
    // Non-blocking receive. Returns false when nothing is pending.
    bool recv(Datagram& out);

    TransportStats stats;

private:
    struct Pending {
        int64_t release_ns;
        uint64_t order;
        NodeId dst;
        std::vector<uint8_t> data;
        bool operator>(const Pending& o) const { return release_ns != o.release_ns ? release_ns > o.release_ns : order > o.order; }
    };
    int port_of(NodeId n) const { return n == kClientId ? cfg_.client_port : cfg_.base_port + n; }
    void raw_send(NodeId dst, const std::vector<uint8_t>& data);
    void delay_loop();

    NodeId self_;
    const ClusterConfig& cfg_;
    Fabric* fabric_;
    Rng rng_;
    std::mutex rng_mu_;
    int fd_ = -1;
    uint32_t seq_ = 0;
    std::mutex mu_;
    std::condition_variable cv_;
    std::priority_queue<Pending, std::vector<Pending>, std::greater<Pending>> q_;
    uint64_t order_ = 0;
    std::atomic<bool> stop_{false};
    std::thread th_;
    std::vector<uint8_t> rxbuf_;
};

}  // namespace nexus
