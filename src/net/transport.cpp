#include "nexus/transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

namespace nexus {

Transport::Transport(NodeId self, const ClusterConfig& cfg, Fabric* fabric, uint64_t seed)
    : self_(self), cfg_(cfg), fabric_(fabric), rng_(seed), rxbuf_(70000) {}

Transport::~Transport() { close(); }

bool Transport::open() {
    fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd_ < 0) return false;
    // No SO_REUSEADDR: on Linux it lets two UDP sockets share a port, which would let a second
    // experiment silently steal datagrams. A busy port must fail loudly instead.
    int sz = 4 << 20;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(uint16_t(port_of(self_)));
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) {
        LOG_E("bind port %d failed: %s", port_of(self_), strerror(errno));
        ::close(fd_);
        fd_ = -1;
        return false;
    }
    stop_ = false;
    th_ = std::thread([this] { delay_loop(); });
    return true;
}

void Transport::close() {
    if (th_.joinable()) {
        {
            std::lock_guard<std::mutex> g(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        th_.join();
    }
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

void Transport::raw_send(NodeId dst, const std::vector<uint8_t>& data) {
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(uint16_t(port_of(dst)));
    ::sendto(fd_, data.data(), data.size(), MSG_DONTWAIT, reinterpret_cast<sockaddr*>(&a), sizeof a);
}

bool Transport::send(NodeId dst, MsgType t, const Writer& body, uint8_t flags) {
    MsgHeader h;
    h.type = t;
    h.flags = flags;
    h.src = self_;
    h.dst = dst;
    int64_t now = now_ns();
    h.tx_ns = now;
    bool direct = (flags & kFlagMgmt) || dst == kClientId || self_ == kClientId || fabric_ == nullptr;
    {
        std::lock_guard<std::mutex> g(mu_);
        h.seq = ++seq_;
    }
    size_t wire = sizeof(MsgHeader) + body.size() + 28;  // + IP/UDP headers
    if (direct) {
        h.rx_hw_ns = now;
        raw_send(dst, encode(h, body));
        return true;
    }
    SendResult r;
    {
        std::lock_guard<std::mutex> g(rng_mu_);
        r = fabric_->control_send(self_, dst, wire, now, rng_);
    }
    stats.msgs_sent++;
    stats.bytes_sent += wire;
    if (flags & kFlagTelemetry) { stats.telem_msgs++; stats.telem_bytes += wire; }
    if (r.dropped) { stats.drops++; return false; }
    h.rx_hw_ns = r.arrival_ns;
    Pending p{r.arrival_ns, 0, dst, encode(h, body)};
    {
        std::lock_guard<std::mutex> g(mu_);
        p.order = ++order_;
        q_.push(std::move(p));
    }
    cv_.notify_one();
    return true;
}

void Transport::delay_loop() {
    std::unique_lock<std::mutex> lk(mu_);
    while (!stop_) {
        if (q_.empty()) {
            cv_.wait(lk);
            continue;
        }
        int64_t now = now_ns();
        int64_t rel = q_.top().release_ns;
        if (rel > now) {
            cv_.wait_for(lk, std::chrono::nanoseconds(rel - now));
            continue;
        }
        Pending p = std::move(const_cast<Pending&>(q_.top()));
        q_.pop();
        lk.unlock();
        raw_send(p.dst, p.data);
        lk.lock();
    }
}

bool Transport::recv(Datagram& out) {
    ssize_t n = ::recv(fd_, rxbuf_.data(), rxbuf_.size(), MSG_DONTWAIT);
    if (n <= 0) return false;
    if (size_t(n) < sizeof(MsgHeader)) return false;
    std::memcpy(&out.h, rxbuf_.data(), sizeof(MsgHeader));
    if (out.h.magic != kWireMagic) return false;
    out.body.assign(rxbuf_.begin() + sizeof(MsgHeader), rxbuf_.begin() + n);
    stats.msgs_recv++;
    stats.bytes_recv += size_t(n);
    return true;
}

}  // namespace nexus
