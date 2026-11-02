// Time-aware distributed state model. Each node keeps one StateView: the newest
// record it has seen from every other node (learned via probes, gossip, polling or
// reservation replies) and its own measured link estimates. Every read is
// qualified by observation age: freshness decays exponentially, uncertainty
// widens with age, and old entries are classified stale -> suspected.
#pragma once

#include <vector>

#include "nexus/protocol.hpp"

namespace nexus {

enum class PeerStatus : uint8_t { Unknown = 0, Fresh = 1, Stale = 2, Suspected = 3 };
inline const char* peer_status_name(PeerStatus s) {
    static const char* n[] = {"unknown", "fresh", "stale", "suspected"};
    return n[int(s)];
}

struct PeerLink {
    Ewma lat{0.25};    // one-way latency, seconds
    Ewma bw{0.3};      // available bandwidth, bytes/s (packet-pair + passive transfer feedback)
    Ewma loss{0.1};    // probe loss fraction
    uint32_t probes_sent = 0, acks = 0;
    int64_t last_lat_ns = 0, last_bw_ns = 0;
    double lat_change = 0;  // recent relative change (drives adaptive probing)
};

struct NetEst {
    bool known = false;
    double lat_s = 0.002, lat_sd = 0.002;
    double bw_Bps = 20e6, bw_lb_Bps = 10e6;  // mean and lower confidence bound
    double loss = 0;
    double age_s = 1e9;
};

struct ViewEntry {
    bool known = false;
    NodeRecord rec;
    int64_t received_ns = 0;
    uint32_t updates = 0;
};

class StateView {
public:
    StateView() = default;
    StateView(NodeId self, int n, const NexusParams& p) : self_(self), P_(p), nodes_(size_t(n)), links_(size_t(n)) {}

    NodeId self() const { return self_; }
    int size() const { return int(nodes_.size()); }

    // Merge a record; returns true if it was newer than what we had.
    bool merge(const NodeRecord& r, int64_t now) {
        if (r.id >= nodes_.size() || r.id == self_) return false;
        auto& e = nodes_[r.id];
        if (e.known && (r.origin_ns < e.rec.origin_ns || (r.origin_ns == e.rec.origin_ns && r.version <= e.rec.version)))
            return false;
        e.known = true;
        e.rec = r;
        e.received_ns = now;
        e.updates++;
        return true;
    }
    void set_self(const NodeRecord& r, int64_t now) {
        auto& e = nodes_[self_];
        e.known = true;
        e.rec = r;
        e.received_ns = now;
    }

    const ViewEntry& entry(NodeId n) const { return nodes_[n]; }
    double age_s(NodeId n, int64_t now) const {
        if (n == self_) return 0;
        const auto& e = nodes_[n];
        return e.known ? std::max(0.0, ns_to_s(now - e.rec.origin_ns)) : 1e9;
    }
    double freshness(NodeId n, int64_t now) const { return std::exp(-age_s(n, now) / P_.fresh_tau_s); }
    PeerStatus status(NodeId n, int64_t now) const {
        if (n == self_) return PeerStatus::Fresh;
        if (!nodes_[n].known) return PeerStatus::Unknown;
        double a = age_s(n, now);
        if (a > P_.suspect_s) return PeerStatus::Suspected;
        if (a > P_.stale_s) return PeerStatus::Stale;
        return PeerStatus::Fresh;
    }
    bool usable(NodeId n, int64_t now) const {
        auto s = status(n, now);
        return s == PeerStatus::Fresh || s == PeerStatus::Stale;
    }

    PeerLink& link(NodeId peer) { return links_[peer]; }
    const PeerLink& link(NodeId peer) const { return links_[peer]; }

    // Network estimate for the directed path a -> b, as seen from this node.
    NetEst net(NodeId a, NodeId b, int64_t now) const {
        NetEst e;
        if (a == b) { e.known = true; e.lat_s = 0; e.lat_sd = 0; e.age_s = 0; return e; }
        auto from_own = [&](NodeId peer) {
            const PeerLink& l = links_[peer];
            if (!l.lat.valid()) return false;
            e.known = true;
            e.lat_s = l.lat.mean;
            e.lat_sd = l.lat.stddev();
            e.loss = l.loss.valid() ? l.loss.mean : 0;
            e.age_s = ns_to_s(now - std::max(l.last_lat_ns, l.last_bw_ns));
            if (l.bw.valid()) { e.bw_Bps = l.bw.mean; e.bw_lb_Bps = std::max(0.05 * l.bw.mean, l.bw.mean - 1.0 * l.bw.stddev()); }
            return true;
        };
        auto from_rec = [&](NodeId owner, NodeId peer) {
            const auto& en = nodes_[owner];
            if (!en.known) return false;
            for (auto& l : en.rec.links)
                if (l.peer == peer) {
                    e.known = true;
                    e.lat_s = l.lat_ms * 1e-3;
                    e.lat_sd = l.lat_sd_ms * 1e-3;
                    e.loss = l.loss;
                    e.age_s = l.age_s + ns_to_s(now - en.rec.origin_ns);
                    if (l.bw_MBps > 0) { e.bw_Bps = l.bw_MBps * 1e6; e.bw_lb_Bps = e.bw_Bps * 0.8; }
                    return true;
                }
            return false;
        };
        bool ok = false;
        if (a == self_) ok = from_own(b);
        else if (b == self_) ok = from_own(a);  // links are close to symmetric
        if (!ok) ok = from_rec(a, b);
        if (!ok) ok = from_rec(b, a);
        // widen uncertainty with age: information decays
        double decay = std::min(0.6, 0.03 * std::min(e.age_s, 20.0));
        e.bw_lb_Bps *= (1.0 - decay);
        e.lat_sd += e.lat_s * decay;
        return e;
    }

    // Choose which records to piggyback as gossip to a given peer: freshest first.
    std::vector<NodeId> gossip_targets(NodeId to, int k, int64_t now) const {
        std::vector<std::pair<double, NodeId>> c;
        for (size_t i = 0; i < nodes_.size(); ++i) {
            NodeId n = NodeId(i);
            if (n == self_ || n == to || !nodes_[i].known) continue;
            if (status(n, now) == PeerStatus::Suspected) continue;
            c.push_back({age_s(n, now), n});
        }
        std::sort(c.begin(), c.end());
        std::vector<NodeId> out;
        for (auto& x : c) {
            if (int(out.size()) >= k) break;
            out.push_back(x.second);
        }
        return out;
    }

private:
    NodeId self_ = 0;
    NexusParams P_;
    std::vector<ViewEntry> nodes_;
    std::vector<PeerLink> links_;
};

}  // namespace nexus
