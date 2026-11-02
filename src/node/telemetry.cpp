// Telemetry discovery: adaptive probing, packet-pair bandwidth estimation,
// piggybacked gossip, and loss accounting under a token-bucket traffic budget.
#include "nexus/node.hpp"

namespace nexus {

// Token bucket bounding all telemetry bytes a node emits (probes, acks, state replies).
void Node::refill_tokens() {
    int64_t now = now_ns();
    double rate = P_.telemetry_budget_kBps * 1000.0;
    tokens_ = std::min(2.0 * rate, tokens_ + ns_to_s(now - tokens_ns_) * rate);
    tokens_ns_ = now;
}

double Node::next_probe_interval_ms() {
    if (opt_.probe == ProbeMode::FixedFast) return P_.fixed_fast_ms;
    if (opt_.probe == ProbeMode::FixedSlow) return P_.fixed_slow_ms;
    int64_t now = now_ns();
    // Volatility: how much recent latency samples moved relative to their estimate.
    double vol = 0;
    int nv = 0, unknown = 0;
    for (int i = 0; i < view_.size(); ++i) {
        if (i == self_) continue;
        const PeerLink& l = view_.link(NodeId(i));
        if (l.lat.valid()) {
            vol += std::min(1.0, l.lat_change);
            ++nv;
        }
        auto st = view_.status(NodeId(i), now);
        if (st == PeerStatus::Unknown || st == PeerStatus::Stale) ++unknown;
    }
    vol = nv ? vol / nv : 1.0;
    double iv = P_.probe_base_ms / (1.0 + 3.0 * vol);
    if (unknown > 0) iv = std::min(iv, 0.5 * P_.probe_base_ms);
    // Quiet, stable cluster => stretch the interval; busy NIC => back off.
    if (vol < 0.05 && unknown == 0) iv *= 2.0;
    if (nic_ewma_.valid() && nic_ewma_.mean > P_.busy_nic_util) iv *= 1.5;
    return clampv(iv, P_.probe_min_ms, P_.probe_max_ms);
}

void Node::probe_tick() {
    int64_t now = now_ns();
    double nic = fabric_.nic_utilisation(self_, now);
    nic_ewma_.add(nic, now);
    refill_tokens();

    double interval = next_probe_interval_ms();
    cur_interval_ms_ = interval;
    bool adaptive = opt_.probe == ProbeMode::Adaptive;
    // Prefer quiet periods: defer while our own NIC is busy, but never past probe_max.
    if (adaptive && nic > P_.busy_nic_util && last_probe_ns_ && ns_to_s(now - last_probe_ns_) * 1e3 < P_.probe_max_ms) {
        ++probes_deferred_;
        timers_.after_ms(100 + rng_.uniform(0, 60), [this] { probe_tick(); });
        return;
    }
    last_probe_ns_ = now;

    // Peer selection: oldest direct measurement first (unknown peers are oldest).
    std::vector<std::pair<double, NodeId>> cand;
    for (int i = 0; i < view_.size(); ++i) {
        if (i == self_) continue;
        const PeerLink& l = view_.link(NodeId(i));
        double age = l.last_lat_ns ? ns_to_s(now - l.last_lat_ns) : 1e6;
        cand.push_back({age + rng_.uniform(0, 0.3), NodeId(i)});
    }
    std::sort(cand.begin(), cand.end(), [](auto& a, auto& b) { return a.first > b.first; });
    int fan = std::min<int>(P_.probe_fanout, int(cand.size()));
    NodeRecord me = make_self_record();
    view_.set_self(me, now);
    for (int k = 0; k < fan; ++k) {
        NodeId peer = cand[size_t(k)].second;
        PeerLink& L = view_.link(peer);
        bool want_bw = (probe_count_[peer] % uint32_t(std::max(1.0, P_.bw_probe_every)) == 0) || !L.bw.valid();
        if (adaptive && nic > 0.5 * P_.busy_nic_util) want_bw = false;  // bandwidth probes only in quiet periods
        ProbeMsg m;
        m.self = me;
        for (NodeId g : view_.gossip_targets(peer, P_.gossip_records, now)) m.gossip.push_back(view_.entry(g).rec);
        Writer w0;
        m.kind = ProbeKind::Latency;
        m.probe_seq = ++probe_seq_;
        m.write(w0);
        uint32_t wire = uint32_t(w0.size() + sizeof(MsgHeader) + 28);
        uint32_t need = wire + (want_bw ? 2u * uint32_t(P_.bw_probe_bytes) + 400u : 0u);
        if (tokens_ < need) {
            if (tokens_ >= wire) want_bw = false, need = wire;
            else { ++probes_budget_skips_; continue; }
        }
        tokens_ -= need;
        send(peer, MsgType::Probe, w0, kFlagTelemetry);
        probes_out_[m.probe_seq] = {peer, now, ProbeKind::Latency, wire};
        ++lat_probes_;
        L.probes_sent++;
        if (want_bw) {
            ProbeMsg a;
            a.self = me;
            a.pad = uint32_t(P_.bw_probe_bytes);
            a.kind = ProbeKind::PairFirst;
            a.probe_seq = ++probe_seq_;
            Writer wa;
            a.write(wa);
            ProbeMsg b = a;
            b.kind = ProbeKind::PairSecond;
            b.probe_seq = ++probe_seq_;
            Writer wb;
            b.write(wb);
            uint32_t wire_b = uint32_t(wb.size() + sizeof(MsgHeader) + 28);
            send(peer, MsgType::Probe, wa, kFlagTelemetry);
            send(peer, MsgType::Probe, wb, kFlagTelemetry);
            probes_out_[a.probe_seq] = {peer, now, ProbeKind::PairFirst, wire_b};
            probes_out_[b.probe_seq] = {peer, now, ProbeKind::PairSecond, wire_b};
            ++bw_probes_;
        }
        probe_count_[peer]++;
    }
    timers_.after_ms(interval * rng_.uniform(0.9, 1.1), [this] { probe_tick(); });
}

void Node::on_probe(const Datagram& dg) {
    Reader r = dg.reader();
    ProbeMsg m = ProbeMsg::read(r);
    merge_record(m.self);
    for (auto& g : m.gossip) merge_record(g);
    NodeId src = dg.h.src;
    ProbeAckMsg a;
    a.probe_seq = m.probe_seq;
    a.kind = m.kind;
    a.probe_tx_ns = dg.h.tx_ns;
    a.probe_rx_hw_ns = dg.h.rx_hw_ns;
    if (m.kind == ProbeKind::PairFirst) {
        pair_first_rx_[{src, m.probe_seq}] = dg.h.rx_hw_ns;
        return;  // only the second of the pair is acknowledged
    }
    if (m.kind == ProbeKind::PairSecond) {
        auto it = pair_first_rx_.find({src, m.probe_seq - 1});
        if (it == pair_first_rx_.end()) return;  // first lost: no estimate
        a.pair_gap_ns = dg.h.rx_hw_ns - it->second;
        a.pair_bytes = uint32_t(dg.body.size() + sizeof(MsgHeader) + 28);
        pair_first_rx_.erase(it);
    }
    a.self = make_self_record();
    // Acks are telemetry too: they draw on the same token bucket. When it is empty we still
    // answer (the prober needs the timestamp) but drop the piggybacked gossip.
    refill_tokens();
    if (m.kind == ProbeKind::Latency && tokens_ > 4000)
        for (NodeId g : view_.gossip_targets(src, P_.gossip_records, now_ns())) a.gossip.push_back(view_.entry(g).rec);
    a.ack_tx_ns = now_ns();
    Writer w;
    a.write(w);
    tokens_ -= double(w.size() + sizeof(MsgHeader) + 28);
    send(src, MsgType::ProbeAck, w, kFlagTelemetry);
}

void Node::on_probe_ack(const Datagram& dg) {
    Reader r = dg.reader();
    ProbeAckMsg m = ProbeAckMsg::read(r);
    merge_record(m.self);
    for (auto& g : m.gossip) merge_record(g);
    int64_t now = now_ns();
    NodeId src = dg.h.src;
    PeerLink& L = view_.link(src);
    if (m.kind == ProbeKind::Latency) {
        // RTT from hardware timestamps, minus the responder's processing time.
        int64_t rtt = (dg.h.rx_hw_ns - m.probe_tx_ns) - (m.ack_tx_ns - m.probe_rx_hw_ns);
        double ow = std::max(1e-6, ns_to_s(rtt) / 2.0);
        double prev = L.lat.valid() ? L.lat.mean : ow;
        L.lat.add(ow, now);
        // relative movement, floored at 0.5 ms so microsecond-scale jitter on short links is not "volatility"
        L.lat_change = 0.7 * L.lat_change + 0.3 * std::min(1.0, std::fabs(ow - prev) / std::max(prev, 5e-4));
        L.last_lat_ns = now;
    } else if (m.kind == ProbeKind::PairSecond && m.pair_gap_ns > 0) {
        double bw = double(m.pair_bytes) / ns_to_s(m.pair_gap_ns);
        L.bw.add(bw, now);
        L.last_bw_ns = now;
    }
    L.acks++;
    L.loss.add(0.0, now);
    probes_out_.erase(m.probe_seq);
    if (m.kind == ProbeKind::PairSecond) probes_out_.erase(m.probe_seq - 1);
}

void Node::probe_timeouts() {
    int64_t now = now_ns();
    for (auto it = probes_out_.begin(); it != probes_out_.end();) {
        if (now - it->second.tx_ns > ms_to_ns(2000)) {
            view_.link(it->second.peer).loss.add(1.0, now);
            it = probes_out_.erase(it);
        } else ++it;
    }
    for (auto it = pair_first_rx_.begin(); it != pair_first_rx_.end();) {
        if (now - it->second > ms_to_ns(5000)) it = pair_first_rx_.erase(it);
        else ++it;
    }
}

}  // namespace nexus
