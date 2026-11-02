// Executor role: reservation admission, local queue with backfilling, input
// staging over the fabric, execution on worker threads, completion feedback,
// opportunistic re-delegation (migration) of queued work, and the data server.
#include "nexus/node.hpp"

namespace nexus {

int Node::tenant_cores(uint8_t tenant) const {
    int s = 0;
    for (auto& [k, lt] : local_) if (lt.d.spec.tenant == tenant) s += lt.d.spec.cores;
    for (auto& [t, r] : reservations_) if (r.tenant == tenant) s += r.cores;
    return s;
}

bool Node::can_admit(const TaskSpec& t, NackReason* why) {
    auto no = [&](NackReason r) { if (why) *why = r; return false; };
    if (t.gpus > 0 && !gpu_ok_) return no(NackReason::Health);
    if (t.cores > spec_.cores || int(t.mem_gb) > spec_.mem_gb || t.gpus > spec_.gpus) return no(NackReason::Capacity);
    if (!cfg_.tenant_allowed(t.tenant, self_)) return no(NackReason::Policy);
    double quota = cfg_.tenants[t.tenant].node_quota_frac * spec_.cores;
    if (tenant_cores(t.tenant) + t.cores > quota + 1e-9) return no(NackReason::Quota);
    int queued = int(reservations_.size());
    for (auto& [k, lt] : local_) if (lt.st == LState::Queued) ++queued;
    if (queued >= P_.max_queue) return no(NackReason::QueueFull);
    if (why) *why = NackReason::None;
    return true;
}

void Node::on_reserve_req(const Datagram& dg) {
    Reader r = dg.reader();
    ReserveReqMsg m = ReserveReqMsg::read(r);
    int64_t now = now_ns();
    ReserveRepMsg rep;
    rep.task = m.task;
    rep.attempt = m.attempt;
    auto ex = reservations_.find(m.task);
    if (ex != reservations_.end() && ex->second.attempt == m.attempt) {
        rep.granted = 1;  // duplicate request: idempotent re-grant
    } else {
        TaskSpec t;
        t.cores = m.cores;
        t.mem_gb = m.mem_gb;
        t.gpus = m.gpus;
        t.tenant = m.tenant;
        NackReason why;
        if (can_admit(t, &why)) {
            Reservation res;
            res.attempt = m.attempt;
            res.expiry = now + ms_to_ns(m.lease_ms);
            res.cores = m.cores;
            res.gpus = m.gpus;
            res.tenant = m.tenant;
            res.est_s = m.est_s;
            reservations_[m.task] = res;
            rep.granted = 1;
            n_grants_++;
        } else {
            rep.granted = 0;
            rep.reason = why;
            n_denies_++;
        }
    }
    rep.fresh = make_self_record();
    Writer w;
    rep.write(w);
    send(dg.h.src, MsgType::ReserveRep, w);
}

void Node::on_delegate(const Datagram& dg) {
    Reader r = dg.reader();
    DelegateMsg d = DelegateMsg::read(r);
    Key k{d.spec.id, d.attempt};
    auto ack = [&] {
        AckMsg a{d.spec.id, d.attempt};
        Writer w;
        a.write(w);
        send(dg.h.src, MsgType::DelegateAck, w);
    };
    if (local_.count(k) || done_unacked_.count(k)) { ack(); return; }
    if (d.reserved) {
        auto it = reservations_.find(d.spec.id);
        if (it != reservations_.end()) reservations_.erase(it);
    }
    local_enqueue(d);
    ack();
}

void Node::local_enqueue(const DelegateMsg& d) {
    Key k{d.spec.id, d.attempt};
    LocalTask lt;
    lt.d = d;
    lt.enq_ns = now_ns();
    TaskSpec& t = lt.d.spec;
    double nominal = t.gpus > 0 ? t.work / (double(t.gpus) * spec_.gpu_speed)
                                : t.work / (double(std::max<uint16_t>(1, t.cores)) * spec_.cpu_speed);
    lt.est_exec = nominal * (corr_[int(t.cls)].valid() ? corr_[int(t.cls)].mean : 1.0);
    local_[k] = std::move(lt);
    order_.push_back(k);
    try_dispatch();
}

void Node::try_dispatch() {
    int64_t now = now_ns();
    std::vector<Key> keys(order_.begin(), order_.end());
    bool head_seen = false;
    for (auto& k : keys) {
        auto it = local_.find(k);
        if (it == local_.end()) continue;
        LocalTask& lt = it->second;
        if (lt.st != LState::Queued || lt.migrating) continue;
        const TaskSpec& t = lt.d.spec;
        if (t.gpus > 0 && !gpu_ok_) { fail_local(k, 2); continue; }
        bool fits = t.cores <= cores_free_ && int(t.mem_gb) <= mem_free_ && t.gpus <= gpus_free_;
        if (fits) {
            start_staging(lt);
        } else if (!head_seen && now - lt.enq_ns > s_to_ns(5.0)) {
            break;  // protect a starving head task from being bypassed forever
        }
        head_seen = true;
    }
}

void Node::report(MsgType type, const LocalTask& lt, uint8_t reason) {
    int64_t now = now_ns();
    TaskReportMsg m;
    m.task = lt.d.spec.id;
    m.attempt = lt.d.attempt;
    m.node = self_;
    m.reason = reason;
    m.wait_s = float(ns_to_s((lt.stage_ns ? lt.stage_ns : now) - lt.enq_ns));
    m.xfer_s = lt.run_ns ? float(ns_to_s(lt.run_ns - lt.stage_ns)) : 0.f;
    m.exec_s = lt.run_ns ? float(ns_to_s(now - lt.run_ns)) : 0.f;
    m.pred_wait = lt.d.pred_wait;
    m.pred_xfer = lt.d.pred_xfer;
    m.pred_exec = lt.d.pred_exec;
    m.in_bytes = lt.in_bytes;
    m.net_bytes = lt.net_bytes;
    m.out_bytes = lt.d.spec.out_bytes;
    m.start_ns = lt.stage_ns;
    m.end_ns = now;
    Writer w;
    m.write(w);
    send(lt.d.home, type, w);
    if (type == MsgType::TaskDone) {
        done_unacked_[{m.task, m.attempt}] = {m, lt.d.home, 0};
        done_last_send_[{m.task, m.attempt}] = now;
    }
    // monitoring-plane copy for the workload driver / dashboard
    json::Value ev;
    ev["type"] = type == MsgType::TaskStarted ? "task_started" : (type == MsgType::TaskDone ? "task_done" : "task_failed");
    ev["t"] = exp_offset_s();
    ev["node"] = int(self_);
    ev["home"] = int(lt.d.home);
    ev["job"] = double(lt.d.spec.job);
    ev["idx"] = int(lt.d.spec.index);
    ev["attempt"] = double(lt.d.attempt);
    ev["cls"] = class_name(lt.d.spec.cls);
    ev["cores"] = int(lt.d.spec.cores);
    ev["gpus"] = int(lt.d.spec.gpus);
    ev["migrations"] = int(lt.d.migrations);
    if (type != MsgType::TaskStarted) {
        ev["wait"] = m.wait_s;
        ev["xfer"] = m.xfer_s;
        ev["exec"] = m.exec_s;
        ev["pred_wait"] = m.pred_wait;
        ev["pred_xfer"] = m.pred_xfer;
        ev["pred_exec"] = m.pred_exec;
        ev["in_bytes"] = double(m.in_bytes);
        ev["net_bytes"] = double(m.net_bytes);
        ev["reason"] = int(reason);
    }
    emit(ev);
}

void Node::start_staging(LocalTask& lt) {
    const TaskSpec& t = lt.d.spec;
    cores_free_ -= t.cores;
    mem_free_ -= int(t.mem_gb);
    gpus_free_ -= t.gpus;
    lt.st = LState::Staging;
    lt.stage_ns = now_ns();
    report(MsgType::TaskStarted, lt);
    lt.fetches.clear();
    lt.inputs_left = int(t.inputs.size());
    for (size_t i = 0; i < t.inputs.size(); ++i) {
        const auto& in = t.inputs[i];
        lt.in_bytes += in.bytes;
        FetchState f;
        f.src = in.src != kNoNode ? in.src : (in.alts.empty() ? self_ : in.alts.front());
        f.bytes = in.bytes;
        f.input_idx = i;
        lt.fetches.push_back(f);
    }
    Key k{t.id, lt.d.attempt};
    if (lt.inputs_left == 0) { start_run(lt); return; }
    for (auto& f : lt.fetches) {
        if (f.src == self_) {
            double secs = double(f.bytes) / (spec_.disk_gbps * 1e9 / 8.0);
            size_t idx = f.input_idx;
            timers_.after_ms(secs * 1e3, [this, k, idx] {
                auto it = local_.find(k);
                if (it == local_.end() || it->second.st != LState::Staging) return;
                auto& ff = it->second.fetches[idx];
                if (ff.done) return;
                ff.done = true;
                input_ready(it->second);
            });
        } else {
            start_fetch(lt, f, true);
        }
    }
}

// new_xfer=false re-queries an existing transfer (the source replays its completion if it
// already finished, or ignores the duplicate if it is still streaming).
void Node::start_fetch(LocalTask& lt, FetchState& f, bool new_xfer) {
    int64_t now = now_ns();
    if (new_xfer || f.xfer_id == 0) {
        f.xfer_id = next_xfer_++;
        f.sent_ns = now;
    }
    NetEst e = view_.net(f.src, self_, now);
    double bw = std::max(40e6, e.known ? e.bw_Bps : 0.0);  // liveness timer, not a prediction
    f.deadline = now + s_to_ns(3.0 + 3.0 * double(f.bytes) / bw);
    FetchMsg m;
    m.task = lt.d.spec.id;
    m.attempt = lt.d.attempt;
    m.xfer_id = f.xfer_id;
    m.bytes = f.bytes;
    Writer w;
    m.write(w);
    send(f.src, MsgType::Fetch, w);
}

void Node::on_data_done(const Datagram& dg) {
    Reader r = dg.reader();
    DataDoneMsg m = DataDoneMsg::read(r);
    auto it = local_.find({m.task, m.attempt});
    if (it == local_.end() || it->second.st != LState::Staging) return;
    LocalTask& lt = it->second;
    for (auto& f : lt.fetches) {
        if (f.xfer_id != m.xfer_id || f.done) continue;
        if (m.ok == 2) {
            // still streaming (the source is shared by other transfers): extend the liveness timer
            NetEst e = view_.net(f.src, self_, now_ns());
            double bw = std::max(20e6, e.known ? e.bw_lb_Bps : 0.0);
            f.deadline = now_ns() + s_to_ns(3.0 + 2.5 * double(m.bytes) / bw);
            f.retries = 0;
            return;
        }
        f.done = true;
        lt.net_bytes += f.bytes;
        // Passive measurement: a real transfer is the best bandwidth probe there is.
        double dur = ns_to_s(dg.h.rx_hw_ns - f.sent_ns);
        PeerLink& L = view_.link(f.src);
        double lat = L.lat.valid() ? L.lat.mean : 0.0005;
        if (f.retries == 0 && f.bytes >= (4ull << 20) && dur > 3 * lat) {
            L.bw.add(double(f.bytes) / std::max(1e-3, dur - 2 * lat), now_ns());
            L.last_bw_ns = now_ns();
        }
        input_ready(lt);
        return;
    }
}

void Node::input_ready(LocalTask& lt) {
    if (--lt.inputs_left <= 0) start_run(lt);
}

void Node::start_run(LocalTask& lt) {
    lt.st = LState::Running;
    lt.run_ns = now_ns();
    lt.true_exec = true_exec(lt.d.spec);
    lt.cancel = std::make_shared<std::atomic<bool>>(false);
    {
        std::lock_guard<std::mutex> g(wmu_);
        wq_.push_back({{lt.d.spec.id, lt.d.attempt}, lt.true_exec, lt.cancel});
    }
    wcv_.notify_one();
}

void Node::on_completion(const Completion& c) {
    auto it = local_.find(c.key);
    if (it == local_.end()) return;
    if (c.cancelled) { fail_local(c.key, 2); return; }
    LocalTask& lt = it->second;
    const TaskSpec& t = lt.d.spec;
    cores_free_ += t.cores;
    mem_free_ += int(t.mem_gb);
    gpus_free_ += t.gpus;
    double exec_s = ns_to_s(now_ns() - lt.run_ns);
    double nominal = t.gpus > 0 ? t.work / (double(t.gpus) * spec_.gpu_speed)
                                : t.work / (double(std::max<uint16_t>(1, t.cores)) * spec_.cpu_speed);
    corr_[int(t.cls)].add(exec_s / std::max(1e-3, nominal));  // feedback: observed vs advertised
    fail_ewma_.add(0.0);
    n_completed_++;
    report(MsgType::TaskDone, lt);
    order_.erase(std::remove(order_.begin(), order_.end(), c.key), order_.end());
    local_.erase(it);
    try_dispatch();
}

void Node::fail_local(const Key& k, uint8_t reason) {
    auto it = local_.find(k);
    if (it == local_.end()) return;
    LocalTask& lt = it->second;
    const TaskSpec& t = lt.d.spec;
    if (lt.st != LState::Queued) {
        cores_free_ += t.cores;
        mem_free_ += int(t.mem_gb);
        gpus_free_ += t.gpus;
    }
    if (lt.cancel) *lt.cancel = true;
    // stop the transfers this attempt no longer needs, so they stop consuming the network
    for (auto& f : lt.fetches) {
        if (f.done || f.src == self_ || f.xfer_id == 0) continue;
        FetchMsg c{t.id, lt.d.attempt, f.xfer_id, 0};
        Writer w;
        c.write(w);
        send(f.src, MsgType::Fetch, w);
    }
    fail_ewma_.add(1.0);
    n_failed_++;
    report(MsgType::TaskFailed, lt, reason);
    order_.erase(std::remove(order_.begin(), order_.end(), k), order_.end());
    local_.erase(it);
}

void Node::exec_tick() {
    int64_t now = now_ns();
    for (auto it = reservations_.begin(); it != reservations_.end();) {
        if (now > it->second.expiry) it = reservations_.erase(it);
        else ++it;
    }
    // fetch timeouts: retry, then fall back to another replica, then give up
    std::vector<Key> failed;
    for (auto& [k, lt] : local_) {
        if (lt.st != LState::Staging) continue;
        for (auto& f : lt.fetches) {
            if (f.done || f.src == self_ || now <= f.deadline) continue;
            f.retries++;
            const auto& in = lt.d.spec.inputs[f.input_idx];
            // Only a source we believe dead is abandoned (and its transfer cancelled); a slow but
            // live source is re-queried, which never starts a duplicate copy of the data.
            bool dead = view_.status(f.src, now) == PeerStatus::Suspected;
            bool switched = false;
            if (dead) {
                NodeId old = f.src;
                uint32_t old_id = f.xfer_id;
                for (NodeId a : in.alts)
                    if (a != f.src && (a == self_ || view_.usable(a, now))) { f.src = a; switched = true; break; }
                if (switched) {
                    FetchMsg c{lt.d.spec.id, lt.d.attempt, old_id, 0};
                    Writer w;
                    c.write(w);
                    send(old, MsgType::Fetch, w);
                    f.retries = 0;
                }
            }
            if (!switched && (f.retries > 8 || (dead && f.retries > 2))) { failed.push_back(k); break; }
            if (f.src == self_) { f.done = true; input_ready(lt); continue; }
            start_fetch(lt, f, switched);
        }
    }
    for (auto& k : failed) fail_local(k, 3);
    // reliable completion reports
    for (auto it = done_unacked_.begin(); it != done_unacked_.end();) {
        DoneRec& d = it->second;
        int64_t& last = done_last_send_[it->first];
        if (now - last > ms_to_ns(1000)) {
            if (++d.sends > 15) {
                done_last_send_.erase(it->first);
                it = done_unacked_.erase(it);
                continue;
            }
            Writer w;
            d.msg.write(w);
            send(d.home, MsgType::TaskDone, w);
            last = now;
        }
        ++it;
    }
    // migrations: unanswered reservations are abandoned; unacknowledged hand-offs are resent,
    // and after three tries the task simply stays here (a duplicate run is possible, never a loss)
    for (auto it = migrating_.begin(); it != migrating_.end();) {
        Migration& mg = it->second;
        if (now <= mg.deadline) { ++it; continue; }
        if (mg.phase == 1 && mg.sends < 3) {
            Writer w;
            mg.d.write(w);
            send(mg.target, MsgType::Delegate, w);
            mg.sends++;
            mg.deadline = now + ms_to_ns(800);
            ++it;
            continue;
        }
        auto lit = local_.find({it->first, mg.attempt});
        if (lit != local_.end()) lit->second.migrating = false;
        it = migrating_.erase(it);
    }
    // old served transfers
    for (auto it = served_xfers_.begin(); it != served_xfers_.end();) {
        if (now - it->second > s_to_ns(30)) it = served_xfers_.erase(it);
        else ++it;
    }
    probe_timeouts();
    try_dispatch();
}

void Node::rebalance_tick() {
    if (!migrating_.empty()) return;
    int64_t now = now_ns();
    refresh_self();
    for (auto& k : order_) {
        auto it = local_.find(k);
        if (it == local_.end()) continue;
        LocalTask& lt = it->second;
        if (lt.st != LState::Queued || lt.migrating || lt.d.migrations >= P_.max_migrations) continue;
        if (now - lt.enq_ns < s_to_ns(1.0)) continue;
        const TaskSpec& t = lt.d.spec;
        // Our own cost for this task: the work queued ahead of it on this node.
        double ahead = 0;
        for (auto& [k2, o] : local_) {
            if (o.st != LState::Queued) ahead += o.est_exec * o.d.spec.cores;  // running work
        }
        for (auto& k2 : order_) {
            if (k2 == k) break;
            auto o = local_.find(k2);
            if (o != local_.end() && o->second.st == LState::Queued) ahead += o->second.est_exec * o->second.d.spec.cores;
        }
        double my_cost = ahead / std::max(1, spec_.cores) + lt.est_exec;
        auto rk = cost_.rank(t, view_, pending_, now);
        const CostBreakdown* best = nullptr;
        for (auto& c : rk) {
            if (c.feasible && c.node == self_) my_cost += class_weights(t.cls).wx * c.xfer;  // staging still ahead of us
            if (c.feasible && c.node != self_ && !best) best = &c;
        }
        if (!best || best->total >= my_cost * (1.0 - P_.migrate_gain)) continue;
        ReserveReqMsg q;
        q.task = t.id;
        q.attempt = lt.d.attempt;
        q.home = lt.d.home;
        q.cores = t.cores;
        q.mem_gb = t.mem_gb;
        q.gpus = t.gpus;
        q.tenant = t.tenant;
        q.cls = t.cls;
        q.est_s = float(best->exec);
        q.lease_ms = uint32_t(P_.reservation_lease_ms);
        Writer w;
        q.write(w);
        send(best->node, MsgType::ReserveReq, w);
        Migration mg;
        mg.attempt = lt.d.attempt;
        mg.target = best->node;
        mg.deadline = now + ms_to_ns(P_.reservation_lease_ms * 0.5);
        mg.choice = *best;
        migrating_[t.id] = mg;
        lt.migrating = true;
        break;  // one migration in flight per node
    }
}

// ---------------------------------------------------------------- data server
void Node::on_fetch(const Datagram& dg) {
    Reader r = dg.reader();
    FetchMsg m = FetchMsg::read(r);
    std::pair<NodeId, uint32_t> sk{dg.h.src, m.xfer_id};
    auto find_out = [&]() {
        for (auto it = outx_.begin(); it != outx_.end(); ++it)
            if (it->second.to == dg.h.src && it->second.xfer_id == m.xfer_id) return it;
        return outx_.end();
    };
    if (m.bytes == 0) {  // cancellation: the requester gave up on this transfer
        auto it = find_out();
        if (it != outx_.end()) outx_.erase(it);
        served_xfers_[sk] = now_ns();
        return;
    }
    auto sit = served_xfers_.find(sk);
    if (sit != served_xfers_.end()) {
        // A re-query never starts a second copy: report completion again, or progress so far.
        DataDoneMsg d{m.task, m.attempt, m.xfer_id, m.bytes, 1};
        if (sit->second == 0) {
            auto it = find_out();
            d.ok = 2;
            d.bytes = it != outx_.end() ? it->second.left : 0;
        }
        Writer w;
        d.write(w);
        send(dg.h.src, MsgType::DataDone, w);
        return;
    }
    served_xfers_[sk] = 0;
    uint64_t key = next_out_key_++;
    outx_[key] = {dg.h.src, m.task, m.attempt, m.xfer_id, m.bytes, m.bytes};
    xfer_step(key);
}

void Node::xfer_step(uint64_t key) {
    auto it = outx_.find(key);
    if (it == outx_.end()) return;
    OutXfer& x = it->second;
    int64_t now = now_ns();
    uint64_t chunk = std::min<uint64_t>(Fabric::kChunkBytes, x.left);
    int64_t arrival = fabric_.bulk_chunk(self_, x.to, chunk, now);
    x.left -= chunk;
    if (x.left > 0) {
        // pace: reserve the next chunk when this one drains, so concurrent flows interleave
        timers_.at(arrival, [this, key] { xfer_step(key); });
    } else {
        OutXfer done = x;
        outx_.erase(it);
        auto notify = [this, done] {
            DataDoneMsg d{done.task, done.attempt, done.xfer_id, done.total, 1};
            Writer w;
            d.write(w);
            send(done.to, MsgType::DataDone, w);
            served_xfers_[{done.to, done.xfer_id}] = now_ns();
        };
        // the completion notice is a datagram: send it twice, 250 ms apart, to ride out a drop
        timers_.at(arrival, notify);
        timers_.at(arrival + ms_to_ns(250), notify);
    }
}

}  // namespace nexus
