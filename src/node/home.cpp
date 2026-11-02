// Home-scheduler role: job intake, dependency tracking, placement decisions,
// reservation/delegation protocol, fault-aware rescheduling, and the coordinator
// logic used by the FIFO and centralized baselines.
#include "nexus/node.hpp"

namespace nexus {

void Node::on_submit(const Datagram& dg) {
    Reader r = dg.reader();
    SubmitMsg m = SubmitMsg::read(r);
    int64_t now = now_ns();
    auto ack = [&] {
        Writer w;
        w.put(m.job.id);
        w.put(m.epoch);
        send(dg.h.src, MsgType::SubmitAck, w, kFlagMgmt);
    };
    auto ex = jobs_.find(m.job.id);
    if (ex != jobs_.end() && ex->second.epoch >= m.epoch) { ack(); return; }
    HomeJob j;
    j.spec = m.job;
    j.epoch = m.epoch;
    size_t n = j.spec.tasks.size();
    j.done.assign(n, 0);
    j.out_loc.assign(n, kNoNode);
    for (auto& d : m.done)
        if (d.first < n) { j.done[d.first] = 1; j.out_loc[d.first] = d.second; }
    j.remaining = 0;
    for (size_t i = 0; i < n; ++i) j.remaining += j.done[i] ? 0 : 1;
    jobs_[m.job.id] = j;
    auto& J = jobs_[m.job.id];
    for (size_t i = 0; i < n; ++i) {
        if (J.done[i]) continue;
        auto& t = J.spec.tasks[i];
        HomeTask ht;
        ht.job = J.spec.id;
        ht.idx = uint16_t(i);
        ht.epoch = m.epoch;
        bool ready = true;
        for (auto d : t.deps) if (!J.done[d]) ready = false;
        ht.st = ready ? HState::Ready : HState::WaitDeps;
        ht.ready_ns = now;
        htasks_[t.id] = ht;
    }
    ack();
    sched_dirty_ = true;
}

void Node::fill_input_sources(TaskSpec& t, const HomeJob& j) {
    int64_t now = now_ns();
    for (auto& in : t.inputs) {
        in.alts.clear();
        if (in.kind == InputKind::Parent) {
            if (in.ref < j.out_loc.size() && j.out_loc[in.ref] != kNoNode) in.alts.push_back(j.out_loc[in.ref]);
        } else {
            // dataset catalogue = advertised holdings of live nodes (incl. ourselves)
            for (int i = 0; i < view_.size(); ++i) {
                const auto& e = view_.entry(NodeId(i));
                if (!e.known || in.ref >= 64) continue;
                if (!(e.rec.datasets & (1ull << in.ref))) continue;
                if (i != self_ && view_.status(NodeId(i), now) == PeerStatus::Suspected) continue;
                in.alts.push_back(NodeId(i));
            }
        }
    }
}

void Node::home_schedule() {
    if (is_coordinated(opt_.policy)) {
        if (is_coord_) coordinator_schedule();
        return;
    }
    refresh_self();
    int64_t now = now_ns();
    for (auto& [id, ht] : htasks_)
        if (ht.st == HState::Ready && ht.not_before <= now) decide(id, ht);
}

void Node::decision_event(TaskId id, const HomeTask& ht, const std::vector<CostBreakdown>& rk, NodeId chosen,
                          const char* mode) {
    json::Value ev;
    ev["type"] = "decision";
    ev["t"] = exp_offset_s();
    ev["home"] = int(self_);
    ev["task"] = fmt("%u.%u", ht.job, ht.idx);
    ev["cls"] = class_name(jobs_.at(ht.job).spec.tasks[ht.idx].cls);
    ev["attempt"] = int(ht.attempt_n);
    ev["chosen"] = int(chosen);
    ev["mode"] = mode;
    ev["policy"] = policy_name(opt_.policy);
    json::Value cs = json::Value::array();
    int k = 0;
    for (auto& c : rk) {
        if (k++ >= 6) break;
        json::Value e;
        e["node"] = int(c.node);
        e["ok"] = c.feasible;
        if (!c.feasible) e["why"] = c.reason;
        else {
            e["total"] = c.total;
            e["wait"] = c.wait;
            e["xfer"] = c.xfer;
            e["exec"] = c.exec;
            e["risk"] = c.risk;
            e["unc"] = c.uncert;
        }
        cs.push(e);
    }
    ev["cands"] = cs;
    (void)id;
    emit(ev);
}

void Node::decide(TaskId id, HomeTask& ht) {
    int64_t t0 = now_ns();
    auto& job = jobs_.at(ht.job);
    TaskSpec t = job.spec.tasks[ht.idx];
    fill_input_sources(t, job);
    auto rk = cost_.rank(t, view_, pending_, t0);
    const CostBreakdown* best = nullptr;
    const CostBreakdown* selfc = nullptr;
    for (auto& c : rk) {
        if (!c.feasible) continue;
        if (c.node == self_) { selfc = &c; continue; }
        if (ht.excluded.count(c.node)) continue;
        if (!best) best = &c;
    }
    NackReason why;
    bool self_ok = selfc && !ht.excluded.count(self_) && can_admit(t, &why);
    n_decisions_++;
    if (!best && !self_ok) {
        ht.not_before = t0 + ms_to_ns(400);
        ht.excluded.clear();
        ht.reserve_tries = 0;
        return;
    }
    bool go_local = self_ok && (!best || selfc->total <= best->total * (1.0 + P_.delegate_margin));
    if (go_local) {
        ht.choice = *selfc;
        send_delegate(id, ht, self_, *selfc, false);
        decision_event(id, ht, rk, self_, "local");
    } else {
        ht.choice = *best;
        ReserveReqMsg q;
        q.task = id;
        q.attempt = ht.attempt();
        q.home = self_;
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
        ht.st = HState::Reserving;
        ht.exec = best->node;
        ht.deadline = t0 + ms_to_ns(P_.reservation_lease_ms * 0.5);
        // build the delegate now so the grant can be acted on immediately
        ht.pending_delegate.spec = t;
        decision_event(id, ht, rk, best->node, "reserve");
    }
    decision_ms_.add(ns_to_s(now_ns() - t0) * 1e3);
}

void Node::send_delegate(TaskId id, HomeTask& ht, NodeId target, const CostBreakdown& c, bool reserved) {
    int64_t now = now_ns();
    auto& job = jobs_.at(ht.job);
    TaskSpec t = job.spec.tasks[ht.idx];
    fill_input_sources(t, job);
    for (size_t i = 0; i < t.inputs.size() && i < c.srcs.size(); ++i) t.inputs[i].src = c.srcs[i];
    DelegateMsg d;
    d.spec = t;
    d.attempt = ht.attempt();
    d.home = self_;
    d.pred_exec = float(c.exec);
    d.pred_xfer = float(c.xfer);
    d.pred_wait = float(c.wait);
    d.reserved = reserved ? 1 : 0;
    ht.pending_delegate = d;
    ht.decided_ns = now;
    ht.exec = target;
    ht.st = HState::Delegated;
    if (target == self_) {
        ht.acked = true;
        n_local_++;
        local_enqueue(d);
    } else {
        ht.acked = false;
        ht.delegate_sends = 1;
        ht.deadline = now + ms_to_ns(1000);
        Writer w;
        d.write(w);
        send(target, MsgType::Delegate, w);
        n_delegated_++;
        pending_.add(target, now, c.exec * t.cores, t.gpus ? c.exec * t.gpus : 0.0, t.cores, t.gpus);
    }
    if (is_coord_) {
        auto& o = outstanding_[target];
        o[0] += t.cores;
        o[1] += int(t.mem_gb);
        o[2] += t.gpus;
    }
    (void)id;
}

void Node::reschedule(TaskId id, HomeTask& ht, const char* why, NodeId bad) {
    if (is_coord_ && ht.exec != kNoNode) {
        auto& t = spec_of(ht);
        auto& o = outstanding_[ht.exec];
        o[0] -= t.cores;
        o[1] -= int(t.mem_gb);
        o[2] -= t.gpus;
    }
    json::Value ev;
    ev["type"] = "reschedule";
    ev["t"] = exp_offset_s();
    ev["home"] = int(self_);
    ev["task"] = fmt("%u.%u", ht.job, ht.idx);
    ev["why"] = why;
    ev["bad"] = bad == kNoNode ? -1 : int(bad);
    emit(ev);
    ht.attempt_n++;
    ht.st = HState::Ready;
    ht.exec = kNoNode;
    ht.excluded.clear();
    if (bad != kNoNode && bad != self_) ht.excluded.insert(bad);
    ht.reserve_tries = 0;
    ht.not_before = now_ns();
    n_reschedules_++;
    sched_dirty_ = true;
    (void)id;
}

void Node::on_reserve_rep(const Datagram& dg) {
    Reader r = dg.reader();
    ReserveRepMsg m = ReserveRepMsg::read(r);
    merge_record(m.fresh);
    NodeId src = dg.h.src;
    int64_t now = now_ns();

    // Reply to a migration we initiated as executor?
    auto mit = migrating_.find(m.task);
    if (mit != migrating_.end() && mit->second.attempt == m.attempt && mit->second.target == src &&
        mit->second.phase == 0) {
        Migration& mg = mit->second;
        Key k{m.task, m.attempt};
        auto lit = local_.find(k);
        if (lit == local_.end()) { migrating_.erase(mit); return; }
        LocalTask& lt = lit->second;
        if (!m.granted || lt.st != LState::Queued) {
            if (!m.granted) n_nacks_++;
            lt.migrating = false;
            migrating_.erase(mit);
            return;
        }
        // Hand-off: the task stays queued here (frozen) until the target acknowledges it.
        DelegateMsg d = lt.d;
        d.migrations++;
        d.reserved = 1;
        for (size_t i = 0; i < d.spec.inputs.size() && i < mg.choice.srcs.size(); ++i) d.spec.inputs[i].src = mg.choice.srcs[i];
        d.pred_exec = float(mg.choice.exec);
        d.pred_xfer = float(mg.choice.xfer);
        d.pred_wait = float(mg.choice.wait);
        mg.d = d;
        mg.phase = 1;
        mg.sends = 1;
        mg.deadline = now + ms_to_ns(800);
        Writer w;
        d.write(w);
        send(src, MsgType::Delegate, w);
        return;
    }

    auto it = htasks_.find(m.task);
    if (it == htasks_.end()) return;
    HomeTask& ht = it->second;
    if (ht.st != HState::Reserving || ht.attempt() != m.attempt || ht.exec != src) return;
    if (m.granted) {
        send_delegate(m.task, ht, src, ht.choice, true);
    } else {
        n_nacks_++;
        json::Value ev;
        ev["type"] = "nack";
        ev["t"] = exp_offset_s();
        ev["task"] = fmt("%u.%u", ht.job, ht.idx);
        ev["node"] = int(src);
        ev["why"] = nack_name(m.reason);
        emit(ev);
        ht.excluded.insert(src);
        ht.reserve_tries++;
        ht.st = HState::Ready;
        ht.exec = kNoNode;
        if (ht.reserve_tries >= P_.max_reserve_attempts) {
            ht.not_before = now + ms_to_ns(300);
            ht.excluded.clear();
            ht.reserve_tries = 0;
        }
        sched_dirty_ = true;
    }
}

void Node::on_task_query(const Datagram& dg) {
    Reader r = dg.reader();
    AckMsg q = AckMsg::read(r);
    if (dg.h.type == MsgType::TaskQuery) {
        Key k{q.task, q.attempt};
        uint8_t known = (local_.count(k) || done_unacked_.count(k) || migrating_.count(q.task)) ? 1 : 0;
        Writer w;
        q.write(w);
        w.put(known);
        send(dg.h.src, MsgType::TaskQueryRep, w);
        return;
    }
    uint8_t known = r.get<uint8_t>();
    auto it = htasks_.find(q.task);
    if (it == htasks_.end()) return;
    HomeTask& ht = it->second;
    if (ht.exec != dg.h.src || ht.attempt() != q.attempt) return;
    if (ht.st != HState::Delegated && ht.st != HState::Running) return;
    if (known) { ht.unknown_replies = 0; return; }
    // Two denials at least 3 s apart (a TaskMoved may still be in flight after the first).
    if (++ht.unknown_replies >= 2) reschedule(q.task, ht, "lost", kNoNode);
}

void Node::finish_migration(TaskId task) {
    auto mit = migrating_.find(task);
    if (mit == migrating_.end()) return;
    Migration mg = mit->second;
    migrating_.erase(mit);
    Key k{task, mg.attempt};
    auto lit = local_.find(k);
    if (lit == local_.end()) return;
    const DelegateMsg& d = mg.d;
    pending_.add(mg.target, now_ns(), mg.choice.exec * d.spec.cores, d.spec.gpus ? mg.choice.exec * d.spec.gpus : 0.0,
                 d.spec.cores, d.spec.gpus);
    TaskReportMsg tm;
    tm.task = task;
    tm.attempt = mg.attempt;
    tm.node = mg.target;
    Writer w2;
    tm.write(w2);
    send(d.home, MsgType::TaskMoved, w2);
    json::Value ev;
    ev["type"] = "migrate";
    ev["t"] = exp_offset_s();
    ev["task"] = fmt("%u.%u", d.spec.job, d.spec.index);
    ev["from"] = int(self_);
    ev["to"] = int(mg.target);
    ev["gain"] = mg.choice.total;
    emit(ev);
    order_.erase(std::remove(order_.begin(), order_.end(), k), order_.end());
    local_.erase(lit);
    n_migrations_++;
}

void Node::on_delegate_ack(const Datagram& dg) {
    Reader r = dg.reader();
    AckMsg a = AckMsg::read(r);
    auto mit = migrating_.find(a.task);
    if (mit != migrating_.end() && mit->second.phase == 1 && mit->second.attempt == a.attempt &&
        mit->second.target == dg.h.src) {
        finish_migration(a.task);
        return;
    }
    auto it = htasks_.find(a.task);
    if (it == htasks_.end()) return;
    if (it->second.attempt() == a.attempt && it->second.st == HState::Delegated) it->second.acked = true;
}

void Node::on_task_report(const Datagram& dg) {
    Reader r = dg.reader();
    TaskReportMsg m = TaskReportMsg::read(r);
    if (dg.h.type == MsgType::TaskDone) {
        AckMsg a{m.task, m.attempt};
        Writer w;
        a.write(w);
        send(dg.h.src, MsgType::DoneAck, w);
    }
    auto it = htasks_.find(m.task);
    if (it == htasks_.end()) return;
    HomeTask& ht = it->second;
    if (ht.st == HState::Done) {
        if (dg.h.type == MsgType::TaskDone) n_dup_done_++;
        return;
    }
    if ((m.attempt >> 16) != ht.epoch) return;
    switch (dg.h.type) {
        case MsgType::TaskStarted:
            if (m.attempt == ht.attempt()) { ht.st = HState::Running; ht.exec = m.node; ht.acked = true; }
            break;
        case MsgType::TaskMoved:
            if (m.attempt == ht.attempt()) {
                if (is_coord_ && ht.exec != kNoNode) {
                    auto& t = spec_of(ht);
                    auto& o1 = outstanding_[ht.exec];
                    o1[0] -= t.cores; o1[1] -= int(t.mem_gb); o1[2] -= t.gpus;
                    auto& o2 = outstanding_[m.node];
                    o2[0] += t.cores; o2[1] += int(t.mem_gb); o2[2] += t.gpus;
                }
                ht.exec = m.node;
                ht.acked = true;
            }
            break;
        case MsgType::TaskDone: {
            if (is_coord_ && ht.exec != kNoNode) {
                auto& t = spec_of(ht);
                auto& o = outstanding_[ht.exec];
                o[0] -= t.cores; o[1] -= int(t.mem_gb); o[2] -= t.gpus;
            }
            ht.st = HState::Done;
            ht.exec = m.node;
            auto& J = jobs_.at(ht.job);
            J.done[ht.idx] = 1;
            J.out_loc[ht.idx] = m.node;
            J.remaining--;
            int64_t now = now_ns();
            for (size_t i = 0; i < J.spec.tasks.size(); ++i) {
                auto tid = J.spec.tasks[i].id;
                auto hit = htasks_.find(tid);
                if (hit == htasks_.end() || hit->second.st != HState::WaitDeps) continue;
                bool ready = true;
                for (auto d : J.spec.tasks[i].deps) if (!J.done[d]) ready = false;
                if (ready) { hit->second.st = HState::Ready; hit->second.ready_ns = now; }
            }
            sched_dirty_ = true;
            break;
        }
        case MsgType::TaskFailed:
            if (m.attempt == ht.attempt()) reschedule(m.task, ht, "exec-failed", m.node);
            break;
        default: break;
    }
}

void Node::home_tick() {
    int64_t now = now_ns();
    for (auto& [id, ht] : htasks_) {
        switch (ht.st) {
            case HState::Reserving:
                if (now > ht.deadline) {
                    n_nacks_++;
                    ht.excluded.insert(ht.exec);
                    ht.exec = kNoNode;
                    ht.st = HState::Ready;
                    if (++ht.reserve_tries >= P_.max_reserve_attempts) {
                        ht.excluded.clear();
                        ht.reserve_tries = 0;
                        ht.not_before = now + ms_to_ns(300);
                    }
                    sched_dirty_ = true;
                }
                break;
            case HState::Delegated:
                if (!ht.acked && now > ht.deadline) {
                    if (ht.delegate_sends < 3) {
                        Writer w;
                        ht.pending_delegate.write(w);
                        send(ht.exec, MsgType::Delegate, w);
                        ht.delegate_sends++;
                        ht.deadline = now + ms_to_ns(1000);
                    } else {
                        reschedule(id, ht, "delegate-timeout", ht.exec);
                        break;
                    }
                }
                [[fallthrough]];
            case HState::Running:
                if (ht.exec != kNoNode && ht.exec != self_ && view_.status(ht.exec, now) == PeerStatus::Suspected) {
                    reschedule(id, ht, "exec-suspected", ht.exec);
                } else if (ht.acked && ht.exec != kNoNode && ht.exec != self_ && now - ht.last_query_ns > s_to_ns(3.0) &&
                           now - ht.decided_ns > s_to_ns(3.0)) {
                    // execution-state tracking: does the executor still hold this attempt?
                    ht.last_query_ns = now;
                    AckMsg q{id, ht.attempt()};
                    Writer w;
                    q.write(w);
                    send(ht.exec, MsgType::TaskQuery, w);
                }
                break;
            default: break;
        }
    }
    sched_dirty_ = true;
}

// ---------------------------------------------------------------- baselines
void Node::coordinator_schedule() {
    refresh_self();
    int64_t now = now_ns();
    std::vector<std::pair<int64_t, TaskId>> ready;
    for (auto& [id, ht] : htasks_)
        if (ht.st == HState::Ready && ht.not_before <= now) ready.push_back({ht.ready_ns, id});
    std::sort(ready.begin(), ready.end());
    for (auto& [rt, id] : ready) {
        HomeTask& ht = htasks_.at(id);
        auto& job = jobs_.at(ht.job);
        TaskSpec t = job.spec.tasks[ht.idx];
        fill_input_sources(t, job);
        int64_t t0 = now_ns();
        n_decisions_++;
        if (opt_.policy == Policy::Fifo) {
            // strict arrival order, first node (by id) with free capacity; head-of-line blocking
            NodeId pick = kNoNode;
            for (int i = 0; i < view_.size(); ++i) {
                NodeId n = NodeId(i);
                if (ht.excluded.count(n)) continue;
                if (!cost_.feasible(t, n, view_, now, nullptr)) continue;
                const auto& rec = view_.entry(n).rec;
                auto& o = outstanding_[n];
                if (o[0] + t.cores > rec.cores || o[1] + int(t.mem_gb) > int(rec.mem_gb) || o[2] + t.gpus > rec.gpus) continue;
                pick = n;
                break;
            }
            if (pick == kNoNode) {
                bool any = false;
                for (int i = 0; i < view_.size(); ++i)
                    if (cost_.feasible(t, NodeId(i), view_, now, nullptr)) any = true;
                if (!any) { ht.not_before = now + ms_to_ns(400); ht.excluded.clear(); continue; }
                break;  // head-of-line blocking
            }
            CostBreakdown c = cost_.evaluate(t, pick, view_, pending_, now);
            ht.choice = c;
            send_delegate(id, ht, pick, c, false);
            decision_event(id, ht, {c}, pick, "assign");
        } else {
            auto rk = cost_.rank(t, view_, pending_, now);
            const CostBreakdown* best = nullptr;
            for (auto& c : rk) {
                if (!c.feasible || ht.excluded.count(c.node)) continue;
                const auto& rec = view_.entry(c.node).rec;
                auto& o = outstanding_[c.node];
                if (o[0] + t.cores > 2 * rec.cores || o[2] + t.gpus > 2 * std::max<int>(1, rec.gpus) ||
                    o[1] + int(t.mem_gb) > 2 * int(rec.mem_gb))
                    continue;  // bound node-level queueing
                best = &c;
                break;
            }
            if (!best) { ht.not_before = now + ms_to_ns(200); ht.excluded.clear(); continue; }
            ht.choice = *best;
            send_delegate(id, ht, best->node, *best, false);
            decision_event(id, ht, rk, best->node, "assign");
        }
        decision_ms_.add(ns_to_s(now_ns() - t0) * 1e3);
    }
}

}  // namespace nexus
