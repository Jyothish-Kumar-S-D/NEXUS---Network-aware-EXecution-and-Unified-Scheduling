// Node process: event loop, message dispatch, self-description, worker pool,
// hardware emulation and monitoring-plane status reports.
#include "nexus/node.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace nexus {

Node::Node(const ClusterConfig& cfg, NodeOptions opt)
    : cfg_(cfg),
      P_(cfg.params),
      opt_(opt),
      self_(opt.id),
      spec_(cfg.nodes.at(opt.id)),
      view_(opt.id, int(cfg.nodes.size()), cfg.params),
      cost_(cfg, opt.policy),
      pending_(int(cfg.nodes.size())),
      rng_(opt.seed * 7919 + opt.id * 104729 + 17) {
    fabric_ = Fabric::attach(opt.fabric_name);
    tx_ = std::make_unique<Transport>(self_, cfg_, &fabric_, opt.seed * 31 + opt.id);
    cores_free_ = spec_.cores;
    mem_free_ = spec_.mem_gb;
    gpus_free_ = spec_.gpus;
    probe_count_.assign(cfg.nodes.size(), 0);
    is_coord_ = is_coordinated(opt.policy) && self_ == opt.coordinator;
    for (auto& c : corr_) c = Ewma(0.3);
    cur_interval_ms_ = P_.probe_base_ms;
}

Node::~Node() {
    {
        std::lock_guard<std::mutex> g(wmu_);
        stop_ = true;
    }
    wcv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
    if (evfd_ >= 0) ::close(evfd_);
}

void Node::every(double ms, std::function<void()> fn) {
    timers_.after_ms(ms, [this, ms, fn] {
        fn();
        every(ms, fn);
    });
}

int Node::run() {
    log_tag() = spec_.name;
    if (!tx_->open()) return 2;
    evfd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    int nworkers = clampv(spec_.cores, 2, 40);
    for (int i = 0; i < nworkers; ++i) workers_.emplace_back([this] { worker_loop(); });

    refresh_self();
    tokens_ = P_.telemetry_budget_kBps * 1000.0;
    tokens_ns_ = now_ns();

    bool probing = opt_.policy != Policy::Fifo;
    if (probing) timers_.after_ms(rng_.uniform(5, 150), [this] { probe_tick(); });
    every(500, [this] {
        // monitoring-plane status
        int64_t now = now_ns();
        json::Value s;
        s["type"] = "status";
        s["node"] = int(self_);
        s["name"] = spec_.name;
        s["t"] = exp_offset_s();
        s["policy"] = policy_name(opt_.policy);
        s["coordinator"] = is_coord_;
        s["cores"] = spec_.cores;
        s["cores_free"] = cores_free_;
        s["mem"] = spec_.mem_gb;
        s["mem_free"] = mem_free_;
        s["gpus"] = spec_.gpus;
        s["gpus_free"] = gpus_free_;
        s["gpu_ok"] = gpu_ok_;
        s["nic_util"] = nic_ewma_.valid() ? nic_ewma_.mean : 0.0;
        NodeRecord me = make_self_record();
        s["backlog_core_s"] = me.backlog_core_s;
        s["backlog_gpu_s"] = me.backlog_gpu_s;
        s["fail_rate"] = me.fail_rate;
        json::Value q = json::Value::array();
        int queued = 0, running = 0, staging = 0;
        for (auto& k : order_) {
            auto it = local_.find(k);
            if (it == local_.end()) continue;
            auto& lt = it->second;
            const char* st = lt.st == LState::Queued ? "queued" : (lt.st == LState::Staging ? "staging" : "running");
            if (lt.st == LState::Queued) ++queued;
            else if (lt.st == LState::Staging) ++staging;
            else ++running;
            if (q.size() < 24) {
                json::Value e;
                e["task"] = fmt("%u.%u", lt.d.spec.job, lt.d.spec.index);
                e["cls"] = class_name(lt.d.spec.cls);
                e["cores"] = int(lt.d.spec.cores);
                e["gpus"] = int(lt.d.spec.gpus);
                e["state"] = st;
                e["age"] = ns_to_s(now - lt.enq_ns);
                e["home"] = int(lt.d.home);
                q.push(e);
            }
        }
        s["queue"] = q;
        s["queued"] = queued;
        s["staging"] = staging;
        s["running"] = running;
        s["reservations"] = int(reservations_.size());
        json::Value tel;
        tel["msgs"] = double(tx_->stats.telem_msgs.load());
        tel["bytes"] = double(tx_->stats.telem_bytes.load());
        tel["ctrl_msgs"] = double(tx_->stats.msgs_sent.load());
        tel["ctrl_bytes"] = double(tx_->stats.bytes_sent.load());
        tel["drops"] = double(tx_->stats.drops.load());
        tel["interval_ms"] = cur_interval_ms_;
        tel["lat_probes"] = double(lat_probes_);
        tel["bw_probes"] = double(bw_probes_);
        tel["deferred"] = double(probes_deferred_);
        tel["budget_skips"] = double(probes_budget_skips_);
        s["telemetry"] = tel;
        json::Value v = json::Value::array();
        for (int i = 0; i < view_.size(); ++i) {
            if (i == self_) continue;
            json::Value e;
            e["peer"] = i;
            e["status"] = peer_status_name(view_.status(NodeId(i), now));
            e["age"] = std::min(99.0, view_.age_s(NodeId(i), now));
            NetEst ne = view_.net(self_, NodeId(i), now);
            e["lat_ms"] = ne.known ? ne.lat_s * 1e3 : -1.0;
            e["bw_MBps"] = view_.link(NodeId(i)).bw.valid() ? view_.link(NodeId(i)).bw.mean / 1e6 : -1.0;
            e["loss"] = ne.loss;
            // probes measure RTT/2, so compare against the mean of both directions
            e["true_lat_ms"] = 0.5e3 * (fabric_.true_one_way_s(self_, NodeId(i), now) +
                                        fabric_.true_one_way_s(NodeId(i), self_, now));
            e["true_bw_MBps"] = fabric_.true_avail_Bps(NodeId(i), self_, now) / 1e6;
            v.push(e);
        }
        s["view"] = v;
        json::Value cr = json::Value::array();
        for (int c = 0; c < kNumClasses; ++c) cr.push(corr_[c].valid() ? corr_[c].mean : 1.0);
        s["corr"] = cr;
        json::Value cnt;
        cnt["decisions"] = double(n_decisions_);
        cnt["local"] = double(n_local_);
        cnt["delegated"] = double(n_delegated_);
        cnt["nacks"] = double(n_nacks_);
        cnt["grants"] = double(n_grants_);
        cnt["denies"] = double(n_denies_);
        cnt["migrations"] = double(n_migrations_);
        cnt["reschedules"] = double(n_reschedules_);
        cnt["completed"] = double(n_completed_);
        cnt["failed"] = double(n_failed_);
        cnt["decision_ms"] = decision_ms_.valid() ? decision_ms_.mean : 0.0;
        int homed = 0;
        for (auto& [id, ht] : htasks_) if (ht.st != HState::Done) ++homed;
        cnt["home_active"] = homed;
        s["counters"] = cnt;
        emit(s);
    });
    every(200, [this] { home_tick(); });
    every(200, [this] { exec_tick(); });
    if (opt_.policy == Policy::Nexus || opt_.policy == Policy::ComputeOnly)
        every(P_.rebalance_ms + rng_.uniform(0, 100), [this] { rebalance_tick(); });
    if (is_coord_) {
        every(P_.central_poll_ms, [this] {
            Writer w;
            w.put(uint32_t(0));
            for (int i = 0; i < int(cfg_.nodes.size()); ++i)
                if (i != self_) send(NodeId(i), MsgType::StateReq, w, kFlagTelemetry);
        });
    }

    json::Value hello;
    hello["type"] = "hello";
    hello["node"] = int(self_);
    hello["pid"] = int(::getpid());
    emit(hello);
    LOG_I("node %s up (policy=%s probe=%s coord=%d)", spec_.name.c_str(), policy_name(opt_.policy),
          probe_mode_name(opt_.probe), int(is_coord_));

    Datagram dg;
    while (!stop_) {
        int64_t now = now_ns();
        timers_.run_due(now);
        {
            std::deque<Completion> done;
            {
                std::lock_guard<std::mutex> g(cmu_);
                done.swap(completions_);
            }
            for (auto& c : done) on_completion(c);
        }
        if (sched_dirty_) {
            sched_dirty_ = false;
            home_schedule();
        }
        int64_t nxt = timers_.next();
        int timeout_ms = int(clampv<int64_t>((nxt - now_ns()) / 1000000, 0, 20));
        if (sched_dirty_) timeout_ms = 0;
        pollfd fds[2] = {{tx_->fd(), POLLIN, 0}, {evfd_, POLLIN, 0}};
        ::poll(fds, 2, timeout_ms);
        if (fds[1].revents & POLLIN) {
            uint64_t v;
            [[maybe_unused]] auto r = ::read(evfd_, &v, sizeof v);
        }
        int budget = 256;
        while (budget-- > 0 && tx_->recv(dg)) {
            try {
                handle(dg);
            } catch (const std::exception& e) {
                LOG_W("bad message type %d from %d: %s", int(dg.h.type), int(dg.h.src), e.what());
            }
        }
    }
    LOG_I("node %s shutting down", spec_.name.c_str());
    debug_dump();
    {
        std::lock_guard<std::mutex> g(wmu_);
        for (auto& [k, lt] : local_)
            if (lt.cancel) *lt.cancel = true;
    }
    wcv_.notify_all();
    tx_->close();
    return 0;
}

void Node::handle(const Datagram& dg) {
    switch (dg.h.type) {
        case MsgType::Probe: on_probe(dg); break;
        case MsgType::ProbeAck: on_probe_ack(dg); break;
        case MsgType::ReserveReq: on_reserve_req(dg); break;
        case MsgType::ReserveRep: on_reserve_rep(dg); break;
        case MsgType::Delegate: on_delegate(dg); break;
        case MsgType::DelegateAck: on_delegate_ack(dg); break;
        case MsgType::TaskStarted:
        case MsgType::TaskDone:
        case MsgType::TaskFailed:
        case MsgType::TaskMoved: on_task_report(dg); break;
        case MsgType::DoneAck: {
            Reader r = dg.reader();
            AckMsg a = AckMsg::read(r);
            done_unacked_.erase({a.task, a.attempt});
            done_last_send_.erase({a.task, a.attempt});
            break;
        }
        case MsgType::TaskQuery:
        case MsgType::TaskQueryRep: on_task_query(dg); break;
        case MsgType::Fetch: on_fetch(dg); break;
        case MsgType::DataDone: on_data_done(dg); break;
        case MsgType::StateReq: {
            Writer w;
            make_self_record().write(w);
            send(dg.h.src, MsgType::StateRep, w, kFlagTelemetry);
            break;
        }
        case MsgType::StateRep: {
            Reader r = dg.reader();
            merge_record(NodeRecord::read(r));
            break;
        }
        case MsgType::Submit: on_submit(dg); break;
        case MsgType::Ping: {
            Writer w;
            w.put(uint32_t(dg.h.seq));
            send(dg.h.src, MsgType::Pong, w, kFlagMgmt);
            break;
        }
        case MsgType::Fault: {
            Reader r = dg.reader();
            auto k = r.get<FaultKind>();
            if (k == FaultKind::GpuFail) {
                gpu_ok_ = false;
                LOG_W("GPU fault injected");
                std::vector<Key> victims;
                for (auto& [key, lt] : local_)
                    if (lt.d.spec.gpus > 0) victims.push_back(key);
                for (auto& key : victims) {
                    auto& lt = local_.at(key);
                    if (lt.st == LState::Running && lt.cancel) *lt.cancel = true;  // worker reports cancellation
                    else fail_local(key, 2);
                }
            } else if (k == FaultKind::GpuRecover) {
                gpu_ok_ = true;
                LOG_I("GPU recovered");
            }
            json::Value ev;
            ev["type"] = "fault";
            ev["node"] = int(self_);
            ev["kind"] = k == FaultKind::GpuFail ? "gpu-fail" : "gpu-recover";
            ev["t"] = exp_offset_s();
            emit(ev);
            break;
        }
        case MsgType::Shutdown: stop_ = true; break;
        default: break;
    }
}

// Shutdown diagnostics: anything still owned or held by this node.
void Node::debug_dump() {
    static const char* hs[] = {"wait-deps", "ready", "reserving", "delegated", "running", "done"};
    static const char* ls[] = {"queued", "staging", "running"};
    int64_t now = now_ns();
    for (auto& [id, ht] : htasks_) {
        if (ht.st == HState::Done) continue;
        LOG_I("open home task %u.%u state=%s exec=%d attempt=%u acked=%d excluded=%zu not_before=%+.1fs", ht.job, ht.idx,
              hs[int(ht.st)], ht.exec == kNoNode ? -1 : int(ht.exec), ht.attempt_n, int(ht.acked), ht.excluded.size(),
              ns_to_s(ht.not_before - now));
    }
    for (auto& [k, lt] : local_) {
        int pend = 0;
        for (auto& f : lt.fetches) pend += f.done ? 0 : 1;
        LOG_I("open local task %u.%u state=%s home=%d age=%.1fs pending_fetches=%d migrating=%d", lt.d.spec.job,
              lt.d.spec.index, ls[int(lt.st)], int(lt.d.home), ns_to_s(now - lt.enq_ns), pend, int(lt.migrating));
    }
    LOG_I("resources free: cores=%d mem=%d gpus=%d; reservations=%zu outgoing_xfers=%zu", cores_free_, mem_free_,
          gpus_free_, reservations_.size(), outx_.size());
}

void Node::emit(json::Value ev) {
    std::string s = json::dump(ev);
    if (s.size() > 60000) return;
    Writer w;
    w.buf.assign(s.begin(), s.end());
    tx_->send(kClientId, MsgType::MonitorJson, w, kFlagMgmt);
}

NodeRecord Node::make_self_record() {
    int64_t now = now_ns();
    NodeRecord n;
    n.id = self_;
    n.version = ++rec_version_;
    n.origin_ns = now;
    n.rack = uint8_t(spec_.rack);
    n.cores = uint16_t(spec_.cores);
    n.mem_gb = uint32_t(spec_.mem_gb);
    n.gpus = uint8_t(spec_.gpus);
    n.cpu_speed = float(spec_.cpu_speed);
    n.gpu_speed = float(spec_.gpu_speed);
    n.disk_GBps = float(spec_.disk_gbps / 8.0);
    n.partition = spec_.partition;
    for (int d : spec_.datasets)
        if (d >= 0 && d < 64) n.datasets |= (1ull << d);
    n.cores_free = uint16_t(std::max(0, cores_free_));
    n.mem_free = uint32_t(std::max(0, mem_free_));
    n.gpus_free = uint8_t(std::max(0, gpus_free_));
    double bc = 0, bg = 0;
    int ql = 0, run = 0;
    for (auto& [k, lt] : local_) {
        double est = lt.est_exec;
        double rem = est;
        if (lt.st == LState::Running) rem = std::max(0.1 * est, est - ns_to_s(now - lt.run_ns));
        if (lt.st == LState::Queued) ++ql;
        else ++run;
        bc += rem * lt.d.spec.cores;
        if (lt.d.spec.gpus) bg += rem * lt.d.spec.gpus;
    }
    for (auto& [t, r] : reservations_) {
        ++ql;
        bc += r.est_s * r.cores;
        bg += r.est_s * r.gpus;
    }
    n.queue_len = uint16_t(ql);
    n.running = uint16_t(run);
    n.backlog_core_s = float(bc);
    n.backlog_gpu_s = float(bg);
    n.health = kCpuOk | kDiskOk | (gpu_ok_ ? kGpuOk : 0);
    if (fail_ewma_.valid() && fail_ewma_.mean > 0.25) n.health |= kDegraded;
    n.fail_rate = fail_ewma_.valid() ? float(fail_ewma_.mean) : 0.f;
    n.nic_util = nic_ewma_.valid() ? float(nic_ewma_.mean) : 0.f;
    for (int c = 0; c < kNumClasses; ++c) n.corr[c] = corr_[c].valid() ? float(corr_[c].mean) : 1.f;
    for (int t = 0; t < 4 && t < int(cfg_.tenants.size()); ++t) n.tenant_cores[t] = uint16_t(tenant_cores(uint8_t(t)));
    for (int i = 0; i < view_.size(); ++i) {
        if (i == self_) continue;
        const PeerLink& l = view_.link(NodeId(i));
        if (!l.lat.valid()) continue;
        LinkEst e;
        e.peer = NodeId(i);
        e.lat_ms = float(l.lat.mean * 1e3);
        e.lat_sd_ms = float(l.lat.stddev() * 1e3);
        e.bw_MBps = l.bw.valid() ? float(l.bw.mean / 1e6) : 0.f;
        e.loss = l.loss.valid() ? float(l.loss.mean) : 0.f;
        e.age_s = float(ns_to_s(now - std::max(l.last_lat_ns, l.last_bw_ns)));
        n.links.push_back(e);
    }
    return n;
}

void Node::refresh_self() { view_.set_self(make_self_record(), now_ns()); }

void Node::merge_record(const NodeRecord& r) {
    if (view_.merge(r, now_ns())) pending_.clear_before(r.id, r.origin_ns);
}

double Node::true_exec(const TaskSpec& t) {
    double base;
    if (t.gpus > 0) base = t.work / (double(t.gpus) * spec_.gpu_speed * spec_.true_gpu_factor);
    else base = t.work / (double(std::max<uint16_t>(1, t.cores)) * spec_.cpu_speed * spec_.true_cpu_factor);
    double now_s = exp_offset_s(), f = 1.0;
    for (auto& w : cfg_.interference)
        if (w.node == self_ && now_s >= w.start_s && now_s < w.end_s) f *= w.factor;
    return base * f * rng_.lognormal(0.0, 0.06);
}

void Node::worker_loop() {
    while (true) {
        Job j;
        {
            std::unique_lock<std::mutex> lk(wmu_);
            wcv_.wait(lk, [&] { return stop_ || !wq_.empty(); });
            if (stop_ && wq_.empty()) return;
            j = std::move(wq_.front());
            wq_.pop_front();
        }
        // Emulated kernel: occupy the slot for the ground-truth duration, honouring cancellation.
        int64_t end = now_ns() + s_to_ns(j.secs);
        bool cancelled = false;
        while (true) {
            int64_t now = now_ns();
            if (now >= end) break;
            if (j.cancel && j.cancel->load()) { cancelled = true; break; }
            if (stop_) return;
            std::this_thread::sleep_for(std::chrono::nanoseconds(std::min<int64_t>(end - now, 20000000)));
        }
        {
            std::lock_guard<std::mutex> g(cmu_);
            completions_.push_back({j.key, cancelled});
        }
        uint64_t one = 1;
        [[maybe_unused]] auto r = ::write(evfd_, &one, sizeof one);
    }
}

}  // namespace nexus
