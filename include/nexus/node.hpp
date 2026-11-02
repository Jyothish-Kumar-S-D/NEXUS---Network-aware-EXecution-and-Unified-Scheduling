// A NEXUS node: one OS process per HPC node. Roles inside the process:
//   * telemetry agent  — adaptive probing, packet-pair bandwidth probes, gossip
//   * home scheduler   — owns submitted jobs, evaluates placements, reserves and delegates
//   * executor         — admits reservations, stages inputs, runs tasks on worker threads
//   * data server      — serves input/output data to peers over the emulated fabric
//   * coordinator      — only for the FIFO / centralized baselines (one designated node)
#pragma once

#include <array>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include "nexus/scheduler.hpp"
#include "nexus/timer_queue.hpp"
#include "nexus/transport.hpp"

namespace nexus {

struct NodeOptions {
    NodeId id = 0;
    std::string fabric_name;
    Policy policy = Policy::Nexus;
    ProbeMode probe = ProbeMode::Adaptive;
    int64_t t0_ns = 0;      // experiment epoch (for hidden interference windows)
    uint64_t seed = 1;
    NodeId coordinator = 0; // for FIFO / centralized
};

class Node {
public:
    Node(const ClusterConfig& cfg, NodeOptions opt);
    ~Node();
    int run();
    void request_stop() { stop_ = true; }

private:
    using Key = std::pair<TaskId, uint32_t>;  // (task, attempt)

    // ------------------------------------------------------------ home role
    enum class HState : uint8_t { WaitDeps, Ready, Reserving, Delegated, Running, Done };
    struct HomeJob {
        JobSpec spec;
        uint16_t epoch = 0;
        std::vector<uint8_t> done;
        std::vector<NodeId> out_loc;
        int remaining = 0;
    };
    struct HomeTask {
        uint32_t job = 0;
        uint16_t idx = 0;
        HState st = HState::WaitDeps;
        uint16_t epoch = 0;
        uint16_t attempt_n = 0;
        NodeId exec = kNoNode;
        int64_t ready_ns = 0, not_before = 0, deadline = 0, decided_ns = 0;
        int reserve_tries = 0, delegate_sends = 0;
        bool acked = false;
        int64_t last_query_ns = 0;
        int unknown_replies = 0;
        std::set<NodeId> excluded;
        DelegateMsg pending_delegate;
        CostBreakdown choice;
        uint32_t attempt() const { return (uint32_t(epoch) << 16) | attempt_n; }
    };

    // ------------------------------------------------------------ executor role
    struct FetchState {
        NodeId src = kNoNode;
        uint64_t bytes = 0;
        uint32_t xfer_id = 0;
        int64_t sent_ns = 0, deadline = 0;
        int retries = 0;
        bool done = false;
        size_t input_idx = 0;
    };
    enum class LState : uint8_t { Queued, Staging, Running };
    struct LocalTask {
        DelegateMsg d;
        LState st = LState::Queued;
        int64_t enq_ns = 0, stage_ns = 0, run_ns = 0;
        double est_exec = 1;
        std::vector<FetchState> fetches;
        int inputs_left = 0;
        uint64_t net_bytes = 0, in_bytes = 0;
        double true_exec = 0;
        std::shared_ptr<std::atomic<bool>> cancel;
        bool migrating = false;
    };
    struct Reservation {
        uint32_t attempt = 0;
        int64_t expiry = 0;
        uint16_t cores = 0;
        uint8_t gpus = 0;
        uint8_t tenant = 0;
        float est_s = 0;
    };
    struct Completion {
        Key key;
        bool cancelled;
    };
    struct OutXfer {
        NodeId to;
        TaskId task;
        uint32_t attempt, xfer_id;
        uint64_t left, total;
    };
    struct Migration {
        uint32_t attempt;
        NodeId target;
        int64_t deadline;
        CostBreakdown choice;
        int phase = 0;  // 0 = reserving at target, 1 = handing off (waiting for DelegateAck)
        int sends = 0;
        DelegateMsg d;
    };
    void finish_migration(TaskId task);
    void on_task_query(const Datagram& dg);

    // infra
    void handle(const Datagram& dg);
    void send(NodeId dst, MsgType t, const Writer& w, uint8_t flags = 0) { tx_->send(dst, t, w, flags); }
    void emit(json::Value ev);  // monitor-plane event to the launcher
    NodeRecord make_self_record();
    void refresh_self();
    double exp_offset_s() const { return ns_to_s(now_ns() - opt_.t0_ns); }

    // telemetry
    void probe_tick();
    double next_probe_interval_ms();
    void on_probe(const Datagram& dg);
    void on_probe_ack(const Datagram& dg);
    void merge_record(const NodeRecord& r);
    void probe_timeouts();
    void refill_tokens();
    void debug_dump();

    // home
    void on_submit(const Datagram& dg);
    void home_schedule();
    void decide(TaskId id, HomeTask& ht);
    void coordinator_schedule();
    void fill_input_sources(TaskSpec& t, const HomeJob& j);
    void send_delegate(TaskId id, HomeTask& ht, NodeId target, const CostBreakdown& c, bool reserved);
    void reschedule(TaskId id, HomeTask& ht, const char* why, NodeId bad);
    void home_tick();
    void on_reserve_rep(const Datagram& dg);
    void on_task_report(const Datagram& dg);
    void on_delegate_ack(const Datagram& dg);
    void decision_event(TaskId id, const HomeTask& ht, const std::vector<CostBreakdown>& rk, NodeId chosen,
                        const char* mode);
    TaskSpec& spec_of(const HomeTask& ht) { return jobs_.at(ht.job).spec.tasks[ht.idx]; }

    // executor
    void on_reserve_req(const Datagram& dg);
    void on_delegate(const Datagram& dg);
    bool can_admit(const TaskSpec& t, NackReason* why);
    void local_enqueue(const DelegateMsg& d);
    void try_dispatch();
    void start_staging(LocalTask& lt);
    void start_fetch(LocalTask& lt, FetchState& f, bool new_xfer);
    void on_data_done(const Datagram& dg);
    void input_ready(LocalTask& lt);
    void start_run(LocalTask& lt);
    void on_completion(const Completion& c);
    void fail_local(const Key& k, uint8_t reason);
    void exec_tick();
    void rebalance_tick();
    void report(MsgType t, const LocalTask& lt, uint8_t reason = 0);
    int tenant_cores(uint8_t tenant) const;
    double true_exec(const TaskSpec& t);

    // data server
    void on_fetch(const Datagram& dg);
    void xfer_step(uint64_t key);

    // workers
    void worker_loop();

    const ClusterConfig& cfg_;
    NexusParams P_;
    NodeOptions opt_;
    NodeId self_;
    const NodeSpec& spec_;
    Fabric fabric_;
    std::unique_ptr<Transport> tx_;
    StateView view_;
    CostModel cost_;
    PendingLoad pending_;
    Rng rng_;
    TimerQueue timers_;
    std::atomic<bool> stop_{false};
    int evfd_ = -1;
    bool is_coord_ = false;
    bool sched_dirty_ = false;
    int64_t last_probe_ns_ = 0;
    std::map<std::pair<NodeId, uint32_t>, int64_t> served_xfers_;  // (requester, xfer id) -> done time
    std::map<Key, int64_t> done_last_send_;
    void every(double ms, std::function<void()> fn);

    // hardware emulation (ground truth)
    bool gpu_ok_ = true;

    // local resources
    int cores_free_, mem_free_, gpus_free_;
    std::map<Key, LocalTask> local_;
    std::deque<Key> order_;
    std::map<TaskId, Reservation> reservations_;
    struct DoneRec { TaskReportMsg msg; NodeId home; int sends; };
    std::map<Key, DoneRec> done_unacked_;  // resend until home acks
    std::map<TaskId, Migration> migrating_;

    // home
    std::map<uint32_t, HomeJob> jobs_;
    std::map<TaskId, HomeTask> htasks_;
    std::map<uint32_t, uint16_t> job_epoch_;
    std::map<NodeId, std::array<int, 3>> outstanding_;  // coordinator bookkeeping: cores, mem, gpus

    // data server
    std::map<uint64_t, OutXfer> outx_;
    uint32_t next_xfer_ = 1;
    uint64_t next_out_key_ = 1;

    // telemetry
    uint32_t probe_seq_ = 0;
    uint32_t rec_version_ = 0;
    struct Outstanding { NodeId peer; int64_t tx_ns; ProbeKind kind; uint32_t wire; };
    std::map<uint32_t, Outstanding> probes_out_;
    std::map<std::pair<NodeId, uint32_t>, int64_t> pair_first_rx_;
    std::vector<uint32_t> probe_count_;
    double tokens_ = 0;
    int64_t tokens_ns_ = 0;
    double cur_interval_ms_ = 400;
    Ewma nic_ewma_{0.3};
    uint64_t bw_probes_ = 0, lat_probes_ = 0, probes_deferred_ = 0, probes_budget_skips_ = 0;

    // learning
    Ewma corr_[kNumClasses];
    Ewma fail_ewma_{0.1};

    // counters
    uint64_t n_decisions_ = 0, n_local_ = 0, n_delegated_ = 0, n_nacks_ = 0, n_migrations_ = 0,
             n_reschedules_ = 0, n_completed_ = 0, n_failed_ = 0, n_dup_done_ = 0, n_grants_ = 0, n_denies_ = 0;
    Ewma decision_ms_{0.1};

    // worker pool
    std::mutex wmu_;
    std::condition_variable wcv_;
    struct Job { Key key; double secs; std::shared_ptr<std::atomic<bool>> cancel; };
    std::deque<Job> wq_;
    std::vector<std::thread> workers_;
    std::mutex cmu_;
    std::deque<Completion> completions_;
};

}  // namespace nexus
