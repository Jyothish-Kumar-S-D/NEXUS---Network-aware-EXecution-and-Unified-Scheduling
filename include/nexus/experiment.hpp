// Experiment orchestration: spawns node processes, drives the workload as an
// external client, injects background traffic and faults, samples metrics, feeds
// the live dashboard, and writes reproducible result files.
#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "nexus/fabric.hpp"
#include "nexus/transport.hpp"
#include "nexus/workload.hpp"

namespace nexus {

struct RunSpec {
    std::string name;
    std::string scenario = "baseline";  // baseline | faults | probe-ablation
    Policy policy = Policy::Nexus;
    ProbeMode probe = ProbeMode::Adaptive;
    uint64_t seed = 1;
    bool faults = false;
    WorkloadParams wp;
    double warmup_s = 4.0;
    double drain_s = 100.0;
    NodeId coordinator = 0;
};

// Shared with the HTTP server thread.
struct MonitorHub {
    std::mutex mu;
    json::Value topology;
    json::Value run_meta;
    json::Value suite = json::Value::array();
    std::vector<std::string> frames;
    std::vector<json::Value> summaries;
    std::string web_dir;
    int run_counter = 0;
};

class Experiment {
public:
    Experiment(const ClusterConfig& cfg, std::string config_path, std::string node_bin, std::string out_root,
               MonitorHub* hub);
    json::Value run(const RunSpec& spec);

private:
    struct ClientJob {
        JobSpec spec;
        NodeId home = 0;
        uint16_t epoch = 0;
        bool acked = false;
        int sends = 0;
        int64_t last_send = 0;
        std::vector<uint8_t> done;
        std::vector<NodeId> out_loc;
        int remaining = 0;
        bool submitted = false;
        double done_t = -1;
        int resubmits = 0;
    };
    struct TaskRec {
        uint32_t job;
        int idx;
        std::string cls;
        int node, home;
        double t_done, wait, xfer, exec, pred_wait, pred_xfer, pred_exec, net_bytes, in_bytes;
        int migrations;
    };
    void spawn(int node, const RunSpec& spec, int64_t t0);
    void submit(ClientJob& j);
    void handle_event(const json::Value& ev, double t);
    void sample(double t, double dt);
    json::Value summarize(const RunSpec& spec, double makespan);
    void write_outputs(const RunSpec& spec, const json::Value& summary);
    NodeId pick_new_home(NodeId old, double t);

    const ClusterConfig& cfg_;
    std::string config_path_, node_bin_, out_root_;
    MonitorHub* hub_;

    // per-run state
    RunSpec spec_;
    std::string fabric_name_, run_dir_;
    Fabric fabric_;
    std::unique_ptr<Transport> tx_;
    std::vector<pid_t> pids_;
    std::vector<int> incarnation_;
    std::vector<double> last_pong_;
    std::vector<bool> killed_, frozen_;
    std::map<uint32_t, ClientJob> jobs_;
    std::vector<TaskRec> tasks_;
    std::map<std::pair<uint32_t, int>, bool> task_done_;
    std::vector<json::Value> latest_status_;
    std::map<std::pair<int, int>, json::Value> last_status_inc_;  // (node, incarnation)
    std::vector<json::Value> frame_events_, frame_decisions_;
    json::Value timeline_ = json::Value::array();
    std::vector<uint64_t> last_busy_ns_;
    std::vector<uint64_t> last_bulk_;
    double last_tel_bytes_ = 0;
    double t_last_sample_ = 0;
    int tasks_done_interval_ = 0;
    uint64_t n_started_ = 0, n_failed_ = 0, n_dup_ = 0, n_resched_ = 0, n_migrate_ = 0, n_nack_ = 0, n_resubmit_ = 0;
    std::vector<double> decision_ms_;
    std::map<std::string, int> nack_reasons_;
    std::vector<double> est_lat_err_, est_bw_err_;
    std::vector<json::Value> fault_log_;
    double run_t0_s_ = 0;
    double run_len_s_ = 0;
    struct Usage { double cpu_s = 0; double maxrss_mb = 0; };
    std::vector<Usage> usage_;
    int64_t t0_ns_ = 0;
};

}  // namespace nexus
