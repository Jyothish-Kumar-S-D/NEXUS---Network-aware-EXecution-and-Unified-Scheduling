// nexus_launcher: host analysis, single runs with the live dashboard, and the
// full benchmark suite (NEXUS vs FIFO vs compute-only vs centralized).
#include <limits.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <map>

#include "nexus/experiment.hpp"
#include "nexus/http.hpp"
#include "nexus/sysinfo.hpp"

using namespace nexus;

#ifndef NEXUS_SOURCE_DIR
#define NEXUS_SOURCE_DIR "."
#endif

static bool exists(const std::string& p) {
    struct stat st{};
    return ::stat(p.c_str(), &st) == 0;
}
static std::string absolute(const std::string& p) {
    char buf[PATH_MAX];
    return ::realpath(p.c_str(), buf) ? std::string(buf) : p;
}
static std::string dirname_of(const std::string& p) {
    auto s = p.rfind('/');
    return s == std::string::npos ? "." : p.substr(0, s);
}
static std::string locate(const std::string& rel) {
    if (exists(rel)) return absolute(rel);
    std::string s = std::string(NEXUS_SOURCE_DIR) + "/" + rel;
    return exists(s) ? absolute(s) : rel;
}

static void usage() {
    std::cout <<
        "nexus_launcher — NEXUS HPC scheduling testbed\n"
        "  sysinfo [--quick]                         analyse host capabilities\n"
        "  run   [--policy nexus|fifo|compute-only|centralized] [--seed N] [--faults]\n"
        "        [--probe adaptive|fixed-fast|fixed-slow] [--duration S] [--rate R] [--hold]\n"
        "  suite [--seeds N] [--duration S] [--rate R] [--no-faults] [--no-ablation] [--hold]\n"
        "common: [--config FILE] [--out DIR] [--port HTTP_PORT] [--web DIR]\n";
}

static const char* kAggMetrics[] = {
    "jobs.completed", "jobs.incomplete", "jobs.makespan_s", "jobs.jct_mean_s", "jobs.jct_p50_s", "jobs.jct_p95_s",
    "jobs.resubmitted", "tasks.wait_mean_s", "tasks.xfer_mean_s", "tasks.exec_mean_s", "tasks.turnaround_mean_s",
    "tasks.network_gb", "tasks.local_input_fraction", "tasks.exec_pred_mape", "tasks.exec_pred_mape_first_half",
    "tasks.exec_pred_mape_second_half", "tasks.total_pred_mape", "tasks.xfer_pred_mae_s", "tasks.migrations",
    "tasks.reschedules", "tasks.failed_attempts", "tasks.duplicate_completions", "utilisation.core_util_mean",
    "utilisation.gpu_util_mean", "utilisation.data_MBps_mean", "overhead.telemetry_kBps_per_node",
    "overhead.telemetry_to_data_ratio", "overhead.control_msgs_total", "overhead.lat_est_rel_err_p50",
    "overhead.bw_est_rel_err_p50", "overhead.probe_interval_ms_mean", "overhead.probes_deferred_busy_nic",
    "scheduling.decision_ms_mean", "scheduling.delegations", "scheduling.reservation_nacks",
    "scheduling.local_placements", "host.node_cpu_share_of_one_core", "host.node_maxrss_mb_mean"};

static double get_path(const json::Value& v, const std::string& path) {
    auto dot = path.find('.');
    auto a = v.find(path.substr(0, dot));
    if (!a) return NAN;
    auto b = a->find(path.substr(dot + 1));
    return b ? b->num(NAN) : NAN;
}

static json::Value aggregate(const std::vector<json::Value>& runs) {
    std::map<std::string, std::vector<const json::Value*>> groups;
    std::vector<std::string> order;
    for (auto& r : runs) {
        std::string k = r.get_str("scenario", "") + "|" + r.get_str("policy", "") + "|" + r.get_str("probe", "");
        if (!groups.count(k)) order.push_back(k);
        groups[k].push_back(&r);
    }
    json::Value out = json::Value::array();
    for (auto& k : order) {
        auto& g = groups[k];
        json::Value e;
        e["scenario"] = g[0]->get_str("scenario", "");
        e["policy"] = g[0]->get_str("policy", "");
        e["probe"] = g[0]->get_str("probe", "");
        e["runs"] = int(g.size());
        json::Value m;
        for (const char* name : kAggMetrics) {
            std::vector<double> v;
            for (auto* r : g) {
                double x = get_path(*r, name);
                if (std::isfinite(x)) v.push_back(x);
            }
            if (v.empty()) continue;
            double mu = 0;
            for (double x : v) mu += x;
            mu /= double(v.size());
            double var = 0;
            for (double x : v) var += (x - mu) * (x - mu);
            double sd = v.size() > 1 ? std::sqrt(var / double(v.size() - 1)) : 0;
            json::Value s;
            s["mean"] = std::round(mu * 1000) / 1000;
            s["sd"] = std::round(sd * 1000) / 1000;
            m[name] = s;
        }
        e["metrics"] = m;
        out.push(e);
    }
    return out;
}

int main(int argc, char** argv) {
    if (argc < 2) { usage(); return 1; }
    std::string cmd = argv[1];
    std::string config = "config/cluster.json", out = "results", web = "web";
    std::string policy = "nexus", probe = "adaptive";
    int port = -1, seeds = 3;
    uint64_t seed = 1;
    bool faults = false, hold = false, quick = false, no_faults = false, no_ablation = false;
    double duration = -1, rate = -1;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--config") config = next();
        else if (a == "--out") out = next();
        else if (a == "--web") web = next();
        else if (a == "--policy") policy = next();
        else if (a == "--probe") probe = next();
        else if (a == "--port") port = std::atoi(next().c_str());
        else if (a == "--seed") seed = std::strtoull(next().c_str(), nullptr, 10);
        else if (a == "--seeds") seeds = std::atoi(next().c_str());
        else if (a == "--duration") duration = std::atof(next().c_str());
        else if (a == "--rate") rate = std::atof(next().c_str());
        else if (a == "--faults") faults = true;
        else if (a == "--no-faults") no_faults = true;
        else if (a == "--no-ablation") no_ablation = true;
        else if (a == "--hold") hold = true;
        else if (a == "--quick") quick = true;
        else { usage(); return 1; }
    }
    ::signal(SIGPIPE, SIG_IGN);

    if (cmd == "sysinfo") {
        auto p = analyze_system(!quick);
        print_profile(p);
        return 0;
    }
    if (cmd != "run" && cmd != "suite") { usage(); return 1; }

    std::string node_bin = absolute(dirname_of(absolute(argv[0])) + "/nexus_node");
    if (!exists(node_bin)) { std::cerr << "cannot find nexus_node next to the launcher\n"; return 1; }
    std::string cfg_path = locate(config);
    ClusterConfig cfg = ClusterConfig::load(cfg_path);
    ::mkdir(out.c_str(), 0755);
    out = absolute(out);

    // 1) host capability analysis decides whether this cluster can be emulated faithfully
    SystemProfile prof = analyze_system(true);
    {
        std::ofstream f(out + "/system_profile.json");
        json::Value j = prof.j;
        j["cluster_nodes"] = int(cfg.nodes.size());
        int cores = 0;
        for (auto& n : cfg.nodes) cores += n.cores;
        j["cluster_emulated_cores"] = cores;
        f << json::dump(j, 2) << "\n";
    }
    std::printf("host: %d logical CPUs, %.0f MB available, timer p99 overshoot %.0f us, UDP RTT p50 %.0f us\n",
                prof.logical_cpus, prof.mem_avail_mb, prof.timer_overshoot_us, prof.udp_rtt_us);
    std::printf("capacity: up to %d node processes; tasks shorter than %.2f s would be timing-distorted\n",
                prof.max_node_processes, prof.min_task_s);
    if (int(cfg.nodes.size()) > prof.max_node_processes) {
        std::fprintf(stderr, "config has %zu nodes but this host supports ~%d node processes\n", cfg.nodes.size(),
                     prof.max_node_processes);
        return 1;
    }

    MonitorHub hub;
    hub.web_dir = locate(web);
    if (port < 0) port = cfg.http_port;
    HttpServer http(&hub);
    if (http.start(port)) std::printf("dashboard: http://localhost:%d/\n", port);
    else std::fprintf(stderr, "warning: could not start dashboard on port %d\n", port);
    std::fflush(stdout);

    WorkloadParams wp = WorkloadParams::from_json(cfg.raw.find("workload"));
    if (duration > 0) wp.duration_s = duration;
    if (rate > 0) wp.rate_per_s = rate;
    Experiment exp(cfg, cfg_path, node_bin, out, &hub);

    std::vector<RunSpec> plan;
    auto mk = [&](const std::string& scen, Policy p, ProbeMode pm, uint64_t s, bool f) {
        RunSpec r;
        r.scenario = scen;
        r.policy = p;
        r.probe = pm;
        r.seed = s;
        r.faults = f;
        r.wp = wp;
        r.name = scen + "_" + policy_name(p) + (pm != ProbeMode::Adaptive ? std::string("_") + probe_mode_name(pm) : "") +
                 "_s" + std::to_string(s);
        return r;
    };
    const Policy all[] = {Policy::Nexus, Policy::Fifo, Policy::ComputeOnly, Policy::Centralized};
    if (cmd == "run") {
        plan.push_back(mk(faults ? "faults" : "baseline", parse_policy(policy), parse_probe_mode(probe), seed, faults));
    } else {
        for (int s = 1; s <= seeds; ++s)
            for (Policy p : all) plan.push_back(mk("baseline", p, ProbeMode::Adaptive, uint64_t(s), false));
        if (!no_faults)
            for (Policy p : all) plan.push_back(mk("faults", p, ProbeMode::Adaptive, 1, true));
        if (!no_ablation) {
            plan.push_back(mk("probe-ablation", Policy::Nexus, ProbeMode::FixedFast, 1, false));
            plan.push_back(mk("probe-ablation", Policy::Nexus, ProbeMode::FixedSlow, 1, false));
        }
    }
    {
        std::lock_guard<std::mutex> g(hub.mu);
        for (auto& r : plan) {
            json::Value e;
            e["name"] = r.name;
            e["state"] = "pending";
            hub.suite.push(e);
        }
    }
    std::vector<json::Value> results;
    for (size_t i = 0; i < plan.size(); ++i) {
        {
            std::lock_guard<std::mutex> g(hub.mu);
            hub.suite[i]["state"] = "running";
        }
        std::printf("[%zu/%zu] %s ...\n", i + 1, plan.size(), plan[i].name.c_str());
        std::fflush(stdout);
        json::Value s;
        try {
            s = exp.run(plan[i]);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "run %s aborted: %s\n", plan[i].name.c_str(), e.what());
            return 2;
        }
        auto& J = *s.find("jobs");
        std::printf("    jobs %d/%d  JCT mean %.2fs p95 %.2fs  makespan %.1fs  util %.2f  telemetry %.2f kB/s/node\n",
                    int(J.get_num("completed", 0)), int(J.get_num("submitted", 0)), J.get_num("jct_mean_s", 0),
                    J.get_num("jct_p95_s", 0), J.get_num("makespan_s", 0), get_path(s, "utilisation.core_util_mean"),
                    get_path(s, "overhead.telemetry_kBps_per_node"));
        std::fflush(stdout);
        s["timeline"] = json::Value();
        results.push_back(s);
        {
            std::lock_guard<std::mutex> g(hub.mu);
            hub.suite[i]["state"] = "done";
        }
        if (cmd == "suite" || plan.size() > 1) {
            json::Value b;
            b["system_profile"] = prof.j;
            b["workload"] = json::Value::object();
            b["workload"]["duration_s"] = wp.duration_s;
            b["workload"]["rate_per_s"] = wp.rate_per_s;
            b["aggregate"] = aggregate(results);
            json::Value rs = json::Value::array();
            for (auto& r : results) rs.push(r);
            b["runs"] = rs;
            std::ofstream f(out + "/benchmark.json");
            f << json::dump(b, 2) << "\n";
        }
    }
    if (hold) {
        std::printf("all runs finished; dashboard stays up at http://localhost:%d/ (Ctrl+C to exit)\n", port);
        std::fflush(stdout);
        while (true) std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return 0;
}
