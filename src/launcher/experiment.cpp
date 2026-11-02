#include "nexus/experiment.hpp"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cmath>
#include <fstream>
#include <thread>

namespace nexus {

static void mkdirs(const std::string& p) {
    std::string cur;
    for (size_t i = 0; i < p.size(); ++i) {
        cur.push_back(p[i]);
        if (p[i] == '/' || i + 1 == p.size()) ::mkdir(cur.c_str(), 0755);
    }
}
static double mean(const std::vector<double>& v) {
    if (v.empty()) return 0;
    double s = 0;
    for (double x : v) s += x;
    return s / double(v.size());
}
static double quant(std::vector<double> v, double q) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    double pos = q * double(v.size() - 1);
    size_t i = size_t(pos);
    double f = pos - double(i);
    return i + 1 < v.size() ? v[i] * (1 - f) + v[i + 1] * f : v[i];
}
static double r3(double x) { return std::round(x * 1000.0) / 1000.0; }

Experiment::Experiment(const ClusterConfig& cfg, std::string config_path, std::string node_bin, std::string out_root,
                       MonitorHub* hub)
    : cfg_(cfg), config_path_(std::move(config_path)), node_bin_(std::move(node_bin)), out_root_(std::move(out_root)),
      hub_(hub) {
    json::Value topo;
    topo["name"] = cfg_.name;
    json::Value racks = json::Value::array();
    for (auto& r : cfg_.racks) {
        json::Value x;
        x["name"] = r.name;
        x["uplink_gbps"] = r.uplink_gbps;
        racks.push(x);
    }
    topo["racks"] = racks;
    json::Value nodes = json::Value::array();
    for (auto& n : cfg_.nodes) {
        json::Value x;
        x["id"] = int(n.id);
        x["name"] = n.name;
        x["rack"] = n.rack;
        x["cores"] = n.cores;
        x["mem"] = n.mem_gb;
        x["gpus"] = n.gpus;
        x["partition"] = n.partition;
        x["cpu_speed"] = n.cpu_speed;
        x["gpu_speed"] = n.gpu_speed;
        x["nic_gbps"] = n.nic_gbps;
        json::Value ds = json::Value::array();
        for (int d : n.datasets) ds.push(d);
        x["datasets"] = ds;
        nodes.push(x);
    }
    topo["nodes"] = nodes;
    json::Value dss = json::Value::array();
    for (auto& d : cfg_.datasets) {
        json::Value x;
        x["id"] = d.id;
        x["name"] = d.name;
        x["size_mb"] = d.size_mb;
        dss.push(x);
    }
    topo["datasets"] = dss;
    if (hub_) {
        std::lock_guard<std::mutex> g(hub_->mu);
        hub_->topology = topo;
    }
}

void Experiment::spawn(int node, const RunSpec& spec, int64_t t0) {
    std::string log = run_dir_ + "/logs/" + cfg_.nodes[size_t(node)].name + ".log";
    std::vector<std::string> args = {node_bin_, "--config", config_path_, "--id", std::to_string(node),
                                     "--fabric", fabric_name_, "--policy", policy_name(spec.policy),
                                     "--probe", probe_mode_name(spec.probe), "--t0", std::to_string(t0),
                                     "--seed", std::to_string(spec.seed), "--coordinator", std::to_string(spec.coordinator)};
    pid_t pid = ::fork();
    if (pid == 0) {
        int fd = ::open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) { ::dup2(fd, 1); ::dup2(fd, 2); ::close(fd); }
        std::vector<char*> av;
        for (auto& a : args) av.push_back(const_cast<char*>(a.c_str()));
        av.push_back(nullptr);
        ::execv(node_bin_.c_str(), av.data());
        ::_exit(127);
    }
    pids_[size_t(node)] = pid;
    killed_[size_t(node)] = false;
}

void Experiment::submit(ClientJob& j) {
    SubmitMsg m;
    m.job = j.spec;
    m.epoch = j.epoch;
    for (size_t i = 0; i < j.done.size(); ++i)
        if (j.done[i]) m.done.push_back({uint16_t(i), j.out_loc[i]});
    Writer w;
    m.write(w);
    tx_->send(j.home, MsgType::Submit, w, kFlagMgmt);
    j.sends++;
    j.last_send = now_ns();
    j.submitted = true;
}

NodeId Experiment::pick_new_home(NodeId old, double t) {
    std::vector<int> load(cfg_.nodes.size(), 0);
    for (auto& [id, j] : jobs_)
        if (j.submitted && j.remaining > 0 && j.home < load.size()) load[j.home]++;
    NodeId best = kNoNode;
    int best_score = 1 << 30;
    for (auto& n : cfg_.nodes) {
        if (n.id == old || t - last_pong_[n.id] > 1.5) continue;
        int score = load[n.id] * 4 + (n.rack == cfg_.nodes[old].rack ? 0 : 3);
        if (score < best_score) { best_score = score; best = n.id; }
    }
    return best;
}

void Experiment::handle_event(const json::Value& ev, double t) {
    std::string type = ev.get_str("type", "");
    int node = int(ev.get_num("node", -1));
    auto fe = [&](json::Value e) {
        if (frame_events_.size() < 200) frame_events_.push_back(std::move(e));
    };
    if (type == "status") {
        if (node < 0 || node >= int(cfg_.nodes.size())) return;
        json::Value s = ev;
        s["_rx"] = t;
        latest_status_[size_t(node)] = s;
        last_status_inc_[{node, incarnation_[size_t(node)]}] = s;
        if (t >= 0) {
            if (auto v = ev.find("view"))
                for (size_t i = 0; i < v->size(); ++i) {
                    auto& e = (*v)[i];
                    double le = e.get_num("lat_ms", -1), lt = e.get_num("true_lat_ms", -1);
                    if (le > 0 && lt > 0) est_lat_err_.push_back(std::fabs(le - lt) / lt);
                    double be = e.get_num("bw_MBps", -1), bt = e.get_num("true_bw_MBps", -1);
                    if (be > 0 && bt > 0) est_bw_err_.push_back(std::fabs(be - bt) / bt);
                }
        }
    } else if (type == "hello") {
        incarnation_[size_t(node)]++;
        last_pong_[size_t(node)] = t;
    } else if (type == "decision") {
        if (frame_decisions_.size() >= 6) frame_decisions_.erase(frame_decisions_.begin());
        frame_decisions_.push_back(ev);
    } else if (type == "task_started") {
        n_started_++;
        json::Value e;
        e["k"] = "start";
        e["task"] = fmt("%u.%d", uint32_t(ev.get_num("job", 0)), int(ev.get_num("idx", 0)));
        e["node"] = node;
        e["cls"] = ev.get_str("cls", "");
        e["t"] = r3(t);
        fe(e);
    } else if (type == "task_done") {
        uint32_t job = uint32_t(ev.get_num("job", 0));
        int idx = int(ev.get_num("idx", 0));
        auto key = std::make_pair(job, idx);
        if (task_done_.count(key)) { n_dup_++; return; }
        auto jit = jobs_.find(job);
        if (jit == jobs_.end()) return;
        task_done_[key] = true;
        ClientJob& j = jit->second;
        TaskRec r;
        r.job = job;
        r.idx = idx;
        r.cls = ev.get_str("cls", "");
        r.node = node;
        r.home = int(ev.get_num("home", -1));
        r.t_done = t;
        r.wait = ev.get_num("wait", 0);
        r.xfer = ev.get_num("xfer", 0);
        r.exec = ev.get_num("exec", 0);
        r.pred_wait = ev.get_num("pred_wait", 0);
        r.pred_xfer = ev.get_num("pred_xfer", 0);
        r.pred_exec = ev.get_num("pred_exec", 0);
        r.net_bytes = ev.get_num("net_bytes", 0);
        r.in_bytes = ev.get_num("in_bytes", 0);
        r.migrations = int(ev.get_num("migrations", 0));
        tasks_.push_back(r);
        tasks_done_interval_++;
        if (size_t(idx) < j.done.size() && !j.done[size_t(idx)]) {
            j.done[size_t(idx)] = 1;
            j.out_loc[size_t(idx)] = NodeId(node);
            j.remaining--;
            if (j.remaining == 0) {
                j.done_t = t;
                json::Value e;
                e["k"] = "job";
                e["job"] = double(job);
                e["kind"] = j.spec.kind;
                e["jct"] = r3(t - j.spec.submit_s);
                e["t"] = r3(t);
                fe(e);
            }
        }
        json::Value e;
        e["k"] = "done";
        e["task"] = fmt("%u.%d", job, idx);
        e["node"] = node;
        e["cls"] = r.cls;
        e["exec"] = r3(r.exec);
        e["xfer"] = r3(r.xfer);
        e["wait"] = r3(r.wait);
        e["t"] = r3(t);
        fe(e);
    } else if (type == "task_failed") {
        n_failed_++;
        json::Value e;
        e["k"] = "fail";
        e["task"] = fmt("%u.%d", uint32_t(ev.get_num("job", 0)), int(ev.get_num("idx", 0)));
        e["node"] = node;
        e["t"] = r3(t);
        fe(e);
    } else if (type == "reschedule") {
        n_resched_++;
        json::Value e;
        e["k"] = "resched";
        e["task"] = ev.get_str("task", "");
        e["why"] = ev.get_str("why", "");
        e["node"] = int(ev.get_num("bad", -1));
        e["t"] = r3(t);
        fe(e);
    } else if (type == "migrate") {
        n_migrate_++;
        json::Value e;
        e["k"] = "migrate";
        e["task"] = ev.get_str("task", "");
        e["from"] = int(ev.get_num("from", -1));
        e["to"] = int(ev.get_num("to", -1));
        e["t"] = r3(t);
        fe(e);
    } else if (type == "nack") {
        n_nack_++;
        nack_reasons_[ev.get_str("why", "?")]++;
    } else if (type == "fault") {
        json::Value e;
        e["k"] = "fault";
        e["node"] = node;
        e["what"] = ev.get_str("kind", "");
        e["t"] = r3(t);
        fe(e);
    }
}

void Experiment::sample(double t, double dt) {
    double now_t = t;
    json::Value f;
    f["t"] = r3(t);
    json::Value nodes = json::Value::array();
    double cores = 0, used = 0, gpus = 0, gused = 0;
    int queued = 0, running = 0;
    for (size_t i = 0; i < cfg_.nodes.size(); ++i) {
        const json::Value& s = latest_status_[i];
        json::Value n;
        n["id"] = int(i);
        bool up = !s.is_null() && now_t - s.get_num("_rx", -1e9) < 2.0 && !killed_[i] && !frozen_[i];
        n["up"] = up;
        if (!s.is_null()) {
            int c = int(s.get_num("cores", 0)), cf = int(s.get_num("cores_free", 0));
            int g = int(s.get_num("gpus", 0)), gf = int(s.get_num("gpus_free", 0));
            n["cf"] = cf;
            n["gf"] = gf;
            n["mf"] = s.get_num("mem_free", 0);
            n["q"] = s.get_num("queued", 0);
            n["sg"] = s.get_num("staging", 0);
            n["run"] = s.get_num("running", 0);
            n["nic"] = r3(s.get_num("nic_util", 0));
            n["bl"] = r3(s.get_num("backlog_core_s", 0));
            n["gpu_ok"] = s.get_bool("gpu_ok", true);
            if (auto tel = s.find("telemetry")) {
                n["iv"] = std::round(tel->get_num("interval_ms", 0));
                n["tb"] = tel->get_num("bytes", 0);
            }
            if (auto cr = s.find("corr")) {
                json::Value a = json::Value::array();
                for (size_t k = 0; k < cr->size(); ++k) a.push(r3((*cr)[k].num()));
                n["corr"] = a;
            }
            if (auto cn = s.find("counters")) {
                json::Value a = json::Value::array();
                for (const char* k : {"decisions", "local", "delegated", "nacks", "migrations", "reschedules", "completed", "failed"})
                    a.push(cn->get_num(k, 0));
                a.push(r3(cn->get_num("decision_ms", 0)));
                n["cnt"] = a;
            }
            if (auto v = s.find("view")) {
                json::Value a = json::Value::array();
                for (size_t k = 0; k < v->size(); ++k) {
                    auto& e = (*v)[k];
                    std::string st = e.get_str("status", "unknown");
                    int sc = st == "fresh" ? 1 : st == "stale" ? 2 : st == "suspected" ? 3 : 0;
                    json::Value row = json::Value::array();
                    row.push(e.get_num("peer", 0));
                    row.push(sc);
                    row.push(r3(e.get_num("age", 0)));
                    row.push(r3(e.get_num("lat_ms", -1)));
                    row.push(r3(e.get_num("true_lat_ms", -1)));
                    row.push(std::round(e.get_num("bw_MBps", -1)));
                    row.push(std::round(e.get_num("true_bw_MBps", -1)));
                    a.push(row);
                }
                n["view"] = a;
            }
            if (auto q = s.find("queue")) {
                json::Value a = json::Value::array();
                for (size_t k = 0; k < q->size() && k < 12; ++k) {
                    auto& e = (*q)[k];
                    json::Value row = json::Value::array();
                    row.push(e.get_str("task", ""));
                    row.push(e.get_str("cls", ""));
                    row.push(e.get_str("state", ""));
                    row.push(e.get_num("cores", 0));
                    row.push(e.get_num("gpus", 0));
                    row.push(r3(e.get_num("age", 0)));
                    a.push(row);
                }
                n["queue"] = a;
            }
            if (up) {
                cores += c;
                used += c - cf;
                gpus += g;
                gused += g - gf;
                queued += int(s.get_num("queued", 0));
                running += int(s.get_num("running", 0) + s.get_num("staging", 0));
            }
        }
        nodes.push(n);
    }
    f["nodes"] = nodes;
    // ground-truth link utilisation from the fabric's counters
    json::Value links = json::Value::array();
    double data_bytes = 0;
    for (int i = 0; i < fabric_.n_links(); ++i) {
        uint64_t b = fabric_.link(i).busy_ns_total.load();
        double u = dt > 0 ? double(b - last_busy_ns_[size_t(i)]) / (dt * 1e9) : 0;
        last_busy_ns_[size_t(i)] = b;
        links.push(r3(std::min(1.0, u)));
        if (i < 2 * int(cfg_.nodes.size()) && i % 2 == 0) {
            uint64_t bb = fabric_.link(i).bytes_bulk.load();
            data_bytes += double(bb - last_bulk_[size_t(i)]);
            last_bulk_[size_t(i)] = bb;
        }
    }
    f["links"] = links;
    double tel = 0;
    for (auto& [k, s] : last_status_inc_)
        if (auto t2 = s.find("telemetry")) tel += t2->get_num("bytes", 0);
    int done_jobs = 0, sub_jobs = 0;
    for (auto& [id, j] : jobs_) {
        if (j.submitted) sub_jobs++;
        if (j.done_t >= 0) done_jobs++;
    }
    json::Value m;
    m["util"] = r3(cores > 0 ? used / cores : 0);
    m["gpu_util"] = r3(gpus > 0 ? gused / gpus : 0);
    m["thr"] = r3(dt > 0 ? tasks_done_interval_ / dt : 0);
    m["jobs_sub"] = sub_jobs;
    m["jobs_done"] = done_jobs;
    m["running"] = running;
    m["queued"] = queued;
    m["tel_kBps"] = r3(dt > 0 ? (tel - last_tel_bytes_) / dt / 1e3 : 0);
    m["data_MBps"] = r3(dt > 0 ? data_bytes / dt / 1e6 : 0);
    m["lat_err"] = r3(est_lat_err_.empty() ? 0 : quant(std::vector<double>(est_lat_err_.end() - std::min<size_t>(est_lat_err_.size(), 400), est_lat_err_.end()), 0.5));
    m["bw_err"] = r3(est_bw_err_.empty() ? 0 : quant(std::vector<double>(est_bw_err_.end() - std::min<size_t>(est_bw_err_.size(), 400), est_bw_err_.end()), 0.5));
    last_tel_bytes_ = tel;
    tasks_done_interval_ = 0;
    f["m"] = m;
    json::Value evs = json::Value::array();
    for (auto& e : frame_events_) evs.push(e);
    f["ev"] = evs;
    json::Value decs = json::Value::array();
    for (auto& d : frame_decisions_) decs.push(d);
    f["dec"] = decs;
    frame_events_.clear();
    frame_decisions_.clear();
    json::Value tl = m;
    tl["t"] = r3(t);
    timeline_.push(tl);
    if (hub_) {
        std::string s = json::dump(f);
        std::lock_guard<std::mutex> g(hub_->mu);
        hub_->frames.push_back(std::move(s));
    }
}

json::Value Experiment::run(const RunSpec& spec) {
    spec_ = spec;
    size_t N = cfg_.nodes.size();
    run_dir_ = out_root_ + "/" + spec.name;
    mkdirs(run_dir_ + "/logs");
    pids_.assign(N, 0);
    incarnation_.assign(N, 0);
    last_pong_.assign(N, -1e9);
    killed_.assign(N, false);
    frozen_.assign(N, false);
    latest_status_.assign(N, json::Value());
    last_status_inc_.clear();
    jobs_.clear();
    tasks_.clear();
    task_done_.clear();
    frame_events_.clear();
    frame_decisions_.clear();
    timeline_ = json::Value::array();
    est_lat_err_.clear();
    est_bw_err_.clear();
    fault_log_.clear();
    usage_.assign(N, Usage{});
    n_started_ = n_failed_ = n_dup_ = n_resched_ = n_migrate_ = n_nack_ = n_resubmit_ = 0;
    nack_reasons_.clear();
    last_tel_bytes_ = 0;
    tasks_done_interval_ = 0;

    static int counter = 0;
    fabric_name_ = "/nexus_" + std::to_string(::getpid()) + "_" + std::to_string(++counter);
    fabric_ = Fabric::create(fabric_name_, cfg_);
    last_busy_ns_.assign(size_t(fabric_.n_links()), 0);
    last_bulk_.assign(size_t(fabric_.n_links()), 0);
    tx_ = std::make_unique<Transport>(kClientId, cfg_, nullptr, spec.seed);
    for (int tries = 0; !tx_->open(); ++tries) {
        if (tries > 10) throw std::runtime_error(fmt("cannot bind client UDP port %d (another launcher running?)", cfg_.client_port));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto trace = generate_workload(cfg_, spec.wp, spec.seed);
    for (auto& js : trace) {
        ClientJob j;
        j.spec = js;
        j.home = is_coordinated(spec.policy) ? spec.coordinator : js.origin;
        j.done.assign(js.tasks.size(), 0);
        j.out_loc.assign(js.tasks.size(), kNoNode);
        j.remaining = int(js.tasks.size());
        jobs_[js.id] = j;
    }
    WorkloadStats ws = workload_stats(cfg_, trace);

    int64_t spawn_ns = now_ns();
    t0_ns_ = spawn_ns + s_to_ns(spec.warmup_s);
    for (size_t i = 0; i < N; ++i) spawn(int(i), spec, t0_ns_);

    json::Value meta;
    meta["name"] = spec.name;
    meta["scenario"] = spec.scenario;
    meta["policy"] = policy_name(spec.policy);
    meta["probe"] = probe_mode_name(spec.probe);
    meta["seed"] = double(spec.seed);
    meta["faults"] = spec.faults;
    meta["jobs"] = ws.jobs;
    meta["tasks"] = ws.tasks;
    meta["duration_s"] = spec.wp.duration_s;
    meta["rate_per_s"] = spec.wp.rate_per_s;
    meta["core_seconds"] = ws.core_seconds;
    meta["gpu_seconds"] = ws.gpu_seconds;
    meta["input_gb"] = ws.input_gb;
    meta["coordinator"] = is_coordinated(spec.policy) ? int(spec.coordinator) : -1;
    if (hub_) {
        std::lock_guard<std::mutex> g(hub_->mu);
        hub_->run_counter++;
        meta["run_id"] = hub_->run_counter;
        hub_->run_meta = meta;
        hub_->frames.clear();
    }
    LOG_I("run %s: %d jobs / %d tasks, %.0f core-s, %.0f gpu-s, %.1f GB input", spec.name.c_str(), ws.jobs, ws.tasks,
          ws.core_seconds, ws.gpu_seconds, ws.input_gb);

    // fault schedule
    struct FaultEv { double t; std::string kind; int node; bool fired; };
    std::vector<FaultEv> faults;
    auto node_by_name = [&](const std::string& n) {
        for (auto& s : cfg_.nodes) if (s.name == n) return int(s.id);
        return 0;
    };
    if (spec.faults) {
        double D = spec.wp.duration_s;
        int k = int(spec.coordinator), g = node_by_name("g1"), f = node_by_name("a3");
        faults = {{0.30 * D, "kill", k, false}, {0.30 * D + 20, "restart", k, false},
                  {0.50 * D, "gpu-fail", g, false}, {0.50 * D + 25, "gpu-recover", g, false},
                  {0.65 * D, "freeze", f, false}, {0.65 * D + 8, "thaw", f, false}};
    }
    // background traffic
    std::vector<std::pair<int, CrossTraffic>> cross;
    for (auto& c : cfg_.cross) {
        int li = fabric_.link_index(c.link, cfg_);
        if (li >= 0) cross.push_back({li, c});
    }

    auto collect = [&](size_t i, int opts) {
        int st;
        rusage ru{};
        pid_t r = ::wait4(pids_[i], &st, opts, &ru);
        if (r == pids_[i]) {
            usage_[i].cpu_s += double(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) + 1e-6 * double(ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
            usage_[i].maxrss_mb = std::max(usage_[i].maxrss_mb, double(ru.ru_maxrss) / 1024.0);
            pids_[i] = 0;
            return true;
        }
        return false;
    };

    int64_t last_cross = 0, last_ping = 0, last_sample = spawn_ns;
    double t_end_trace = spec.wp.duration_s;
    double finished_at = -1;
    bool started_ok = false;
    Datagram dg;
    while (true) {
        int64_t now = now_ns();
        double t = ns_to_s(now - t0_ns_);
        if (!started_ok && now - spawn_ns > s_to_ns(3.0)) {
            int up = 0;
            for (size_t i = 0; i < N; ++i) up += incarnation_[i] >= 1 ? 1 : 0;
            if (up < int(N)) {
                for (size_t i = 0; i < N; ++i)
                    if (pids_[i]) { ::kill(pids_[i], SIGKILL); ::waitpid(pids_[i], nullptr, 0); }
                tx_->close();
                Fabric::unlink(fabric_name_);
                throw std::runtime_error(fmt("only %d of %zu node processes came up (UDP ports %d-%zu busy? see %s/logs)", up, N,
                                             cfg_.base_port, cfg_.base_port + N - 1, run_dir_.c_str()));
            }
            started_ok = true;
        }
        // background traffic every 50 ms
        if (now - last_cross >= ms_to_ns(50)) {
            double dts = last_cross ? ns_to_s(now - last_cross) : 0.05;
            last_cross = now;
            double ts = ns_to_s(now - spawn_ns);
            for (auto& [li, c] : cross) {
                double ph = std::fmod(ts + c.phase_s, c.period_s) / c.period_s;
                if (ph < c.duty) fabric_.inject(li, uint64_t(c.load * fabric_.link(li).cap_Bps * dts), now);
            }
        }
        // submissions
        if (t >= 0)
            for (auto& [id, j] : jobs_)
                if (!j.submitted && j.spec.submit_s <= t) submit(j);
        for (auto& [id, j] : jobs_)
            if (j.submitted && !j.acked && j.remaining > 0 && now - j.last_send > ms_to_ns(1000) && j.sends < 30) submit(j);
        // liveness pings & client-side recovery
        if (now - last_ping >= ms_to_ns(1000)) {
            last_ping = now;
            Writer w;
            w.put(uint32_t(0));
            for (size_t i = 0; i < N; ++i) tx_->send(NodeId(i), MsgType::Ping, w, kFlagMgmt);
            if (t > 0) {
                for (auto& [id, j] : jobs_) {
                    if (!j.submitted || j.remaining == 0) continue;
                    bool home_dead = t - last_pong_[j.home] > 3.0;
                    // coordinated baselines cannot move jobs: they wait for the coordinator to return
                    if (is_coordinated(spec.policy)) continue;
                    if (home_dead) {
                        NodeId nh = pick_new_home(j.home, t);
                        if (nh == kNoNode) continue;
                        j.home = nh;
                        j.epoch++;
                        j.acked = false;
                        j.sends = 0;
                        j.resubmits++;
                        n_resubmit_++;
                        submit(j);
                    }
                }
            }
        }
        // faults
        for (auto& f : faults) {
            if (f.fired || t < f.t) continue;
            f.fired = true;
            json::Value fl;
            fl["t"] = f.t;
            fl["kind"] = f.kind;
            fl["node"] = cfg_.nodes[size_t(f.node)].name;
            fault_log_.push_back(fl);
            json::Value e;
            e["k"] = "fault";
            e["node"] = f.node;
            e["what"] = f.kind;
            e["t"] = r3(t);
            frame_events_.push_back(e);
            LOG_I("fault @%.1fs: %s %s", t, f.kind.c_str(), cfg_.nodes[size_t(f.node)].name.c_str());
            size_t n = size_t(f.node);
            if (f.kind == "kill" && pids_[n]) {
                ::kill(pids_[n], SIGKILL);
                collect(n, 0);
                killed_[n] = true;
            } else if (f.kind == "restart") {
                // fresh process, empty state; for the coordinated baselines the client notices the
                // new incarnation through its hello and re-submits unfinished jobs (see below)
                spawn(f.node, spec, t0_ns_);
            } else if (f.kind == "freeze" && pids_[n]) {
                ::kill(pids_[n], SIGSTOP);
                frozen_[n] = true;
            } else if (f.kind == "thaw" && pids_[n]) {
                ::kill(pids_[n], SIGCONT);
                frozen_[n] = false;
            } else if (f.kind == "gpu-fail" || f.kind == "gpu-recover") {
                Writer w;
                w.put(f.kind == "gpu-fail" ? FaultKind::GpuFail : FaultKind::GpuRecover);
                tx_->send(NodeId(n), MsgType::Fault, w, kFlagMgmt);
            }
        }
        // sampling
        if (now - last_sample >= ms_to_ns(1000)) {
            double dt = ns_to_s(now - last_sample);
            last_sample = now;
            sample(t, dt);
        }
        // receive
        pollfd p{tx_->fd(), POLLIN, 0};
        ::poll(&p, 1, 5);
        int budget = 2000;
        while (budget-- > 0 && tx_->recv(dg)) {
            if (dg.h.type == MsgType::MonitorJson) {
                try {
                    std::string s(dg.body.begin(), dg.body.end());
                    json::Value ev = json::parse(s);
                    int prev_inc = -1;
                    int nd = int(ev.get_num("node", -1));
                    if (nd >= 0 && size_t(nd) < N) prev_inc = incarnation_[size_t(nd)];
                    handle_event(ev, ns_to_s(now_ns() - t0_ns_));
                    // restarted coordinator: re-submit unfinished jobs with a new epoch
                    if (is_coordinated(spec.policy) && ev.get_str("type", "") == "hello" && nd == int(spec.coordinator) &&
                        prev_inc >= 1) {
                        for (auto& [id, j] : jobs_) {
                            if (!j.submitted || j.remaining == 0) continue;
                            j.epoch++;
                            j.acked = false;
                            j.sends = 0;
                            j.resubmits++;
                            n_resubmit_++;
                            submit(j);
                        }
                    }
                } catch (const std::exception& e) {
                    LOG_W("bad monitor json: %s", e.what());
                }
            } else if (dg.h.type == MsgType::SubmitAck) {
                Reader r = dg.reader();
                uint32_t jid = r.get<uint32_t>();
                uint16_t ep = r.get<uint16_t>();
                auto it = jobs_.find(jid);
                if (it != jobs_.end() && it->second.epoch == ep) it->second.acked = true;
            } else if (dg.h.type == MsgType::Pong) {
                if (dg.h.src < N) last_pong_[dg.h.src] = ns_to_s(now_ns() - t0_ns_);
            }
        }
        // termination
        bool all_done = true;
        for (auto& [id, j] : jobs_)
            if (j.remaining > 0) { all_done = false; break; }
        if (all_done && finished_at < 0) finished_at = t;
        if (finished_at >= 0 && t - finished_at > 1.0) break;
        if (t > t_end_trace + spec.drain_s) {
            LOG_W("run %s hit the drain limit with unfinished jobs", spec.name.c_str());
            break;
        }
    }
    double t_stop = ns_to_s(now_ns() - t0_ns_);
    sample(t_stop, 1.0);
    run_len_s_ = t_stop + spec.warmup_s;

    // shut down node processes
    for (size_t i = 0; i < N; ++i) {
        if (!pids_[i]) continue;
        if (frozen_[i]) ::kill(pids_[i], SIGCONT);
        ::kill(pids_[i], SIGTERM);
    }
    int64_t deadline = now_ns() + s_to_ns(3);
    while (now_ns() < deadline) {
        bool any = false;
        for (size_t i = 0; i < N; ++i)
            if (pids_[i] && !collect(i, WNOHANG)) any = true;
        if (!any) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    for (size_t i = 0; i < N; ++i)
        if (pids_[i]) { ::kill(pids_[i], SIGKILL); collect(i, 0); }
    tx_->close();
    Fabric::unlink(fabric_name_);

    double first_sub = 1e18, last_done = 0;
    for (auto& [id, j] : jobs_) {
        first_sub = std::min(first_sub, j.spec.submit_s);
        if (j.done_t >= 0) last_done = std::max(last_done, j.done_t);
    }
    json::Value summary = summarize(spec, last_done - std::min(first_sub, last_done));
    write_outputs(spec, summary);
    if (hub_) {
        std::lock_guard<std::mutex> g(hub_->mu);
        json::Value s = summary;
        s["timeline"] = json::Value();
        hub_->summaries.push_back(s);
    }
    return summary;
}

json::Value Experiment::summarize(const RunSpec& spec, double makespan) {
    json::Value s;
    s["name"] = spec.name;
    s["scenario"] = spec.scenario;
    s["policy"] = policy_name(spec.policy);
    s["probe"] = probe_mode_name(spec.probe);
    s["seed"] = double(spec.seed);
    s["faults"] = spec.faults;
    // jobs
    std::vector<double> jct;
    std::map<std::string, std::vector<double>> by_kind;
    int incomplete = 0;
    for (auto& [id, j] : jobs_) {
        if (j.done_t < 0) { incomplete++; continue; }
        double v = j.done_t - j.spec.submit_s;
        jct.push_back(v);
        by_kind[j.spec.kind].push_back(v);
    }
    json::Value J;
    J["submitted"] = int(jobs_.size());
    J["completed"] = int(jct.size());
    J["incomplete"] = incomplete;
    J["makespan_s"] = r3(makespan);
    J["jct_mean_s"] = r3(mean(jct));
    J["jct_p50_s"] = r3(quant(jct, 0.5));
    J["jct_p95_s"] = r3(quant(jct, 0.95));
    J["jct_p99_s"] = r3(quant(jct, 0.99));
    J["resubmitted"] = double(n_resubmit_);
    json::Value K;
    for (auto& [k, v] : by_kind) K[k] = r3(mean(v));
    J["jct_mean_by_kind_s"] = K;
    s["jobs"] = J;
    // tasks
    std::vector<double> wait, xfer, exec, turn, pe_exec, pe_total, ae_xfer;
    double net = 0, inb = 0;
    int remote_tasks = 0, migrated = 0;
    for (auto& t : tasks_) {
        wait.push_back(t.wait);
        xfer.push_back(t.xfer);
        exec.push_back(t.exec);
        turn.push_back(t.wait + t.xfer + t.exec);
        net += t.net_bytes;
        inb += t.in_bytes;
        if (t.net_bytes > 0) remote_tasks++;
        if (t.migrations > 0) migrated++;
        if (t.pred_exec > 0 && t.exec > 0) pe_exec.push_back(std::fabs(t.pred_exec - t.exec) / t.exec);
        double pt = t.pred_wait + t.pred_xfer + t.pred_exec, at = t.wait + t.xfer + t.exec;
        if (pt > 0 && at > 0) pe_total.push_back(std::fabs(pt - at) / at);
        ae_xfer.push_back(std::fabs(t.pred_xfer - t.xfer));
    }
    json::Value T;
    T["completed"] = int(tasks_.size());
    T["wait_mean_s"] = r3(mean(wait));
    T["xfer_mean_s"] = r3(mean(xfer));
    T["exec_mean_s"] = r3(mean(exec));
    T["turnaround_mean_s"] = r3(mean(turn));
    T["turnaround_p95_s"] = r3(quant(turn, 0.95));
    T["network_gb"] = r3(net / 1e9);
    T["input_gb"] = r3(inb / 1e9);
    T["local_input_fraction"] = r3(inb > 0 ? 1.0 - net / inb : 0);
    T["remote_input_tasks"] = remote_tasks;
    T["migrated_tasks"] = migrated;
    T["exec_pred_mape"] = r3(mean(pe_exec));
    T["exec_pred_mape_first_half"] = r3(mean(std::vector<double>(pe_exec.begin(), pe_exec.begin() + long(pe_exec.size() / 2))));
    T["exec_pred_mape_second_half"] = r3(mean(std::vector<double>(pe_exec.begin() + long(pe_exec.size() / 2), pe_exec.end())));
    T["total_pred_mape"] = r3(mean(pe_total));
    T["xfer_pred_mae_s"] = r3(mean(ae_xfer));
    T["failed_attempts"] = double(n_failed_);
    T["duplicate_completions"] = double(n_dup_);
    T["reschedules"] = double(n_resched_);
    T["migrations"] = double(n_migrate_);
    s["tasks"] = T;
    // utilisation over the active window
    std::vector<double> u, gu, tel, data;
    for (size_t i = 0; i < timeline_.size(); ++i) {
        auto& p = timeline_[i];
        double t = p.get_num("t", 0);
        if (t < 0 || t > makespan + 1) continue;
        u.push_back(p.get_num("util", 0));
        gu.push_back(p.get_num("gpu_util", 0));
        tel.push_back(p.get_num("tel_kBps", 0));
        data.push_back(p.get_num("data_MBps", 0));
    }
    json::Value U;
    U["core_util_mean"] = r3(mean(u));
    U["gpu_util_mean"] = r3(mean(gu));
    U["data_MBps_mean"] = r3(mean(data));
    s["utilisation"] = U;
    // control / telemetry overhead (sum over every node incarnation)
    double tel_bytes = 0, tel_msgs = 0, ctrl_bytes = 0, ctrl_msgs = 0, dec = 0, loc = 0, del = 0, nacks = 0, migs = 0,
           grants = 0, denies = 0, lat_probes = 0, bw_probes = 0, deferred = 0, skips = 0;
    std::vector<double> dms, ivs;
    for (auto& [k, st] : last_status_inc_) {
        if (auto t = st.find("telemetry")) {
            tel_bytes += t->get_num("bytes", 0);
            tel_msgs += t->get_num("msgs", 0);
            ctrl_bytes += t->get_num("ctrl_bytes", 0);
            ctrl_msgs += t->get_num("ctrl_msgs", 0);
            lat_probes += t->get_num("lat_probes", 0);
            bw_probes += t->get_num("bw_probes", 0);
            deferred += t->get_num("deferred", 0);
            skips += t->get_num("budget_skips", 0);
            ivs.push_back(t->get_num("interval_ms", 0));
        }
        if (auto c = st.find("counters")) {
            dec += c->get_num("decisions", 0);
            loc += c->get_num("local", 0);
            del += c->get_num("delegated", 0);
            nacks += c->get_num("nacks", 0);
            migs += c->get_num("migrations", 0);
            grants += c->get_num("grants", 0);
            denies += c->get_num("denies", 0);
            double d = c->get_num("decision_ms", 0);
            if (d > 0) dms.push_back(d);
        }
    }
    double run_len = std::max(1.0, run_len_s_);  // wall time the node processes were up
    json::Value O;
    O["telemetry_bytes"] = tel_bytes;
    O["telemetry_msgs"] = tel_msgs;
    O["telemetry_kBps_per_node"] = r3(tel_bytes / run_len / 1e3 / double(cfg_.nodes.size()));
    O["control_bytes_total"] = ctrl_bytes;
    O["control_msgs_total"] = ctrl_msgs;
    O["telemetry_to_data_ratio"] = r3(net > 0 ? tel_bytes / net : 0);
    O["latency_probes"] = lat_probes;
    O["bandwidth_probes"] = bw_probes;
    O["probes_deferred_busy_nic"] = deferred;
    O["probes_skipped_budget"] = skips;
    O["probe_interval_ms_mean"] = r3(mean(ivs));
    O["lat_est_rel_err_p50"] = r3(quant(est_lat_err_, 0.5));
    O["bw_est_rel_err_p50"] = r3(quant(est_bw_err_, 0.5));
    s["overhead"] = O;
    json::Value S;
    S["decisions"] = dec;
    S["local_placements"] = loc;
    S["delegations"] = del;
    S["reservation_nacks"] = nacks;
    S["reservations_granted"] = grants;
    S["reservations_denied"] = denies;
    S["migrations"] = migs;
    S["decision_ms_mean"] = r3(mean(dms));
    json::Value NR;
    for (auto& [k, v] : nack_reasons_) NR[k] = v;
    S["nack_reasons"] = NR;
    s["scheduling"] = S;
    double cpu = 0, rss = 0;
    for (auto& x : usage_) { cpu += x.cpu_s; rss += x.maxrss_mb; }
    json::Value H;
    H["node_processes"] = int(cfg_.nodes.size());
    H["node_cpu_seconds_total"] = r3(cpu);
    H["node_cpu_share_of_one_core"] = r3(cpu / run_len);
    H["node_maxrss_mb_mean"] = r3(rss / double(std::max<size_t>(1, usage_.size())));
    s["host"] = H;
    json::Value F = json::Value::array();
    for (auto& f : fault_log_) F.push(f);
    s["fault_log"] = F;
    s["timeline"] = timeline_;
    return s;
}

void Experiment::write_outputs(const RunSpec& spec, const json::Value& summary) {
    {
        std::ofstream f(run_dir_ + "/summary.json");
        f << json::dump(summary, 2) << "\n";
    }
    {
        std::ofstream f(run_dir_ + "/tasks.csv");
        f << "job,idx,kind,cls,node,home,t_done,wait_s,xfer_s,exec_s,pred_wait_s,pred_xfer_s,pred_exec_s,net_bytes,in_bytes,migrations\n";
        for (auto& t : tasks_) {
            auto& j = jobs_.at(t.job);
            f << t.job << "," << t.idx << "," << j.spec.kind << "," << t.cls << "," << t.node << "," << t.home << ","
              << t.t_done << "," << t.wait << "," << t.xfer << "," << t.exec << "," << t.pred_wait << "," << t.pred_xfer << ","
              << t.pred_exec << "," << t.net_bytes << "," << t.in_bytes << "," << t.migrations << "\n";
        }
    }
    {
        std::ofstream f(run_dir_ + "/jobs.csv");
        f << "job,kind,tenant,origin,submit_s,done_s,jct_s,tasks,resubmits\n";
        for (auto& [id, j] : jobs_)
            f << id << "," << j.spec.kind << "," << cfg_.tenants[j.spec.tenant].name << "," << j.spec.origin << ","
              << j.spec.submit_s << "," << j.done_t << "," << (j.done_t >= 0 ? j.done_t - j.spec.submit_s : -1) << ","
              << j.spec.tasks.size() << "," << j.resubmits << "\n";
    }
    {
        std::ofstream f(run_dir_ + "/timeline.csv");
        f << "t,util,gpu_util,thr,jobs_sub,jobs_done,running,queued,tel_kBps,data_MBps,lat_err,bw_err\n";
        for (size_t i = 0; i < timeline_.size(); ++i) {
            auto& p = timeline_[i];
            f << p.get_num("t", 0) << "," << p.get_num("util", 0) << "," << p.get_num("gpu_util", 0) << ","
              << p.get_num("thr", 0) << "," << p.get_num("jobs_sub", 0) << "," << p.get_num("jobs_done", 0) << ","
              << p.get_num("running", 0) << "," << p.get_num("queued", 0) << "," << p.get_num("tel_kBps", 0) << ","
              << p.get_num("data_MBps", 0) << "," << p.get_num("lat_err", 0) << "," << p.get_num("bw_err", 0) << "\n";
        }
    }
    if (hub_) {
        std::lock_guard<std::mutex> g(hub_->mu);
        std::ofstream f(run_dir_ + "/replay.json");
        json::Value s = summary;
        s["timeline"] = json::Value();
        f << "{\"meta\":" << json::dump(hub_->run_meta) << ",\"topology\":" << json::dump(hub_->topology)
          << ",\"summary\":" << json::dump(s) << ",\"frames\":[";
        for (size_t i = 0; i < hub_->frames.size(); ++i) {
            if (i) f << ",";
            f << hub_->frames[i];
        }
        f << "]}\n";
    }
    (void)spec;
}

}  // namespace nexus
