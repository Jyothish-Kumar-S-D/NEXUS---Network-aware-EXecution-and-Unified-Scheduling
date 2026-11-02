#include "nexus/sysinfo.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <sstream>
#include <thread>

#include "nexus/common.hpp"

namespace nexus {

static std::string read_file(const std::string& p) {
    std::ifstream f(p);
    if (!f) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}
static std::string trim(std::string s) {
    while (!s.empty() && isspace((unsigned char)s.back())) s.pop_back();
    size_t i = 0;
    while (i < s.size() && isspace((unsigned char)s[i])) ++i;
    return s.substr(i);
}
static double meminfo_mb(const std::string& key) {
    std::istringstream in(read_file("/proc/meminfo"));
    std::string line;
    while (std::getline(in, line))
        if (line.rfind(key + ":", 0) == 0) return std::atof(line.substr(key.size() + 1).c_str()) / 1024.0;
    return 0;
}
static double pct(std::vector<double> v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, size_t(p * double(v.size() - 1)))];
}

static json::Value bench_timer() {
    std::vector<double> over;
    for (int i = 0; i < 300; ++i) {
        int64_t a = now_ns();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        over.push_back(ns_to_s(now_ns() - a - 1000000) * 1e6);
    }
    json::Value j;
    j["sleep_1ms_overshoot_p50_us"] = pct(over, 0.5);
    j["sleep_1ms_overshoot_p99_us"] = pct(over, 0.99);
    return j;
}

static json::Value bench_udp() {
    json::Value j;
    int a = ::socket(AF_INET, SOCK_DGRAM, 0), b = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in sa{}, sb{};
    sa.sin_family = sb.sin_family = AF_INET;
    sa.sin_addr.s_addr = sb.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(a, (sockaddr*)&sa, sizeof sa);
    ::bind(b, (sockaddr*)&sb, sizeof sb);
    socklen_t l = sizeof sa;
    ::getsockname(a, (sockaddr*)&sa, &l);
    l = sizeof sb;
    ::getsockname(b, (sockaddr*)&sb, &l);
    const int N = 2000;
    pid_t pid = ::fork();
    if (pid == 0) {  // echo process
        char buf[256];
        for (int i = 0; i < N; ++i) {
            ssize_t n = ::recv(b, buf, sizeof buf, 0);
            if (n > 0) ::sendto(b, buf, size_t(n), 0, (sockaddr*)&sa, sizeof sa);
        }
        ::_exit(0);
    }
    std::vector<double> rtt;
    char buf[64] = {0}, rb[256];
    timeval tv{1, 0};
    ::setsockopt(a, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    for (int i = 0; i < N; ++i) {
        int64_t t = now_ns();
        ::sendto(a, buf, sizeof buf, 0, (sockaddr*)&sb, sizeof sb);
        if (::recv(a, rb, sizeof rb, 0) > 0) rtt.push_back(ns_to_s(now_ns() - t) * 1e6);
    }
    ::waitpid(pid, nullptr, 0);
    ::close(a);
    ::close(b);
    j["udp_loopback_rtt_p50_us"] = pct(rtt, 0.5);
    j["udp_loopback_rtt_p99_us"] = pct(rtt, 0.99);
    j["udp_loopback_samples"] = int(rtt.size());
    return j;
}

static json::Value bench_spawn() {
    std::vector<double> v;
    for (int i = 0; i < 20; ++i) {
        int64_t t = now_ns();
        pid_t p = ::fork();
        if (p == 0) {
            ::execl("/bin/true", "true", (char*)nullptr);
            ::_exit(127);
        }
        ::waitpid(p, nullptr, 0);
        v.push_back(ns_to_s(now_ns() - t) * 1e3);
    }
    json::Value j;
    j["fork_exec_wait_p50_ms"] = pct(v, 0.5);
    return j;
}

static json::Value bench_cpu() {
    json::Value j;
    // single-thread integer throughput
    volatile uint64_t x = 1;
    int64_t t = now_ns();
    uint64_t iters = 0;
    while (now_ns() - t < 200000000) {
        for (int i = 0; i < 100000; ++i) x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        iters += 100000;
    }
    j["lcg_mops_per_s_1thread"] = double(iters) / ns_to_s(now_ns() - t) / 1e6;
    std::atomic<int64_t> a{0};
    t = now_ns();
    for (int i = 0; i < 2000000; ++i) {
        int64_t v = a.load();
        a.compare_exchange_weak(v, v + 1);
    }
    j["cas_ns_per_op"] = double(now_ns() - t) / 2e6;
    return j;
}

SystemProfile analyze_system(bool run_benchmarks) {
    SystemProfile p;
    json::Value hw;
    {
        std::istringstream in(read_file("/proc/cpuinfo"));
        std::string line, model, flags;
        while (std::getline(in, line)) {
            if (model.empty() && line.rfind("model name", 0) == 0) model = trim(line.substr(line.find(':') + 1));
            if (flags.empty() && line.rfind("flags", 0) == 0) flags = line;
        }
        hw["cpu_model"] = model;
        json::Value isa = json::Value::array();
        for (const char* f : {"sse4_2", "avx", "avx2", "fma", "avx512f", "avx512_bf16", "amx_tile"})
            if (flags.find(std::string(" ") + f) != std::string::npos) isa.push(f);
        hw["isa"] = isa;
    }
    p.logical_cpus = int(sysconf(_SC_NPROCESSORS_ONLN));
    hw["logical_cpus"] = p.logical_cpus;
    std::string cpumax = trim(read_file("/sys/fs/cgroup/cpu.max"));
    hw["cgroup_cpu_max"] = cpumax.empty() ? "n/a" : cpumax;
    hw["mem_total_mb"] = meminfo_mb("MemTotal");
    p.mem_avail_mb = meminfo_mb("MemAvailable");
    hw["mem_available_mb"] = p.mem_avail_mb;
    hw["swap_total_mb"] = meminfo_mb("SwapTotal");
    std::string memmax = trim(read_file("/sys/fs/cgroup/memory.max"));
    hw["cgroup_mem_max"] = memmax.empty() ? "n/a" : memmax;
    struct statvfs sv{};
    if (::statvfs("/dev/shm", &sv) == 0) hw["dev_shm_mb"] = double(sv.f_bavail) * double(sv.f_frsize) / 1048576.0;
    utsname u{};
    ::uname(&u);
    hw["kernel"] = std::string(u.sysname) + " " + u.release;
    hw["hypervisor"] = read_file("/proc/cpuinfo").find("hypervisor") != std::string::npos;

    json::Value os;
    rlimit rl{};
    ::getrlimit(RLIMIT_NOFILE, &rl);
    os["max_open_files"] = double(rl.rlim_cur);
    ::getrlimit(RLIMIT_NPROC, &rl);
    os["max_user_processes"] = rl.rlim_cur == RLIM_INFINITY ? -1.0 : double(rl.rlim_cur);
    os["loopback_mtu"] = std::atof(read_file("/sys/class/net/lo/mtu").c_str());
    os["udp_rmem_max"] = std::atof(read_file("/proc/sys/net/core/rmem_max").c_str());
    os["pid_max"] = std::atof(read_file("/proc/sys/kernel/pid_max").c_str());

    p.j["hardware"] = hw;
    p.j["os"] = os;
    if (run_benchmarks) {
        json::Value b;
        auto t = bench_timer();
        auto u2 = bench_udp();
        auto s = bench_spawn();
        auto c = bench_cpu();
        for (auto* src : {&t, &u2, &s, &c})
            for (auto& kv : *src->o) b[kv.first] = kv.second;
        p.timer_overshoot_us = t.get_num("sleep_1ms_overshoot_p99_us", 100);
        p.udp_rtt_us = u2.get_num("udp_loopback_rtt_p50_us", 50);
        p.spawn_ms = s.get_num("fork_exec_wait_p50_ms", 2);
        p.j["benchmarks"] = b;
    }
    // Sizing: a node process is mostly idle (event loop + sleeping worker threads), so the
    // binding constraints are memory and event-loop CPU. ~15 MB RSS and ~2% of a CPU per node.
    int by_mem = int(p.mem_avail_mb * 0.5 / 15.0);
    int by_cpu = p.logical_cpus * 24;
    p.max_node_processes = std::max(2, std::min({64, by_mem, by_cpu}));
    p.min_task_s = std::max(0.05, p.timer_overshoot_us * 1e-6 * 100.0);
    json::Value rec;
    rec["max_node_processes"] = p.max_node_processes;
    rec["min_task_duration_s"] = p.min_task_s;
    rec["execution_mode"] = "emulated-timed";
    rec["rationale"] =
        "Node processes, sockets, threads, protocols and timing are real; task kernels are emulated as timed "
        "occupancy against a hidden ground-truth hardware model because the host has far fewer physical cores "
        "than the emulated cluster, so running real kernels would measure host contention instead of scheduling.";
    p.j["recommendation"] = rec;
    return p;
}

void print_profile(const SystemProfile& p) {
    std::printf("== NEXUS host capability analysis ==\n%s\n", json::dump(p.j, 2).c_str());
}

}  // namespace nexus
