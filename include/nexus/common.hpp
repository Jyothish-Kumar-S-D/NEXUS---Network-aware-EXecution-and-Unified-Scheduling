// NEXUS — Network-aware EXecution and Unified Scheduling
// common.hpp: clock, ids, logging, RNG, small numeric helpers.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <random>
#include <string>
#include <vector>

namespace nexus {

using NodeId = uint16_t;
using TaskId = uint64_t;
constexpr NodeId kNoNode = 0xFFFF;
constexpr int kMaxNodes = 64;

// CLOCK_MONOTONIC is shared by every process on the host, so timestamps
// taken in different node processes are directly comparable. This plays the
// role of PTP-synchronised hardware clocks in a real cluster.
inline int64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return int64_t(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
}
inline double ns_to_s(int64_t ns) { return double(ns) * 1e-9; }
inline int64_t s_to_ns(double s) { return int64_t(s * 1e9); }
inline int64_t ms_to_ns(double ms) { return int64_t(ms * 1e6); }

enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };
inline std::atomic<int>& log_level() { static std::atomic<int> l{int(LogLevel::Info)}; return l; }
inline std::string& log_tag() { static std::string t = "nexus"; return t; }

#define NX_LOG(lvl, ...)                                                          \
    do {                                                                          \
        if (int(lvl) >= ::nexus::log_level().load()) {                            \
            char _b[1024];                                                        \
            std::snprintf(_b, sizeof(_b), __VA_ARGS__);                           \
            std::fprintf(stderr, "[%10.3f] %-8s %s\n",                            \
                         ::nexus::ns_to_s(::nexus::now_ns()) , ::nexus::log_tag().c_str(), _b); \
        }                                                                         \
    } while (0)
#define LOG_D(...) NX_LOG(::nexus::LogLevel::Debug, __VA_ARGS__)
#define LOG_I(...) NX_LOG(::nexus::LogLevel::Info, __VA_ARGS__)
#define LOG_W(...) NX_LOG(::nexus::LogLevel::Warn, __VA_ARGS__)
#define LOG_E(...) NX_LOG(::nexus::LogLevel::Error, __VA_ARGS__)

struct Rng {
    std::mt19937_64 g;
    explicit Rng(uint64_t seed = 42) : g(seed) {}
    double uniform(double a = 0.0, double b = 1.0) { return std::uniform_real_distribution<double>(a, b)(g); }
    int uniform_int(int a, int b) { return std::uniform_int_distribution<int>(a, b)(g); }
    double exponential(double rate) { return std::exponential_distribution<double>(rate)(g); }
    double lognormal(double mu, double sigma) { return std::lognormal_distribution<double>(mu, sigma)(g); }
    double normal(double m, double s) { return std::normal_distribution<double>(m, s)(g); }
    bool chance(double p) { return uniform() < p; }
};

// Exponentially weighted estimator that also tracks variance (Jacobson/Karels
// style), used for RTT, bandwidth, loss and prediction-correction factors.
struct Ewma {
    double mean = 0.0, var = 0.0;
    double alpha = 0.2;
    uint32_t n = 0;
    int64_t last_update_ns = 0;
    explicit Ewma(double a = 0.2) : alpha(a) {}
    void add(double x, int64_t t = 0) {
        if (n == 0) { mean = x; var = 0.0; }
        else {
            double d = x - mean;
            mean += alpha * d;
            var = (1 - alpha) * (var + alpha * d * d);
        }
        ++n;
        last_update_ns = t ? t : now_ns();
    }
    double stddev() const { return std::sqrt(std::max(0.0, var)); }
    bool valid() const { return n > 0; }
};

template <class T> inline T clampv(T v, T lo, T hi) { return std::max(lo, std::min(hi, v)); }

inline std::string fmt(const char* f, ...) __attribute__((format(printf, 1, 2)));
inline std::string fmt(const char* f, ...) {
    char buf[4096];
    va_list ap;
    va_start(ap, f);
    std::vsnprintf(buf, sizeof(buf), f, ap);
    va_end(ap);
    return buf;
}

}  // namespace nexus
