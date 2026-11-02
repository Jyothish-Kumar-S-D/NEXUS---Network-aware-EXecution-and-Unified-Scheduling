// nexus_node: one HPC node as an OS process.
#include <csignal>
#include <cstdlib>
#include <iostream>

#include "nexus/node.hpp"

using namespace nexus;

static Node* g_node = nullptr;
static void on_signal(int) {
    if (g_node) g_node->request_stop();
}

int main(int argc, char** argv) {
    std::string config;
    NodeOptions o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--config") config = next();
        else if (a == "--id") o.id = NodeId(std::atoi(next().c_str()));
        else if (a == "--fabric") o.fabric_name = next();
        else if (a == "--policy") o.policy = parse_policy(next());
        else if (a == "--probe") o.probe = parse_probe_mode(next());
        else if (a == "--t0") o.t0_ns = std::atoll(next().c_str());
        else if (a == "--seed") o.seed = std::strtoull(next().c_str(), nullptr, 10);
        else if (a == "--coordinator") o.coordinator = NodeId(std::atoi(next().c_str()));
        else if (a == "--quiet") log_level() = int(LogLevel::Warn);
        else if (a == "--verbose") log_level() = int(LogLevel::Debug);
        else {
            std::cerr << "usage: nexus_node --config FILE --id N --fabric /shm-name [--policy nexus|fifo|compute-only|centralized]"
                         " [--probe adaptive|fixed-fast|fixed-slow] [--t0 NS] [--seed S] [--coordinator N]\n";
            return 1;
        }
    }
    if (config.empty() || o.fabric_name.empty()) {
        std::cerr << "nexus_node: --config and --fabric are required\n";
        return 1;
    }
    if (o.t0_ns == 0) o.t0_ns = now_ns();
    try {
        ClusterConfig cfg = ClusterConfig::load(config);
        if (o.id >= cfg.nodes.size()) { std::cerr << "bad node id\n"; return 1; }
        Node node(cfg, o);
        g_node = &node;
        std::signal(SIGTERM, on_signal);
        std::signal(SIGINT, on_signal);
        std::signal(SIGPIPE, SIG_IGN);
        return node.run();
    } catch (const std::exception& e) {
        std::cerr << "nexus_node fatal: " << e.what() << "\n";
        return 3;
    }
}
