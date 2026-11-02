// Minimal HTTP/1.1 server for the live dashboard (static page + JSON API).
#pragma once

#include <atomic>
#include <thread>

#include "nexus/experiment.hpp"

namespace nexus {

class HttpServer {
public:
    explicit HttpServer(MonitorHub* hub) : hub_(hub) {}
    ~HttpServer() { stop(); }
    bool start(int port);
    void stop();
    int port() const { return port_; }

private:
    void loop();
    void serve(int fd);
    MonitorHub* hub_;
    int lfd_ = -1, port_ = 0;
    std::atomic<bool> stop_{false};
    std::thread th_;
};

}  // namespace nexus
