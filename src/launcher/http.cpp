#include "nexus/http.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <fstream>
#include <sstream>

namespace nexus {

bool HttpServer::start(int port) {
    lfd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (lfd_ < 0) return false;
    int one = 1;
    ::setsockopt(lfd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons(uint16_t(port));
    if (::bind(lfd_, (sockaddr*)&a, sizeof a) != 0 || ::listen(lfd_, 16) != 0) {
        ::close(lfd_);
        lfd_ = -1;
        return false;
    }
    port_ = port;
    th_ = std::thread([this] { loop(); });
    return true;
}

void HttpServer::stop() {
    stop_ = true;
    if (th_.joinable()) th_.join();
    if (lfd_ >= 0) { ::close(lfd_); lfd_ = -1; }
}

void HttpServer::loop() {
    while (!stop_) {
        pollfd p{lfd_, POLLIN, 0};
        if (::poll(&p, 1, 200) <= 0) continue;
        int fd = ::accept4(lfd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (fd < 0) continue;
        timeval tv{2, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        serve(fd);
        ::close(fd);
    }
}

static void respond(int fd, int code, const std::string& type, const std::string& body) {
    std::ostringstream h;
    h << "HTTP/1.1 " << code << (code == 200 ? " OK" : " Not Found") << "\r\nContent-Type: " << type
      << "\r\nContent-Length: " << body.size() << "\r\nCache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n";
    std::string s = h.str() + body;
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, MSG_NOSIGNAL);
        if (n <= 0) break;
        off += size_t(n);
    }
}

void HttpServer::serve(int fd) {
    char buf[4096];
    ssize_t n = ::recv(fd, buf, sizeof buf - 1, 0);
    if (n <= 0) return;
    buf[n] = 0;
    std::string req(buf), method, path;
    std::istringstream in(req);
    in >> method >> path;
    std::string query;
    auto q = path.find('?');
    if (q != std::string::npos) { query = path.substr(q + 1); path = path.substr(0, q); }
    auto qparam = [&](const std::string& k) -> long {
        auto p = query.find(k + "=");
        return p == std::string::npos ? -1 : std::atol(query.c_str() + p + k.size() + 1);
    };
    if (path == "/" || path == "/index.html" || path == "/dashboard.html") {
        std::ifstream f(hub_->web_dir + "/dashboard.html");
        std::stringstream ss;
        // the page is authored as a body fragment (it is also embedded in a hosted wrapper),
        // so the live server supplies the document skeleton
        ss << "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
              "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,viewport-fit=cover\">"
              "<style>[hidden]{display:none!important}</style></head><body>\n"
           << f.rdbuf() << "\n</body></html>";
        respond(fd, 200, "text/html; charset=utf-8", ss.str());
    } else if (path == "/api/meta") {
        std::lock_guard<std::mutex> g(hub_->mu);
        json::Value v;
        v["topology"] = hub_->topology;
        v["run"] = hub_->run_meta;
        v["suite"] = hub_->suite;
        respond(fd, 200, "application/json", json::dump(v));
    } else if (path == "/api/frames") {
        std::lock_guard<std::mutex> g(hub_->mu);
        long run = qparam("run"), since = qparam("since");
        if (run != hub_->run_counter || since < 0) since = 0;
        std::string body = "{\"run\":" + std::to_string(hub_->run_counter) + ",\"base\":" + std::to_string(since) +
                           ",\"meta\":" + json::dump(hub_->run_meta) + ",\"frames\":[";
        for (size_t i = size_t(since); i < hub_->frames.size(); ++i) {
            if (i > size_t(since)) body += ",";
            body += hub_->frames[i];
        }
        body += "]}";
        respond(fd, 200, "application/json", body);
    } else if (path == "/api/summaries") {
        std::lock_guard<std::mutex> g(hub_->mu);
        json::Value a = json::Value::array();
        for (auto& s : hub_->summaries) a.push(s);
        respond(fd, 200, "application/json", json::dump(a));
    } else {
        respond(fd, 404, "text/plain", "not found");
    }
}

}  // namespace nexus
