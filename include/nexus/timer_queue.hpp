// Single-threaded timer wheel for a node's event loop.
#pragma once

#include <functional>
#include <queue>
#include <vector>

#include "nexus/common.hpp"

namespace nexus {

class TimerQueue {
public:
    using Fn = std::function<void()>;
    void at(int64_t t_ns, Fn fn) { q_.push({t_ns, ++seq_, std::move(fn)}); }
    void after_ms(double ms, Fn fn) { at(now_ns() + ms_to_ns(ms), std::move(fn)); }
    // Run every timer that is due; timers may schedule new timers.
    int run_due(int64_t now) {
        int n = 0;
        while (!q_.empty() && q_.top().t <= now) {
            Fn f = std::move(const_cast<Item&>(q_.top()).fn);
            q_.pop();
            f();
            ++n;
        }
        return n;
    }
    int64_t next() const { return q_.empty() ? INT64_MAX : q_.top().t; }
    size_t size() const { return q_.size(); }

private:
    struct Item {
        int64_t t;
        uint64_t seq;
        Fn fn;
        bool operator>(const Item& o) const { return t != o.t ? t > o.t : seq > o.seq; }
    };
    std::priority_queue<Item, std::vector<Item>, std::greater<Item>> q_;
    uint64_t seq_ = 0;
};

}  // namespace nexus
