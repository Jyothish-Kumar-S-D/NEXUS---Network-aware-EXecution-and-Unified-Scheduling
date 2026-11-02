// Compact little-endian binary serialization for control-plane messages.
#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace nexus {

class Writer {
public:
    std::vector<uint8_t> buf;
    template <class T>
    void put(const T& v) {
        static_assert(std::is_trivially_copyable_v<T>);
        size_t o = buf.size();
        buf.resize(o + sizeof(T));
        std::memcpy(buf.data() + o, &v, sizeof(T));
    }
    void str(const std::string& s) {
        put<uint16_t>(uint16_t(s.size()));
        buf.insert(buf.end(), s.begin(), s.end());
    }
    void bytes(size_t n, uint8_t fill = 0) { buf.insert(buf.end(), n, fill); }
    size_t size() const { return buf.size(); }
};

class Reader {
public:
    Reader(const uint8_t* p, size_t n) : p_(p), n_(n) {}
    template <class T>
    T get() {
        static_assert(std::is_trivially_copyable_v<T>);
        if (i_ + sizeof(T) > n_) throw std::runtime_error("reader: underflow");
        T v;
        std::memcpy(&v, p_ + i_, sizeof(T));
        i_ += sizeof(T);
        return v;
    }
    std::string str() {
        uint16_t n = get<uint16_t>();
        if (i_ + n > n_) throw std::runtime_error("reader: string underflow");
        std::string s(reinterpret_cast<const char*>(p_ + i_), n);
        i_ += n;
        return s;
    }
    void skip(size_t n) {
        if (i_ + n > n_) throw std::runtime_error("reader: skip underflow");
        i_ += n;
    }
    size_t remaining() const { return n_ - i_; }

private:
    const uint8_t* p_;
    size_t n_;
    size_t i_ = 0;
};

}  // namespace nexus
