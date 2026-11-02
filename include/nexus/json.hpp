// Minimal JSON value / parser / writer (no external dependencies).
// Used for configuration, the management/monitoring plane and result files.
#pragma once

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace nexus::json {

struct Value;
using Array = std::vector<Value>;
using Object = std::vector<std::pair<std::string, Value>>;  // keeps insertion order

struct Value {
    enum class T { Null, Bool, Num, Str, Arr, Obj } t = T::Null;
    bool b = false;
    double n = 0;
    std::string s;
    std::shared_ptr<Array> a;
    std::shared_ptr<Object> o;

    Value() = default;
    Value(std::nullptr_t) {}
    Value(bool v) : t(T::Bool), b(v) {}
    Value(int v) : t(T::Num), n(v) {}
    Value(unsigned v) : t(T::Num), n(v) {}
    Value(long v) : t(T::Num), n(double(v)) {}
    Value(unsigned long v) : t(T::Num), n(double(v)) {}
    Value(long long v) : t(T::Num), n(double(v)) {}
    Value(unsigned long long v) : t(T::Num), n(double(v)) {}
    Value(double v) : t(T::Num), n(v) {}
    Value(const char* v) : t(T::Str), s(v) {}
    Value(std::string v) : t(T::Str), s(std::move(v)) {}
    Value(Array v) : t(T::Arr), a(std::make_shared<Array>(std::move(v))) {}
    Value(Object v) : t(T::Obj), o(std::make_shared<Object>(std::move(v))) {}

    static Value array() { return Value(Array{}); }
    static Value object() { return Value(Object{}); }

    bool is_null() const { return t == T::Null; }
    bool is_obj() const { return t == T::Obj; }
    bool is_arr() const { return t == T::Arr; }
    bool is_num() const { return t == T::Num; }
    bool is_str() const { return t == T::Str; }

    // object access
    const Value* find(const std::string& k) const {
        if (t != T::Obj) return nullptr;
        for (auto& kv : *o) if (kv.first == k) return &kv.second;
        return nullptr;
    }
    Value& operator[](const std::string& k) {
        if (t == T::Null) { t = T::Obj; o = std::make_shared<Object>(); }
        if (t != T::Obj) throw std::runtime_error("json: not an object");
        for (auto& kv : *o) if (kv.first == k) return kv.second;
        o->emplace_back(k, Value());
        return o->back().second;
    }
    const Value& at(const std::string& k) const {
        auto p = find(k);
        if (!p) throw std::runtime_error("json: missing key '" + k + "'");
        return *p;
    }
    // array access
    size_t size() const { return t == T::Arr ? a->size() : (t == T::Obj ? o->size() : 0); }
    const Value& operator[](size_t i) const { return (*a)[i]; }
    Value& operator[](size_t i) { return (*a)[i]; }
    void push(Value v) {
        if (t == T::Null) { t = T::Arr; a = std::make_shared<Array>(); }
        a->push_back(std::move(v));
    }

    double num(double def = 0) const { return t == T::Num ? n : (t == T::Bool ? (b ? 1 : 0) : def); }
    std::string str(const std::string& def = "") const { return t == T::Str ? s : def; }
    bool boolean(bool def = false) const { return t == T::Bool ? b : (t == T::Num ? n != 0 : def); }
    double get_num(const std::string& k, double def) const { auto p = find(k); return p ? p->num(def) : def; }
    std::string get_str(const std::string& k, const std::string& def) const { auto p = find(k); return p ? p->str(def) : def; }
    bool get_bool(const std::string& k, bool def) const { auto p = find(k); return p ? p->boolean(def) : def; }
};

inline void escape_to(std::string& out, const std::string& s) {
    out.push_back('"');
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); out += b; }
                else out.push_back(char(c));
        }
    }
    out.push_back('"');
}

inline void dump_to(std::string& out, const Value& v, int indent, int depth) {
    auto nl = [&](int d) {
        if (indent < 0) return;
        out.push_back('\n');
        out.append(size_t(d * indent), ' ');
    };
    switch (v.t) {
        case Value::T::Null: out += "null"; break;
        case Value::T::Bool: out += v.b ? "true" : "false"; break;
        case Value::T::Num: {
            if (!std::isfinite(v.n)) { out += "null"; break; }
            double r = std::round(v.n);
            char b[40];
            if (std::fabs(v.n - r) < 1e-12 && std::fabs(r) < 9e15) std::snprintf(b, sizeof b, "%.0f", r);
            else std::snprintf(b, sizeof b, "%.6g", v.n);
            out += b;
            break;
        }
        case Value::T::Str: escape_to(out, v.s); break;
        case Value::T::Arr: {
            out.push_back('[');
            for (size_t i = 0; i < v.a->size(); ++i) {
                if (i) out.push_back(',');
                nl(depth + 1);
                dump_to(out, (*v.a)[i], indent, depth + 1);
            }
            if (!v.a->empty()) nl(depth);
            out.push_back(']');
            break;
        }
        case Value::T::Obj: {
            out.push_back('{');
            size_t i = 0;
            for (auto& kv : *v.o) {
                if (i++) out.push_back(',');
                nl(depth + 1);
                escape_to(out, kv.first);
                out += indent >= 0 ? ": " : ":";
                dump_to(out, kv.second, indent, depth + 1);
            }
            if (!v.o->empty()) nl(depth);
            out.push_back('}');
            break;
        }
    }
}

inline std::string dump(const Value& v, int indent = -1) {
    std::string out;
    dump_to(out, v, indent, 0);
    return out;
}

class Parser {
public:
    explicit Parser(const std::string& s) : s_(s) {}
    Value parse() {
        Value v = value();
        ws();
        if (i_ != s_.size()) err("trailing characters");
        return v;
    }

private:
    const std::string& s_;
    size_t i_ = 0;
    [[noreturn]] void err(const char* m) { throw std::runtime_error(std::string("json parse error: ") + m + " at " + std::to_string(i_)); }
    void ws() { while (i_ < s_.size() && (s_[i_] == ' ' || s_[i_] == '\n' || s_[i_] == '\t' || s_[i_] == '\r')) ++i_; }
    char peek() { ws(); if (i_ >= s_.size()) err("unexpected end"); return s_[i_]; }
    void expect(char c) { if (peek() != c) err("unexpected character"); ++i_; }
    Value value() {
        char c = peek();
        if (c == '{') return object();
        if (c == '[') return array();
        if (c == '"') return Value(string());
        if (c == 't') { lit("true"); return Value(true); }
        if (c == 'f') { lit("false"); return Value(false); }
        if (c == 'n') { lit("null"); return Value(); }
        return number();
    }
    void lit(const char* w) {
        size_t n = std::strlen(w);
        if (s_.compare(i_, n, w) != 0) err("bad literal");
        i_ += n;
    }
    Value number() {
        size_t st = i_;
        if (s_[i_] == '-') ++i_;
        while (i_ < s_.size() && (isdigit((unsigned char)s_[i_]) || s_[i_] == '.' || s_[i_] == 'e' || s_[i_] == 'E' || s_[i_] == '+' || s_[i_] == '-')) ++i_;
        if (st == i_) err("bad number");
        return Value(std::strtod(s_.c_str() + st, nullptr));
    }
    std::string string() {
        expect('"');
        std::string out;
        while (i_ < s_.size() && s_[i_] != '"') {
            char c = s_[i_++];
            if (c == '\\') {
                if (i_ >= s_.size()) err("bad escape");
                char e = s_[i_++];
                switch (e) {
                    case 'n': out.push_back('\n'); break;
                    case 't': out.push_back('\t'); break;
                    case 'r': out.push_back('\r'); break;
                    case 'b': out.push_back('\b'); break;
                    case 'f': out.push_back('\f'); break;
                    case 'u': {
                        if (i_ + 4 > s_.size()) err("bad unicode");
                        unsigned cp = std::stoul(s_.substr(i_, 4), nullptr, 16);
                        i_ += 4;
                        if (cp < 0x80) out.push_back(char(cp));
                        else if (cp < 0x800) { out.push_back(char(0xC0 | (cp >> 6))); out.push_back(char(0x80 | (cp & 0x3F))); }
                        else { out.push_back(char(0xE0 | (cp >> 12))); out.push_back(char(0x80 | ((cp >> 6) & 0x3F))); out.push_back(char(0x80 | (cp & 0x3F))); }
                        break;
                    }
                    default: out.push_back(e);
                }
            } else out.push_back(c);
        }
        if (i_ >= s_.size()) err("unterminated string");
        ++i_;
        return out;
    }
    Value array() {
        expect('[');
        Value v = Value::array();
        if (peek() == ']') { ++i_; return v; }
        while (true) {
            v.push(value());
            char c = peek();
            ++i_;
            if (c == ']') break;
            if (c != ',') err("expected , or ]");
        }
        return v;
    }
    Value object() {
        expect('{');
        Value v = Value::object();
        if (peek() == '}') { ++i_; return v; }
        while (true) {
            if (peek() != '"') err("expected key");
            std::string k = string();
            expect(':');
            v[k] = value();
            char c = peek();
            ++i_;
            if (c == '}') break;
            if (c != ',') err("expected , or }");
        }
        return v;
    }
};

inline Value parse(const std::string& s) { return Parser(s).parse(); }

}  // namespace nexus::json
