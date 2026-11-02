// Wire protocol: message header, telemetry records, task descriptors and
// control messages exchanged between node processes over UDP.
#pragma once

#include <string>
#include <vector>

#include "nexus/common.hpp"
#include "nexus/config.hpp"
#include "nexus/serialize.hpp"

namespace nexus {

constexpr NodeId kClientId = 0xFFFE;  // the launcher (workload driver + monitor plane)
constexpr uint16_t kWireMagic = 0x4E58;

enum class MsgType : uint8_t {
    Probe = 1, ProbeAck, ReserveReq, ReserveRep, Delegate, TaskStarted, TaskDone, TaskFailed, TaskMoved,
    Fetch, DataDone, StateReq, StateRep, Submit, SubmitAck, Ping, Pong, Fault, Shutdown, MonitorJson,
    DelegateAck, DoneAck, TaskQuery, TaskQueryRep
};

// Small acknowledgement body: (task, attempt).
struct AckMsg {
    TaskId task = 0;
    uint32_t attempt = 0;
    void write(Writer& w) const { w.put(task); w.put(attempt); }
    static AckMsg read(Reader& r) { AckMsg m; m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); return m; }
};

enum MsgFlags : uint8_t {
    kFlagMgmt = 1,       // management plane: bypasses the emulated fabric
    kFlagTelemetry = 2,  // counts towards telemetry overhead
};

struct MsgHeader {
    uint16_t magic = kWireMagic;
    MsgType type = MsgType::Ping;
    uint8_t flags = 0;
    NodeId src = 0, dst = 0;
    uint32_t seq = 0;
    int64_t tx_ns = 0;      // sender timestamp
    int64_t rx_hw_ns = 0;   // emulated NIC receive timestamp (set by the fabric)
};

// ---------------------------------------------------------------- telemetry
struct LinkEst {
    NodeId peer = 0;
    float lat_ms = 0, lat_sd_ms = 0;  // one-way latency estimate
    float bw_MBps = 0;                // available bandwidth estimate (0 = unknown)
    float loss = 0;
    float age_s = 0;                  // age of the newest measurement at record creation
};

enum HealthBits : uint8_t { kCpuOk = 1, kGpuOk = 2, kDiskOk = 4, kDegraded = 8 };

struct NodeRecord {
    NodeId id = 0;
    uint32_t version = 0;
    int64_t origin_ns = 0;  // when the origin node produced this record
    // capabilities (static)
    uint8_t rack = 0;
    uint16_t cores = 0;
    uint32_t mem_gb = 0;
    uint8_t gpus = 0;
    float cpu_speed = 1, gpu_speed = 1;
    float disk_GBps = 1;
    std::string partition;
    uint64_t datasets = 0;  // bitmask of dataset ids held locally
    // dynamic state
    uint16_t cores_free = 0;
    uint32_t mem_free = 0;
    uint8_t gpus_free = 0;
    uint16_t queue_len = 0;
    uint16_t running = 0;
    float backlog_core_s = 0;  // queued+remaining core-seconds
    float backlog_gpu_s = 0;
    uint8_t health = kCpuOk | kGpuOk | kDiskOk;
    float fail_rate = 0;
    float nic_util = 0;
    float corr[kNumClasses] = {1, 1, 1, 1, 1};  // learned actual/nominal execution ratio per class
    uint16_t tenant_cores[4] = {0, 0, 0, 0};    // cores held (queued+running+reserved) per tenant: live quota state
    std::vector<LinkEst> links;

    void write(Writer& w) const {
        w.put(id); w.put(version); w.put(origin_ns);
        w.put(rack); w.put(cores); w.put(mem_gb); w.put(gpus);
        w.put(cpu_speed); w.put(gpu_speed); w.put(disk_GBps);
        w.str(partition); w.put(datasets);
        w.put(cores_free); w.put(mem_free); w.put(gpus_free); w.put(queue_len); w.put(running);
        w.put(backlog_core_s); w.put(backlog_gpu_s); w.put(health); w.put(fail_rate); w.put(nic_util);
        for (float c : corr) w.put(c);
        for (auto tc : tenant_cores) w.put(tc);
        w.put(uint8_t(links.size()));
        for (auto& l : links) { w.put(l.peer); w.put(l.lat_ms); w.put(l.lat_sd_ms); w.put(l.bw_MBps); w.put(l.loss); w.put(l.age_s); }
    }
    static NodeRecord read(Reader& r) {
        NodeRecord n;
        n.id = r.get<NodeId>(); n.version = r.get<uint32_t>(); n.origin_ns = r.get<int64_t>();
        n.rack = r.get<uint8_t>(); n.cores = r.get<uint16_t>(); n.mem_gb = r.get<uint32_t>(); n.gpus = r.get<uint8_t>();
        n.cpu_speed = r.get<float>(); n.gpu_speed = r.get<float>(); n.disk_GBps = r.get<float>();
        n.partition = r.str(); n.datasets = r.get<uint64_t>();
        n.cores_free = r.get<uint16_t>(); n.mem_free = r.get<uint32_t>(); n.gpus_free = r.get<uint8_t>();
        n.queue_len = r.get<uint16_t>(); n.running = r.get<uint16_t>();
        n.backlog_core_s = r.get<float>(); n.backlog_gpu_s = r.get<float>(); n.health = r.get<uint8_t>();
        n.fail_rate = r.get<float>(); n.nic_util = r.get<float>();
        for (float& c : n.corr) c = r.get<float>();
        for (auto& tc : n.tenant_cores) tc = r.get<uint16_t>();
        uint8_t nl = r.get<uint8_t>();
        n.links.resize(nl);
        for (auto& l : n.links) {
            l.peer = r.get<NodeId>(); l.lat_ms = r.get<float>(); l.lat_sd_ms = r.get<float>();
            l.bw_MBps = r.get<float>(); l.loss = r.get<float>(); l.age_s = r.get<float>();
        }
        return n;
    }
};

// ---------------------------------------------------------------- tasks
enum class InputKind : uint8_t { Dataset = 0, Parent = 1 };

struct InputRef {
    InputKind kind = InputKind::Dataset;
    uint32_t ref = 0;       // dataset id, or parent task index within the job
    uint64_t bytes = 0;
    NodeId src = kNoNode;   // source chosen by the scheduler
    std::vector<NodeId> alts;  // alternative replicas
    void write(Writer& w) const {
        w.put(kind); w.put(ref); w.put(bytes); w.put(src);
        w.put(uint8_t(alts.size()));
        for (auto a : alts) w.put(a);
    }
    static InputRef read(Reader& r) {
        InputRef i;
        i.kind = r.get<InputKind>(); i.ref = r.get<uint32_t>(); i.bytes = r.get<uint64_t>(); i.src = r.get<NodeId>();
        uint8_t n = r.get<uint8_t>();
        for (int k = 0; k < n; ++k) i.alts.push_back(r.get<NodeId>());
        return i;
    }
};

struct TaskSpec {
    TaskId id = 0;
    uint32_t job = 0;
    uint16_t index = 0;
    TaskClass cls = TaskClass::CpuBatch;
    uint8_t tenant = 0;
    uint16_t cores = 1;
    uint32_t mem_gb = 1;
    uint8_t gpus = 0;
    float work = 1;          // reference seconds (core-s for CPU classes, gpu-s for GPU)
    uint64_t out_bytes = 0;
    std::vector<InputRef> inputs;
    std::vector<uint16_t> deps;
    void write(Writer& w) const {
        w.put(id); w.put(job); w.put(index); w.put(cls); w.put(tenant); w.put(cores); w.put(mem_gb); w.put(gpus);
        w.put(work); w.put(out_bytes);
        w.put(uint8_t(inputs.size()));
        for (auto& i : inputs) i.write(w);
        w.put(uint8_t(deps.size()));
        for (auto d : deps) w.put(d);
    }
    static TaskSpec read(Reader& r) {
        TaskSpec t;
        t.id = r.get<TaskId>(); t.job = r.get<uint32_t>(); t.index = r.get<uint16_t>(); t.cls = r.get<TaskClass>();
        t.tenant = r.get<uint8_t>(); t.cores = r.get<uint16_t>(); t.mem_gb = r.get<uint32_t>(); t.gpus = r.get<uint8_t>();
        t.work = r.get<float>(); t.out_bytes = r.get<uint64_t>();
        uint8_t ni = r.get<uint8_t>();
        for (int k = 0; k < ni; ++k) t.inputs.push_back(InputRef::read(r));
        uint8_t nd = r.get<uint8_t>();
        for (int k = 0; k < nd; ++k) t.deps.push_back(r.get<uint16_t>());
        return t;
    }
};

inline TaskId make_task_id(uint32_t job, uint16_t index) { return (TaskId(job) << 16) | index; }

struct JobSpec {
    uint32_t id = 0;
    uint8_t tenant = 0;
    std::string kind;
    NodeId origin = 0;
    double submit_s = 0;  // offset from experiment start
    std::vector<TaskSpec> tasks;
};

// Submission (also used for client-driven re-submission after a home node failure).
struct SubmitMsg {
    JobSpec job;
    uint16_t epoch = 0;
    std::vector<std::pair<uint16_t, NodeId>> done;  // tasks already completed + output location
    void write(Writer& w) const {
        w.put(job.id); w.put(job.tenant); w.str(job.kind); w.put(job.origin); w.put(job.submit_s);
        w.put(uint16_t(job.tasks.size()));
        for (auto& t : job.tasks) t.write(w);
        w.put(epoch);
        w.put(uint16_t(done.size()));
        for (auto& d : done) { w.put(d.first); w.put(d.second); }
    }
    static SubmitMsg read(Reader& r) {
        SubmitMsg m;
        m.job.id = r.get<uint32_t>(); m.job.tenant = r.get<uint8_t>(); m.job.kind = r.str();
        m.job.origin = r.get<NodeId>(); m.job.submit_s = r.get<double>();
        uint16_t n = r.get<uint16_t>();
        for (int k = 0; k < n; ++k) m.job.tasks.push_back(TaskSpec::read(r));
        m.epoch = r.get<uint16_t>();
        uint16_t nd = r.get<uint16_t>();
        for (int k = 0; k < nd; ++k) { uint16_t a = r.get<uint16_t>(); NodeId b = r.get<NodeId>(); m.done.push_back({a, b}); }
        return m;
    }
};

// ---------------------------------------------------------------- control messages
enum class ProbeKind : uint8_t { Latency = 0, PairFirst = 1, PairSecond = 2 };

struct ProbeMsg {
    uint32_t probe_seq = 0;
    ProbeKind kind = ProbeKind::Latency;
    NodeRecord self;
    std::vector<NodeRecord> gossip;
    uint32_t pad = 0;  // payload padding (packet-pair probes)
    void write(Writer& w) const {
        w.put(probe_seq); w.put(kind); self.write(w);
        w.put(uint8_t(gossip.size()));
        for (auto& g : gossip) g.write(w);
        w.put(pad);
        w.bytes(pad);
    }
    static ProbeMsg read(Reader& r) {
        ProbeMsg m;
        m.probe_seq = r.get<uint32_t>(); m.kind = r.get<ProbeKind>(); m.self = NodeRecord::read(r);
        uint8_t n = r.get<uint8_t>();
        for (int k = 0; k < n; ++k) m.gossip.push_back(NodeRecord::read(r));
        m.pad = r.get<uint32_t>();
        r.skip(m.pad);
        return m;
    }
};

struct ProbeAckMsg {
    uint32_t probe_seq = 0;
    ProbeKind kind = ProbeKind::Latency;
    int64_t probe_tx_ns = 0, probe_rx_hw_ns = 0, ack_tx_ns = 0;
    int64_t pair_gap_ns = 0;     // for PairSecond: rx gap between the pair
    uint32_t pair_bytes = 0;
    NodeRecord self;
    std::vector<NodeRecord> gossip;
    void write(Writer& w) const {
        w.put(probe_seq); w.put(kind); w.put(probe_tx_ns); w.put(probe_rx_hw_ns); w.put(ack_tx_ns);
        w.put(pair_gap_ns); w.put(pair_bytes); self.write(w);
        w.put(uint8_t(gossip.size()));
        for (auto& g : gossip) g.write(w);
    }
    static ProbeAckMsg read(Reader& r) {
        ProbeAckMsg m;
        m.probe_seq = r.get<uint32_t>(); m.kind = r.get<ProbeKind>(); m.probe_tx_ns = r.get<int64_t>();
        m.probe_rx_hw_ns = r.get<int64_t>(); m.ack_tx_ns = r.get<int64_t>(); m.pair_gap_ns = r.get<int64_t>();
        m.pair_bytes = r.get<uint32_t>(); m.self = NodeRecord::read(r);
        uint8_t n = r.get<uint8_t>();
        for (int k = 0; k < n; ++k) m.gossip.push_back(NodeRecord::read(r));
        return m;
    }
};

enum class NackReason : uint8_t { None = 0, Capacity = 1, Quota = 2, Policy = 3, Health = 4, QueueFull = 5 };
inline const char* nack_name(NackReason r) {
    static const char* n[] = {"ok", "capacity", "quota", "policy", "health", "queue-full"};
    return n[int(r)];
}

struct ReserveReqMsg {
    TaskId task = 0;
    uint32_t attempt = 0;
    NodeId home = 0;
    uint16_t cores = 1;
    uint32_t mem_gb = 1;
    uint8_t gpus = 0;
    uint8_t tenant = 0;
    TaskClass cls = TaskClass::CpuBatch;
    float est_s = 1;
    uint32_t lease_ms = 1000;
    void write(Writer& w) const {
        w.put(task); w.put(attempt); w.put(home); w.put(cores); w.put(mem_gb); w.put(gpus); w.put(tenant);
        w.put(cls); w.put(est_s); w.put(lease_ms);
    }
    static ReserveReqMsg read(Reader& r) {
        ReserveReqMsg m;
        m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); m.home = r.get<NodeId>(); m.cores = r.get<uint16_t>();
        m.mem_gb = r.get<uint32_t>(); m.gpus = r.get<uint8_t>(); m.tenant = r.get<uint8_t>(); m.cls = r.get<TaskClass>();
        m.est_s = r.get<float>(); m.lease_ms = r.get<uint32_t>();
        return m;
    }
};

struct ReserveRepMsg {
    TaskId task = 0;
    uint32_t attempt = 0;
    uint8_t granted = 0;
    NackReason reason = NackReason::None;
    NodeRecord fresh;
    void write(Writer& w) const { w.put(task); w.put(attempt); w.put(granted); w.put(reason); fresh.write(w); }
    static ReserveRepMsg read(Reader& r) {
        ReserveRepMsg m;
        m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); m.granted = r.get<uint8_t>();
        m.reason = r.get<NackReason>(); m.fresh = NodeRecord::read(r);
        return m;
    }
};

struct DelegateMsg {
    TaskSpec spec;
    uint32_t attempt = 0;
    NodeId home = 0;
    float pred_exec = 0, pred_xfer = 0, pred_wait = 0;
    uint8_t migrations = 0;
    uint8_t reserved = 0;
    void write(Writer& w) const {
        spec.write(w); w.put(attempt); w.put(home); w.put(pred_exec); w.put(pred_xfer); w.put(pred_wait);
        w.put(migrations); w.put(reserved);
    }
    static DelegateMsg read(Reader& r) {
        DelegateMsg m;
        m.spec = TaskSpec::read(r); m.attempt = r.get<uint32_t>(); m.home = r.get<NodeId>();
        m.pred_exec = r.get<float>(); m.pred_xfer = r.get<float>(); m.pred_wait = r.get<float>();
        m.migrations = r.get<uint8_t>(); m.reserved = r.get<uint8_t>();
        return m;
    }
};

struct TaskReportMsg {  // TaskStarted / TaskDone / TaskFailed / TaskMoved
    TaskId task = 0;
    uint32_t attempt = 0;
    NodeId node = 0;     // executing node (or new node for TaskMoved)
    uint8_t reason = 0;  // failure reason
    float wait_s = 0, xfer_s = 0, exec_s = 0;
    float pred_wait = 0, pred_xfer = 0, pred_exec = 0;
    uint64_t in_bytes = 0, net_bytes = 0, out_bytes = 0;
    int64_t start_ns = 0, end_ns = 0;
    void write(Writer& w) const {
        w.put(task); w.put(attempt); w.put(node); w.put(reason); w.put(wait_s); w.put(xfer_s); w.put(exec_s);
        w.put(pred_wait); w.put(pred_xfer); w.put(pred_exec); w.put(in_bytes); w.put(net_bytes); w.put(out_bytes);
        w.put(start_ns); w.put(end_ns);
    }
    static TaskReportMsg read(Reader& r) {
        TaskReportMsg m;
        m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); m.node = r.get<NodeId>(); m.reason = r.get<uint8_t>();
        m.wait_s = r.get<float>(); m.xfer_s = r.get<float>(); m.exec_s = r.get<float>();
        m.pred_wait = r.get<float>(); m.pred_xfer = r.get<float>(); m.pred_exec = r.get<float>();
        m.in_bytes = r.get<uint64_t>(); m.net_bytes = r.get<uint64_t>(); m.out_bytes = r.get<uint64_t>();
        m.start_ns = r.get<int64_t>(); m.end_ns = r.get<int64_t>();
        return m;
    }
};

struct FetchMsg {
    TaskId task = 0;
    uint32_t attempt = 0;
    uint32_t xfer_id = 0;
    uint64_t bytes = 0;
    void write(Writer& w) const { w.put(task); w.put(attempt); w.put(xfer_id); w.put(bytes); }
    static FetchMsg read(Reader& r) {
        FetchMsg m;
        m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); m.xfer_id = r.get<uint32_t>(); m.bytes = r.get<uint64_t>();
        return m;
    }
};

struct DataDoneMsg {
    TaskId task = 0;
    uint32_t attempt = 0;
    uint32_t xfer_id = 0;
    uint64_t bytes = 0;
    uint8_t ok = 1;
    void write(Writer& w) const { w.put(task); w.put(attempt); w.put(xfer_id); w.put(bytes); w.put(ok); }
    static DataDoneMsg read(Reader& r) {
        DataDoneMsg m;
        m.task = r.get<TaskId>(); m.attempt = r.get<uint32_t>(); m.xfer_id = r.get<uint32_t>();
        m.bytes = r.get<uint64_t>(); m.ok = r.get<uint8_t>();
        return m;
    }
};

enum class FaultKind : uint8_t { GpuFail = 1, GpuRecover = 2 };

// Encode a message into a datagram.
inline std::vector<uint8_t> encode(const MsgHeader& h, const Writer& body) {
    Writer w;
    w.put(h);
    w.buf.insert(w.buf.end(), body.buf.begin(), body.buf.end());
    return std::move(w.buf);
}

}  // namespace nexus
