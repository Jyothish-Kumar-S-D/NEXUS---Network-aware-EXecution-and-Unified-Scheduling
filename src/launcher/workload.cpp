#include "nexus/workload.hpp"

namespace nexus {

WorkloadParams WorkloadParams::from_json(const json::Value* j) {
    WorkloadParams w;
    if (!j) return w;
    w.duration_s = j->get_num("duration_s", w.duration_s);
    w.rate_per_s = j->get_num("rate_per_s", w.rate_per_s);
    w.scale = j->get_num("scale", w.scale);
    if (auto m = j->find("mix")) {
        w.w_cpu = m->get_num("cpu-batch", w.w_cpu);
        w.w_gpu = m->get_num("gpu-train", w.w_gpu);
        w.w_pipeline = m->get_num("pipeline", w.w_pipeline);
        w.w_interactive = m->get_num("interactive", w.w_interactive);
        w.w_mem = m->get_num("mem-heavy", w.w_mem);
    }
    return w;
}

static uint64_t mb(double v) { return uint64_t(v * 1024.0 * 1024.0); }

std::vector<JobSpec> generate_workload(const ClusterConfig& cfg, const WorkloadParams& wp, uint64_t seed) {
    Rng rng(seed * 0x9E3779B97F4A7C15ULL + 12345);
    int physics = std::max(0, cfg.tenant_index("physics"));
    int ml = std::max(0, cfg.tenant_index("ml"));
    int bio = std::max(0, cfg.tenant_index("bio"));
    auto ds_mb = [&](int id) { auto d = cfg.dataset(id); return d ? d->size_mb : 200.0; };
    auto input = [&](int ds, double lo_frac, double hi_frac) {
        InputRef in;
        in.kind = InputKind::Dataset;
        in.ref = uint32_t(ds);
        in.bytes = mb(ds_mb(ds) * rng.uniform(lo_frac, hi_frac));
        return in;
    };
    auto parent = [&](uint16_t idx, double out_mb) {
        InputRef in;
        in.kind = InputKind::Parent;
        in.ref = idx;
        in.bytes = mb(out_mb);
        return in;
    };
    double total_w = wp.w_cpu + wp.w_gpu + wp.w_pipeline + wp.w_interactive + wp.w_mem;
    std::vector<JobSpec> jobs;
    double t = rng.exponential(wp.rate_per_s);
    uint32_t id = 1;
    int N = int(cfg.nodes.size());
    while (t < wp.duration_s) {
        JobSpec j;
        j.id = id++;
        j.submit_s = t;
        j.origin = NodeId(rng.uniform_int(0, N - 1));
        double u = rng.uniform(0, total_w);
        auto task = [&](uint16_t idx, TaskClass cls, uint16_t cores, uint32_t mem, uint8_t gpus, double work) {
            TaskSpec s;
            s.id = make_task_id(j.id, idx);
            s.job = j.id;
            s.index = idx;
            s.cls = cls;
            s.tenant = j.tenant;
            s.cores = cores;
            s.mem_gb = mem;
            s.gpus = gpus;
            s.work = float(work * wp.scale);
            return s;
        };
        if ((u -= wp.w_cpu) < 0) {
            j.kind = "cpu-batch";
            j.tenant = uint8_t(physics);
            uint16_t c = uint16_t(rng.uniform_int(1, 2) * 4);
            auto s = task(0, TaskClass::CpuBatch, c, uint32_t(rng.uniform_int(8, 32)), 0, c * rng.uniform(5, 14));
            s.inputs.push_back(input(rng.chance(0.6) ? 0 : 1, 0.3, 0.8));
            s.out_bytes = mb(rng.uniform(20, 80));
            j.tasks.push_back(s);
        } else if ((u -= wp.w_gpu) < 0) {
            j.kind = "gpu-train";
            j.tenant = uint8_t(ml);
            auto s = task(0, TaskClass::GpuTrain, 4, 32, 1, rng.uniform(8, 22));
            s.inputs.push_back(input(2, 0.4, 0.9));
            s.out_bytes = mb(rng.uniform(50, 150));
            j.tasks.push_back(s);
        } else if ((u -= wp.w_pipeline) < 0) {
            j.kind = "pipeline";
            j.tenant = uint8_t(bio);
            double o0 = rng.uniform(200, 400), o1 = rng.uniform(80, 180), o2 = rng.uniform(80, 180);
            auto e = task(0, TaskClass::DataEtl, 4, 16, 0, 4 * rng.uniform(2.5, 5));
            e.inputs.push_back(input(1, 0.5, 1.0));
            e.out_bytes = mb(o0);
            auto a = task(1, TaskClass::CpuBatch, 4, 16, 0, 4 * rng.uniform(4, 8));
            a.inputs.push_back(parent(0, o0 * 0.5));
            a.deps = {0};
            a.out_bytes = mb(o1);
            auto b = task(2, TaskClass::CpuBatch, 4, 16, 0, 4 * rng.uniform(4, 8));
            b.inputs.push_back(parent(0, o0 * 0.5));
            b.deps = {0};
            b.out_bytes = mb(o2);
            auto r = task(3, TaskClass::DataEtl, 2, 8, 0, 2 * rng.uniform(1.5, 3));
            r.inputs.push_back(parent(1, o1));
            r.inputs.push_back(parent(2, o2));
            r.deps = {1, 2};
            r.out_bytes = mb(20);
            j.tasks = {e, a, b, r};
        } else if ((u -= wp.w_interactive) < 0) {
            j.kind = "interactive";
            j.tenant = uint8_t(rng.chance(0.5) ? physics : bio);
            auto s = task(0, TaskClass::Interactive, 1, 2, 0, rng.uniform(0.5, 2.0));
            s.inputs.push_back(input(0, 0.01, 0.04));
            s.out_bytes = mb(1);
            j.tasks.push_back(s);
        } else {
            j.kind = "mem-heavy";
            j.tenant = uint8_t(bio);
            auto s = task(0, TaskClass::MemHeavy, 8, uint32_t(rng.uniform_int(160, 360)), 0, 8 * rng.uniform(4, 10));
            s.inputs.push_back(input(1, 0.3, 0.6));
            s.out_bytes = mb(rng.uniform(50, 100));
            j.tasks.push_back(s);
        }
        jobs.push_back(std::move(j));
        t += rng.exponential(wp.rate_per_s);
    }
    return jobs;
}

WorkloadStats workload_stats(const ClusterConfig& cfg, const std::vector<JobSpec>& jobs) {
    WorkloadStats s;
    (void)cfg;
    for (auto& j : jobs) {
        s.jobs++;
        for (auto& t : j.tasks) {
            s.tasks++;
            if (t.gpus) s.gpu_seconds += t.work;
            else s.core_seconds += t.work;
            for (auto& i : t.inputs) s.input_gb += double(i.bytes) / 1e9;
        }
    }
    return s;
}

}  // namespace nexus
