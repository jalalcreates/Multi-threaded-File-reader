#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <future>
#include <mutex>
#include <atomic>
#include <queue>
#include <memory>
#include <chrono>
#include <functional>
#include <numeric>
#include <sstream>
#include <iomanip>

using namespace std;

enum class CType { LOG, IMG, CSV };

struct Chunk {
    int id;
    CType type;
    string data;
};

struct Result {
    int id;
    CType type;
    size_t bytes;
    uint64_t hash;
    string summary;
};

struct alignas(64) Metrics {
    atomic<uint64_t> bytes{0};
    atomic<uint64_t> done{0};
};

struct Bucket {
    string name;
    vector<Result> items;
    mutex mtx;
};

template <typename T, size_t Cap>
class RingBuf {
    T buf[Cap];
    atomic<size_t> w_idx{0};
    atomic<size_t> r_idx{0};
public:
    bool push(const T& item) {
        size_t cw = w_idx.load(memory_order_relaxed);
        size_t nw = (cw + 1) % Cap;
        if (nw == r_idx.load(memory_order_acquire)) return false;
        
        while (!w_idx.compare_exchange_weak(cw, nw, memory_order_release, memory_order_relaxed)) {
            nw = (cw + 1) % Cap;
            if (nw == r_idx.load(memory_order_acquire)) return false;
        }
        buf[cw] = item;
        return true;
    }
    
    bool pop(T& item) {
        size_t cr = r_idx.load(memory_order_relaxed);
        if (cr == w_idx.load(memory_order_acquire)) return false;
        size_t nr = (cr + 1) % Cap;
        while (!r_idx.compare_exchange_weak(cr, nr, memory_order_release, memory_order_relaxed)) {
            if (cr == w_idx.load(memory_order_acquire)) return false;
            nr = (cr + 1) % Cap;
        }
        item = buf[cr];
        return true;
    }
    size_t pop_batch(vector<T>& out, size_t max_items) {
    size_t count = 0;
    while(count < max_items) {
        T item;
        if (!pop(item)) break;
        out.push_back(move(item));
        count++;
    }
    return count;
}
        size_t size() {
        return w_idx.load() - r_idx.load();
    }
};

class Pool {
    struct Lane {
        thread t;
        queue<function<void()>> q;
        mutex mtx;
        condition_variable cv;
        bool stop = false;
    };
    vector<Lane> lanes;
    size_t sz;
    atomic<size_t> next{0};
    atomic<bool> cancelled{false};
    
        void loop(size_t id) {
        while (true) {
            if (cancelled.load()) return;
            function<void()> task;
            {
                unique_lock<mutex> lk(lanes[id].mtx);
                lanes[id].cv.wait(lk, [&]{ return !lanes[id].q.empty() || lanes[id].stop; });
                if (lanes[id].stop && lanes[id].q.empty()) return;
                if (!lanes[id].q.empty()) {
                    task = move(lanes[id].q.front());
                    lanes[id].q.pop();
                }
            }
            if (!task) {
                for (size_t off = 1; off < sz; ++off) {
                    size_t nid = (id + off) % sz;
                    unique_lock<mutex> slk(lanes[nid].mtx, try_to_lock);
                    if (slk.owns_lock() && !lanes[nid].q.empty()) {
                        task = move(lanes[nid].q.front());
                        lanes[nid].q.pop();
                        break;
                    }
                }
            }
            if (task) task();
        }
    }
public:
    Pool(size_t n) : sz(n), lanes(n) {
        for (size_t i = 0; i < sz; ++i) {
            lanes[i].t = thread(&Pool::loop, this, i);
        }
    }
    
    template <typename F, typename... A>
    auto submit(F&& f, A&&... a) -> future<typename invoke_result<F, A...>::type> {
        using R = typename invoke_result<F, A...>::type;
        auto t = make_shared<packaged_task<R()>>(bind(forward<F>(f), forward<A>(a)...));
        future<R> fut = t->get_future();
        size_t idx = next.fetch_add(1, memory_order_relaxed) % sz;
        {
            lock_guard<mutex> lk(lanes[idx].mtx);
            lanes[idx].q.push([t]{ (*t)(); });
        }
        lanes[idx].cv.notify_one();
        return fut;
    }
    void cancel() {
        cancelled.store(true);
        for (size_t i = 0; i < sz; ++i) {
            lanes[i].cv.notify_all();
        }
    }
    
    ~Pool() {
        for (size_t i = 0; i < sz; ++i) {
            {
                lock_guard<mutex> lk(lanes[i].mtx);
                lanes[i].stop = true;
            }
            lanes[i].cv.notify_all();
            lanes[i].t.join();
        }
    }
};
struct RawStats {
    atomic<int> hits[4];
};
RawStats raw_s;
struct BatchStats {
    atomic<int> count{0};
    atomic<size_t> total_bytes{0};
};
BatchStats batch_stats;
atomic<int> debug_hits{0};
Result process(const Chunk& c, Metrics& m,int core_id) {
    debug_hits.fetch_add(1, memory_order_relaxed);
    raw_s.hits[core_id % 4].fetch_add(1, memory_order_relaxed);
    batch_stats.count.fetch_add(1, memory_order_relaxed);
    batch_stats.total_bytes.fetch_add(c.data.size(), memory_order_relaxed);
    uint64_t h = 14695981039346656037ULL;
    for (char ch : c.data) {
        h ^= (uint64_t)ch;
        h *= 1099511628211ULL;
    }
    this_thread::sleep_for(chrono::milliseconds(30));
    m.bytes.fetch_add(c.data.size(), memory_order_relaxed);
    m.done.fetch_add(1, memory_order_relaxed);
    
    string sum;
    if (c.type == CType::LOG) sum = "log audit done";
    else if (c.type == CType::IMG) sum = "img filter done";
    else sum = "csv analytics done";
    
    return {c.id, c.type, c.data.size(), h, sum};
}

void transfer(Bucket& src, Bucket& dst, Result& r) {
    scoped_lock lk(src.mtx, dst.mtx);
    dst.items.push_back(r);
}
int main() {
    cout << "starting hydra engine...\n";
    const size_t cores = 4;
    Pool pool(cores);
    RingBuf<Chunk, 16> rb;
    vector<Metrics> metrics(cores);
    Bucket staging{"staging", {}, {}}, archive{"archive", {}, {}};
    
    auto prod = async(launch::async, [&]{
        vector<Chunk> in = {
            {101, CType::LOG, "2026-07-22 10:00:00 ERROR 500 Database Connection Failed"},
            {102, CType::IMG, "\xFF\x00\xA5\xBB\xCC\xDD\xEE\xFF Pixel raw byte stream"},
            {103, CType::CSV, "AAPL,100,180.50\nMSFT,50,420.10\nNVDA,200,120.00"},
            {104, CType::LOG, "2026-07-22 10:00:05 WARN 404 Route Not Found"},
            {105, CType::IMG, "\x11\x22\x33\x44\x55\x66\x77\x88 Second image slice"}
        };
        for (auto& c : in) {
            while (!rb.push(c)) this_thread::yield();
        }
        return in.size();
    });
    size_t total = prod.get();
    cout << "buf size: " << rb.size() << "\n";
    cout << "ingested " << total << " chunks\n";
    vector<future<Result>> futs;
    size_t assign = 0;
    vector<Chunk> batch;
    while (total > 0) {
        batch.clear();
        rb.pop_batch(batch, 3);
        
        if (batch.empty()) {
            this_thread::yield(); // Give the CPU a break so the OS doesn't freeze
            continue;
        }

        for(auto& c : batch) {
            size_t core = assign % cores;
            assign++;
            futs.push_back(pool.submit([c, &metrics, core]{ return process(c, metrics[core],core); }));
            total--;
        }
    }
    this_thread::sleep_for(chrono::milliseconds(100));
    pool.cancel();
    
    auto audit = async(launch::deferred, [&]{
    cout << "computing deep audit...\n";
    uint64_t sum = 0;
    for (size_t i = 0; i < futs.size(); ++i) {
        if (futs[i].valid()) {
            sum += futs[i].get().hash;
            futs[i] = future<Result>();
        }
    }
    ostringstream ss;
    ss << "checksum: 0x" << hex << sum;
    return ss.str();
});
    
    cout << "harvesting results...\n";
    for (size_t i = 0; i < futs.size(); ++i) {
        Result r = futs[i].get();
        cout << "chunk " << r.id << " hash: 0x" << hex << r.hash << dec << " | " << r.summary << "\n";
        transfer(staging, archive, r);
    }
    
    cout << "run audit? ";
    string rep = audit.get();
    cout << rep << "\n";
    
    cout << "metrics:\n";
    for (size_t i = 0; i < cores; ++i) {
        cout << "core " << i << " -> chunks: " << metrics[i].done.load() << " | bytes: " << metrics[i].bytes.load() << "\n";
    }
        cout << "debug hits: " << debug_hits << "\n";
        cout << "batch stats: " << batch_stats.count.load() << " batches, " << batch_stats.total_bytes.load() << " bytes\n";
    return 0;
}