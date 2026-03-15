#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include <vector>
#include <string>
#include <filesystem>
#include <random>

#include "storage/uring_io.h"
#include "storage/lsm_store.h"
#include "leveldb/db.h"
#include "leveldb/options.h"

using namespace zujan::storage;

static void BM_URing_SyncRead(benchmark::State &state)
{
    URingIOContext io_ctx(256);
    auto           _res = io_ctx.Init();

    int               fd = open("bench_test.tmp", O_RDWR | O_CREAT, 0644);
    std::vector<char> data(4096, 'a');
    ::pwrite(fd, data.data(), data.size(), 0);

    std::vector<char> buf(4096);
    for (auto _ : state)
    {
        auto res = io_ctx.ReadAligned(fd, buf, 0);
        benchmark::DoNotOptimize(res);
    }

    close(fd);
    unlink("bench_test.tmp");
}

static void BM_URing_AsyncRead(benchmark::State &state)
{
    URingIOContext io_ctx(256);
    auto           _res = io_ctx.Init();

    int               fd = open("bench_test.tmp", O_RDWR | O_CREAT, 0644);
    std::vector<char> data(4096, 'a');
    ::pwrite(fd, data.data(), data.size(), 0);

    std::vector<char> buf(4096);
    for (auto _ : state)
    {
        auto future = io_ctx.ReadAsync(fd, buf, 0);
        auto res = future.get();
        benchmark::DoNotOptimize(res);
    }

    close(fd);
    unlink("bench_test.tmp");
}

static void BM_URing_AsyncReadParallel(benchmark::State &state)
{
    URingIOContext io_ctx(256);
    auto           _res = io_ctx.Init();

    int               fd = open("bench_test.tmp", O_RDWR | O_CREAT, 0644);
    std::vector<char> data(4096, 'a');
    ::pwrite(fd, data.data(), data.size(), 0);

    std::vector<char> buf(4096);
    int               batch_size = state.range(0);
    for (auto _ : state)
    {
        std::vector<std::future<std::expected<int, Error>>> futures;
        for (int i = 0; i < batch_size; ++i)
        {
            futures.push_back(io_ctx.ReadAsync(fd, buf, 0));
        }
        for (auto &f : futures)
        {
            auto res = f.get();
            benchmark::DoNotOptimize(res);
        }
    }

    close(fd);
    unlink("bench_test.tmp");
}

BENCHMARK(BM_URing_SyncRead);
BENCHMARK(BM_URing_AsyncRead);
BENCHMARK(BM_URing_AsyncReadParallel)->Range(2, 64);

static LSMStore* g_store_seq = nullptr;
static void BM_LSMStore_SeqWrite(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_lsm_seq_write";
        std::filesystem::remove_all(dir);
        auto store_res = LSMStore::Open(dir);
        g_store_seq = store_res.value().release();
    }
    
    uint64_t i = 0;
    std::string val(256, 'v');
    for (auto _ : state) {
        std::string key = "key" + std::to_string(state.thread_index()) + "_" + std::to_string(i++);
        auto res = g_store_seq->Put(key, val);
        benchmark::DoNotOptimize(res);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_store_seq;
        std::filesystem::remove_all("bench_lsm_seq_write");
    }
}
BENCHMARK(BM_LSMStore_SeqWrite)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static LSMStore* g_store_seq_nowal = nullptr;
static void BM_LSMStore_SeqWrite_NoWAL(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_lsm_seq_write_nowal";
        std::filesystem::remove_all(dir);
        LSMStoreOptions opts;
        opts.disable_wal = true;
        auto store_res = LSMStore::Open(dir, opts);
        g_store_seq_nowal = store_res.value().release();
    }
    
    uint64_t i = 0;
    std::string val(256, 'v');
    for (auto _ : state) {
        std::string key = "key" + std::to_string(state.thread_index()) + "_" + std::to_string(i++);
        auto res = g_store_seq_nowal->Put(key, val);
        benchmark::DoNotOptimize(res);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_store_seq_nowal;
        std::filesystem::remove_all("bench_lsm_seq_write_nowal");
    }
}
BENCHMARK(BM_LSMStore_SeqWrite_NoWAL)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static LSMStore* g_store_rand = nullptr;
static void BM_LSMStore_RandomWrite(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_lsm_rand_write";
        std::filesystem::remove_all(dir);
        auto store_res = LSMStore::Open(dir);
        g_store_rand = store_res.value().release();
    }
    
    std::random_device rd;
    std::mt19937 gen(rd() ^ state.thread_index());
    std::uniform_int_distribution<uint64_t> distrib(1, 10000000);
    
    std::string val(256, 'v');
    for (auto _ : state) {
        std::string key = "key" + std::to_string(distrib(gen));
        auto res = g_store_rand->Put(key, val);
        benchmark::DoNotOptimize(res);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_store_rand;
        std::filesystem::remove_all("bench_lsm_rand_write");
    }
}
BENCHMARK(BM_LSMStore_RandomWrite)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static LSMStore* g_store_rand_nowal = nullptr;
static void BM_LSMStore_RandomWrite_NoWAL(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_lsm_rand_write_nowal";
        std::filesystem::remove_all(dir);
        LSMStoreOptions opts;
        opts.disable_wal = true;
        auto store_res = LSMStore::Open(dir, opts);
        g_store_rand_nowal = store_res.value().release();
    }
    
    std::random_device rd;
    std::mt19937 gen(rd() ^ state.thread_index());
    std::uniform_int_distribution<uint64_t> distrib(1, 10000000);
    
    std::string val(256, 'v');
    for (auto _ : state) {
        std::string key = "key" + std::to_string(distrib(gen));
        auto res = g_store_rand_nowal->Put(key, val);
        benchmark::DoNotOptimize(res);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_store_rand_nowal;
        std::filesystem::remove_all("bench_lsm_rand_write_nowal");
    }
}
BENCHMARK(BM_LSMStore_RandomWrite_NoWAL)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static LSMStore* g_store_read = nullptr;
static void BM_LSMStore_RandomRead(benchmark::State &state) {
    std::string dir = "bench_lsm_rand_read";
    
    // Setup Phase
    if (state.thread_index() == 0) {
        std::filesystem::remove_all(dir);
        auto store_res = LSMStore::Open(dir);
        g_store_read = store_res.value().release();
        std::string val(256, 'v');
        for (uint64_t i = 0; i < 100000; ++i) {
            g_store_read->Put("key" + std::to_string(i), val);
        }
    }
    
    std::random_device rd;
    std::mt19937 gen(rd() ^ state.thread_index());
    std::uniform_int_distribution<uint64_t> distrib(1, 100000);
    ReadOptions ropt;

    for (auto _ : state) {
        std::string key = "key" + std::to_string(distrib(gen));
        auto res = g_store_read->Get(ropt, key);
        benchmark::DoNotOptimize(res);
    }
    
    state.SetItemsProcessed(state.iterations());
    if (state.thread_index() == 0) {
        delete g_store_read;
        std::filesystem::remove_all(dir);
    }
}
BENCHMARK(BM_LSMStore_RandomRead)->Threads(1)->Threads(4)->Threads(8);

static leveldb::DB* g_ldb_seq = nullptr;
static void BM_LevelDB_SeqWrite(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_leveldb_seq_write";
        std::filesystem::remove_all(dir);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB::Open(options, dir, &g_ldb_seq);
    }
    
    uint64_t i = 0;
    std::string val(256, 'v');
    leveldb::WriteOptions wopt;
    for (auto _ : state) {
        std::string key = "key" + std::to_string(state.thread_index()) + "_" + std::to_string(i++);
        auto status = g_ldb_seq->Put(wopt, key, val);
        benchmark::DoNotOptimize(status);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_ldb_seq;
        std::filesystem::remove_all("bench_leveldb_seq_write");
    }
}
BENCHMARK(BM_LevelDB_SeqWrite)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static leveldb::DB* g_ldb_rand = nullptr;
static void BM_LevelDB_RandomWrite(benchmark::State &state) {
    if (state.thread_index() == 0) {
        std::string dir = "bench_leveldb_rand_write";
        std::filesystem::remove_all(dir);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB::Open(options, dir, &g_ldb_rand);
    }
    
    std::random_device rd;
    std::mt19937 gen(rd() ^ state.thread_index());
    std::uniform_int_distribution<uint64_t> distrib(1, 10000000);
    
    std::string val(256, 'v');
    leveldb::WriteOptions wopt;
    for (auto _ : state) {
        std::string key = "key" + std::to_string(distrib(gen));
        auto status = g_ldb_rand->Put(wopt, key, val);
        benchmark::DoNotOptimize(status);
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * (16 + 256));
    
    if (state.thread_index() == 0) {
        delete g_ldb_rand;
        std::filesystem::remove_all("bench_leveldb_rand_write");
    }
}
BENCHMARK(BM_LevelDB_RandomWrite)->Threads(1)->Threads(4)->Threads(8)->UseRealTime();

static leveldb::DB* g_ldb_read = nullptr;
static void BM_LevelDB_RandomRead(benchmark::State &state) {
    std::string dir = "bench_leveldb_rand_read";
    
    if (state.thread_index() == 0) {
        std::filesystem::remove_all(dir);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB::Open(options, dir, &g_ldb_read);
        std::string val(256, 'v');
        leveldb::WriteOptions wopt;
        for (uint64_t i = 0; i < 100000; ++i) {
            g_ldb_read->Put(wopt, "key" + std::to_string(i), val);
        }
    }
    
    std::random_device rd;
    std::mt19937 gen(rd() ^ state.thread_index());
    std::uniform_int_distribution<uint64_t> distrib(1, 100000);
    leveldb::ReadOptions ropt;

    for (auto _ : state) {
        std::string key = "key" + std::to_string(distrib(gen));
        std::string val;
        auto status = g_ldb_read->Get(ropt, key, &val);
        benchmark::DoNotOptimize(status);
    }
    
    state.SetItemsProcessed(state.iterations());
    if (state.thread_index() == 0) {
        delete g_ldb_read;
        std::filesystem::remove_all(dir);
    }
}
BENCHMARK(BM_LevelDB_RandomRead)->Threads(1)->Threads(4)->Threads(8);

BENCHMARK_MAIN();
