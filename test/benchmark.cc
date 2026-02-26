#include <benchmark/benchmark.h>
#include <fcntl.h>
#include <unistd.h>

#include <vector>

#include "storage/uring_io.h"

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
BENCHMARK_MAIN();
