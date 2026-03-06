#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>

#include "storage/lsm_store.h"

namespace zujan
{
namespace storage
{
namespace
{

TEST(LSMTest, PutAndGet)
{
    auto store_res = zujan::storage::LSMStore::Open("test_lsm_store");
    ASSERT_TRUE(store_res.has_value());
    auto store = std::move(store_res.value());

    zujan::storage::ReadOptions ropt;

    auto put_res = store->Put("key1", "value1");
    ASSERT_TRUE(put_res.has_value());

    auto get_res = store->Get(ropt, "key1");
    ASSERT_TRUE(get_res.has_value());
    ASSERT_TRUE(get_res.value().has_value());
    EXPECT_EQ(get_res.value().value(), "value1");

    get_res = store->Get(ropt, "key2");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_FALSE(get_res.value().has_value());

    auto del_res = store->Delete("key1");
    ASSERT_TRUE(del_res.has_value());

    get_res = store->Get(ropt, "key1");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_FALSE(get_res.value().has_value());
}

TEST(LSMTest, Compaction)
{
    std::error_code ec;
    std::filesystem::remove_all("test_lsm_store_compaction", ec);
    auto store_res = LSMStore::Open("test_lsm_store_compaction");
    ASSERT_TRUE(store_res) << "Failed to open LSMStore";
    auto store = std::move(*store_res);

    zujan::storage::ReadOptions ropt;
    std::string                 large_val(1024, 'v');  // 1KB value

    for (int i = 0; i < 15000; ++i)
    {
        std::string key = "key" + std::to_string(i);
        auto        err = store->Put(key, large_val + std::to_string(i));
        ASSERT_TRUE(err);
    }

    // Verify
    for (int i = 0; i < 15000; i += 100)
    {
        std::string key = "key" + std::to_string(i);
        auto        get_res = store->Get(ropt, key);
        ASSERT_TRUE(get_res.has_value());
        if (!get_res.value().has_value())
        {
            std::cerr << "Key missing in pass 1: " << key << std::endl;
        }
        else
        {
            ASSERT_EQ(get_res.value().value(), large_val + std::to_string(i));
        }
    }

    // Overwrite
    for (int i = 0; i < 1000; ++i)
    {
        std::string key = "key" + std::to_string(i);
        auto        err = store->Put(key, "new_value" + std::to_string(i));
        ASSERT_TRUE(err);
    }

    // Trigger more flushes (L0 creation, possibly compaction)
    for (int i = 15000; i < 25000; ++i)
    {
        std::string key = "key" + std::to_string(i);
        auto        err = store->Put(key, large_val + std::to_string(i));
        ASSERT_TRUE(err);
    }

    // Delete
    for (int i = 1000; i < 2000; ++i)
    {
        std::string key = "key" + std::to_string(i);
        auto        err = store->Delete(key);
        ASSERT_TRUE(err);
    }

    for (int i = 25000; i < 35000; ++i)
    {
        std::string key = "key" + std::to_string(i);
        auto        err = store->Put(key, large_val + std::to_string(i));
        ASSERT_TRUE(err);
    }

    // Give background thread time
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Verify overwritten
    for (int i = 0; i < 1000; i += 100)
    {
        std::string key = "key" + std::to_string(i);
        auto        get_res = store->Get(ropt, key);
        ASSERT_TRUE(get_res.has_value());
        ASSERT_TRUE(get_res.value().has_value());
        ASSERT_EQ(get_res.value().value(), "new_value" + std::to_string(i));
    }

    // Verify deleted
    for (int i = 1100; i < 1900; i += 100)
    {
        std::string key = "key" + std::to_string(i);
        auto        get_res = store->Get(ropt, key);
        ASSERT_TRUE(get_res);
        ASSERT_FALSE(get_res->has_value());
    }

    std::filesystem::remove_all("test_lsm_store_compaction", ec);
}

TEST(LSMTest, RecoveryAndWALCleanup)
{
    std::error_code ec;
    std::filesystem::remove_all("test_lsm_store_recovery", ec);

    std::string large_val(1024, 'r');  // 1KB value

    {
        auto store_res = LSMStore::Open("test_lsm_store_recovery");
        ASSERT_TRUE(store_res) << "Failed to open LSMStore";
        auto store = std::move(*store_res);

        // Put enough keys to trigger at least one flush
        for (int i = 0; i < 5000; ++i)
        {
            std::string key = "rec_key" + std::to_string(i);
            auto        err = store->Put(key, large_val + std::to_string(i));
            ASSERT_TRUE(err);
        }

        // Give background thread time to flush
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Put some more keys that will remain in the MemTable/WAL
        for (int i = 5000; i < 6000; ++i)
        {
            std::string key = "rec_key" + std::to_string(i);
            auto        err = store->Put(key, large_val + std::to_string(i));
            ASSERT_TRUE(err);
        }
    }  // Store is closed, background thread joined, WAL file left.

    // Reopen
    {
        auto store_res = LSMStore::Open("test_lsm_store_recovery");
        ASSERT_TRUE(store_res) << "Failed to reopen LSMStore";
        auto store = std::move(*store_res);

        zujan::storage::ReadOptions ropt;

        // Verify keys flushed to SSTable
        for (int i = 0; i < 5000; i += 100)
        {
            std::string key = "rec_key" + std::to_string(i);
            auto        get_res = store->Get(ropt, key);
            ASSERT_TRUE(get_res);
            ASSERT_TRUE(get_res->has_value()) << "Flush recovered key missing: " << key;
            ASSERT_EQ(get_res->value(), large_val + std::to_string(i));
        }

        // Verify keys recovered from WAL
        for (int i = 5000; i < 6000; i += 100)
        {
            std::string key = "rec_key" + std::to_string(i);
            auto        get_res = store->Get(ropt, key);
            ASSERT_TRUE(get_res);
            ASSERT_TRUE(get_res->has_value()) << "MemTable unrecovered key missing: " << key;
            ASSERT_EQ(get_res->value(), large_val + std::to_string(i));
        }

        // Verify old WALs are cleaned up
        int         wal_count = 0;
        std::string logs_str = "";
        for (const auto& entry : std::filesystem::directory_iterator("test_lsm_store_recovery"))
        {
            if (entry.path().extension() == ".log")
            {
                wal_count++;
                logs_str += entry.path().filename().string() + " ";
            }
        }
        EXPECT_LE(wal_count, 2) << "Found logs: " << logs_str;
    }

    std::filesystem::remove_all("test_lsm_store_recovery", ec);
}

}  // namespace
}  // namespace storage
}  // namespace zujan
