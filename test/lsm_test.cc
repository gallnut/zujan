#include <gtest/gtest.h>

#include "storage/lsm_store.h"

namespace zujan
{
namespace storage
{
namespace
{

TEST(LSMTest, PutAndGet)
{
    auto store_res = LSMStore::Open();
    ASSERT_TRUE(store_res) << "Failed to open LSMStore";
    auto store = std::move(*store_res);

    auto put_err = store->Put("key1", "value1");
    EXPECT_TRUE(put_err);

    auto get_err = store->Get("key1");
    EXPECT_TRUE(get_err);
    EXPECT_TRUE(get_err->has_value());
    EXPECT_EQ(get_err->value(), "value1");

    get_err = store->Get("key2");
    EXPECT_TRUE(get_err);
    EXPECT_FALSE(get_err->has_value());

    auto del_err = store->Delete("key1");
    EXPECT_TRUE(del_err);

    get_err = store->Get("key1");
    EXPECT_TRUE(get_err);
    EXPECT_FALSE(get_err->has_value());
}

}  // namespace
}  // namespace storage
}  // namespace zujan
