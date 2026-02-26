#include "consensus/raft_meta.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include "storage/posix_io.h"

namespace zujan
{
namespace consensus
{
namespace
{

TEST(RaftMetaTest, SaveAndRecover)
{
    const std::string meta_file = "test_raft_meta.tmp";
    ::unlink(meta_file.c_str());

    storage::PosixIOContext io_ctx;
    EXPECT_TRUE(io_ctx.Init());

    {
        auto meta_res = RaftMeta::Open(io_ctx, meta_file);
        ASSERT_TRUE(meta_res);
        auto meta = std::move(*meta_res);

        EXPECT_EQ(meta->CurrentTerm(), 0);
        EXPECT_EQ(meta->VotedFor(), 0);

        EXPECT_TRUE(meta->Save(5, 100));
        EXPECT_EQ(meta->CurrentTerm(), 5);
        EXPECT_EQ(meta->VotedFor(), 100);
    }

    // Recover
    {
        auto meta_res = RaftMeta::Open(io_ctx, meta_file);
        ASSERT_TRUE(meta_res);
        auto meta = std::move(*meta_res);

        EXPECT_EQ(meta->CurrentTerm(), 5);
        EXPECT_EQ(meta->VotedFor(), 100);

        // Overwrite
        EXPECT_TRUE(meta->Save(6, 101));
    }

    // Recover again
    {
        auto meta_res = RaftMeta::Open(io_ctx, meta_file);
        ASSERT_TRUE(meta_res);
        auto meta = std::move(*meta_res);

        EXPECT_EQ(meta->CurrentTerm(), 6);
        EXPECT_EQ(meta->VotedFor(), 101);
    }

    ::unlink(meta_file.c_str());
}

}  // namespace
}  // namespace consensus
}  // namespace zujan
