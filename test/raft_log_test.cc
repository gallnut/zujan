#include "consensus/raft_log.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include "storage/posix_io.h"

namespace zujan
{
namespace consensus
{
namespace
{

TEST(RaftLogTest, AppendAndRecover)
{
    const std::string log_file = "test_raft_log.tmp";
    ::unlink(log_file.c_str());

    storage::PosixIOContext io_ctx;
    EXPECT_TRUE(io_ctx.Init());

    {
        auto log_res = RaftLog::Open(io_ctx, log_file);
        ASSERT_TRUE(log_res);
        auto log = std::move(*log_res);

        EXPECT_EQ(log->LastIndex(), 0);
        EXPECT_EQ(log->LastTerm(), 0);

        proto::LogEntry e1;
        e1.set_term(1);
        e1.set_index(1);
        e1.set_key("k1");
        e1.set_value("v1");
        e1.set_type(proto::LogEntry::PUT);
        EXPECT_TRUE(log->Append(e1));

        proto::LogEntry e2;
        e2.set_term(1);
        e2.set_index(2);
        e2.set_key("k2");
        e2.set_value("v2");
        e2.set_type(proto::LogEntry::PUT);
        EXPECT_TRUE(log->Append(e2));

        EXPECT_EQ(log->LastIndex(), 2);
        EXPECT_EQ(log->LastTerm(), 1);
    }

    // Recover
    {
        auto log_res = RaftLog::Open(io_ctx, log_file);
        ASSERT_TRUE(log_res);
        auto log = std::move(*log_res);

        EXPECT_EQ(log->LastIndex(), 2);
        EXPECT_EQ(log->LastTerm(), 1);

        auto e1_res = log->Get(1);
        ASSERT_TRUE(e1_res);
        EXPECT_EQ(e1_res->key(), "k1");
        EXPECT_EQ(e1_res->value(), "v1");

        auto e2_res = log->Get(2);
        ASSERT_TRUE(e2_res);
        EXPECT_EQ(e2_res->key(), "k2");

        auto range_res = log->GetRange(1, 3);
        ASSERT_TRUE(range_res);
        EXPECT_EQ(range_res->size(), 2);
        EXPECT_EQ((*range_res)[0].key(), "k1");
        EXPECT_EQ((*range_res)[1].key(), "k2");

        // Truncate
        EXPECT_TRUE(log->TruncateFrom(2));
        EXPECT_EQ(log->LastIndex(), 1);
    }

    // Recover post-truncate
    {
        auto log_res = RaftLog::Open(io_ctx, log_file);
        ASSERT_TRUE(log_res);
        auto log = std::move(*log_res);
        EXPECT_EQ(log->LastIndex(), 1);
    }

    ::unlink(log_file.c_str());
}

}  // namespace
}  // namespace consensus
}  // namespace zujan
