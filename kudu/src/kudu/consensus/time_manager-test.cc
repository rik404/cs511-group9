// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/consensus/time_manager.h"

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::thread;
using std::unique_ptr;
using std::vector;

METRIC_DECLARE_entity(server);

namespace kudu {
namespace consensus {

class TimeManagerTest : public KuduTest {
 public:
  TimeManagerTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "time-manager-test")),
        clock_(metric_entity_) {
  }

  void SetUp() override {
    CHECK_OK(clock_.Init());
  }

  void TearDown() override {
    for (auto& thread : threads_) {
      thread.join();
    }
  }

 protected:
  void InitTimeManager(Timestamp initial_safe_time = Timestamp::kMin) {
    time_manager_.reset(new TimeManager(&clock_, initial_safe_time));
  }

  // Returns a latch that allows to wait for TimeManager to consider 'safe_time' safe.
  CountDownLatch* WaitForSafeTimeAsync(Timestamp safe_time) {
    latches_.emplace_back(new CountDownLatch(1));
    CountDownLatch* latch = latches_.back().get();
    threads_.emplace_back([=]() {
      CHECK_OK(time_manager_->WaitUntilSafe(safe_time, MonoTime::Max()));
      // When the waiter unblocks safe time should be higher than or equal to 'safe_time'
      CHECK_GE(time_manager_->GetSafeTime(), safe_time);
      latch->CountDown();
    });
    return latch;
  }

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  clock::HybridClock clock_;
  unique_ptr<TimeManager> time_manager_;
  vector<unique_ptr<CountDownLatch>> latches_;
  vector<thread> threads_;
};

// Tests TimeManager's functionality in non-leader mode and the transition to leader mode.
TEST_F(TimeManagerTest, TestTimeManagerNonLeaderMode) {
  // TimeManager should start in non-leader mode and consider the initial timestamp safe.
  Timestamp before = clock_.Now();
  Timestamp init(before.value() + 1);
  Timestamp after(init.value() + 1);
  InitTimeManager(init);
  ASSERT_EQ(time_manager_->mode_, TimeManager::NON_LEADER);
  ASSERT_EQ(time_manager_->last_serial_ts_assigned_, init);
  ASSERT_EQ(time_manager_->GetSafeTime(), init);

  // Check that 'before' is safe, as is 'init'. 'after' shouldn't be safe.
  ASSERT_TRUE(time_manager_->IsTimestampSafe(before));
  ASSERT_TRUE(time_manager_->IsTimestampSafe(init));
  ASSERT_FALSE(time_manager_->IsTimestampSafe(after));

  // Shouldn't be able to assign timestamps.
  ReplicateMsg message;
  ASSERT_TRUE(time_manager_->AssignTimestamp(&message).IsIllegalState());

  message.set_timestamp(after.value());
  // Should accept messages from the leader.
  ASSERT_OK(time_manager_->MessageReceivedFromLeader(message));
  ASSERT_EQ(time_manager_->last_serial_ts_assigned_, after);
  // .. but shouldn't advance safe time (until we have leader leases).
  ASSERT_EQ(time_manager_->GetSafeTime(), init);

  // Waiting for safe time at this point should time out since we're not moving it.
  MonoTime after_small = MonoTime::Now() + MonoDelta::FromMilliseconds(100);
  ASSERT_TRUE(time_manager_->WaitUntilSafe(after, after_small).IsTimedOut());

  // Create a latch to wait on 'after' to be safe.
  CountDownLatch* after_latch = WaitForSafeTimeAsync(after);

  // Accepting messages from the leader shouldn't advance safe time.
  ASSERT_EQ(time_manager_->GetSafeTime(), init);
  ASSERT_EQ(after_latch->count(), 1);

  // Advancing safe time with a message should unblock the waiter and advance safe time.
  message.set_timestamp(after.value());
  time_manager_->AdvanceSafeTimeWithMessage(message);
  after_latch->Wait();
  ASSERT_EQ(time_manager_->GetSafeTime(), after);

  // Committing an old message shouldn't move safe time back.
  message.set_timestamp(before.value());
  time_manager_->AdvanceSafeTimeWithMessage(message);
  ASSERT_EQ(time_manager_->GetSafeTime(), after);

  // Advance 'after' again and test advancing safe time with an explicit timestamp like
  // the leader sends on (empty) heartbeat messages.
  after = clock_.Now();
  after_latch = WaitForSafeTimeAsync(after);
  time_manager_->AdvanceSafeTime(after);
  after_latch->Wait();
  ASSERT_EQ(time_manager_->GetSafeTime(), after);

  // Changing to leader mode should advance safe time.
  after = clock_.Now();
  after_latch = WaitForSafeTimeAsync(after);
  time_manager_->SetLeaderMode();
  after_latch->Wait();
  ASSERT_GE(time_manager_->GetSafeTime(), after);
}

// Tests the TimeManager's functionality in leader mode and the transition to non-leader mode.
TEST_F(TimeManagerTest, TestTimeManagerLeaderMode) {
  Timestamp init = clock_.Now();
  InitTimeManager(init);
  time_manager_->SetLeaderMode();
  Timestamp safe_before = time_manager_->GetSafeTime();

  ReplicateMsg message;
  // In leader mode we should be able to assign timestamps and the timestamp should be higher
  // than 'init'.
  ASSERT_OK(time_manager_->AssignTimestamp(&message));
  ASSERT_TRUE(message.has_timestamp());
  Timestamp message_ts(message.timestamp());
  ASSERT_GT(message_ts, safe_before);

  // In leader mode calling MessageReceivedFromLeader() should cause a CHECK failure.
  EXPECT_DEATH({
     time_manager_->MessageReceivedFromLeader(message);
    }, "Cannot receive messages from a leader in leader mode.");

  // .. as should AdvanceSafeTime()
  EXPECT_DEATH({
     time_manager_->AdvanceSafeTime(clock_.Now());
    }, "Cannot advance safe time by timestamp in leader mode.");

  // Since we haven't appended the message to the queue, safe time should be 'pinned' to
  // 'safe_before'.
  ASSERT_EQ(time_manager_->GetSafeTime(), safe_before);

  // When we append the message to the queue safe time should advance again.
  time_manager_->AdvanceSafeTimeWithMessage(message);
  ASSERT_GT(time_manager_->GetSafeTime(), message_ts);

  // 'Now' should be safe.
  Timestamp now = clock_.Now();
  ASSERT_TRUE(time_manager_->IsTimestampSafe(now));
  ASSERT_GT(time_manager_->GetSafeTime(), now);

  // When changing to non-leader mode a timestamp after the last safe time shouldn't be
  // safe anymore (even if that time came before the actual change).
  now = clock_.Now();
  time_manager_->SetNonLeaderMode();
  Timestamp safe_after = time_manager_->GetSafeTime();
  ASSERT_LE(safe_after, now);

  // In leader mode GetSafeTime() usually moves it, but since we changed to non-leader mode
  // safe time shouldn't move anymore ...
  ASSERT_EQ(time_manager_->GetSafeTime(), safe_after);
  now = clock_.Now();
  const auto after_small = MonoTime::Now() + MonoDelta::FromMilliseconds(100);
  ASSERT_TRUE(time_manager_->WaitUntilSafe(now, after_small).IsTimedOut());

  // ... unless we get a message from the leader.
  now = clock_.Now();
  CountDownLatch* after_latch = WaitForSafeTimeAsync(now);
  message.set_timestamp(now.value());
  ASSERT_OK(time_manager_->MessageReceivedFromLeader(message));
  time_manager_->AdvanceSafeTimeWithMessage(message);
  after_latch->Wait();
}

// Test simulating safe time advancement as should be performed by an op that
// finalizes a transaction's commit timestamp. The commit timestamp should be
// used as a lower bound on new op timestamps.
TEST_F(TimeManagerTest, TestUpdateClockWithCommitTimestamp) {
  Timestamp init = clock_.Now();
  InitTimeManager(init);
  time_manager_->SetLeaderMode();
  const auto kShortTimeout = MonoDelta::FromMilliseconds(10);
  {
    // Operate on a commit timestamp a while (five seconds) in the future.
    Timestamp txn1_commit_ts(init.value() + 5000000000);
    const auto safe_time_before_update = time_manager_->GetSafeTime();
    ASSERT_OK(time_manager_->UpdateClockAndLastAssignedTimestamp(txn1_commit_ts));

    // The serial timestamp should have been bumped forward, but not the safe
    // time, which is pinned until the call to AdvanceSafeTimeWithMessage().
    // Thus, the next timestamp assigned should be higher than the commit
    // timestamp, but the safe time should still not have moved.
    ASSERT_EQ(safe_time_before_update, time_manager_->GetSafeTime());
    ReplicateMsg txn1_commit_replicate;
    ASSERT_OK(time_manager_->AssignTimestamp(&txn1_commit_replicate));
    ASSERT_GT(txn1_commit_replicate.timestamp(), txn1_commit_ts.value());
    ASSERT_EQ(safe_time_before_update, time_manager_->GetSafeTime());
    Status s = time_manager_->WaitUntilSafe(txn1_commit_ts, MonoTime::Now() + kShortTimeout);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

    // Once the safe time is advanced, we should readily be able to wait for
    // the safe time to pass.
    time_manager_->AdvanceSafeTimeWithMessage(txn1_commit_replicate);
    ASSERT_GT(time_manager_->GetSafeTime(), safe_time_before_update);
    ASSERT_OK(time_manager_->WaitUntilSafe(txn1_commit_ts, MonoTime::Now() + kShortTimeout));
  }

  // If we update the clock with a timestamp in the past (e.g. if the commit
  // timestamp assigned for a transaction is in the past), the last assigned
  // timestamp should remain where it was, and since no ops are otherwise known
  // to be in-flight, safe time should march forward.
  {
    Timestamp txn2_commit_ts(init);
    const auto safe_time_before_update = time_manager_->GetSafeTime();
    ASSERT_OK(time_manager_->UpdateClockAndLastAssignedTimestamp(txn2_commit_ts));

    // Safe time should move forward.
    ASSERT_LT(safe_time_before_update, time_manager_->GetSafeTime());

    // The next timestamp assigned should be higher than the commit timestamp,
    // and safe time should be pinned until explicitly advanced.
    ReplicateMsg txn2_commit_replicate;
    ASSERT_OK(time_manager_->AssignTimestamp(&txn2_commit_replicate));
    const auto commit_op_ts = Timestamp(txn2_commit_replicate.timestamp());
    ASSERT_GT(commit_op_ts, txn2_commit_ts);
    ASSERT_GT(commit_op_ts, safe_time_before_update);
    ASSERT_GT(commit_op_ts, time_manager_->GetSafeTime());
    ASSERT_OK(time_manager_->WaitUntilSafe(txn2_commit_ts, MonoTime::Now() + kShortTimeout));
    Status s = time_manager_->WaitUntilSafe(commit_op_ts, MonoTime::Now() + kShortTimeout);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

    // Once the safe time is bumped, it should be unpinned and return a value
    // higher than any timestamp we've previously assigned.
    time_manager_->AdvanceSafeTimeWithMessage(txn2_commit_replicate);
    ASSERT_LT(txn2_commit_replicate.timestamp(), time_manager_->GetSafeTime().value());
    ASSERT_OK(time_manager_->WaitUntilSafe(txn2_commit_ts, MonoTime::Now() + kShortTimeout));
    ASSERT_OK(time_manager_->WaitUntilSafe(commit_op_ts, MonoTime::Now() + kShortTimeout));
  }

  // Finally, when in non-leader mode, bumping the last assigned timestamp is
  // disallowed.
  time_manager_->SetNonLeaderMode();
  const auto& now = clock_.Now();
  Status s = time_manager_->UpdateClockAndLastAssignedTimestamp(now);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
}

} // namespace consensus
} // namespace kudu
