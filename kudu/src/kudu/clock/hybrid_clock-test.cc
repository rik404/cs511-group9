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

#include "kudu/clock/hybrid_clock.h"

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/test/mini_chronyd.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/webui_checker.h"
#include "kudu/util/atomic.h"
#include "kudu/util/cloud/instance_detector.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(inject_unsync_time_errors);
DECLARE_string(builtin_ntp_servers);
DECLARE_string(cloud_curl_dns_servers_for_testing);
DECLARE_string(time_source);
DECLARE_uint32(cloud_metadata_server_request_timeout_ms);

METRIC_DECLARE_entity(server);

using kudu::cloud::InstanceDetector;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace clock {

class ClockTest : public KuduTest {
 public:
  ClockTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "clock-test")) {
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

class MockHybridClockTest : public ClockTest {
};

clock::MockNtp* mock_ntp(const HybridClock& clock) {
  return down_cast<clock::MockNtp*>(clock.time_service());
}

TEST_F(MockHybridClockTest, TestMockedSystemClock) {
  google::FlagSaver saver;
  FLAGS_time_source = "mock";
  HybridClock clock(metric_entity_);
  ASSERT_OK(clock.Init());
  Timestamp timestamp;
  uint64_t max_error_usec;
  ASSERT_OK(clock.NowWithError(&timestamp, &max_error_usec));
  ASSERT_EQ(timestamp.ToUint64(), 0);
  ASSERT_EQ(max_error_usec, 0);
  // If we read the clock again we should see the logical component be incremented.
  ASSERT_OK(clock.NowWithError(&timestamp, &max_error_usec));
  ASSERT_EQ(timestamp.ToUint64(), 1);
  // Now set an arbitrary time and check that is the time returned by the clock.
  uint64_t time = 1234 * 1000;
  uint64_t error = 100 * 1000;
  mock_ntp(clock)->SetMockClockWallTimeForTests(time);
  mock_ntp(clock)->SetMockMaxClockErrorForTests(error);
  ASSERT_OK(clock.NowWithError(&timestamp, &max_error_usec));
  ASSERT_EQ(timestamp.ToUint64(),
            HybridClock::TimestampFromMicrosecondsAndLogicalValue(time, 0).ToUint64());
  ASSERT_EQ(max_error_usec, error);
  // Perform another read, we should observe the logical component increment, again.
  ASSERT_OK(clock.NowWithError(&timestamp, &max_error_usec));
  ASSERT_EQ(timestamp.ToUint64(),
            HybridClock::TimestampFromMicrosecondsAndLogicalValue(time, 1).ToUint64());
}

// Test that, if the rate at which the clock is read is greater than the maximum
// resolution of the logical counter (12 bits in our implementation), it properly
// "overflows" into the physical portion of the clock, and maintains all ordering
// guarantees even as the physical clock continues to increase.
//
// This is a regression test for KUDU-1345.
TEST_F(MockHybridClockTest, TestClockDealsWithWrapping) {
  google::FlagSaver saver;
  FLAGS_time_source = "mock";
  HybridClock clock(metric_entity_);
  ASSERT_OK(clock.Init());
  mock_ntp(clock)->SetMockClockWallTimeForTests(1000);

  Timestamp prev = clock.Now();

  // Update the clock from 10us in the future
  ASSERT_OK(clock.Update(HybridClock::TimestampFromMicroseconds(1010)));

  // Now read the clock value enough times so that the logical value wraps
  // over, and should increment the _physical_ portion of the clock.
  for (int i = 0; i < 10000; i++) {
    Timestamp now = clock.Now();
    ASSERT_GT(now.value(), prev.value());
    prev = now;
  }
  ASSERT_EQ(1012, HybridClock::GetPhysicalValueMicros(prev));

  // Advance the time microsecond by microsecond, and ensure the clock never
  // goes backwards.
  for (int time = 1001; time < 1020; time++) {
    mock_ntp(clock)->SetMockClockWallTimeForTests(time);
    Timestamp now = clock.Now();

    // Clock should run strictly forwards.
    ASSERT_GT(now.value(), prev.value());

    // Additionally, once the physical time surpasses the logical time, we should
    // be running on the physical clock. Otherwise, we should stick with the physical
    // time we had rolled forward to above.
    if (time > 1012) {
      ASSERT_EQ(time, HybridClock::GetPhysicalValueMicros(now));
    } else {
      ASSERT_EQ(1012, HybridClock::GetPhysicalValueMicros(now));
    }

    prev = now;
  }
}

class HybridClockTest : public ClockTest {
 public:
  HybridClockTest()
      : clock_(metric_entity_) {
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(clock_.Init());
  }

 protected:
  HybridClock clock_;
};

// Test that two subsequent time reads are monotonically increasing.
TEST_F(HybridClockTest, NowValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_.Now();
  const Timestamp now2 = clock_.Now();
  ASSERT_LT(now1.value(), now2.value());
}

// Tests the clock updates with the incoming value if it is higher.
TEST_F(HybridClockTest, UpdateLogicalValueIncreasesByAmount) {
  Timestamp now = clock_.Now();
  uint64_t now_micros = HybridClock::GetPhysicalValueMicros(now);

  // increase the logical value
  uint64_t logical = HybridClock::GetLogicalValue(now);
  logical += 10;

  // increase the physical value so that we're sure the clock will take this
  // one, 200 msecs should be more than enough.
  now_micros += 200000;

  auto now_increased = HybridClock::TimestampFromMicrosecondsAndLogicalValue(
      now_micros, logical);

  ASSERT_OK(clock_.Update(now_increased));

  Timestamp now2 = clock_.Now();
  ASSERT_EQ(logical + 1, HybridClock::GetLogicalValue(now2));
  ASSERT_EQ(HybridClock::GetPhysicalValueMicros(now) + 200000,
            HybridClock::GetPhysicalValueMicros(now2));
}

// Test that the incoming event is in the past, i.e. less than now - max_error
TEST_F(HybridClockTest, WaitUntilAfterCase1) {
  MonoTime no_deadline;
  MonoTime before = MonoTime::Now();

  Timestamp past_ts;
  uint64_t max_error;
  ASSERT_OK(clock_.NowWithError(&past_ts, &max_error));

  // make the event 3 * the max. possible error in the past
  Timestamp past_ts_changed = HybridClock::AddPhysicalTimeToTimestamp(
      past_ts,
      MonoDelta::FromMicroseconds(-3 * static_cast<int64_t>(max_error)));
  ASSERT_OK(clock_.WaitUntilAfter(past_ts_changed, no_deadline));

  MonoTime after = MonoTime::Now();
  MonoDelta delta = after - before;
  // The delta should be close to 0, but it takes some time for the hybrid
  // logical clock to decide that it doesn't need to wait.
  ASSERT_LT(delta.ToMicroseconds(), 25000);
}

// The normal case for transactions. Obtain a timestamp and then wait until
// we're sure that tx_latest < now_earliest.
TEST_F(HybridClockTest, WaitUntilAfterCase2) {
  const MonoTime before = MonoTime::Now();

  // we do no time adjustment, this event should fall right within the possible
  // error interval
  Timestamp past_ts;
  uint64_t past_max_error;
  ASSERT_OK(clock_.NowWithError(&past_ts, &past_max_error));
  // Make sure the error is at least a small number of microseconds, to ensure
  // that we always have to wait.
  past_max_error = std::max(past_max_error, static_cast<uint64_t>(2000));
  Timestamp wait_until = HybridClock::AddPhysicalTimeToTimestamp(
      past_ts,
      MonoDelta::FromMicroseconds(past_max_error));

  Timestamp current_ts;
  uint64_t current_max_error;
  ASSERT_OK(clock_.NowWithError(&current_ts, &current_max_error));

  // Check waiting with a deadline which already expired.
  {
    MonoTime deadline = before;
    Status s = clock_.WaitUntilAfter(wait_until, deadline);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Wait with a deadline well in the future. This should succeed.
  {
    MonoTime deadline = before + MonoDelta::FromSeconds(60);
    ASSERT_OK(clock_.WaitUntilAfter(wait_until, deadline));
  }

  MonoTime after = MonoTime::Now();
  MonoDelta delta = after - before;

  // In the common case current_max_error >= past_max_error and we should have waited
  // 2 * past_max_error, but if the clock's error is reset between the two reads we might
  // have waited less time, but always more than 'past_max_error'.
  if (current_max_error >= past_max_error) {
    ASSERT_GE(delta.ToMicroseconds(), 2 * past_max_error);
  } else {
    ASSERT_GE(delta.ToMicroseconds(), past_max_error);
  }
}

TEST_F(HybridClockTest, TestIsAfter) {
  Timestamp ts1 = clock_.Now();
  ASSERT_TRUE(clock_.IsAfter(ts1));

  // Update the clock in the future, make sure it still
  // handles "IsAfter" properly even when it's running in
  // "logical" mode.
  Timestamp now_increased = HybridClock::TimestampFromMicroseconds(
    HybridClock::GetPhysicalValueMicros(ts1) + 1 * 1000 * 1000);
  ASSERT_OK(clock_.Update(now_increased));
  Timestamp ts2 = clock_.Now();

  ASSERT_TRUE(clock_.IsAfter(ts1));
  ASSERT_TRUE(clock_.IsAfter(ts2));
}

// Thread which loops polling the clock and updating it slightly
// into the future.
void StresserThread(HybridClock* clock, AtomicBool* stop) {
  Random rng(GetRandomSeed32());
  Timestamp prev(0);
  while (!stop->Load()) {
    Timestamp t = clock->Now();
    CHECK_GT(t.value(), prev.value());
    prev = t;

    // Add a random bit of offset to the clock, and perform an update.
    Timestamp new_ts = HybridClock::AddPhysicalTimeToTimestamp(
        t, MonoDelta::FromMicroseconds(rng.Uniform(10000)));
    CHECK_OK(clock->Update(new_ts));
  }
}

// Regression test for KUDU-953: if threads are updating and polling the
// clock concurrently, the clock should still never run backwards.
TEST_F(HybridClockTest, TestClockDoesntGoBackwardsWithUpdates) {
  vector<thread> threads;
  AtomicBool stop(false);

  SCOPED_CLEANUP({
    stop.Store(true);
    for (auto& t : threads) {
      t.join();
    }
  });

  for (int i = 0; i < 4; i++) {
    threads.emplace_back([&]() { StresserThread(&clock_, &stop); });
  }

  SleepFor(MonoDelta::FromSeconds(1));
}

TEST_F(HybridClockTest, TestGetPhysicalComponentDifference) {
  Timestamp now1 = HybridClock::TimestampFromMicrosecondsAndLogicalValue(100, 100);
  SleepFor(MonoDelta::FromMilliseconds(1));
  Timestamp now2 = HybridClock::TimestampFromMicrosecondsAndLogicalValue(200, 0);
  MonoDelta delta = clock_.GetPhysicalComponentDifference(now2, now1);
  MonoDelta negative_delta = clock_.GetPhysicalComponentDifference(now1, now2);
  ASSERT_EQ(100, delta.ToMicroseconds());
  ASSERT_EQ(-100, negative_delta.ToMicroseconds());
}

TEST_F(HybridClockTest, TestRideOverNtpInterruption) {
  Timestamp timestamps[3];
  uint64_t max_error_usec[3];

  // Get the clock once, with a working NTP.
  ASSERT_OK(clock_.NowWithError(&timestamps[0], &max_error_usec[0]));

  // Try to read the clock again a second later, but with an error
  // injected. It should extrapolate from the first read.
  SleepFor(MonoDelta::FromSeconds(1));
  FLAGS_inject_unsync_time_errors = true;
  ASSERT_OK(clock_.NowWithError(&timestamps[1], &max_error_usec[1]));

  // The new clock reading should be a second or longer from the
  // first one, since SleepFor guarantees sleeping at least as long
  // as specified.
  MonoDelta phys_diff = clock_.GetPhysicalComponentDifference(
      timestamps[1], timestamps[0]);
  ASSERT_GE(phys_diff.ToSeconds(), 1);

  // The new clock reading should have higher error than the first.
  // The error should have increased based on the clock skew.
  int64_t error_diff = max_error_usec[1] - max_error_usec[0];
  ASSERT_NEAR(error_diff, clock_.time_service()->skew_ppm() * phys_diff.ToSeconds(),
              20);

  // Now restore the ability to read the system clock, and
  // read it again.
  FLAGS_inject_unsync_time_errors = false;
  ASSERT_OK(clock_.NowWithError(&timestamps[2], &max_error_usec[2]));

  ASSERT_LT(timestamps[0].ToUint64(), timestamps[1].ToUint64());
  ASSERT_LT(timestamps[1].ToUint64(), timestamps[2].ToUint64());
}

// This scenario emulates slow initialisation of the hybrid clock's underlying
// time source while Kudu servers are busy with bootstrapping (in this
// particular case, the time source is the built-in NTP client). During the
// bootstrapping, the embedded web servers are flooded with requests to the
// '/metrics' URLs. Kudu masters and tablet servers should not crash and be able
// to start and function as expected once the time source is up and running.
//
// In addition, this scenario verifies the presence of time-specific metrics in
// the data retrieved from the '/metrics' endpoint.
TEST_F(HybridClockTest, SlowClockInitialisation) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const vector<string> kExtraFlags = {
    // Inject delay to emulate slow initialization of the hybrid clock.
    "--hybrid_clock_inject_init_delay_ms=100",

    // Switching from the default "auto" to "enabled" for faster startup times:
    // this test scenario is sensitive to start up timing.
    "--wall_clock_jump_detection=enabled",
  };

  ExternalMiniClusterOptions opts;
  opts.num_masters = 3;
  opts.num_ntp_servers = 1;
  opts.extra_master_flags = kExtraFlags;
  opts.extra_tserver_flags = kExtraFlags;

  // The PeriodicWebUIChecker needs to know the addresses which master and
  // tserver embedded web servers are bound to. So, first start all the
  // processes, then stop them, and then instantiate the PeriodicWebUIChecker
  // passing information on the bound addresses which will be used upon next
  // start of masters/tservers. This way there will be requests coming to the
  // embedded web servers while Kudu servers are bootstrapping.
  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());
  cluster.Shutdown();

  auto ntp_servers = cluster.ntp_servers();
  ASSERT_EQ(1, ntp_servers.size());
  auto* ntp_server = ntp_servers.front();
  ASSERT_OK(ntp_server->Pause());
  Status ntp_server_resumer_status;
  thread ntp_server_resumer([&] {
    SleepFor(MonoDelta::FromMilliseconds(500));
    ntp_server_resumer_status = ntp_server->Resume();
  });
  SCOPED_CLEANUP({
    ntp_server_resumer.join();
  });

  // Start pounding the master and tserver's web server.
  PeriodicWebUIChecker checker(cluster, MonoDelta::FromMilliseconds(1),
                               "", { "/metrics" }, { "/metrics" });

  // Start bootstrapping masters and tablet servers.
  ASSERT_OK(cluster.Restart());
  SleepFor(MonoDelta::FromSeconds(1));
  NO_FATALS(cluster.AssertNoCrashes());
  cluster.Shutdown();
  ASSERT_OK(cluster.Restart());
  SleepFor(MonoDelta::FromSeconds(1));
  NO_FATALS(cluster.AssertNoCrashes());
  ASSERT_OK(ntp_server_resumer_status);

  // Make sure certain time-related metrics are present.
  vector<string> addresses;
  addresses.reserve(cluster.num_masters() + cluster.num_tablet_servers());
  for (auto idx = 0; idx < cluster.num_masters(); ++idx) {
    addresses.emplace_back(
        cluster.master(idx)->bound_http_hostport().ToString());
  }
  for (auto idx = 0; idx < cluster.num_tablet_servers(); ++idx) {
    addresses.emplace_back(
          cluster.tablet_server(idx)->bound_http_hostport().ToString());
  }

  EasyCurl c;
  for (const auto& addr : addresses) {
    faststring buf;
    ASSERT_OK(c.FetchURL(Substitute("http://$0/metrics", addr), &buf));
    const auto str = buf.ToString();
    ASSERT_STR_CONTAINS(str, "builtin_ntp_error");
    ASSERT_STR_CONTAINS(str, "builtin_ntp_local_clock_delta");
    ASSERT_STR_CONTAINS(str, "builtin_ntp_max_errors");
    ASSERT_STR_CONTAINS(str, "builtin_ntp_time");
    ASSERT_STR_CONTAINS(str, "hybrid_clock_error");
    ASSERT_STR_CONTAINS(str, "hybrid_clock_extrapolating");
    ASSERT_STR_CONTAINS(str, "hybrid_clock_extrapolation_intervals");
    ASSERT_STR_CONTAINS(str, "hybrid_clock_max_errors");
    ASSERT_STR_CONTAINS(str, "hybrid_clock_timestamp");
  }
}

// A simple scenario to verify that 'auto' is recognized as one of the possible
// time sources: the auto-selection works and the resulting hybrid clock
// is functional.
TEST_F(HybridClockTest, TimeSourceAutoSelection) {
  FLAGS_time_source = "auto";
  HybridClock clock(metric_entity_);
  const auto s = clock.Init();
  bool preconditions_satisfied = true;
  if (!s.ok()) {
    // In a testing environment where the auto-detected time source is 'system'
    // (the local machine clock synchronized by the kernel NTP discipline),
    // it doesn't make much sense running the test if corresponding system calls
    // return an error (e.g., EPERM) or the clock is not synchronized.
    // The code below is for a few cases like that: it's a set of cases seen
    // so far in the wild.
    if (s.IsRuntimeError()) {
      ASSERT_STR_CONTAINS(s.ToString(), "Operation not permitted");
      preconditions_satisfied = false;
    } else if (s.IsServiceUnavailable()) {
      // For the effective time source auto-selected, the pattern for the error
      // message below covers the 'builtin' and the 'system' cases
      // correspondingly. The only other possible case left is 'system_unsync':
      // if the 'system_unsync' time source is auto-selected, there shouldn't be
      // any clock synchronization error unless the --inject_unsync_time_errors
      // flag is set to 'true', which isn't the case in this test scenario.
      static const string kErrMsgPattern = "(wallclock is not synchronized|"
          "Error reading clock\\. Clock considered unsynchronized)";
      ASSERT_STR_MATCHES(s.ToString(), kErrMsgPattern);
      preconditions_satisfied = false;
    }
  }
  if (!preconditions_satisfied) {
    LOG(WARNING) << "preconditions are not satisfied, skipping the test";
    GTEST_SKIP();
  }

  ASSERT_OK(s);
  Timestamp timestamp[2];
  uint64_t max_error_usec[2];
  ASSERT_OK(clock.NowWithError(&timestamp[0], &max_error_usec[0]));
  ASSERT_OK(clock.NowWithError(&timestamp[1], &max_error_usec[1]));
  ASSERT_LE(timestamp[0].value(), timestamp[1].value());
}

// This test scenario verifies that if the environment doesn't provide a
// dedicated NTP server when Kudu is running with --time_source=auto, the
// auto-selected time source is 'builtin' with the set of reference servers for
// the built-in NTP client as configured per --builtin_ntp_servers, or
// 'system' ('system_unsync' for platforms not supporting get_ntptime() API)
// when --builtin_ntp_servers is empty or unparsable.
TEST_F(HybridClockTest, AutoTimeSourceNoDedicatedNtpServer) {
  // Set very short interval for the instance detection timeout.
  FLAGS_cloud_metadata_server_request_timeout_ms = 1;

  // Configure a bad DNS server to ensure a timeout,
  // even when run on fast cloud instances.
  FLAGS_cloud_curl_dns_servers_for_testing = "192.0.2.0";

  {
    // Check for the pre-condition: the auto-detection fails as expected due
    // to the super-short timeout setting for the cloud metadata requests and
    // phony DNS server to be used by libcurl.
    InstanceDetector detector;
    std::unique_ptr<cloud::InstanceMetadata> md;
    const auto s = detector.Detect(&md);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  // With --builtin_ntp_servers set to a valid sequence of NTP server addresses,
  // the auto-selected time source should result in 'builtin'.
  {
    FLAGS_builtin_ntp_servers = "127.0.0.1";
    HybridClock::TimeSource time_source;
    ASSERT_OK(HybridClock::SelectTimeSource("auto", &time_source));
    ASSERT_EQ(HybridClock::TimeSource::NTP_SYNC_BUILTIN, time_source)
        << HybridClock::TimeSourceToString(time_source);
  }

  // With an empty or invalid value for --builtin_ntp_servers the auto-selected
  // time source should be 'system' for the platforms which support it and
  // 'system_unsync' otherwise.
  for (const auto ntp_servers : { "", "," }) {
    FLAGS_builtin_ntp_servers = ntp_servers;
    HybridClock::TimeSource time_source;
    ASSERT_OK(HybridClock::SelectTimeSource("auto", &time_source));
#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
    ASSERT_EQ(HybridClock::TimeSource::NTP_SYNC_SYSTEM, time_source)
        << HybridClock::TimeSourceToString(time_source);
#else
    ASSERT_EQ(HybridClock::TimeSource::UNSYNC_SYSTEM, time_source)
        << HybridClock::TimeSourceToString(time_source);
#endif // #if defined(KUDU_HAS_SYSTEM_TIME_SOURCE) ... #else ...
  }
}

// Imitate the clock jumping well other the threshold.
TEST_F(HybridClockTest, ClockJumpDetection) {
  static constexpr uint64_t kThresholdSeconds = 100;
  FLAGS_time_source = "mock";
  HybridClock clock(metric_entity_, kThresholdSeconds * 1000 * 1000);

  ASSERT_OK(clock.Init());

  const auto time_setter = [&](MonoDelta delta) {
    uint64_t now = HybridClock::GetPhysicalValueMicros(clock.Now());
    uint64_t to_update = now + delta.ToMicroseconds();
    auto* hybrid_clock = down_cast<HybridClock*>(&clock);
    auto* mock_ntp = down_cast<clock::MockNtp*>(hybrid_clock->time_service());
    mock_ntp->SetMockClockWallTimeForTests(to_update);
  };

  // Move the 'now' timestamp of the mock clock to make sure the proper 'if'
  // branch is taken in HybridClock::NowWithErrorUnlocked().
  time_setter(MonoDelta::FromMicroseconds(1));
  {
    Timestamp ts;
    uint64_t max_error_usec;
    ASSERT_OK(clock.NowWithError(&ts, &max_error_usec));
    ASSERT_EQ(1, HybridClock::GetPhysicalValueMicros(ts));
  }

  // Get closer to the threshold. It should still work since the threshold
  // isn't crossed yet.
  time_setter(MonoDelta::FromSeconds(kThresholdSeconds));
  {
    Timestamp ts;
    uint64_t max_error_usec;
    ASSERT_OK(clock.NowWithError(&ts, &max_error_usec));
    ASSERT_EQ(kThresholdSeconds * 1000 * 1000 + 1,
              HybridClock::GetPhysicalValueMicros(ts));
  }

  // Now make the clock jumping well over the threshold: with previous jump
  // it should be about two times over the threshold.
  time_setter(MonoDelta::FromSeconds(2 * kThresholdSeconds));
  {
    Timestamp ts;
    uint64_t max_error_usec;
    const auto s = clock.NowWithError(&ts, &max_error_usec);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
    ASSERT_STR_MATCHES(s.ToString(),
        "wall \\(200000000 us\\) and monotonic \\([0-9]+ us\\) "
        "clock deltas diverged too much \\(threshold 100000000 us\\)");
  }
}

#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
TEST_F(HybridClockTest, TestNtpDiagnostics) {
  FLAGS_time_source = "system";
  HybridClock clock(metric_entity_);

  // Even if HybridClock's initialization failed, the point of this scenario
  // is to verify that diagnostics run appropriate tools. By design, the set
  // of tools run these purposes doesn't depend on the initialization status.
  WARN_NOT_OK(clock.Init(), "clock initialization failed");

  vector<string> log;
  clock.time_service()->DumpDiagnostics(&log);
  string s = JoinStrings(log, "\n");
  SCOPED_TRACE(s);
  // This is to verify that appropriate diagnostic tools are run or at least
  // attempted to run. The tools might not be available, and that's expected
  // pre-condition as well. The output either contains report from the tools or
  // error messages that corresponding binaries cannot be found. In either case
  // it proves the tools were attempted to run to collect NTP-related diagnostic
  // information.
  ASSERT_STR_CONTAINS(s, "ntpstat");
  ASSERT_STR_CONTAINS(s, "ntptime");
  ASSERT_STR_CONTAINS(s, "ntpq");
  ASSERT_STR_CONTAINS(s, "ntpdc");
  ASSERT_STR_CONTAINS(s, "chronyc");

  // Verify the output contains relevant metrics.
  ASSERT_STR_CONTAINS(s, "clock_ntp_status");
  ASSERT_STR_CONTAINS(s, "hybrid_clock_extrapolating");
  ASSERT_STR_CONTAINS(s, "hybrid_clock_error");
  ASSERT_STR_CONTAINS(s, "hybrid_clock_timestamp");
  ASSERT_STR_CONTAINS(s, "hybrid_clock_max_errors");
  ASSERT_STR_CONTAINS(s, "hybrid_clock_extrapolation_intervals");
}
#endif // #if defined(KUDU_HAS_SYSTEM_TIME_SOURCE) ...

// The boolean parameter is to specify whether the wall clock protection is
// enabled or not ('true' -- enabled, 'false' -- disabled).
class HybridClockJumpProtectionTest : public ClockTest,
                            public ::testing::WithParamInterface<bool> {
};
INSTANTIATE_TEST_SUITE_P(Perf, HybridClockJumpProtectionTest, ::testing::Bool());

// This is a scenario for basic peformance assessment of the
// HybridClock::NowWithError() method with clock jump check enabled/disabled.
TEST_P(HybridClockJumpProtectionTest, BasicPerf) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const int kNumIterations = 1000000;
  FLAGS_time_source = "system_unsync";
  const auto enable_protection = GetParam();
  HybridClock clock(metric_entity_, enable_protection ? 10000000UL : 0UL);
  ASSERT_OK(clock.Init());

  Stopwatch sw;
  sw.start();
  Status res_status;
  for (auto i = 0; i < kNumIterations; ++i) {
    Timestamp timestamp;
    uint64_t max_error_usec;
    auto s = clock.NowWithError(&timestamp, &max_error_usec);
    if (PREDICT_FALSE(!s.ok())) {
      res_status = s;
      break;
    }
  }
  sw.stop();

  ASSERT_OK(res_status);

  LOG(INFO) << Substitute("$0 iterations in $1 ms",
                          kNumIterations, sw.elapsed().wall_millis());
}

}  // namespace clock
}  // namespace kudu
