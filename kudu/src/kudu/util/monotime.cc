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

#include "kudu/util/monotime.h"

#include <sys/time.h>

#include <cstdint>
#include <ctime>
#include <limits>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/thread_restrictions.h"
#if defined(__APPLE__)
#include "kudu/gutil/walltime.h"
#endif

namespace kudu {

#define MAX_MONOTONIC_SECONDS \
  (((1ULL<<63) - 1ULL) /(int64_t)MonoTime::kNanosecondsPerSecond)


///
/// MonoDelta
///

const int64_t MonoDelta::kUninitialized = kint64min;

MonoDelta MonoDelta::FromSeconds(double seconds) {
  return MonoDelta(seconds * MonoTime::kNanosecondsPerSecond);
}

MonoDelta MonoDelta::FromMilliseconds(int64_t ms) {
  return MonoDelta(ms * MonoTime::kNanosecondsPerMillisecond);
}

MonoDelta MonoDelta::FromMicroseconds(int64_t us) {
  return MonoDelta(us * MonoTime::kNanosecondsPerMicrosecond);
}

MonoDelta MonoDelta::FromNanoseconds(int64_t ns) {
  return MonoDelta(ns);
}

MonoDelta::MonoDelta()
    : nano_delta_(kUninitialized) {
}

bool MonoDelta::Initialized() const {
  return nano_delta_ != kUninitialized;
}

bool MonoDelta::LessThan(const MonoDelta& rhs) const {
  return *this < rhs;
}

bool MonoDelta::MoreThan(const MonoDelta& rhs) const {
  return *this > rhs;
}

bool MonoDelta::Equals(const MonoDelta& rhs) const {
  return *this == rhs;
}

std::string MonoDelta::ToString() const {
  return StringPrintf("%.3fs", ToSeconds());
}

MonoDelta::MonoDelta(int64_t delta)
    : nano_delta_(delta) {
}

double MonoDelta::ToSeconds() const {
  DCHECK(Initialized());
  return static_cast<double>(nano_delta_) / MonoTime::kNanosecondsPerSecond;
}

int64_t MonoDelta::ToNanoseconds() const {
  DCHECK(Initialized());
  return nano_delta_;
}

int64_t MonoDelta::ToMicroseconds() const {
  DCHECK(Initialized());
  return nano_delta_ / MonoTime::kNanosecondsPerMicrosecond;
}

int64_t MonoDelta::ToMilliseconds() const {
  DCHECK(Initialized());
  return nano_delta_ / MonoTime::kNanosecondsPerMillisecond;
}

void MonoDelta::ToTimeVal(struct timeval* tv) const {
  DCHECK(Initialized());
  tv->tv_sec = nano_delta_ / MonoTime::kNanosecondsPerSecond;
  tv->tv_usec = (nano_delta_ - (tv->tv_sec * MonoTime::kNanosecondsPerSecond))
      / MonoTime::kNanosecondsPerMicrosecond;

  // tv_usec must be between 0 and 999999.
  // There is little use for negative timevals so wrap it in PREDICT_FALSE.
  if (PREDICT_FALSE(tv->tv_usec < 0)) {
    --(tv->tv_sec);
    tv->tv_usec += 1000000;
  }

  // Catch positive corner case where we "round down" and could potentially set a timeout of 0.
  // Make it 1 usec.
  if (PREDICT_FALSE(tv->tv_usec == 0 && tv->tv_sec == 0 && nano_delta_ > 0)) {
    tv->tv_usec = 1;
  }

  // Catch negative corner case where we "round down" and could potentially set a timeout of 0.
  // Make it -1 usec (but normalized, so tv_usec is not negative).
  if (PREDICT_FALSE(tv->tv_usec == 0 && tv->tv_sec == 0 && nano_delta_ < 0)) {
    tv->tv_sec = -1;
    tv->tv_usec = 999999;
  }
}

void MonoDelta::ToTimeSpec(struct timespec* ts) const {
  DCHECK(Initialized());
  NanosToTimeSpec(nano_delta_, ts);
}

void MonoDelta::NanosToTimeSpec(int64_t nanos, struct timespec* ts) {
  ts->tv_sec = nanos / MonoTime::kNanosecondsPerSecond;
  ts->tv_nsec = nanos - (ts->tv_sec * MonoTime::kNanosecondsPerSecond);

  // tv_nsec must be between 0 and 999999999.
  // There is little use for negative timespecs so wrap it in PREDICT_FALSE.
  if (PREDICT_FALSE(ts->tv_nsec < 0)) {
    --(ts->tv_sec);
    ts->tv_nsec += MonoTime::kNanosecondsPerSecond;
  }
}

MonoDelta& MonoDelta::operator+=(const MonoDelta& delta) {
  nano_delta_ += delta.nano_delta_;
  return *this;
}

MonoDelta& MonoDelta::operator-=(const MonoDelta& delta) {
  nano_delta_ -= delta.nano_delta_;
  return *this;
}

///
/// MonoTime
///

MonoTime MonoTime::Now() {
#if defined(__APPLE__)
  return MonoTime(walltime_internal::GetMonoTimeNanos());
# else
  struct timespec ts;
  PCHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
  return MonoTime(ts);
#endif // defined(__APPLE__)
}

MonoTime MonoTime::Max() {
  return MonoTime(std::numeric_limits<int64_t>::max());
}

MonoTime MonoTime::Min() {
  return MonoTime(1);
}

const MonoTime& MonoTime::Earliest(const MonoTime& a, const MonoTime& b) {
  if (b.nanos_ < a.nanos_) {
    return b;
  }
  return a;
}

MonoTime::MonoTime() KUDU_MONOTIME_NOEXCEPT
    : nanos_(0) {
}

bool MonoTime::Initialized() const {
  return nanos_ != 0;
}

MonoDelta MonoTime::GetDeltaSince(const MonoTime& rhs) const {
  return rhs - *this;
}

void MonoTime::AddDelta(const MonoDelta& delta) {
  this->operator+=(delta);
}

bool MonoTime::ComesBefore(const MonoTime& rhs) const {
  return *this < rhs;
}

std::string MonoTime::ToString() const {
  return StringPrintf("%.3fs", ToSeconds());
}

void MonoTime::ToTimeSpec(struct timespec* ts) const {
  DCHECK(Initialized());
  MonoDelta::NanosToTimeSpec(nanos_, ts);
}

bool MonoTime::Equals(const MonoTime& other) const {
  return *this == other;
}

MonoTime& MonoTime::operator+=(const MonoDelta& delta) {
  DCHECK(Initialized());
  DCHECK(delta.Initialized());
  nanos_ += delta.nano_delta_;
  return *this;
}

MonoTime& MonoTime::operator-=(const MonoDelta& delta) {
  DCHECK(Initialized());
  DCHECK(delta.Initialized());
  nanos_ -= delta.nano_delta_;
  return *this;
}

MonoTime::MonoTime(const struct timespec& ts) KUDU_MONOTIME_NOEXCEPT {
  // Monotonic time resets when the machine reboots.  The 64-bit limitation
  // means that we can't represent times larger than 292 years, which should be
  // adequate.
  CHECK_LT(ts.tv_sec, MAX_MONOTONIC_SECONDS);
  nanos_ = ts.tv_sec;
  nanos_ *= MonoTime::kNanosecondsPerSecond;
  nanos_ += ts.tv_nsec;
}

MonoTime::MonoTime(int64_t nanos) KUDU_MONOTIME_NOEXCEPT
    : nanos_(nanos) {
}

double MonoTime::ToSeconds() const {
  return static_cast<double>(nanos_) / MonoTime::kNanosecondsPerSecond;
}

void SleepFor(const MonoDelta& delta) {
  ThreadRestrictions::AssertWaitAllowed();
  base::SleepForNanoseconds(delta.ToNanoseconds());
}

bool operator==(const MonoDelta& lhs, const MonoDelta& rhs) {
  DCHECK(lhs.Initialized());
  DCHECK(rhs.Initialized());
  return lhs.nano_delta_ == rhs.nano_delta_;
}

bool operator!=(const MonoDelta& lhs, const MonoDelta& rhs) {
  return !(lhs == rhs);
}

bool operator<(const MonoDelta& lhs, const MonoDelta& rhs) {
  DCHECK(lhs.Initialized());
  DCHECK(rhs.Initialized());
  return lhs.nano_delta_ < rhs.nano_delta_;
}

bool operator<=(const MonoDelta& lhs, const MonoDelta& rhs) {
  return !(lhs > rhs);
}

bool operator>(const MonoDelta& lhs, const MonoDelta& rhs) {
  return rhs < lhs;
}

bool operator>=(const MonoDelta& lhs, const MonoDelta& rhs) {
  return !(lhs < rhs);
}

MonoDelta operator-(const MonoDelta& lhs, const MonoDelta& rhs) {
  return MonoDelta(lhs.nano_delta_ - rhs.nano_delta_);
}

MonoDelta operator+(const MonoDelta& lhs, const MonoDelta& rhs) {
  return MonoDelta(lhs.nano_delta_ + rhs.nano_delta_);
}

bool operator==(const MonoTime& lhs, const MonoTime& rhs) {
  return lhs.nanos_ == rhs.nanos_;
}

bool operator!=(const MonoTime& lhs, const MonoTime& rhs) {
  return !(lhs == rhs);
}

bool operator<(const MonoTime& lhs, const MonoTime& rhs) {
  DCHECK(lhs.Initialized());
  DCHECK(rhs.Initialized());
  return lhs.nanos_ < rhs.nanos_;
}

bool operator<=(const MonoTime& lhs, const MonoTime& rhs) {
  return !(lhs > rhs);
}

bool operator>(const MonoTime& lhs, const MonoTime& rhs) {
  return rhs < lhs;
}

bool operator>=(const MonoTime& lhs, const MonoTime& rhs) {
  return !(lhs < rhs);
}

MonoTime operator+(const MonoTime& t, const MonoDelta& delta) {
  DCHECK(t.Initialized());
  DCHECK(delta.Initialized());
  return MonoTime(t.nanos_ + delta.nano_delta_);
}

MonoTime operator-(const MonoTime& t, const MonoDelta& delta) {
  DCHECK(t.Initialized());
  DCHECK(delta.Initialized());
  return MonoTime(t.nanos_ - delta.nano_delta_);
}

MonoDelta operator-(const MonoTime& t_end, const MonoTime& t_beg) {
  DCHECK(t_beg.Initialized());
  DCHECK(t_end.Initialized());
  return MonoDelta(t_end.nanos_ - t_beg.nanos_);
}

std::ostream& operator<<(std::ostream& os, const kudu::MonoTime& time) {
  struct timespec ts;
  time.ToTimeSpec(&ts);
  os << ts.tv_nsec << "ns";
  return os;
}

} // namespace kudu
