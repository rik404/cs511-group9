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
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/ref_counted.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace transactions {

typedef std::pair<std::string, TxnParticipantEntryPB> ParticipantIdAndPB;

// Representations of entries in the transaction status table. Currently there
// are two entry types:
// - Transaction Entries: these indicate the existence of a transaction, and
//   encapsulate encapsulate metadata that pertain to the entire transactions
//   (e.g. owner, commit status, commit timestamp).
// - Participant Entries: these indicate the existence of a tablet that is
//   participating in a transaction.
//
// There is a 1:N relationship between transaction entries and participant
// entries. Represents the metadata persisted with a participant entry in the
// transaction status table.
struct PersistentParticipantEntry {
  TxnParticipantEntryPB pb;
};
class ParticipantEntry : public RefCountedThreadSafe<ParticipantEntry> {
 public:
  typedef PersistentParticipantEntry cow_state;

  ParticipantEntry() = default;
  const CowObject<PersistentParticipantEntry>& metadata() const { return metadata_; }
  CowObject<PersistentParticipantEntry>* mutable_metadata() { return &metadata_; }

 private:
  friend class RefCountedThreadSafe<ParticipantEntry>;
  ~ParticipantEntry() = default;

  // Mutable state for this participant with concurrent access controlled via
  // copy-on-write locking.
  CowObject<PersistentParticipantEntry> metadata_;
};

// Represents the metadata persisted with a status entry in the transaction
// status table.
struct PersistentTransactionEntry {
  TxnStatusEntryPB pb;
};
class TransactionEntry : public RefCountedThreadSafe<TransactionEntry> {
 public:
  typedef PersistentTransactionEntry cow_state;

  TransactionEntry(int64_t txn_id, std::string user);

  const CowObject<PersistentTransactionEntry>& metadata() const { return metadata_; }
  CowObject<PersistentTransactionEntry>* mutable_metadata() { return &metadata_; }

  // Adds a participant with the given tablet ID, or returns the one if it
  // already exists.
  scoped_refptr<ParticipantEntry> GetOrCreateParticipant(const std::string& tablet_id);

  // Returns the list of tablet IDs associated with this transaction.
  std::vector<std::string> GetParticipantIds() const;

  const std::string& user() const {
    return user_;
  }

  // Return the last recorded heartbeat timestamp for the transaction.
  MonoTime last_heartbeat_time() const {
    return last_heartbeat_time_.load();
  }
  // Set the timestamp of last seen keep-alive heartbeat for the transaction.
  void SetLastHeartbeatTime(const MonoTime& hbtime) {
    last_heartbeat_time_.store(hbtime);
  }

  // An accessor to the transaction's state. Concurrent access is controlled
  // via the locking provided by the underlying copy-on-write metadata_ object.
  TxnStatePB state() const;

  // An accessor to the transaction's commit timestamp. Concurrent access is
  // controlled via the locking provided by the underlying copy-on-write
  // metadata_ object. Should only be used if the transaction is expected to
  // have a commit timestamp already set.
  int64_t commit_timestamp() const;

 private:
  friend class RefCountedThreadSafe<TransactionEntry>;
  ~TransactionEntry() = default;

  const int64_t txn_id_;

  // While this is redundant with the field in the protobuf, it's convenient to
  // cache this so we don't have to lock this entry to get the user.
  const std::string user_;

  // Protects participants_. If adding a new participant, the entry should also
  // be locked in read mode and the transaction should be open.
  mutable simple_spinlock lock_;
  std::unordered_map<std::string, scoped_refptr<ParticipantEntry>> participants_;

  // Mutable state for the transaction status record with concurrent access
  // controlled via copy-on-write locking.
  CowObject<PersistentTransactionEntry> metadata_;

  // Time of the last keep-alive heartbeat received for the transaction
  // identified by txn_id_. Using std::atomic wrapper since the field can be
  // accessed concurrently by multiple threads:
  //   * the field can be updated by concurrent KeepAlive requests from
  //     different clients sending writes in the context of the same transaction
  //   * the field is read by the stale transaction tracker in TxnStatusManager
  std::atomic<MonoTime> last_heartbeat_time_;
};

typedef MetadataLock<TransactionEntry> TransactionEntryLock;
typedef MetadataLock<ParticipantEntry> ParticipantEntryLock;

} // namespace transactions
} // namespace kudu
