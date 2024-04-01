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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <sparsehash/dense_hash_set>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/txn_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduStatusCallback;

namespace internal {

class ErrorCollector;
class RemoteTablet;
class WriteRpc;
struct InFlightOp;

// A Batcher is the class responsible for collecting row operations, routing them to the
// correct tablet server, and possibly batching them together for better efficiency.
//
// It is a reference-counted class: the client session creating the batch holds one
// reference, and all of the in-flight operations hold others. This allows the client
// session to be destructed while ops are still in-flight, without the async callbacks
// attempting to access a destructed Batcher.
class Batcher : public RefCountedThreadSafe<Batcher> {
 public:
  // Create a new batcher associated with the given session.
  //
  // Any errors which come back from operations performed by this batcher
  // are posted to the provided ErrorCollector.
  //
  // Takes a reference on error_collector. Takes a weak_ptr to session -- this
  // is to break circular dependencies (a session keeps a reference to its
  // current batcher) and make it possible to call notify a session
  // (if it's around) from a batcher which does its job using other threads.
  Batcher(KuduClient* client,
          scoped_refptr<ErrorCollector> error_collector,
          client::sp::weak_ptr<KuduSession> session,
          kudu::client::KuduSession::ExternalConsistencyMode consistency_mode,
          const kudu::TxnId& txn_id);

  // Abort the current batch. Any writes that were buffered and not yet sent are
  // discarded. Those that were sent may still be delivered.  If there is a pending Flush
  // callback, it will be called immediately with an error status.
  void Abort();

  // Set the timeout for this batcher.
  //
  // The timeout is currently set on all of the RPCs, but in the future will be relative
  // to when the Flush call is made (eg even if the lookup of the TS takes a long time, it
  // may time out before even sending an op). TODO: implement that
  void SetTimeout(const MonoDelta& timeout);

  // Add a new operation to the batch. Requires that the batch has not yet been flushed.
  //
  // NOTE: If this returns not-OK, does not take ownership of 'write_op'.
  Status Add(KuduWriteOperation* write_op) WARN_UNUSED_RESULT;

  // Return true if any operations are still pending. An operation is no longer considered
  // pending once it has either errored or succeeded.  Operations are considering pending
  // as soon as they are added, even if Flush has not been called.
  bool HasPendingOperations() const;

  // Return the number of buffered operations. These are only those operations which are
  // "corked" (i.e not yet flushed). Once Flush has been called, this returns 0.
  int CountBufferedOperations() const;

  // Flush any buffered operations. The callback will be called once there are no
  // more pending operations from this Batcher. If all of the operations succeeded,
  // then the callback will receive Status::OK. Otherwise, it will receive IOError,
  // and the caller must inspect the ErrorCollector to retrieve more detailed
  // information on which operations failed.
  void FlushAsync(KuduStatusCallback* cb);

  // Returns the consistency mode set on the batcher by the session when it was initially
  // created.
  kudu::client::KuduSession::ExternalConsistencyMode external_consistency_mode() const {
    return consistency_mode_;
  }

  // Get time of the first operation in the batch.  If no operations are in
  // there yet, the returned MonoTime object is not initialized
  // (i.e. MonoTime::Initialized() returns false).
  const MonoTime& first_op_time() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return first_op_time_;
  }

  // Return the total size (number of bytes) of all pending write operations
  // accumulated by the batcher.
  int64_t buffer_bytes_used() const {
    return buffer_bytes_used_;
  }

  // Return the identifier of a multi-row transaction (if any) that all the
  // accumulated write operations are part of.
  const TxnId& txn_id() const {
    return txn_id_;
  }

  // Compute in-buffer size for the given write operation.
  static int64_t GetOperationSizeInBuffer(KuduWriteOperation* write_op) {
    return write_op->SizeInBuffer();
  }

 private:
  friend class RefCountedThreadSafe<Batcher>;
  friend class WriteRpc;

  ~Batcher();

  // Add an op to the in-flight set and increment the ref-count.
  void AddInFlightOp(InFlightOp* op);

  void RemoveInFlightOp(InFlightOp* op);

  // Return true if the batch has been aborted, and any in-flight ops should stop
  // processing wherever they are.
  bool IsAbortedUnlocked() const;

  // Remove an op from the in-flight op list, and delete the op itself.
  // The operation is reported to the ErrorReporter as having failed with the
  // given status.
  void MarkInFlightOpFailed(InFlightOp* op, const Status& s);
  void MarkInFlightOpFailedUnlocked(InFlightOp* op, const Status& s);

  void CheckForFinishedFlush();
  void FlushBuffersIfReady();
  void FlushBuffer(RemoteTablet* tablet, const std::vector<InFlightOp*>& ops);

  // Cleans up an RPC response, scooping out any errors and passing them up
  // to the batcher.
  void ProcessWriteResponse(const WriteRpc& rpc, const Status& s);

  // Async Callbacks.
  void TabletLookupFinished(InFlightOp* op, const Status& s);

  // Compute a new deadline based on timeout_. If no timeout_ has been set,
  // uses a hard-coded default and issues periodic warnings.
  MonoTime ComputeDeadlineUnlocked() const;

  // See note about lock ordering in batcher.cc
  mutable simple_spinlock lock_;

  enum State {
    kGatheringOps,
    kFlushing,
    kFlushed,
    kAborted
  };
  State state_;

  KuduClient* const client_;
  client::sp::weak_ptr<KuduSession> weak_session_;

  // The consistency mode set in the session.
  const kudu::client::KuduSession::ExternalConsistencyMode consistency_mode_;

  // The identifier of a transaction that is associated with operations
  // processed by this batcher. All operations accumulated by one batcher either
  // related to the same multi-row transaction or all of the accumulated
  // operations are not a part of any multi-row transaction. In the latter case,
  // txn_id_.IsValid() would return 'false'.
  const TxnId txn_id_;

  // Errors are reported into this error collector.
  scoped_refptr<ErrorCollector> error_collector_;

  // The time when the very first operation was added into the batcher.
  MonoTime first_op_time_;

  // If state is kFlushing, this member will be set to the user-provided
  // callback. Once there are no more in-flight operations, the callback
  // will be called exactly once (and the state changed to kFlushed).
  KuduStatusCallback* flush_callback_;

  // All buffered or in-flight ops.
  google::dense_hash_set<InFlightOp*> ops_;
  // Each tablet's buffered ops.
  typedef std::unordered_map<RemoteTablet*, std::vector<InFlightOp*> > OpsMap;
  OpsMap per_tablet_ops_;

  // When each operation is added to the batcher, it is assigned a sequence number
  // which preserves the user's intended order. Preserving order is critical when
  // a batch contains multiple operations against the same row key. This member
  // assigns the sequence numbers.
  // Protected by lock_.
  int next_op_sequence_number_;

  // Amount of time to wait for a given op, from start to finish.
  //
  // Set by SetTimeout().
  MonoDelta timeout_;

  // After flushing, the absolute deadline for all in-flight ops.
  MonoTime deadline_;

  // Number of outstanding lookups across all in-flight ops.
  std::atomic<int32_t> outstanding_lookups_;

  // The number of bytes used in the buffer for pending operations.
  std::atomic<int64_t> buffer_bytes_used_;

  // Set to true if there was at least one error from this Batcher.
  std::atomic<bool> had_errors_;

  Arena arena_;

  DISALLOW_COPY_AND_ASSIGN(Batcher);
};

} // namespace internal
} // namespace client
} // namespace kudu
