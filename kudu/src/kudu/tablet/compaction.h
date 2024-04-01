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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/rowblock.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class Schema;

namespace fs {
class FsErrorManager;
struct IOContext;
}  // namespace fs

namespace tablet {

class DiskRowSet;
class MemRowSet;
class Mutation;
class MvccSnapshot;
class RollingDiskRowSetWriter;
struct CompactionInputRow;

// Options related to tablet history garbage collection.
class HistoryGcOpts {
 public:
  static HistoryGcOpts Enabled(Timestamp ahm) {
    return HistoryGcOpts(true, ahm);
  }

  static HistoryGcOpts Disabled() {
    return HistoryGcOpts(false, Timestamp(0));
  }

  // Returns true if Timestamp 't' is considered "ancient history" and is
  // eligible for garbage collection. If GC is disabled, will return false for
  // any Timestamp with a value >= 0 (that is, all valid Timestamps).
  bool IsAncientHistory(Timestamp t) const {
    return t < ancient_history_mark_;
  }

  // Returns true if history GC is enabled.
  bool gc_enabled() const {
    return gc_enabled_;
  }

  // Returns the ancient history mark.
  Timestamp ancient_history_mark() const {
    return ancient_history_mark_;
  }

 private:
  HistoryGcOpts(bool gc_enabled, Timestamp ahm)
      : gc_enabled_(gc_enabled),
        ancient_history_mark_(ahm) {
  }

  // Whether historical records prior to the ancient history mark should be
  // garbage-collected (deleted).
  const bool gc_enabled_;

  // A timestamp prior to which no history will be preserved.
  // Ignored if 'enabled' != GC_ENABLED.
  const Timestamp ancient_history_mark_;
};

// Interface for an input feeding into a compaction or flush.
class CompactionOrFlushInput {
 public:
  // Create an input which reads from the given rowset, yielding base rows
  // prior to the given snapshot.
  //
  // NOTE: For efficiency, this doesn't currently filter the mutations to only
  // include those committed in the given snapshot. It does, however, filter out
  // rows that weren't inserted prior to this snapshot. Users of this input still
  // need to call snap.IsApplied() on each mutation.
  //
  // TODO: can we make the above less messy?
  static Status Create(const DiskRowSet& rowset,
                       const Schema* projection,
                       const MvccSnapshot& snap,
                       const fs::IOContext* io_context,
                       std::unique_ptr<CompactionOrFlushInput>* out);

  // Create an input which reads from the given memrowset, yielding base rows and updates
  // prior to the given snapshot.
  static CompactionOrFlushInput* Create(const MemRowSet& memrowset,
                                        const Schema* projection,
                                        const MvccSnapshot& snap);

  // Create a collection of merge states containing a state per input. The inputs are merged
  // in key-order according to the given schema. All inputs must have matching schemas.
  static CompactionOrFlushInput* Merge(
      const std::vector<std::shared_ptr<CompactionOrFlushInput>>& inputs,
      const Schema* schema);

  virtual ~CompactionOrFlushInput() = default;

  virtual Status Init() = 0;
  virtual Status PrepareBlock(std::vector<CompactionInputRow>* block) = 0;

  // Returns the arena for this compaction input corresponding to the last
  // prepared block. This must be called *after* PrepareBlock() as if this
  // is a MergeCompactionInput only then will the right arena be selected.
  virtual Arena* PreparedBlockArena() = 0;

  virtual Status FinishBlock() = 0;

  virtual bool HasMoreBlocks() = 0;
  virtual const Schema& schema() const = 0;

  // Return an estimate on the maximum amount of memory used by the object
  // during its lifecycle while initializing, reading and processing data, etc.
  virtual size_t memory_footprint() const = 0;
};

// The set of rowsets which are taking part in a given compaction or flush operation.
class RowSetsInCompactionOrFlush {
 public:
  void AddRowSet(const std::shared_ptr<RowSet>& rowset,
                 std::unique_lock<std::mutex> lock) {
    CHECK(lock.owns_lock());

    locks_.push_back(std::move(lock));
    rowsets_.push_back(rowset);
  }

  // Create the appropriate input for this compaction/flush -- either a merge
  // of all the inputs, or the single input if there was only one.
  //
  // 'schema' is the schema for the output of the compaction/flush, and must
  // remain valid for the lifetime of the returned CompactionOrFlushInput.
  Status CreateCompactionOrFlushInput(const MvccSnapshot& snap,
                                      const Schema* schema,
                                      const fs::IOContext* io_context,
                                      std::shared_ptr<CompactionOrFlushInput>* out) const;

  // Dump a log message indicating the chosen rowsets.
  void DumpToLog() const;

  const RowSetVector& rowsets() const { return rowsets_; }

  size_t num_rowsets() const {
    return rowsets_.size();
  }

 private:
  RowSetVector rowsets_;
  std::vector<std::unique_lock<std::mutex>> locks_;
};

// One row yielded by CompactionOrFlushInput::PrepareBlock.
// Looks like this (assuming n UNDO records and m REDO records):
// UNDO_n <- ... <- UNDO_1 <- UNDO_head <- row -> REDO_head -> REDO_1 -> ... -> REDO_m
struct CompactionInputRow {
  // The compaction input base row.
  RowBlockRow row;
  // The current redo head for this row, may be null if the base row has no mutations.
  Mutation* redo_head;
  // The current undo head for this row, may be null if all undos were garbage collected.
  Mutation* undo_head;

  // When the same row is found in multiple rowsets because of ghost rows, this
  // points to one that is older in terms of row history.
  CompactionInputRow* previous_ghost;

  CompactionInputRow() :
      redo_head(nullptr),
      undo_head(nullptr),
      previous_ghost(nullptr) {}
};

// Function shared by flushes and compactions. Removes UNDO Mutations
// considered "ancient" from the given CompactionInputRow, modifying the undo
// mutation list in-place.
// 'is_garbage_collected': Set to true if the row was marked as deleted prior
// to the ancient history mark, with no reinsertions after that. In such a
// case, all traces of the row should be removed from disk by the caller.
//
// This is supposed to be called after ApplyMutationsAndGenerateUndos() where REDOS
// are transformed in UNDOs. There can be at most one REDO in 'redo_head', a DELETE.
void RemoveAncientUndos(const HistoryGcOpts& history_gc_opts,
                        Mutation** undo_head,
                        const Mutation* redo_head,
                        bool* is_garbage_collected);

// Function shared by flushes, compactions and major delta compactions. Applies all the REDO
// mutations from 'src_row' to the 'dst_row', and generates the related UNDO mutations. Some
// handling depends on the nature of the operation being performed:
//  - Flush: Applies all the REDOs to all the columns.
//  - Compaction: Applies all the REDOs to all the columns.
//  - Major delta compaction: Applies only the REDOs that have corresponding columns in the schema
//                            belonging to 'dst_row'. Those that don't belong to that schema are
//                            ignored.
Status ApplyMutationsAndGenerateUndos(const MvccSnapshot& snap,
                                      const CompactionInputRow& src_row,
                                      Mutation** new_undo_head,
                                      Mutation** new_redo_head,
                                      Arena* arena,
                                      RowBlockRow* dst_row,
                                      const HistoryGcOpts& history_gc_opts);

// Iterate through this compaction input, flushing all rows to the given RollingDiskRowSetWriter.
// The 'snap' argument should match the MvccSnapshot used to create the compaction input.
//
// After return of this function, this CompactionOrFlushInput object is "used up" and will
// no longer be useful.
Status FlushCompactionInput(const std::string& tablet_id,
                            const scoped_refptr<fs::FsErrorManager>& error_manager,
                            CompactionOrFlushInput* input,
                            const MvccSnapshot& snap,
                            const HistoryGcOpts& history_gc_opts,
                            RollingDiskRowSetWriter* out);

// Iterate through this compaction input, finding any mutations which came
// between snap_to_exclude and snap_to_include (ie those ops that were not yet
// committed in 'snap_to_exclude' but _are_ committed in 'snap_to_include').
// For each such mutation, propagate it into the compaction's output rowsets.
//
// The output rowsets passed in must be non-overlapping and in ascending key order:
// typically they are the resulting rowsets from a RollingDiskRowSetWriter.
//
// After return of this function, this CompactionOrFlushInput object is "used up" and will
// yield no further rows.
Status ReupdateMissedDeltas(const fs::IOContext* io_context,
                            CompactionOrFlushInput* input,
                            const HistoryGcOpts& history_gc_opts,
                            const MvccSnapshot& snap_to_exclude,
                            const MvccSnapshot& snap_to_include,
                            const RowSetVector& output_rowsets);

// Dump the given compaction input to 'lines' or LOG(INFO) if it is NULL.
// This consumes no more rows from the compaction input than specified by the 'rows_left' parameter.
// If 'rows_left' is nullptr, there is no limit on the number of rows to dump.
// If the content of 'rows_left' is equal to or less than 0, no rows will be dumped.
Status DebugDumpCompactionInput(CompactionOrFlushInput* input, int64_t* rows_left,
                                std::vector<std::string>* lines);

// Helper methods to print a row with full history.
std::string RowToString(const RowBlockRow& row,
                        const Mutation* redo_head,
                        const Mutation* undo_head);
std::string CompactionInputRowToString(const CompactionInputRow& input_row);

} // namespace tablet
} // namespace kudu
