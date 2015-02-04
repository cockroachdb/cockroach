// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#include <algorithm>
#include <limits>
#include <google/protobuf/repeated_field.h>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "api.pb.h"
#include "data.pb.h"
#include "internal.pb.h"
#include "db.h"
#include "encoding.h"

extern "C" {

struct DBBatch {
  rocksdb::WriteBatch rep;
};

struct DBEngine {
  rocksdb::DB* rep;
};

struct DBIterator {
  rocksdb::Iterator* rep;
};

struct DBSnapshot {
  rocksdb::DB* db;
  const rocksdb::Snapshot* rep;
};

}  // extern "C"

namespace {

// NOTE: these constants must be kept in sync with the values
// in storage/engine/keys.go.
const rocksdb::Slice kKeyLocalRangeIDPrefix("\x00\x00\x00i", 4);
const rocksdb::Slice kKeyLocalRangeKeyPrefix("\x00\x00\x00k", 4);
const rocksdb::Slice kKeyLocalResponseCacheSuffix("res-");
const rocksdb::Slice kKeyLocalTransactionSuffix("txn-");

const DBStatus kSuccess = { NULL, 0 };

std::string ToString(DBSlice s) {
  return std::string(s.data, s.len);
}

rocksdb::Slice ToSlice(DBSlice s) {
  return rocksdb::Slice(s.data, s.len);
}

DBSlice ToDBSlice(const rocksdb::Slice& s) {
  DBSlice result;
  result.data = const_cast<char*>(s.data());
  result.len = s.size();
  return result;
}

DBString ToDBString(const rocksdb::Slice& s) {
  DBString result;
  result.len = s.size();
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s.data(), s.size());
  return result;
}

DBStatus ToDBStatus(const rocksdb::Status& status) {
  if (status.ok()) {
    return kSuccess;
  }
  return ToDBString(status.ToString());
}

rocksdb::ReadOptions MakeReadOptions(DBSnapshot* snap) {
  rocksdb::ReadOptions options;
  if (snap != NULL) {
    options.snapshot = snap->rep;
  }
  return options;
}

// GetResponseHeader extracts the response header for each type of
// response in the ReadWriteCmdResponse union.
const proto::ResponseHeader* GetResponseHeader(const proto::ReadWriteCmdResponse& rwResp) {
  if (rwResp.has_put()) {
    return &rwResp.put().header();
  } else if (rwResp.has_conditional_put()) {
    return &rwResp.conditional_put().header();
  } else if (rwResp.has_increment()) {
    return &rwResp.increment().header();
  } else if (rwResp.has_delete_()) {
    return &rwResp.delete_().header();
  } else if (rwResp.has_delete_range()) {
    return &rwResp.delete_range().header();
  } else if (rwResp.has_end_transaction()) {
    return &rwResp.end_transaction().header();
  } else if (rwResp.has_reap_queue()) {
    return &rwResp.reap_queue().header();
  } else if (rwResp.has_enqueue_update()) {
    return &rwResp.enqueue_update().header();
  } else if (rwResp.has_enqueue_message()) {
    return &rwResp.enqueue_message().header();
  } else if (rwResp.has_internal_heartbeat_txn()) {
    return &rwResp.internal_heartbeat_txn().header();
  } else if (rwResp.has_internal_gc()) {
    return &rwResp.internal_gc().header();
  } else if (rwResp.has_internal_push_txn()) {
    return &rwResp.internal_push_txn().header();
  } else if (rwResp.has_internal_resolve_intent()) {
    return &rwResp.internal_resolve_intent().header();
  } else if (rwResp.has_internal_merge()) {
    return &rwResp.internal_merge().header();
  } else if (rwResp.has_internal_truncate_log()) {
    return &rwResp.internal_truncate_log().header();
  }
  return NULL;
}

// DBCompactionFilter implements our garbage collection policy for
// key/value pairs which can be considered in isolation. This
// includes:
//
// - Response cache: response cache entries are garbage collected
//   based on their age vs. the current wall time. They're kept for a
//   configurable window.
//
// - Transactions: transaction table entries are garbage collected
//   according to the commit time of the transaction vs. the oldest
//   remaining write intent across the entire system. The oldest write
//   intent is maintained as a low-water mark, updated after polling
//   all ranges in the map.
class DBCompactionFilter : public rocksdb::CompactionFilter {
 public:
  DBCompactionFilter(int64_t min_txn_ts,
                     int64_t min_rcache_ts)
      : min_txn_ts_(min_txn_ts),
        min_rcache_ts_(min_rcache_ts) {
  }

  // IsKeyOfType determines whether key, when binary-decoded, matches
  // the format: <prefix>[enc-value]\x00<suffix>[remainder].
  bool IsKeyOfType(const rocksdb::Slice& key, const rocksdb::Slice& prefix, const rocksdb::Slice& suffix) const {
    std::string decStr;
    if (!DecodeBinary(key, &decStr, NULL)) {
      return false;
    }
    rocksdb::Slice decKey(decStr);
    if (!decKey.starts_with(prefix)) {
      return false;
    }
    decKey.remove_prefix(prefix.size());

    // Remove bytes up to including the first null byte.
    int i = 0;
    for (; i < decKey.size(); i++) {
      if (decKey[i] == 0x0) {
        break;
      }
    }
    if (i == decKey.size()) {
      return false;
    }
    decKey.remove_prefix(i+1);
    return decKey.starts_with(suffix);
  }

  bool IsResponseCacheEntry(const rocksdb::Slice& key) const {
    return IsKeyOfType(key, kKeyLocalRangeIDPrefix, kKeyLocalResponseCacheSuffix);
  }

  bool IsTransactionRecord(const rocksdb::Slice& key) const {
    return IsKeyOfType(key, kKeyLocalRangeKeyPrefix, kKeyLocalTransactionSuffix);
  }

  virtual bool Filter(int level,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& existing_value,
                      std::string* new_value,
                      bool* value_changed) const {
    *value_changed = false;

    // Only filter response cache entries and transaction rows.
    bool is_rcache = IsResponseCacheEntry(key);
    bool is_txn = IsTransactionRecord(key);
    if (!is_rcache && !is_txn) {
      return false;
    }
    // Parse MVCC metadata for inlined value.
    proto::MVCCMetadata meta;
    if (!meta.ParseFromArray(existing_value.data(), existing_value.size())) {
      // *error_msg = (char*)"failed to parse mvcc metadata entry";
      return false;
    }
    if (!meta.has_value()) {
      // *error_msg = (char*)"not an inlined mvcc value";
      return false;
    }
    // Response cache rows are GC'd if their timestamp is older than the
    // response cache GC timeout.
    if (is_rcache) {
      proto::ReadWriteCmdResponse rwResp;
      if (!rwResp.ParseFromArray(meta.value().bytes().data(), meta.value().bytes().size())) {
        // *error_msg = (char*)"failed to parse response cache entry";
        return false;
      }
      const proto::ResponseHeader* header = GetResponseHeader(rwResp);
      if (header == NULL) {
        // *error_msg = (char*)"failed to parse response cache header";
        return false;
      }
      if (header->timestamp().wall_time() <= min_rcache_ts_) {
        return true;
      }
    } else if (is_txn) {
      // Transaction rows are GC'd if their timestamp is older than
      // the system-wide minimum write intent timestamp. This
      // system-wide minimum write intent is periodically computed via
      // map-reduce over all ranges and gossiped.
      proto::Transaction txn;
      if (!txn.ParseFromArray(meta.value().bytes().data(), meta.value().bytes().size())) {
        // *error_msg = (char*)"failed to parse transaction entry";
        return false;
      }
      if (txn.timestamp().wall_time() <= min_txn_ts_) {
        return true;
      }
    }
    return false;
  }

  virtual const char* Name() const {
    return "cockroach_compaction_filter";
  }

 private:
  const int64_t min_txn_ts_;
  const int64_t min_rcache_ts_;
};

class DBCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  DBCompactionFilterFactory() {}

  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    google::protobuf::MutexLock l(&mu_); // Protect access to gc timeouts.
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new DBCompactionFilter(min_txn_ts_, min_rcache_ts_));
  }

  virtual const char* Name() const override {
    return "cockroach_compaction_filter_factory";
  }

  void SetGCTimeouts(int64_t min_txn_ts, int64_t min_rcache_ts) {
    google::protobuf::MutexLock l(&mu_);
    min_txn_ts_ = min_txn_ts;
    min_rcache_ts_ = min_rcache_ts;
  }

 private:
  google::protobuf::Mutex mu_; // Protects values below.
  int64_t min_txn_ts_;
  int64_t min_rcache_ts_;
};

bool WillOverflow(int64_t a, int64_t b) {
  // Morally MinInt64 < a+b < MaxInt64, but without overflows.
  // First make sure that a <= b. If not, swap them.
  if (a > b) {
    std::swap(a, b);
  }
  // Now b is the larger of the numbers, and we compare sizes
  // in a way that can never over- or underflow.
  if (b > 0) {
    return a > (std::numeric_limits<int64_t>::max() - b);
  }
  return (std::numeric_limits<int64_t>::min() - b) > a;
}

// Method used to sort InternalTimeSeriesSamples.
bool TimeSeriesSampleOrdering(const proto::InternalTimeSeriesSample* a,
        const proto::InternalTimeSeriesSample* b) {
    return a->offset() < b->offset();
}

// IsTimeSeriesData returns true if the given protobuffer Value contains a
// TimeSeriesData message.
bool IsTimeSeriesData(const proto::Value *val) {
    return val->has_tag()
        && val->tag() == proto::InternalValueType_Name(proto::_CR_TS);
}

long GetIntMax(const proto::InternalTimeSeriesSample *sample) {
    if (sample->has_int_max()) return sample->int_max();
    if (sample->has_int_sum()) return sample->int_sum();
    return std::numeric_limits<long>::min();
}

long GetIntMin(const proto::InternalTimeSeriesSample *sample) {
    if (sample->has_int_min()) return sample->int_min();
    if (sample->has_int_sum()) return sample->int_sum();
    return std::numeric_limits<long>::max();
}

float GetFloatMax(const proto::InternalTimeSeriesSample *sample) {
    if (sample->has_float_max()) return sample->float_max();
    if (sample->has_float_sum()) return sample->float_sum();
    return std::numeric_limits<float>::min();
}

float GetFloatMin(const proto::InternalTimeSeriesSample *sample) {
    if (sample->has_float_min()) return sample->float_min();
    if (sample->has_float_sum()) return sample->float_sum();
    return std::numeric_limits<float>::max();
}

// AccumulateTimeSeriesSamples accumulates the individual values of two
// InternalTimeSeriesSamples which have a matching timestamp. The dest parameter
// is modified to contain the accumulated values.
void AccumulateTimeSeriesSamples(proto::InternalTimeSeriesSample* dest,
        const proto::InternalTimeSeriesSample &src) {
    // Accumulate integer values
    int total_int_count = dest->int_count() + src.int_count();
    if (total_int_count > 1) {
        // Keep explicit max and min values.
        dest->set_int_max(std::max(GetIntMax(dest), GetIntMax(&src)));
        dest->set_int_min(std::min(GetIntMin(dest), GetIntMin(&src)));
    }
    if (total_int_count > 0) {
        dest->set_int_sum(dest->int_sum() + src.int_sum());
    }
    dest->set_int_count(total_int_count);

    int total_float_count = dest->float_count() + src.float_count();
    if (total_float_count > 1) {
        // Keep explicit max and min values.
        dest->set_float_max(std::max(GetFloatMax(dest), GetFloatMax(&src)));
        dest->set_float_min(std::min(GetFloatMin(dest), GetFloatMin(&src)));
    }
    if (total_float_count > 0) {
        dest->set_float_sum(dest->float_sum() + src.float_sum());
    }
    dest->set_float_count(total_float_count);
}

// MergeTimeSeriesValues attempts to merge two Values which contain
// InternalTimeSeriesData messages. The messages cannot be merged if they have
// different start timestamps or sample durations. Returns true if the merge is
// successful.
bool MergeTimeSeriesValues(proto::Value *left, const proto::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    // Attempt to parse TimeSeriesData from both Values.
    proto::InternalTimeSeriesData left_ts;
    proto::InternalTimeSeriesData right_ts;
    if (!left_ts.ParseFromString(left->bytes())) {
        rocksdb::Warn(logger,
                "left InternalTimeSeriesData could not be parsed from bytes.");
        return false;
    }
    if (!right_ts.ParseFromString(right.bytes())) {
        rocksdb::Warn(logger,
                "right InternalTimeSeriesData could not be parsed from bytes.");
        return false;
    }

    // Ensure that both InternalTimeSeriesData have the same timestamp and
    // sample_duration.
    if (left_ts.start_timestamp_nanos() != right_ts.start_timestamp_nanos()) {
        rocksdb::Warn(logger,
                "TimeSeries merge failed due to mismatched start timestamps");
        return false;
    }
    if (left_ts.sample_duration_nanos() !=
            right_ts.sample_duration_nanos()) {
        rocksdb::Warn(logger,
                "TimeSeries merge failed due to mismatched sample durations.");
        return false;
    }

    // If only a partial merge, do not sort and combine - instead, just quickly
    // merge the two values together. Values will be processed later after a
    // full merge.
    if (!full_merge) {
        left_ts.MergeFrom(right_ts);
        left_ts.SerializeToString(left->mutable_bytes());
        return true;
    }

    // Initialize new_ts and its primitive data fields. Values from the left and
    // right collections will be merged into the new collection.
    proto::InternalTimeSeriesData new_ts;
    new_ts.set_start_timestamp_nanos(left_ts.start_timestamp_nanos());
    new_ts.set_sample_duration_nanos(left_ts.sample_duration_nanos());

    // Sort values in right_ts. Assume values in left_ts have been sorted.
    std::sort(right_ts.mutable_samples()->pointer_begin(),
            right_ts.mutable_samples()->pointer_end(),
            TimeSeriesSampleOrdering);

    // Merge sample values of left and right into new_ts.
    auto left_front = left_ts.samples().begin(),
         left_end = left_ts.samples().end(),
         right_front = right_ts.samples().begin(),
         right_end = right_ts.samples().end();

    // Loop until samples from both sides have been exhausted.
    while(left_front != left_end || right_front != right_end) {
        // Select the lowest offset from either side.
        long next_offset;
        if (left_front == left_end) {
            next_offset = right_front->offset();
        } else if (right_front == right_end) {
            next_offset = left_front->offset();
        } else if (left_front->offset()<=right_front->offset()) {
            next_offset = left_front->offset();
        } else {
            next_offset = right_front->offset();
        }

        // Create an empty sample in the output collection with the selected
        // offset.  Accumulate data from all samples at the front of either left
        // or right which match the selected timestamp. This behavior is needed
        // because each side may individually have duplicated offsets.
        proto::InternalTimeSeriesSample* ns = new_ts.add_samples();
        ns->set_offset(next_offset);
        while (left_front != left_end && left_front->offset() == ns->offset()) {
            AccumulateTimeSeriesSamples(ns, *left_front);
            left_front++;
        }
        while (right_front != right_end && right_front->offset() == ns->offset()) {
            AccumulateTimeSeriesSamples(ns, *right_front);
            right_front++;
        }
    }

    // Serialize the new TimeSeriesData into the left value's byte field.
    new_ts.SerializeToString(left->mutable_bytes());
    return true;
}

bool MergeValues(proto::Value *left, const proto::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    if (left->has_bytes()) {
        if (!right.has_bytes()) {
            rocksdb::Warn(logger,
                    "inconsistent value types for merge (left = bytes, right = ?)");
            return false;
        }
        if (IsTimeSeriesData(left)) {
            // The right operand must also be a time series.
            if (!IsTimeSeriesData(&right)) {
                rocksdb::Warn(logger,
                        "inconsistent value types for merge (left = TimeSeriesData, right = bytes)");
                return false;
            }
            return MergeTimeSeriesValues(left, right, full_merge, logger);
        } else if (IsTimeSeriesData(&right)) {
            // The right operand was a time series, but the left was not.
            rocksdb::Warn(logger,
                    "inconsistent value types for merge (left = bytes, right = TimeSeriesData");
            return false;
        } else {
            *left->mutable_bytes() += right.bytes();
        }
        return true;
    } else if (left->has_integer()) {
        if (!right.has_integer()) {
            rocksdb::Warn(logger,
                    "inconsistent value types for merge (left = integer, right = ?)");
            return false;
        }
        if (WillOverflow(left->integer(), right.integer())) {
            rocksdb::Warn(logger, "merge would result in integer overflow.");
            return false;
        }
        left->set_integer(left->integer() + right.integer());
        return true;
    } else {
        *left = right;
        return true;
    }
}


// MergeResult serializes the result MVCCMetadata value into a byte slice.
DBStatus MergeResult(proto::MVCCMetadata* meta, DBString* result) {
  // TODO(pmattis): Should recompute checksum here. Need a crc32
  // implementation and need to verify the checksumming is identical
  // to what is being done in Go. Zlib's crc32 should be sufficient.
  meta->mutable_value()->clear_checksum();
  result->len = meta->ByteSize();
  result->data = static_cast<char*>(malloc(result->len));
  if (!meta->SerializeToArray(result->data, result->len)) {
    return ToDBString("serialization error");
  }
  return kSuccess;
}

class DBMergeOperator : public rocksdb::MergeOperator {
  virtual const char* Name() const {
    return "cockroach_merge_operator";
  }

  virtual bool FullMerge(
      const rocksdb::Slice& key,
      const rocksdb::Slice* existing_value,
      const std::deque<std::string>& operand_list,
      std::string* new_value,
      rocksdb::Logger* logger) const {
    // TODO(pmattis): Taken from the old merger code, below are some
    // details about how errors returned by the merge operator are
    // handled. Need to test various error scenarios and decide on
    // desired behavior. Clear the key and it's gone. Corrupt it
    // properly and RocksDB might refuse to work with it at all until
    // you clear it manually, which may also not be what we want. The
    // problem with merges is that RocksDB won't really carry them out
    // while we have a chance to talk back to clients.
    //
    // If we indicate failure (*success = false), then the call to the
    // merger via rocksdb_merge will not return an error, but simply
    // remove or truncate the offending key (at least when the settings
    // specify that missing keys should be created; otherwise a
    // corruption error will be returned, but likely only after the next
    // read of the key). In effect, there is no propagation of error
    // information to the client.

    proto::MVCCMetadata meta;
    if (existing_value != NULL) {
      if (!meta.ParseFromArray(existing_value->data(), existing_value->size())) {
        // Corrupted existing value.
        rocksdb::Warn(logger, "corrupted existing value");
        return false;
      }
    }

    for (int i = 0; i < operand_list.size(); i++) {
      if (!MergeOne(&meta, operand_list[i], true, logger)) {
        return false;
      }
    }

    if (!meta.SerializeToString(new_value)) {
      rocksdb::Warn(logger, "serialization error");
      return false;
    }
    return true;
  }

  virtual bool PartialMergeMulti(
      const rocksdb::Slice& key,
      const std::deque<rocksdb::Slice>& operand_list,
      std::string* new_value,
      rocksdb::Logger* logger) const {
    proto::MVCCMetadata meta;

    for (int i = 0; i < operand_list.size(); i++) {
      if (!MergeOne(&meta, operand_list[i], false, logger)) {
        return false;
      }
    }

    if (!meta.SerializeToString(new_value)) {
      rocksdb::Warn(logger, "serialization error");
      return false;
    }
    return true;
  }

 private:
  bool MergeOne(proto::MVCCMetadata* meta,
                const rocksdb::Slice& operand,
                bool full_merge,
                rocksdb::Logger* logger) const {
    proto::MVCCMetadata operand_meta;
    if (!operand_meta.ParseFromArray(operand.data(), operand.size())) {
      rocksdb::Warn(logger, "corrupted operand value");
      return false;
    }
    return MergeValues(meta->mutable_value(), operand_meta.value(),
            full_merge, logger);
  }
};

class DBLogger : public rocksdb::Logger {
 public:
  DBLogger(DBLoggerFunc f)
      : func_(f) {
  }
  virtual void Logv(const char* format, va_list ap) {
    // TODO(pmattis): forward to Go logging. Also need to benchmark
    // calling Go exported methods from C++ to determine if this is
    // too slow.
    vfprintf(stderr, format, ap);
    fprintf(stderr, "\n");
  }

 private:
  const DBLoggerFunc func_;
};

}  // namespace

DBStatus DBOpen(DBEngine **db, DBSlice dir, DBOptions db_opts) {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(db_opts.cache_size);

  rocksdb::Options options;
  options.allow_os_buffer = db_opts.allow_os_buffer;
  options.compaction_filter_factory.reset(new DBCompactionFilterFactory());
  options.create_if_missing = true;
  options.info_log.reset(new DBLogger(db_opts.logger));
  options.merge_operator.reset(new DBMergeOperator);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  rocksdb::DB *db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, ToString(dir), &db_ptr);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *db = new DBEngine;
  (*db)->rep = db_ptr;
  return kSuccess;
}

DBStatus DBDestroy(DBSlice dir) {
  rocksdb::Options options;
  return ToDBStatus(rocksdb::DestroyDB(ToString(dir), options));
}

void DBClose(DBEngine* db) {
  delete db->rep;
  delete db;
}

DBStatus DBFlush(DBEngine* db) {
  rocksdb::FlushOptions options;
  options.wait = true;
  return ToDBStatus(db->rep->Flush(options));
}

void DBSetGCTimeouts(DBEngine * db, int64_t min_txn_ts, int64_t min_rcache_ts) {
  DBCompactionFilterFactory *db_cff =
      (DBCompactionFilterFactory*)db->rep->GetOptions().compaction_filter_factory.get();
  db_cff->SetGCTimeouts(min_txn_ts, min_rcache_ts);
}

DBStatus DBCompactRange(DBEngine* db, DBSlice* start, DBSlice* end) {
  rocksdb::Slice s;
  rocksdb::Slice e;
  rocksdb::Slice* sPtr = NULL;
  rocksdb::Slice* ePtr = NULL;
  if (start != NULL) {
    sPtr = &s;
    s = ToSlice(*start);
  }
  if (end != NULL) {
    ePtr = &e;
    e = ToSlice(*end);
  }
  return ToDBStatus(db->rep->CompactRange(sPtr, ePtr));
}

uint64_t DBApproximateSize(DBEngine* db, DBSlice start, DBSlice end) {
  const rocksdb::Range r(ToSlice(start), ToSlice(end));
  uint64_t result;
  db->rep->GetApproximateSizes(&r, 1, &result);
  return result;
}

DBStatus DBPut(DBEngine* db, DBSlice key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(db->rep->Put(options, ToSlice(key), ToSlice(value)));
}

DBStatus DBMerge(DBEngine* db, DBSlice key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(db->rep->Merge(options, ToSlice(key), ToSlice(value)));
}

DBStatus DBGet(DBEngine* db, DBSnapshot* snap, DBSlice key, DBString* value) {
  std::string tmp;
  rocksdb::Status s = db->rep->Get(MakeReadOptions(snap), ToSlice(key), &tmp);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // This mirrors the logic in rocksdb_get(). It doesn't seem like
      // a good idea, but some code in engine_test.go depends on it.
      value->data = NULL;
      value->len = 0;
      return kSuccess;
    }
    return ToDBStatus(s);
  }
  *value = ToDBString(tmp);
  return kSuccess;
}

DBStatus DBDelete(DBEngine* db, DBSlice key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(db->rep->Delete(options, ToSlice(key)));
}

DBStatus DBWrite(DBEngine* db, DBBatch *batch) {
  rocksdb::WriteOptions options;
  return ToDBStatus(db->rep->Write(options, &batch->rep));
}

DBSnapshot* DBNewSnapshot(DBEngine* db)  {
  DBSnapshot *snap = new DBSnapshot;
  snap->db = db->rep;
  snap->rep = db->rep->GetSnapshot();
  return snap;
}

void DBSnapshotRelease(DBSnapshot* snap) {
  snap->db->ReleaseSnapshot(snap->rep);
  delete snap;
}

DBIterator* DBNewIter(DBEngine* db, DBSnapshot* snap) {
  DBIterator* iter = new DBIterator;
  iter->rep = db->rep->NewIterator(MakeReadOptions(snap));
  return iter;
}

void DBIterDestroy(DBIterator* iter) {
  delete iter->rep;
  delete iter;
}

void DBIterSeek(DBIterator* iter, DBSlice key) {
  iter->rep->Seek(ToSlice(key));
}

void DBIterSeekToFirst(DBIterator* iter) {
  iter->rep->SeekToFirst();
}

void DBIterSeekToLast(DBIterator* iter) {
  iter->rep->SeekToLast();
}

int DBIterValid(DBIterator* iter) {
  return iter->rep->Valid();
}

void DBIterNext(DBIterator* iter) {
  iter->rep->Next();
}

DBSlice DBIterKey(DBIterator* iter) {
  return ToDBSlice(iter->rep->key());
}

DBSlice DBIterValue(DBIterator* iter) {
  return ToDBSlice(iter->rep->value());
}

DBStatus DBIterError(DBIterator* iter) {
  return ToDBStatus(iter->rep->status());
}

DBBatch* DBNewBatch() {
  return new DBBatch;
}

void DBBatchDestroy(DBBatch* batch) {
  delete batch;
}

void DBBatchPut(DBBatch* batch, DBSlice key, DBSlice value) {
  batch->rep.Put(ToSlice(key), ToSlice(value));
}

void DBBatchMerge(DBBatch* batch, DBSlice key, DBSlice value) {
  batch->rep.Merge(ToSlice(key), ToSlice(value));
}

void DBBatchDelete(DBBatch* batch, DBSlice key) {
  batch->rep.Delete(ToSlice(key));
}

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  new_value->len = 0;

  proto::MVCCMetadata meta;
  if (!meta.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  proto::MVCCMetadata update_meta;
  if (!update_meta.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(meta.mutable_value(), update_meta.value(), true, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}
