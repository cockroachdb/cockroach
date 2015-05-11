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
#include "rocksdb/utilities/write_batch_with_index.h"
#include "cockroach/proto/api.pb.h"
#include "cockroach/proto/data.pb.h"
#include "cockroach/proto/internal.pb.h"
#include "db.h"
#include "encoding.h"

extern "C" {
#include "_cgo_export.h"
}  // extern "C"

extern "C" {

struct DBBatch {
  int updates;
  rocksdb::WriteBatchWithIndex rep;

  DBBatch()
      : updates(0) {
  }
};

struct DBEngine {
  rocksdb::DB* rep;
  rocksdb::Env* memenv;
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

// NOTE: these constants must be kept in sync with the values in
// storage/engine/keys.go. Both kKeyLocalRangeIDPrefix and
// kKeyLocalRangeKeyPrefix are the mvcc-encoded prefixes.
const int kKeyLocalRangePrefixSize = 4;
const rocksdb::Slice kKeyLocalRangeIDPrefix("\x00\xff\x00\xff\x00\xffi", 7);
const rocksdb::Slice kKeyLocalRangeKeyPrefix("\x00\xff\x00\xff\x00\xffk", 7);
const rocksdb::Slice kKeyLocalResponseCacheSuffix("res-", 4);
const rocksdb::Slice kKeyLocalTransactionSuffix("\x00\x01txn-", 6);

const DBStatus kSuccess = { NULL, 0 };

std::string ToString(DBSlice s) {
  return std::string(s.data, s.len);
}

rocksdb::Slice ToSlice(DBSlice s) {
  return rocksdb::Slice(s.data, s.len);
}

rocksdb::Slice ToSlice(DBString s) {
  return rocksdb::Slice(s.data, s.len);
}

DBSlice ToDBSlice(const rocksdb::Slice& s) {
  DBSlice result;
  result.data = const_cast<char*>(s.data());
  result.len = s.size();
  return result;
}

DBSlice ToDBSlice(const DBString& s) {
  DBSlice result;
  result.data = s.data;
  result.len = s.len;
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
const cockroach::proto::ResponseHeader* GetResponseHeader(const cockroach::proto::ReadWriteCmdResponse& rwResp) {
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

  // For debugging:
  // static std::string BinaryToHex(const rocksdb::Slice& b) {
  //   const char kHexChars[] = "0123456789abcdef";
  //   std::string h(b.size() * 2 + (b.size() - 1), ' ');
  //   const uint8_t* p = (const uint8_t*)b.data();
  //   for (int i = 0; i < b.size(); ++i) {
  //     const int c = p[i];
  //     h[3 * i] = kHexChars[c >> 4];
  //     h[3 * i + 1] = kHexChars[c & 0xf];
  //   }
  //   return h;
  // }

  bool IsResponseCacheEntry(rocksdb::Slice key) const {
    // The response cache key format is:
    //   <prefix><varint64-range-id><suffix>[remainder].
    if (!key.starts_with(kKeyLocalRangeIDPrefix)) {
      return false;
    }

    std::string decStr;
    if (!DecodeBytes(&key, &decStr)) {
      return false;
    }
    rocksdb::Slice decKey(decStr);
    decKey.remove_prefix(kKeyLocalRangePrefixSize);

    uint64_t dummy;
    if (!DecodeUvarint64(&decKey, &dummy)) {
      return false;
    }

    return decKey.starts_with(kKeyLocalResponseCacheSuffix);
  }

  bool IsTransactionRecord(rocksdb::Slice key) const {
    // The transaction key format is:
    //   <prefix>[key]<suffix>[remainder].
    if (!key.starts_with(kKeyLocalRangeKeyPrefix)) {
      return false;
    }

    std::string decStr;
    if (!DecodeBytes(&key, &decStr)) {
      return false;
    }
    rocksdb::Slice decKey(decStr);
    decKey.remove_prefix(kKeyLocalRangePrefixSize);

    // Search for "suffix" within "decKey".
    const rocksdb::Slice suffix(kKeyLocalTransactionSuffix);
    const char *result = std::search(
        decKey.data(), decKey.data() + decKey.size(),
        suffix.data(), suffix.data() + suffix.size());
    const int xpos = result - decKey.data();
    return xpos + suffix.size() <= decKey.size();
  }

  virtual bool Filter(int level,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& existing_value,
                      std::string* new_value,
                      bool* value_changed) const {
    *value_changed = false;

    // Only filter response cache entries and transaction rows.
    bool is_rcache = IsResponseCacheEntry(key);
    bool is_txn = !is_rcache && IsTransactionRecord(key);
    if (!is_rcache && !is_txn) {
      return false;
    }
    // Parse MVCC metadata for inlined value.
    cockroach::proto::MVCCMetadata meta;
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
      cockroach::proto::ReadWriteCmdResponse rwResp;
      if (!rwResp.ParseFromArray(meta.value().bytes().data(), meta.value().bytes().size())) {
        // *error_msg = (char*)"failed to parse response cache entry";
        return false;
      }
      const cockroach::proto::ResponseHeader* header = GetResponseHeader(rwResp);
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
      cockroach::proto::Transaction txn;
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

// IsMVCCStatsData returns true if the given protobuffer Value contains an
// MVCCStats message.
bool IsMVCCStatsData(const cockroach::proto::Value *val) {
    return val->has_tag()
        && val->tag() == cockroach::proto::InternalValueType_Name(cockroach::proto::_CR_STATS);
}

// MergeMVCCStatsValues merges two Values which contain MVCCStats values by
// accumulating each of the int64 fields. Returns true if the merge is
// successful.
//
// Note that this method must be kept in sync with the fields available in
// data.proto.
bool MergeMVCCStatsValues(cockroach::proto::Value *left, const cockroach::proto::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    // Attempt to parse MVCCStats from both Values.
    cockroach::proto::MVCCStats left_ms;
    cockroach::proto::MVCCStats right_ms;
    if (!left_ms.ParseFromString(left->bytes())) {
        rocksdb::Warn(logger,
                "left MVCCStats could not be parsed from bytes.");
        return false;
    }
    if (!right_ms.ParseFromString(right.bytes())) {
        rocksdb::Warn(logger,
                "right MVCCStats could not be parsed from bytes.");
        return false;
    }
    // Static check which enforces the note in the comment above that
    // this code must be updated to reflect any additions to the
    // MVCCStats struct. This is meant to prevent any added fields
    // being silently ignored on merge.
    if (sizeof(cockroach::proto::MVCCStats) != 128) {
        fprintf(stderr, "sizeof MVCCStats struct %lu != 128\n", sizeof(cockroach::proto::MVCCStats));
        rocksdb::Warn(logger,
                "MVCCStats custom merge iterator has not been updated to match proto definition.");
        return false;
    }

    // Accumulate values regardless of whether or not this is a full merge.
#define set(x) left_ms.set_##x(left_ms.x() + right_ms.x())

    set(live_bytes);
    set(key_bytes);
    set(val_bytes);
    set(intent_bytes);
    set(live_count);
    set(key_count);
    set(val_count);
    set(intent_count);
    set(intent_age);
    set(gc_bytes_age);
    set(sys_bytes);
    set(sys_count);
    set(last_update_nanos);

    left_ms.SerializeToString(left->mutable_bytes());

    return true;
}

// Method used to sort InternalTimeSeriesSamples.
bool TimeSeriesSampleOrdering(const cockroach::proto::InternalTimeSeriesSample* a,
        const cockroach::proto::InternalTimeSeriesSample* b) {
    return a->offset() < b->offset();
}

// IsTimeSeriesData returns true if the given protobuffer Value contains a
// TimeSeriesData message.
bool IsTimeSeriesData(const cockroach::proto::Value *val) {
    return val->has_tag()
        && val->tag() == cockroach::proto::InternalValueType_Name(cockroach::proto::_CR_TS);
}

double GetMax(const cockroach::proto::InternalTimeSeriesSample *sample) {
    if (sample->has_max()) return sample->max();
    if (sample->has_sum()) return sample->sum();
    return std::numeric_limits<double>::min();
}

double GetMin(const cockroach::proto::InternalTimeSeriesSample *sample) {
    if (sample->has_min()) return sample->min();
    if (sample->has_sum()) return sample->sum();
    return std::numeric_limits<double>::max();
}

// AccumulateTimeSeriesSamples accumulates the individual values of two
// InternalTimeSeriesSamples which have a matching timestamp. The dest parameter
// is modified to contain the accumulated values.
void AccumulateTimeSeriesSamples(cockroach::proto::InternalTimeSeriesSample* dest,
        const cockroach::proto::InternalTimeSeriesSample &src) {
    // Accumulate integer values
    int total_count = dest->count() + src.count();
    if (total_count > 1) {
        // Keep explicit max and min values.
        dest->set_max(std::max(GetMax(dest), GetMax(&src)));
        dest->set_min(std::min(GetMin(dest), GetMin(&src)));
    }
    if (total_count > 0) {
        dest->set_sum(dest->sum() + src.sum());
    }
    dest->set_count(total_count);
}

// MergeTimeSeriesValues attempts to merge two Values which contain
// InternalTimeSeriesData messages. The messages cannot be merged if they have
// different start timestamps or sample durations. Returns true if the merge is
// successful.
bool MergeTimeSeriesValues(cockroach::proto::Value *left, const cockroach::proto::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    // Attempt to parse TimeSeriesData from both Values.
    cockroach::proto::InternalTimeSeriesData left_ts;
    cockroach::proto::InternalTimeSeriesData right_ts;
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
    cockroach::proto::InternalTimeSeriesData new_ts;
    new_ts.set_start_timestamp_nanos(left_ts.start_timestamp_nanos());
    new_ts.set_sample_duration_nanos(left_ts.sample_duration_nanos());

    // Sort values in right_ts. Assume values in left_ts have been sorted.
    std::sort(right_ts.mutable_samples()->pointer_begin(),
              right_ts.mutable_samples()->pointer_end(),
              TimeSeriesSampleOrdering);

    // Merge sample values of left and right into new_ts.
    auto left_front = left_ts.samples().begin();
    auto left_end = left_ts.samples().end();
    auto right_front = right_ts.samples().begin();
    auto right_end = right_ts.samples().end();

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
        cockroach::proto::InternalTimeSeriesSample* ns = new_ts.add_samples();
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

// ConsolidateTimeSeriesValue processes a single value which contains
// InternalTimeSeriesData messages. This method will sort the sample collection
// of the value, combining any samples with duplicate offsets. This method is
// the single-value equivalent of MergeTimeSeriesValues, and is used in the case
// where the first value is merged into the key. Returns true if the merge is
// successful.
bool ConsolidateTimeSeriesValue(cockroach::proto::Value *val, rocksdb::Logger* logger) {
    // Attempt to parse TimeSeriesData from both Values.
    cockroach::proto::InternalTimeSeriesData val_ts;
    if (!val_ts.ParseFromString(val->bytes())) {
        rocksdb::Warn(logger,
                "InternalTimeSeriesData could not be parsed from bytes.");
        return false;
    }

    // Initialize new_ts and its primitive data fields.
    cockroach::proto::InternalTimeSeriesData new_ts;
    new_ts.set_start_timestamp_nanos(val_ts.start_timestamp_nanos());
    new_ts.set_sample_duration_nanos(val_ts.sample_duration_nanos());

    // Sort values in the ts value.
    std::sort(val_ts.mutable_samples()->pointer_begin(),
              val_ts.mutable_samples()->pointer_end(),
              TimeSeriesSampleOrdering);

    // Merge sample values of left and right into new_ts.
    auto front = val_ts.samples().begin();
    auto end = val_ts.samples().end();

    // Loop until samples have been exhausted.
    while (front != end) {
        // Create an empty sample in the output collection with the selected
        // offset.  Accumulate data from all samples at the front of the sample
        // collection which match the selected timestamp. This behavior is
        // needed because even a single value may have duplicated offsets.
        cockroach::proto::InternalTimeSeriesSample* ns = new_ts.add_samples();
        ns->set_offset(front->offset());
        while (front != end && front->offset() == ns->offset()) {
            AccumulateTimeSeriesSamples(ns, *front);
            ++front;
        }
    }

    // Serialize the new TimeSeriesData into the value's byte field.
    new_ts.SerializeToString(val->mutable_bytes());
    return true;
}

bool MergeValues(cockroach::proto::Value *left, const cockroach::proto::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    if (left->has_bytes()) {
        if (!right.has_bytes()) {
            rocksdb::Warn(logger,
                    "inconsistent value types for merge (left = bytes, right = ?)");
            return false;
        }
        if (IsTimeSeriesData(left) || IsTimeSeriesData(&right)) {
            // The right operand must also be a time series.
            if (!IsTimeSeriesData(left) || !IsTimeSeriesData(&right)) {
                rocksdb::Warn(logger,
                        "inconsistent value types for merging time series data (type(left) != type(right))");
                return false;
            }
            return MergeTimeSeriesValues(left, right, full_merge, logger);
        } else if (IsMVCCStatsData(left) || IsMVCCStatsData(&right)) {
            if (!IsMVCCStatsData(left) || !IsMVCCStatsData(&right)) {
                rocksdb::Warn(logger,
                        "inconsistent value types for merging mvcc stats data (type(left) != type(right))");
                return false;
            }
            return MergeMVCCStatsValues(left, right, full_merge, logger);
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
        if (full_merge && IsTimeSeriesData(left)) {
            ConsolidateTimeSeriesValue(left, logger);
        }
        return true;
    }
}


// MergeResult serializes the result MVCCMetadata value into a byte slice.
DBStatus MergeResult(cockroach::proto::MVCCMetadata* meta, DBString* result) {
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

    cockroach::proto::MVCCMetadata meta;
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
    cockroach::proto::MVCCMetadata meta;

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
  bool MergeOne(cockroach::proto::MVCCMetadata* meta,
                const rocksdb::Slice& operand,
                bool full_merge,
                rocksdb::Logger* logger) const {
    cockroach::proto::MVCCMetadata operand_meta;
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
  DBLogger(bool enabled)
      : enabled_(enabled) {
  }
  virtual void Logv(const char* format, va_list ap) {
    // TODO(pmattis): Benchmark calling Go exported methods from C++
    // to determine if this is too slow.
    if (!enabled_) {
      return;
    }

    // First try with a small fixed size buffer.
    char space[1024];

    // It's possible for methods that use a va_list to invalidate the data in
    // it upon use. The fix is to make a copy of the structure before using it
    // and use that copy instead.
    va_list backup_ap;
    va_copy(backup_ap, ap);
    int result = vsnprintf(space, sizeof(space), format, backup_ap);
    va_end(backup_ap);

    if ((result >= 0) && (result < sizeof(space))) {
      rocksDBLog(space, result);
      return;
    }

    // Repeatedly increase buffer size until it fits.
    int length = sizeof(space);
    while (true) {
      if (result < 0) {
        // Older behavior: just try doubling the buffer size.
        length *= 2;
      } else {
        // We need exactly "result+1" characters.
        length = result+1;
      }
      char* buf = new char[length];

      // Restore the va_list before we use it again
      va_copy(backup_ap, ap);
      result = vsnprintf(buf, length, format, backup_ap);
      va_end(backup_ap);

      if ((result >= 0) && (result < length)) {
        // It fit
        rocksDBLog(buf, result);
        delete[] buf;
        return;
      }
      delete[] buf;
    }
  }

 private:
  const bool enabled_;
};

// Getter defines an interface for retrieving a value from either an
// iterator or an engine. It is used by ProcessDeltaKey to abstract
// whether the "base" layer is an iterator or an engine.
struct Getter {
  virtual DBStatus Get(DBString* value) = 0;
};

// IteratorGetter is an implementation of the Getter interface which
// retrieves the value currently pointed to by the supplied
// iterator. It is ok for the supplied iterator to be NULL in which
// case no value will be retrieved.
struct IteratorGetter : public Getter {
  IteratorGetter(rocksdb::Iterator* iter)
      : base_(iter) {
  }

  virtual DBStatus Get(DBString* value) {
    if (base_ == NULL) {
      value->data = NULL;
      value->len = 0;
    } else {
      *value = ToDBString(base_->value());
    }
    return kSuccess;
  }

  rocksdb::Iterator* const base_;
};

// EngineGetter is an implementation of the Getter interface which
// retrieves the value for the supplied key from a DBEngine.
struct EngineGetter : public Getter {
  EngineGetter(DBEngine* db, DBSlice key)
      : db_(db),
        key_(key) {
  }

  virtual DBStatus Get(DBString* value) {
    return DBGet(db_, NULL, key_, value);
  }

  DBEngine* const db_;
  DBSlice const key_;
};

// ProcessDeltaKey performs the heavy lifting of processing the deltas
// for "key" contained in a batch and determining what the resulting
// value is. "delta" should have been seeked to "key", but may not be
// pointing to "key" if no updates existing for that key in the batch.
//
// Note that RocksDB WriteBatches append updates
// internally. WBWIIterator maintains an index for these updates on
// <key, seq-num>. Looping over the entries in WBWIIterator will
// return the keys in sorted order and, for each key, the updates as
// they were added to the batch.
DBStatus ProcessDeltaKey(Getter* base, rocksdb::WBWIIterator* delta,
                         rocksdb::Slice key, DBString* value) {
  if (value->data != NULL) {
    free(value->data);
  }
  value->data = NULL;
  value->len = 0;

  int count = 0;
  for (; delta->Valid() && delta->Entry().key == key;
       ++count, delta->Next()) {
    rocksdb::WriteEntry entry = delta->Entry();
    switch (entry.type) {
      case rocksdb::kPutRecord:
        if (value->data != NULL) {
          free(value->data);
        }
        *value = ToDBString(entry.value);
        break;
      case rocksdb::kMergeRecord: {
        DBString existing;
        if (count == 0) {
          // If this is the first record for the key, then we need to
          // merge with the record in base.
          DBStatus status = base->Get(&existing);
          if (status.data != NULL) {
            if (value->data != NULL) {
              free(value->data);
              value->data = NULL;
              value->len = 0;
            }
            return status;
          }
        } else {
          // Merge with the value we've built up so far.
          existing = *value;
          value->data = NULL;
          value->len = 0;
        }
        if (existing.data != NULL) {
          DBStatus status = DBMergeOne(
              ToDBSlice(existing), ToDBSlice(entry.value), value);
          free(existing.data);
          if (status.data != NULL) {
            return status;
          }
        } else {
          *value = ToDBString(entry.value);
        }
        break;
      }
      case rocksdb::kDeleteRecord:
        if (value->data != NULL) {
          free(value->data);
        }
        // This mirrors the logic in DBGet(): a deleted entry is
        // indicated by a value with NULL data.
        value->data = NULL;
        value->len = 0;
        break;
      default:
        break;
    }
  }

  if (count > 0) {
    return kSuccess;
  }
  return base->Get(value);
}

// This was cribbed from RocksDB and modified to support merge
// records.
class BaseDeltaIterator : public rocksdb::Iterator {
 public:
  BaseDeltaIterator(rocksdb::Iterator* base_iterator, rocksdb::WBWIIterator* delta_iterator)
      : current_at_base_(true),
        equal_keys_(false),
        status_(rocksdb::Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        comparator_(rocksdb::BytewiseComparator()) {
    merged_.data = NULL;
  }

  virtual ~BaseDeltaIterator() {
    ClearMerged();
  }

  bool Valid() const override {
    return current_at_base_ ? BaseValid() : DeltaValid();
  }

  void SeekToFirst() override {
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent();
  }

  void SeekToLast() override {
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent();
  }

  void Seek(const rocksdb::Slice& k) override {
    base_iterator_->Seek(k);
    delta_iterator_->Seek(k);
    UpdateCurrent();
  }

  void Next() override {
    if (!Valid()) {
      status_ = rocksdb::Status::NotSupported("Next() on invalid iterator");
    }
    Advance();
  }

  void Prev() override {
    status_ = rocksdb::Status::NotSupported("Prev() not supported");
  }

  rocksdb::Slice key() const override {
    return current_at_base_ ? base_iterator_->key()
                            : delta_iterator_->Entry().key;
  }

  rocksdb::Slice value() const override {
    if (current_at_base_) {
      return base_iterator_->value();
    }
    return ToSlice(merged_);
  }

  rocksdb::Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!base_iterator_->status().ok()) {
      return base_iterator_->status();
    }
    return delta_iterator_->status();
  }

 private:
  // -1 -- delta less advanced than base
  // 0 -- delta == base
  // 1 -- delta more advanced than base
  int Compare() const {
    assert(delta_iterator_->Valid() && base_iterator_->Valid());
    return comparator_->Compare(delta_iterator_->Entry().key,
                                base_iterator_->key());
  }
  void AssertInvariants() {
#ifndef NDEBUG
    if (!Valid()) {
      return;
    }
    if (!BaseValid()) {
      assert(!current_at_base_ && delta_iterator_->Valid());
      return;
    }
    if (!DeltaValid()) {
      assert(current_at_base_ && base_iterator_->Valid());
      return;
    }
    // we don't support those yet
    assert(delta_iterator_->Entry().type != rocksdb::kLogDataRecord);
    int compare = comparator_->Compare(delta_iterator_->Entry().key,
                                       base_iterator_->key());
    // current_at_base -> compare < 0
    assert(!current_at_base_ || compare < 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare >= 0);
    // equal_keys_ <=> compare == 0
    assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
  }

  void Advance() {
    if (equal_keys_) {
      assert(BaseValid() && DeltaValid());
      AdvanceBase();
      AdvanceDelta();
    } else {
      if (current_at_base_) {
        assert(BaseValid());
        AdvanceBase();
      } else {
        assert(DeltaValid());
        AdvanceDelta();
      }
    }
    UpdateCurrent();
  }

  void AdvanceDelta() {
    delta_iterator_->Next();
    ClearMerged();
  }
  bool ProcessDelta() {
    IteratorGetter base(equal_keys_ ? base_iterator_.get() : NULL);
    DBStatus status = ProcessDeltaKey(&base, delta_iterator_.get(),
                                      delta_iterator_->Entry().key.ToString(),
                                      &merged_);
    if (status.data != NULL) {
      status_ = rocksdb::Status::Corruption("unable to merge records");
      free(status.data);
      return false;
    }

    // We advanced past the last entry for key and want to back up the
    // delta iterator, but we can only back up if the iterator is
    // valid.
    if (delta_iterator_->Valid()) {
      delta_iterator_->Prev();
    } else {
      delta_iterator_->SeekToLast();
    }

    return merged_.data == NULL;
  }
  void AdvanceBase() {
    base_iterator_->Next();
  }
  bool BaseValid() const { return base_iterator_->Valid(); }
  bool DeltaValid() const { return delta_iterator_->Valid(); }
  void UpdateCurrent() {
    ClearMerged();

    for (;;) {
      equal_keys_ = false;
      if (!BaseValid()) {
        // Base has finished.
        if (!DeltaValid()) {
          // Finished
          return;
        }
        if (!ProcessDelta()) {
          current_at_base_ = false;
          return;
        }
        AdvanceDelta();
        continue;
      }

      if (!DeltaValid()) {
        // Delta has finished.
        current_at_base_ = true;
        return;
      }

      int compare = Compare();
      if (compare > 0) {   // delta less than base
        current_at_base_ = true;
        return;
      }
      if (compare == 0) {
        equal_keys_ = true;
      }
      if (!ProcessDelta()) {
        current_at_base_ = false;
        return;
      }

      // Delta is less advanced and is delete.
      AdvanceDelta();
      if (equal_keys_) {
        AdvanceBase();
      }
    }

    AssertInvariants();
  }

  void ClearMerged() const {
    if (merged_.data != NULL) {
      free(merged_.data);
      merged_.data = NULL;
      merged_.len = 0;
    }
  }

  bool current_at_base_;
  bool equal_keys_;
  mutable rocksdb::Status status_;
  mutable DBString merged_;
  std::unique_ptr<rocksdb::Iterator> base_iterator_;
  std::unique_ptr<rocksdb::WBWIIterator> delta_iterator_;
  const rocksdb::Comparator* comparator_;  // not owned
};

}  // namespace

DBStatus DBOpen(DBEngine **db, DBSlice dir, DBOptions db_opts) {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(
      db_opts.cache_size, 4 /* num-shard-bits */);

  rocksdb::Options options;
  options.allow_os_buffer = db_opts.allow_os_buffer;
  options.compression = rocksdb::kSnappyCompression;
  options.compaction_filter_factory.reset(new DBCompactionFilterFactory());
  options.create_if_missing = true;
  options.info_log.reset(new DBLogger(db_opts.logging_enabled));
  options.merge_operator.reset(new DBMergeOperator);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 64 << 20;           // 64 MB
  options.target_file_size_base = 64 << 20;       // 64 MB
  options.max_bytes_for_level_base = 512 << 20;   // 512 MB

  rocksdb::Env* memenv = NULL;
  if (dir.len == 0) {
    memenv = rocksdb::NewMemEnv(rocksdb::Env::Default());
    options.env = memenv;
  }

  rocksdb::DB *db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, ToString(dir), &db_ptr);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *db = new DBEngine;
  (*db)->rep = db_ptr;
  (*db)->memenv = memenv;
  return kSuccess;
}

DBStatus DBDestroy(DBSlice dir) {
  rocksdb::Options options;
  return ToDBStatus(rocksdb::DestroyDB(ToString(dir), options));
}

void DBClose(DBEngine* db) {
  delete db->rep;
  delete db->memenv;
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
  if (batch->updates == 0) {
    return kSuccess;
  }
  rocksdb::WriteOptions options;
  return ToDBStatus(db->rep->Write(options, batch->rep.GetWriteBatch()));
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
  ++batch->updates;
  batch->rep.Put(ToSlice(key), ToSlice(value));
}

void DBBatchMerge(DBBatch* batch, DBSlice key, DBSlice value) {
  ++batch->updates;
  batch->rep.Merge(ToSlice(key), ToSlice(value));
}

DBStatus DBBatchGet(DBEngine* db, DBBatch* batch, DBSlice key, DBString* value) {
  if (batch->updates == 0) {
    return DBGet(db, NULL, key, value);
  }

  std::unique_ptr<rocksdb::WBWIIterator> iter(batch->rep.NewIterator());
  rocksdb::Slice rkey(ToSlice(key));
  iter->Seek(rkey);
  EngineGetter base(db, key);
  return ProcessDeltaKey(&base, iter.get(), rkey, value);
}

void DBBatchDelete(DBBatch* batch, DBSlice key) {
  ++batch->updates;
  batch->rep.Delete(ToSlice(key));
}

DBIterator* DBBatchNewIter(DBEngine* db, DBBatch* batch) {
  if (batch->updates == 0) {
    // Don't bother to create a batch iterator if the batch contains
    // no updates.
    return DBNewIter(db, NULL);
  }

  DBIterator* iter = new DBIterator;
  rocksdb::Iterator* base = db->rep->NewIterator(MakeReadOptions(NULL));
  rocksdb::WBWIIterator *delta = batch->rep.NewIterator();
  iter->rep = new BaseDeltaIterator(base, delta);
  return iter;
}

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  new_value->len = 0;

  cockroach::proto::MVCCMetadata meta;
  if (!meta.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  cockroach::proto::MVCCMetadata update_meta;
  if (!update_meta.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(meta.mutable_value(), update_meta.value(), true, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}
