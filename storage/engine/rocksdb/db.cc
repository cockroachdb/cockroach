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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#include <algorithm>
#include <limits>
#include <stdarg.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/stringprintf.h>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "cockroach/roachpb/api.pb.h"
#include "cockroach/roachpb/data.pb.h"
#include "cockroach/roachpb/internal.pb.h"
#include "cockroach/storage/engine/mvcc.pb.h"
#include "db.h"
#include "encoding.h"

extern "C" {
#include "_cgo_export.h"

struct DBEngine {
  rocksdb::DB* const rep;

  DBEngine(rocksdb::DB* r)
      : rep(r) {
  }
  virtual ~DBEngine() { }

  virtual DBStatus Put(DBSlice key, DBSlice value) = 0;
  virtual DBStatus Merge(DBSlice key, DBSlice value) = 0;
  virtual DBStatus Delete(DBSlice key) = 0;
  virtual DBStatus WriteBatch() = 0;
  virtual DBStatus Get(DBSlice key, DBString* value) = 0;
  virtual DBIterator* NewIter() = 0;
};

struct DBImpl : public DBEngine {
  std::unique_ptr<rocksdb::Env> memenv;
  std::unique_ptr<rocksdb::DB> rep_deleter;
  rocksdb::ReadOptions const read_opts;

  // Construct a new DBImpl from the specified DB and Env. Both the DB
  // and Env will be deleted when the DBImpl is deleted. It is ok to
  // pass NULL for the Env.
  DBImpl(rocksdb::DB* r, rocksdb::Env* m)
      : DBEngine(r),
        memenv(m),
        rep_deleter(r) {
  }
  virtual ~DBImpl() {
  }

  virtual DBStatus Put(DBSlice key, DBSlice value);
  virtual DBStatus Merge(DBSlice key, DBSlice value);
  virtual DBStatus Delete(DBSlice key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBSlice key, DBString* value);
  virtual DBIterator* NewIter();
};

struct DBBatch : public DBEngine {
  int updates;
  rocksdb::WriteBatchWithIndex batch;
  rocksdb::ReadOptions const read_opts;

  DBBatch(DBEngine* db)
      : DBEngine(db->rep),
        updates(0) {
  }
  virtual ~DBBatch() {
  }

  virtual DBStatus Put(DBSlice key, DBSlice value);
  virtual DBStatus Merge(DBSlice key, DBSlice value);
  virtual DBStatus Delete(DBSlice key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBSlice key, DBString* value);
  virtual DBIterator* NewIter();
};

struct DBSnapshot : public DBEngine {
  const rocksdb::Snapshot* snapshot;
  rocksdb::ReadOptions read_opts;

  DBSnapshot(DBEngine *db)
      : DBEngine(db->rep),
        snapshot(db->rep->GetSnapshot()) {
    read_opts.snapshot = snapshot;
  }
  virtual ~DBSnapshot() {
    rep->ReleaseSnapshot(snapshot);
  }

  virtual DBStatus Put(DBSlice key, DBSlice value);
  virtual DBStatus Merge(DBSlice key, DBSlice value);
  virtual DBStatus Delete(DBSlice key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBSlice key, DBString* value);
  virtual DBIterator* NewIter();
};

struct DBIterator {
  std::unique_ptr<rocksdb::Iterator> rep;
};

}  // extern "C"

namespace {

// NOTE: these constants must be kept in sync with the values in
// storage/engine/keys.go. Both kKeyLocalRangeIDPrefix and
// kKeyLocalRangePrefix are the mvcc-encoded prefixes.
const int kKeyLocalRangePrefixSize = 4;
const rocksdb::Slice kKeyLocalRangeIDPrefix("\x31\x00\xff\x00\xff\x00\xffi", 8);
const rocksdb::Slice kKeyLocalRangePrefix("\x31\x00\xff\x00\xff\x00\xffk", 8);
const rocksdb::Slice kKeyLocalTransactionSuffix("\x00\x01txn-", 6);
const rocksdb::Slice kKeyLocalMax("\x00\x00\x01", 3);

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

DBStatus FmtStatus(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string str;
  google::protobuf::StringAppendV(&str, fmt, ap);
  va_end(ap);
  return ToDBString(str);
}

DBIterState DBIterGetState(DBIterator* iter) {
  DBIterState state;
  state.valid = iter->rep->Valid();
  if (state.valid) {
    state.key = ToDBSlice(iter->rep->key());
    state.value = ToDBSlice(iter->rep->value());
  } else {
    state.key.data = NULL;
    state.key.len = 0;
    state.value.data = NULL;
    state.value.len = 0;
  }
  return state;
}

// DBCompactionFilter implements our garbage collection policy for
// key/value pairs which can be considered in isolation. This
// includes:
//
// - Transactions: transaction table entries are garbage collected
//   according to the commit time of the transaction vs. the oldest
//   remaining write intent across the entire system. The oldest write
//   intent is maintained as a low-water mark, updated after polling
//   all ranges in the map.
class DBCompactionFilter : public rocksdb::CompactionFilter {
 public:
  DBCompactionFilter(int64_t min_txn_ts)
      : min_txn_ts_(min_txn_ts) {
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

  bool IsTransactionRecord(rocksdb::Slice key) const {
    // The transaction key format is:
    //   <prefix>[key]<suffix>[remainder].
    if (!key.starts_with(kKeyLocalRangePrefix)) {
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

    // Only filter transaction rows.
    if (!IsTransactionRecord(key)) {
      return false;
    }
    // Parse MVCC metadata for inlined value.
    cockroach::storage::engine::MVCCMetadata meta;
    if (!meta.ParseFromArray(existing_value.data(), existing_value.size())) {
      // *error_msg = (char*)"failed to parse mvcc metadata entry";
      return false;
    }
    if (!meta.has_value()) {
      // *error_msg = (char*)"not an inlined mvcc value";
      return false;
    }
    // Transaction rows are GC'd if their timestamp is older than
    // the system-wide minimum write intent timestamp. This
    // system-wide minimum write intent is periodically computed via
    // map-reduce over all ranges and gossiped.
    cockroach::roachpb::Transaction txn;
    if (!txn.ParseFromArray(meta.value().raw_bytes().data(), meta.value().raw_bytes().size())) {
      // *error_msg = (char*)"failed to parse transaction entry";
      return false;
    }
    if (txn.timestamp().wall_time() <= min_txn_ts_) {
      return true;
    }
    return false;
  }

  virtual const char* Name() const {
    return "cockroach_compaction_filter";
  }

 private:
  const int64_t min_txn_ts_;
};

class DBCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  DBCompactionFilterFactory() {}

  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    google::protobuf::MutexLock l(&mu_); // Protect access to gc timeouts.
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new DBCompactionFilter(min_txn_ts_));
  }

  virtual const char* Name() const override {
    return "cockroach_compaction_filter_factory";
  }

  void SetGCTimeouts(int64_t min_txn_ts) {
    google::protobuf::MutexLock l(&mu_);
    min_txn_ts_ = min_txn_ts;
  }

 private:
  google::protobuf::Mutex mu_; // Protects values below.
  int64_t min_txn_ts_;
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
bool TimeSeriesSampleOrdering(const cockroach::roachpb::InternalTimeSeriesSample* a,
        const cockroach::roachpb::InternalTimeSeriesSample* b) {
    return a->offset() < b->offset();
}

// IsTimeSeriesData returns true if the given protobuffer Value contains a
// TimeSeriesData message.
bool IsTimeSeriesData(const cockroach::roachpb::Value *val) {
    return val->has_tag()
        && val->tag() == cockroach::roachpb::TIMESERIES;
}

double GetMax(const cockroach::roachpb::InternalTimeSeriesSample *sample) {
    if (sample->has_max()) return sample->max();
    if (sample->has_sum()) return sample->sum();
    return std::numeric_limits<double>::min();
}

double GetMin(const cockroach::roachpb::InternalTimeSeriesSample *sample) {
    if (sample->has_min()) return sample->min();
    if (sample->has_sum()) return sample->sum();
    return std::numeric_limits<double>::max();
}

// AccumulateTimeSeriesSamples accumulates the individual values of two
// InternalTimeSeriesSamples which have a matching timestamp. The dest parameter
// is modified to contain the accumulated values.
void AccumulateTimeSeriesSamples(cockroach::roachpb::InternalTimeSeriesSample* dest,
        const cockroach::roachpb::InternalTimeSeriesSample &src) {
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
bool MergeTimeSeriesValues(cockroach::roachpb::Value *left, const cockroach::roachpb::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    // Attempt to parse TimeSeriesData from both Values.
    cockroach::roachpb::InternalTimeSeriesData left_ts;
    cockroach::roachpb::InternalTimeSeriesData right_ts;
    if (!left_ts.ParseFromString(left->raw_bytes())) {
        rocksdb::Warn(logger,
                "left InternalTimeSeriesData could not be parsed from bytes.");
        return false;
    }
    if (!right_ts.ParseFromString(right.raw_bytes())) {
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
        left_ts.SerializeToString(left->mutable_raw_bytes());
        return true;
    }

    // Initialize new_ts and its primitive data fields. Values from the left and
    // right collections will be merged into the new collection.
    cockroach::roachpb::InternalTimeSeriesData new_ts;
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
        cockroach::roachpb::InternalTimeSeriesSample* ns = new_ts.add_samples();
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
    new_ts.SerializeToString(left->mutable_raw_bytes());
    return true;
}

// ConsolidateTimeSeriesValue processes a single value which contains
// InternalTimeSeriesData messages. This method will sort the sample collection
// of the value, combining any samples with duplicate offsets. This method is
// the single-value equivalent of MergeTimeSeriesValues, and is used in the case
// where the first value is merged into the key. Returns true if the merge is
// successful.
bool ConsolidateTimeSeriesValue(cockroach::roachpb::Value *val, rocksdb::Logger* logger) {
    // Attempt to parse TimeSeriesData from both Values.
    cockroach::roachpb::InternalTimeSeriesData val_ts;
    if (!val_ts.ParseFromString(val->raw_bytes())) {
        rocksdb::Warn(logger,
                "InternalTimeSeriesData could not be parsed from bytes.");
        return false;
    }

    // Initialize new_ts and its primitive data fields.
    cockroach::roachpb::InternalTimeSeriesData new_ts;
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
        cockroach::roachpb::InternalTimeSeriesSample* ns = new_ts.add_samples();
        ns->set_offset(front->offset());
        while (front != end && front->offset() == ns->offset()) {
            AccumulateTimeSeriesSamples(ns, *front);
            ++front;
        }
    }

    // Serialize the new TimeSeriesData into the value's byte field.
    new_ts.SerializeToString(val->mutable_raw_bytes());
    return true;
}

bool MergeValues(cockroach::roachpb::Value *left, const cockroach::roachpb::Value &right,
        bool full_merge, rocksdb::Logger* logger) {
    if (left->has_raw_bytes()) {
        if (!right.has_raw_bytes()) {
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
        } else {
            *left->mutable_raw_bytes() += right.raw_bytes();
        }
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
DBStatus MergeResult(cockroach::storage::engine::MVCCMetadata* meta, DBString* result) {
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

    cockroach::storage::engine::MVCCMetadata meta;
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
    cockroach::storage::engine::MVCCMetadata meta;

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
  bool MergeOne(cockroach::storage::engine::MVCCMetadata* meta,
                const rocksdb::Slice& operand,
                bool full_merge,
                rocksdb::Logger* logger) const {
    cockroach::storage::engine::MVCCMetadata operand_meta;
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
  rocksdb::Iterator* const base;

  IteratorGetter(rocksdb::Iterator* iter)
      : base(iter) {
  }

  virtual DBStatus Get(DBString* value) {
    if (base == NULL) {
      value->data = NULL;
      value->len = 0;
    } else {
      *value = ToDBString(base->value());
    }
    return kSuccess;
  }
};

// DBGetter is an implementation of the Getter interface which
// retrieves the value for the supplied key from a rocksdb::DB.
struct DBGetter : public Getter {
  rocksdb::DB *const rep;
  rocksdb::ReadOptions const options;
  rocksdb::Slice const key;

  DBGetter(rocksdb::DB *const r, rocksdb::ReadOptions opts, DBSlice k)
      : rep(r),
        options(opts),
        key(ToSlice(k)) {
  }

  virtual DBStatus Get(DBString* value) {
    std::string tmp;
    rocksdb::Status s = rep->Get(options, key, &tmp);
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

  std::unique_ptr<rocksdb::Env> memenv;
  if (dir.len == 0) {
    memenv.reset(rocksdb::NewMemEnv(rocksdb::Env::Default()));
    options.env = memenv.get();
  }

  rocksdb::DB *db_ptr;
  rocksdb::Status status = rocksdb::DB::Open(options, ToString(dir), &db_ptr);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *db = new DBImpl(db_ptr, memenv.release());
  return kSuccess;
}

DBStatus DBDestroy(DBSlice dir) {
  rocksdb::Options options;
  return ToDBStatus(rocksdb::DestroyDB(ToString(dir), options));
}

void DBClose(DBEngine* db) {
  delete db;
}

DBStatus DBFlush(DBEngine* db) {
  rocksdb::FlushOptions options;
  options.wait = true;
  return ToDBStatus(db->rep->Flush(options));
}

void DBSetGCTimeouts(DBEngine * db, int64_t min_txn_ts) {
  DBCompactionFilterFactory *db_cff =
      (DBCompactionFilterFactory*)db->rep->GetOptions().compaction_filter_factory.get();
  db_cff->SetGCTimeouts(min_txn_ts);
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
  return ToDBStatus(db->rep->CompactRange(rocksdb::CompactRangeOptions(), sPtr, ePtr));
}

uint64_t DBApproximateSize(DBEngine* db, DBSlice start, DBSlice end) {
  const rocksdb::Range r(ToSlice(start), ToSlice(end));
  uint64_t result;
  db->rep->GetApproximateSizes(&r, 1, &result);
  return result;
}

DBStatus DBImpl::Put(DBSlice key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Put(options, ToSlice(key), ToSlice(value)));
}

DBStatus DBBatch::Put(DBSlice key, DBSlice value) {
  ++updates;
  batch.Put(ToSlice(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBSnapshot::Put(DBSlice key, DBSlice value) {
  return FmtStatus("unsupported");
}

DBStatus DBPut(DBEngine* db, DBSlice key, DBSlice value) {
  return db->Put(key, value);
}

DBStatus DBImpl::Merge(DBSlice key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Merge(options, ToSlice(key), ToSlice(value)));
}

DBStatus DBBatch::Merge(DBSlice key, DBSlice value) {
  ++updates;
  batch.Merge(ToSlice(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBSnapshot::Merge(DBSlice key, DBSlice value) {
  return FmtStatus("unsupported");
}

DBStatus DBMerge(DBEngine* db, DBSlice key, DBSlice value) {
  return db->Merge(key, value);
}

DBStatus DBImpl::Get(DBSlice key, DBString* value) {
  DBGetter base(rep, read_opts, key);
  return base.Get(value);
}

DBStatus DBBatch::Get(DBSlice key, DBString* value) {
  DBGetter base(rep, read_opts, key);
  if (updates == 0) {
    return base.Get(value);
  }
  std::unique_ptr<rocksdb::WBWIIterator> iter(batch.NewIterator());
  rocksdb::Slice rkey(ToSlice(key));
  iter->Seek(rkey);
  return ProcessDeltaKey(&base, iter.get(), rkey, value);
}

DBStatus DBSnapshot::Get(DBSlice key, DBString* value) {
  DBGetter base(rep, read_opts, key);
  return base.Get(value);
}

DBStatus DBGet(DBEngine* db, DBSlice key, DBString* value) {
  return db->Get(key, value);
}

DBStatus DBImpl::Delete(DBSlice key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Delete(options, ToSlice(key)));
}

DBStatus DBBatch::Delete(DBSlice key) {
  ++updates;
  batch.Delete(ToSlice(key));
  return kSuccess;
}

DBStatus DBSnapshot::Delete(DBSlice key) {
  return FmtStatus("unsupported");
}

DBStatus DBDelete(DBEngine *db, DBSlice key) {
  return db->Delete(key);
}

DBStatus DBImpl::WriteBatch() {
  return FmtStatus("unsupported");
}

DBStatus DBBatch::WriteBatch() {
  if (updates == 0) {
    return kSuccess;
  }
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Write(options, batch.GetWriteBatch()));
}

DBStatus DBSnapshot::WriteBatch() {
  return FmtStatus("unsupported");
}

DBStatus DBWriteBatch(DBEngine* db) {
  return db->WriteBatch();
}

DBEngine* DBNewSnapshot(DBEngine* db)  {
  return new DBSnapshot(db);
}

DBEngine* DBNewBatch(DBEngine *db) {
  return new DBBatch(db);
}

DBIterator* DBImpl::NewIter() {
  DBIterator* iter = new DBIterator;
  iter->rep.reset(rep->NewIterator(read_opts));
  return iter;
}

DBIterator* DBBatch::NewIter() {
  DBIterator* iter = new DBIterator;
  rocksdb::Iterator* base = rep->NewIterator(read_opts);
  if (updates == 0) {
    iter->rep.reset(base);
  } else {
    rocksdb::WBWIIterator* delta = batch.NewIterator();
    iter->rep.reset(new BaseDeltaIterator(base, delta));
  }
  return iter;
}

DBIterator* DBSnapshot::NewIter() {
  DBIterator* iter = new DBIterator;
  iter->rep.reset(rep->NewIterator(read_opts));
  return iter;
}

DBIterator* DBNewIter(DBEngine* db) {
  return db->NewIter();
}

void DBIterDestroy(DBIterator* iter) {
  delete iter;
}

DBIterState DBIterSeek(DBIterator* iter, DBSlice key) {
  iter->rep->Seek(ToSlice(key));
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToFirst(DBIterator* iter) {
  iter->rep->SeekToFirst();
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToLast(DBIterator* iter) {
  iter->rep->SeekToLast();
  return DBIterGetState(iter);
}

DBIterState DBIterNext(DBIterator* iter) {
  iter->rep->Next();
  return DBIterGetState(iter);
}

DBIterState DBIterPrev(DBIterator* iter){
  iter->rep->Prev();
  return DBIterGetState(iter);
}

DBStatus DBIterError(DBIterator* iter) {
  return ToDBStatus(iter->rep->status());
}

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  new_value->len = 0;

  cockroach::storage::engine::MVCCMetadata meta;
  if (!meta.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  cockroach::storage::engine::MVCCMetadata update_meta;
  if (!update_meta.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(meta.mutable_value(), update_meta.value(), true, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}

MVCCStatsResult MVCCComputeStats(
    DBIterator* iter, DBSlice start, DBSlice end, int64_t now_nanos) {
  const int mvcc_version_timestamp_size = 12;

  MVCCStatsResult stats;
  memset(&stats, 0, sizeof(stats));

  rocksdb::Iterator *const iter_rep = iter->rep.get();
  iter_rep->Seek(ToSlice(start));
  const rocksdb::Slice end_key = ToSlice(end);

  cockroach::storage::engine::MVCCMetadata meta;
  std::string decoded;
  bool first = false;

  for (; iter_rep->Valid() && iter_rep->key().compare(end_key) < 0; iter_rep->Next()) {
    const rocksdb::Slice key = iter_rep->key();
    const rocksdb::Slice value = iter_rep->value();

    rocksdb::Slice buf = key;
    decoded.clear();
    if (!DecodeBytes(&buf, &decoded)) {
      stats.status = FmtStatus("unable to decode key");
      break;
    }

    const bool isSys = (rocksdb::Slice(decoded).compare(kKeyLocalMax) < 0);
    const bool isValue = (buf.size() == mvcc_version_timestamp_size);

    if (!isValue) {
      if (buf.size() != 0) {
        stats.status = FmtStatus("there should be %d bytes for encoded timestamp",
                                 mvcc_version_timestamp_size);
        break;
      }

      const int64_t total_bytes = key.size() + value.size();
      first = true;

      if (!meta.ParseFromArray(value.data(), value.size())) {
        stats.status = FmtStatus("unable to decode MVCCMetadata");
        break;
      }

      if (isSys) {
        stats.sys_bytes += total_bytes;
        stats.sys_count++;
      } else {
        if (!meta.deleted()) {
          stats.live_bytes += total_bytes;
          stats.live_count++;
        } else {
          stats.gc_bytes_age += total_bytes * ((now_nanos - meta.timestamp().wall_time()) / 1e9);
        }
        stats.key_bytes += key.size();
        stats.val_bytes += value.size();
        stats.key_count++;
        if (meta.has_value()) {
          stats.val_count++;
        }
      }
    } else {
      const int64_t total_bytes = value.size() + mvcc_version_timestamp_size;
      if (isSys) {
        stats.sys_bytes += total_bytes;
      } else {
        if (first) {
          first = false;
          if (!meta.deleted()) {
            stats.live_bytes += total_bytes;
          } else {
            stats.gc_bytes_age += total_bytes * ((now_nanos - meta.timestamp().wall_time()) / 1e9);
          }
          if (meta.has_txn()) {
            stats.intent_bytes += total_bytes;
            stats.intent_count++;
            stats.intent_age += (now_nanos - meta.timestamp().wall_time()) / 1e9;
          }
          if (meta.key_bytes() != mvcc_version_timestamp_size) {
            stats.status = FmtStatus("expected mvcc metadata val bytes to equal %d; got %d",
                                     mvcc_version_timestamp_size, int(meta.key_bytes()));
            break;
          }
          if (meta.val_bytes() != value.size()) {
            stats.status = FmtStatus("expected mvcc metadata val bytes to equal %d; got %d",
                                     int(value.size()), int(meta.val_bytes()));
            break;
          }
        } else {
          uint64_t wall_time;
          if (!DecodeUint64Decreasing(&buf, &wall_time)) {
            stats.status = FmtStatus("unable to decode mvcc timestamp");
            break;
          }
          stats.gc_bytes_age += total_bytes * ((now_nanos - wall_time) / 1e9);
        }
        stats.key_bytes += mvcc_version_timestamp_size;
        stats.val_bytes += value.size();
        stats.val_count++;
      }
    }
  }

  return stats;
}
