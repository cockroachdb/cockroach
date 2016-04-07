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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#include <algorithm>
#include <atomic>
#include <stdarg.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/stringprintf.h>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "cockroach/roachpb/data.pb.h"
#include "cockroach/roachpb/internal.pb.h"
#include "cockroach/storage/engine/mvcc.pb.h"
#include "db.h"
#include "encoding.h"
#include "eventlistener.h"

extern "C" {
#include "_cgo_export.h"

struct DBEngine {
  rocksdb::DB* const rep;

  DBEngine(rocksdb::DB* r)
      : rep(r) {
  }
  virtual ~DBEngine() { }

  virtual DBStatus Put(DBKey key, DBSlice value) = 0;
  virtual DBStatus Merge(DBKey key, DBSlice value) = 0;
  virtual DBStatus Delete(DBKey key) = 0;
  virtual DBStatus WriteBatch() = 0;
  virtual DBStatus Get(DBKey key, DBString* value) = 0;
  virtual DBIterator* NewIter(DBSlice prefix) = 0;
  virtual DBStatus GetStats(DBStatsResult* stats) = 0;
};

struct DBImpl : public DBEngine {
  std::unique_ptr<rocksdb::Env> memenv;
  std::unique_ptr<rocksdb::DB> rep_deleter;
  rocksdb::ReadOptions const read_opts;
  std::shared_ptr<rocksdb::Cache> block_cache;
  std::shared_ptr<DBEventListener> event_listener;

  // Construct a new DBImpl from the specified DB and Env. Both the DB
  // and Env will be deleted when the DBImpl is deleted. It is ok to
  // pass NULL for the Env.
  DBImpl(rocksdb::DB* r, rocksdb::Env* m, std::shared_ptr<rocksdb::Cache> bc,
    std::shared_ptr<DBEventListener> event_listener)
      : DBEngine(r),
        memenv(m),
        rep_deleter(r),
        block_cache(bc),
        event_listener(event_listener) {
  }
  virtual ~DBImpl() {
    const rocksdb::Options &opts = rep->GetOptions();
    const std::shared_ptr<rocksdb::Statistics> &s = opts.statistics;
    rocksdb::Info(opts.info_log, "bloom filter utility:    %0.1f%%",
                  (100.0 * s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL)) /
                  s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED));
  }

  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(DBSlice prefix);
  virtual DBStatus GetStats(DBStatsResult* stats);
};

struct DBBatch : public DBEngine {
  int updates;
  rocksdb::WriteBatchWithIndex batch;
  rocksdb::ReadOptions const read_opts;

  DBBatch(DBEngine* db);
  virtual ~DBBatch() {
  }

  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(DBSlice prefix);
  virtual DBStatus GetStats(DBStatsResult* stats);
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

  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus WriteBatch();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(DBSlice prefix);
  virtual DBStatus GetStats(DBStatsResult* stats);
};

struct DBIterator {
  std::unique_ptr<rocksdb::Iterator> rep;
  std::string upper_bound_str;
  rocksdb::Slice upper_bound_slice;

  DBIterator(DBSlice prefix);

  rocksdb::Slice* upper_bound() {
    if (upper_bound_slice.size() > 0) {
      return &upper_bound_slice;
    }
    return NULL;
  }
};

}  // extern "C"

namespace {

// NOTE: these constants must be kept in sync with the values in
// storage/engine/keys.go. Both kKeyLocalRangeIDPrefix and
// kKeyLocalRangePrefix are the mvcc-encoded prefixes.
const rocksdb::Slice kKeyLocalRangeIDPrefix("\x01i", 2);
const rocksdb::Slice kKeyLocalMax("\x02", 1);

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

const int kMVCCVersionTimestampSize = 12;

// MVCC keys are encoded as <key>[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k) {
  std::string s;
  const bool ts = k.wall_time != 0 || k.logical != 0;
  s.reserve(k.key.len + 1 + (ts ? 1 + kMVCCVersionTimestampSize : 0));
  s.append(k.key.data, k.key.len);
  if (ts) {
    // Add a NUL prefix to the timestamp data. See DBPrefixExtractor.Transform
    // for more details.
    s.push_back(0);
    EncodeUint64(&s, uint64_t(k.wall_time));
    if (k.logical != 0) {
      // TODO(peter): Use varint encoding here. Logical values will
      // usually be small.
      EncodeUint32(&s, uint32_t(k.logical));
    }
  }
  s.push_back(char(s.size() - k.key.len));
  return s;
}

// When we're performing a prefix scan, we want to limit the scan to
// the keys that have the matching prefix. Prefix in this case refers
// to an exact match on the non-timestamp portion of a key. We do this
// by constructing an encoded mvcc key which has a zero timestamp
// (hence the trailing 0) and is the "next" key (thus the additional
// 0). See EncodeKey and SplitKey for more details on the encoded key
// format.
std::string EncodePrefixNextKey(DBSlice k) {
  std::string s;
  if (k.len > 0) {
    s.reserve(k.len + 2);
    s.append(k.data, k.len);
    s.push_back(0);
    s.push_back(0);
  }
  return s;
}

bool SplitKey(rocksdb::Slice buf, rocksdb::Slice *key, rocksdb::Slice *timestamp) {
  if (buf.empty()) {
    return false;
  }
  const char ts_size = buf[buf.size() - 1];
  if (ts_size >= buf.size()) {
    return false;
  }
  *key = rocksdb::Slice(buf.data(), buf.size() - ts_size - 1);
  *timestamp = rocksdb::Slice(key->data() + key->size(), ts_size);
  return true;
}

bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice *key, int64_t *wall_time, int32_t *logical) {
  key->clear();

  rocksdb::Slice timestamp;
  if (!SplitKey(buf, key, &timestamp)) {
    return false;
  }
  if (timestamp.size() > 0) {
    timestamp.remove_prefix(1);  // The NUL prefix.
    uint64_t w;
    if (!DecodeUint64(&timestamp, &w)) {
      return false;
    }
    *wall_time = int64_t(w);
    *logical = 0;
    if (timestamp.size() > 0) {
      // TODO(peter): Use varint decoding here.
      uint32_t l;
      if (!DecodeUint32(&timestamp, &l)) {
        return false;
      }
      *logical = int32_t(l);
    }
  }
  return timestamp.empty();
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
  state.key.key.data = NULL;
  state.key.key.len = 0;
  state.key.wall_time = 0;
  state.key.logical = 0;
  state.value.data = NULL;
  state.value.len = 0;

  if (state.valid) {
    rocksdb::Slice key;
    state.valid = DecodeKey(iter->rep->key(), &key,
                            &state.key.wall_time, &state.key.logical);
    if (state.valid) {
      state.key.key = ToDBSlice(key);
      state.value = ToDBSlice(iter->rep->value());
    }
  }
  return state;
}

const int kChecksumSize = 4;
const int kTagPos = kChecksumSize;
const int kHeaderSize = kTagPos + 1;

rocksdb::Slice ValueDataBytes(const std::string &val) {
  if (val.size() < kHeaderSize) {
    return rocksdb::Slice();
  }
  return rocksdb::Slice(val.data() + kHeaderSize, val.size() - kHeaderSize);
}

cockroach::roachpb::ValueType GetTag(const std::string &val) {
  if (val.size() < kHeaderSize) {
    return cockroach::roachpb::UNKNOWN;
  }
  return cockroach::roachpb::ValueType(val[kTagPos]);
}

void SetTag(std::string *val, cockroach::roachpb::ValueType tag) {
  (*val)[kTagPos] = tag;
}

bool ParseProtoFromValue(const std::string &val, google::protobuf::Message *msg) {
  if (val.size() < kHeaderSize) {
    return false;
  }
  const rocksdb::Slice d = ValueDataBytes(val);
  return msg->ParseFromArray(d.data(), d.size());
}

void SerializeProtoToValue(std::string *val, const google::protobuf::Message &msg) {
  val->resize(kHeaderSize);
  std::fill(val->begin(), val->end(), 0);
  SetTag(val, cockroach::roachpb::BYTES);
  msg.AppendToString(val);
}

class DBComparator : public rocksdb::Comparator {
 public:
  DBComparator() {
  }

  virtual const char* Name() const override {
    return "cockroach_comparator";
  }

  virtual int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    rocksdb::Slice key_a, key_b;
    rocksdb::Slice ts_a, ts_b;
    if (!SplitKey(a, &key_a, &ts_a) ||
        !SplitKey(b, &key_b, &ts_b)) {
      // This should never happen unless there is some sort of corruption of
      // the keys.
      return a.compare(b);
    }

    const int c = key_a.compare(key_b);
    if (c != 0) {
      return c;
    }
    if (ts_a.empty()) {
      if (ts_b.empty()) {
        return 0;
      }
      return -1;
    } else if (ts_b.empty()) {
      return +1;
    }
    return ts_b.compare(ts_a);
  }

  virtual bool Equal(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    return a == b;
  }

  // The RocksDB docs say it is safe to leave these two methods unimplemented.
  virtual void FindShortestSeparator(
      std::string *start, const rocksdb::Slice &limit) const override {
  }

  virtual void FindShortSuccessor(std::string *key) const override {
  }
};

const DBComparator kComparator;

class DBPrefixExtractor : public rocksdb::SliceTransform {
 public:
  DBPrefixExtractor() {
  }

  virtual const char* Name() const {
    return "cockroach_prefix_extractor";
  }

  // MVCC keys are encoded as <user-key>/<timestamp>. Extract the <user-key>
  // prefix which will allow for more efficient iteration over the keys
  // matching a particular <user-key>. Specifically, the <user-key> will be
  // added to the per table bloom filters and will be used to skip tables
  // which do not contain the <user-key>.
  virtual rocksdb::Slice Transform(const rocksdb::Slice& src) const {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (!SplitKey(src, &key, &ts)) {
      return src;
    }
    // RocksDB requires that keys generated via Transform be comparable with
    // normal encoded MVCC keys. Encoded MVCC keys have a suffix indicating the
    // number of bytes of timestamp data. MVCC keys without a timestamp have a
    // suffix of 0. We're careful in EncodeKey to make sure that the user-key
    // always has a trailing 0. If there is no timestamp this falls out
    // naturally. If there is a timestamp we prepend a 0 to the encoded
    // timestamp data.
    assert(src.size() > key.size() && src[key.size()] == 0);
    return rocksdb::Slice(key.data(), key.size() + 1);
  }

  virtual bool InDomain(const rocksdb::Slice& src) const {
    return true;
  }

  virtual bool InRange(const rocksdb::Slice& dst) const {
    return Transform(dst) == dst;
  }
};

// Method used to sort InternalTimeSeriesSamples.
bool TimeSeriesSampleOrdering(const cockroach::roachpb::InternalTimeSeriesSample* a,
        const cockroach::roachpb::InternalTimeSeriesSample* b) {
  return a->offset() < b->offset();
}

// IsTimeSeriesData returns true if the given protobuffer Value contains a
// TimeSeriesData message.
bool IsTimeSeriesData(const std::string &val) {
  return GetTag(val) == cockroach::roachpb::TIMESERIES;
}

double GetMax(const cockroach::roachpb::InternalTimeSeriesSample *sample) {
  if (sample->has_max()) return sample->max();
  return sample->sum();
}

double GetMin(const cockroach::roachpb::InternalTimeSeriesSample *sample) {
  if (sample->has_min()) return sample->min();
  return sample->sum();
}

// AccumulateTimeSeriesSamples accumulates the individual values of two
// InternalTimeSeriesSamples which have a matching timestamp. The dest parameter
// is modified to contain the accumulated values. Message src MUST have a
// non-zero count of samples; it is assumed that no system will attempt to merge
// a sample with zero datapoints.
void AccumulateTimeSeriesSamples(cockroach::roachpb::InternalTimeSeriesSample* dest,
                                 const cockroach::roachpb::InternalTimeSeriesSample &src) {
  assert(src.has_sum());
  assert(src.count() > 0);

  // If dest is empty, just copy from the src.
  if (dest->count() == 0) {
    dest->CopyFrom(src);
    return;
  }
  assert(dest->has_sum());

  // Keep explicit max and min values.
  dest->set_max(std::max(GetMax(dest), GetMax(&src)));
  dest->set_min(std::min(GetMin(dest), GetMin(&src)));
  // Accumulate sum and count.
  dest->set_sum(dest->sum() + src.sum());
  dest->set_count(dest->count() + src.count());
}

void SerializeTimeSeriesToValue(
    std::string *val, const cockroach::roachpb::InternalTimeSeriesData &ts) {
  SerializeProtoToValue(val, ts);
  SetTag(val, cockroach::roachpb::TIMESERIES);
}

// MergeTimeSeriesValues attempts to merge two Values which contain
// InternalTimeSeriesData messages. The messages cannot be merged if they have
// different start timestamps or sample durations. Returns true if the merge is
// successful.
bool MergeTimeSeriesValues(
    std::string *left, const std::string &right, bool full_merge, rocksdb::Logger* logger) {
  // Attempt to parse TimeSeriesData from both Values.
  cockroach::roachpb::InternalTimeSeriesData left_ts;
  cockroach::roachpb::InternalTimeSeriesData right_ts;
  if (!ParseProtoFromValue(*left, &left_ts)) {
    rocksdb::Warn(logger,
                  "left InternalTimeSeriesData could not be parsed from bytes.");
    return false;
  }
  if (!ParseProtoFromValue(right, &right_ts)) {
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
    SerializeTimeSeriesToValue(left, left_ts);
    return true;
  }

  // Initialize new_ts and its primitive data fields. Values from the left and
  // right collections will be merged into the new collection.
  cockroach::roachpb::InternalTimeSeriesData new_ts;
  new_ts.set_start_timestamp_nanos(left_ts.start_timestamp_nanos());
  new_ts.set_sample_duration_nanos(left_ts.sample_duration_nanos());

  // Sort values in right_ts. Assume values in left_ts have been sorted.
  std::stable_sort(right_ts.mutable_samples()->pointer_begin(),
                   right_ts.mutable_samples()->pointer_end(),
                   TimeSeriesSampleOrdering);

  // Merge sample values of left and right into new_ts.
  auto left_front = left_ts.samples().begin();
  auto left_end = left_ts.samples().end();
  auto right_front = right_ts.samples().begin();
  auto right_end = right_ts.samples().end();

  // Loop until samples from both sides have been exhausted.
  while (left_front != left_end || right_front != right_end) {
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

    // Create an empty sample in the output collection.
    cockroach::roachpb::InternalTimeSeriesSample* ns = new_ts.add_samples();

    // Only the most recently merged value with a given sample offset is kept;
    // samples merged earlier at the same offset are discarded. We will now
    // parse through the left and right sample sets, finding the most recently
    // merged sample at the current offset.
    cockroach::roachpb::InternalTimeSeriesSample src;
    while (left_front != left_end && left_front->offset() == next_offset) {
      src = *left_front;
      left_front++;
    }
    while (right_front != right_end && right_front->offset() == next_offset) {
      src = *right_front;
      right_front++;
    }

    ns->CopyFrom(src);
  }

  // Serialize the new TimeSeriesData into the left value's byte field.
  SerializeTimeSeriesToValue(left, new_ts);
  return true;
}

// ConsolidateTimeSeriesValue processes a single value which contains
// InternalTimeSeriesData messages. This method will sort the sample collection
// of the value, combining any samples with duplicate offsets. This method is
// the single-value equivalent of MergeTimeSeriesValues, and is used in the case
// where the first value is merged into the key. Returns true if the merge is
// successful.
bool ConsolidateTimeSeriesValue(std::string *val, rocksdb::Logger* logger) {
  // Attempt to parse TimeSeriesData from both Values.
  cockroach::roachpb::InternalTimeSeriesData val_ts;
  if (!ParseProtoFromValue(*val, &val_ts)) {
    rocksdb::Warn(logger,
                  "InternalTimeSeriesData could not be parsed from bytes.");
    return false;
  }

  // Initialize new_ts and its primitive data fields.
  cockroach::roachpb::InternalTimeSeriesData new_ts;
  new_ts.set_start_timestamp_nanos(val_ts.start_timestamp_nanos());
  new_ts.set_sample_duration_nanos(val_ts.sample_duration_nanos());

  // Sort values in the ts value.
  std::stable_sort(val_ts.mutable_samples()->pointer_begin(),
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
      ns->CopyFrom(*front);
      ++front;
    }
  }

  // Serialize the new TimeSeriesData into the value's byte field.
  SerializeTimeSeriesToValue(val, new_ts);
  return true;
}

bool MergeValues(cockroach::storage::engine::MVCCMetadata *left,
                 const cockroach::storage::engine::MVCCMetadata &right,
                 bool full_merge, rocksdb::Logger* logger) {
  if (left->has_raw_bytes()) {
    if (!right.has_raw_bytes()) {
      rocksdb::Warn(logger, "inconsistent value types for merge (left = bytes, right = ?)");
      return false;
    }

    // Replay Advisory: Because merge commands pass through raft, it is possible
    // for merging values to be "replayed". Currently, the only actual use of
    // the merge system is for time series data, which is safe against replay;
    // however, this property is not general for all potential mergeable types.
    // If a future need arises to merge another type of data, replay protection
    // will likely need to be a consideration.

    if (IsTimeSeriesData(left->raw_bytes()) || IsTimeSeriesData(right.raw_bytes())) {
      // The right operand must also be a time series.
      if (!IsTimeSeriesData(left->raw_bytes()) || !IsTimeSeriesData(right.raw_bytes())) {
        rocksdb::Warn(logger,
                      "inconsistent value types for merging time series data (type(left) != type(right))");
        return false;
      }
      return MergeTimeSeriesValues(left->mutable_raw_bytes(), right.raw_bytes(), full_merge, logger);
    } else {
      const rocksdb::Slice rdata = ValueDataBytes(right.raw_bytes());
      left->mutable_raw_bytes()->append(rdata.data(), rdata.size());
    }
    return true;
  } else {
    left->mutable_raw_bytes()->assign(right.raw_bytes());
    if (right.has_merge_timestamp()) {
      left->mutable_merge_timestamp()->CopyFrom(right.merge_timestamp());
    }
    if (full_merge && IsTimeSeriesData(left->raw_bytes())) {
      ConsolidateTimeSeriesValue(left->mutable_raw_bytes(), logger);
    }
    return true;
  }
}


// MergeResult serializes the result MVCCMetadata value into a byte slice.
DBStatus MergeResult(cockroach::storage::engine::MVCCMetadata* meta, DBString* result) {
  // TODO(pmattis): Should recompute checksum here. Need a crc32
  // implementation and need to verify the checksumming is identical
  // to what is being done in Go. Zlib's crc32 should be sufficient.
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
    return MergeValues(meta, operand_meta, full_merge, logger);
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
  std::string const key;

  DBGetter(rocksdb::DB *const r, rocksdb::ReadOptions opts, std::string &&k)
      : rep(r),
        options(opts),
        key(std::move(k)) {
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
  BaseDeltaIterator(rocksdb::Iterator* base_iterator,
                    rocksdb::WBWIIterator* delta_iterator,
                    const rocksdb::Slice* upper_bound)
      : current_at_base_(true),
        equal_keys_(false),
        done_(false),
        status_(rocksdb::Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        upper_bound_(upper_bound) {
    merged_.data = NULL;
  }

  virtual ~BaseDeltaIterator() {
    ClearMerged();
  }

  bool Valid() const override {
    return !done_ && (current_at_base_ ? BaseValid() : DeltaValid());
  }

  void SeekToFirst() override {
    done_ = false;
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent();
  }

  void SeekToLast() override {
    done_ = false;
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent();
  }

  void Seek(const rocksdb::Slice& k) override {
    done_ = false;
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
    return current_at_base_ ? base_iterator_->key() : delta_key_;
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
    return kComparator.Compare(delta_iterator_->Entry().key,
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
    int compare = kComparator.Compare(delta_iterator_->Entry().key,
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
    // The contents of WBWIIterator.Entry() are only valid until the
    // next mutation to the write batch. So keep a copy of the key
    // we're pointing at.
    delta_key_ = delta_iterator_->Entry().key.ToString();
    DBStatus status = ProcessDeltaKey(&base, delta_iterator_.get(),
                                      delta_key_, &merged_);
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

  // CheckUpperBound checks the specified key against the iteration
  // upper-bound (if present), returning true if the key exceeds the
  // upper-bound and false otherwise.
  bool CheckUpperBound(const rocksdb::Slice key) {
    if (upper_bound_ != NULL &&
        kComparator.Compare(key, *upper_bound_) >= 0) {
      done_ = true;
      return true;
    }
    return false;
  }

  bool BaseValid() const {
    return base_iterator_->Valid();
  }

  bool DeltaValid() const {
    return delta_iterator_->Valid();
  }

  void UpdateCurrent() {
    ClearMerged();

    for (;;) {
      equal_keys_ = false;
      if (!BaseValid()) {
        // Base has finished.
        if (!DeltaValid()) {
          // Both base and delta have finished.
          return;
        }
        if (CheckUpperBound(delta_iterator_->Entry().key)) {
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
      if (compare > 0) {
        // Delta is greater than base.
        if (CheckUpperBound(base_iterator_->key())) {
          return;
        }
        current_at_base_ = true;
        return;
      }
      // Delta is less than or equal to base.
      if (CheckUpperBound(delta_iterator_->Entry().key)) {
        return;
      }
      if (compare == 0) {
        // Delta is equal to base.
        equal_keys_ = true;
      }
      if (!ProcessDelta()) {
        current_at_base_ = false;
        return;
      }

      // Delta is less than or equal to base and is a deletion
      // tombstone.
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
  bool done_;
  mutable rocksdb::Status status_;
  mutable DBString merged_;
  std::unique_ptr<rocksdb::Iterator> base_iterator_;
  std::unique_ptr<rocksdb::WBWIIterator> delta_iterator_;
  std::string delta_key_;
  const rocksdb::Slice* upper_bound_;
};

}  // namespace

DBBatch::DBBatch(DBEngine* db)
    : DBEngine(db->rep),
      batch(&kComparator),
      updates(0) {
}

DBStatus DBOpen(DBEngine **db, DBSlice dir, DBOptions db_opts) {
  // Divide the cache space into two levels: the fast row cache
  // and the slower but more space-efficient block cache.
  // TODO(bdarnell): do we need both? how much of each?
  // TODO(peter): disabled for now until benchmarks show improvement.
  const auto row_cache_size = 0 * db_opts.cache_size;
  const auto block_cache_size = db_opts.cache_size - row_cache_size;
  const int num_cache_shard_bits = 4;
  rocksdb::BlockBasedTableOptions table_options;
  if (block_cache_size > 0) {
    table_options.block_cache = rocksdb::NewLRUCache(
        block_cache_size, num_cache_shard_bits);
  }
  // Pass false for use_blocked_base_builder creates a per file
  // (sstable) filter instead of a per-block filter. The per file
  // filter can be consulted before going to the index which saves an
  // index lookup. The cost is an 4-bytes per key in memory during
  // compactions, which seems a small price to pay.
  table_options.filter_policy.reset(
      rocksdb::NewBloomFilterPolicy(10, false /* !block_based */));
  table_options.format_version = 2;

  rocksdb::ColumnFamilyOptions cf_options;
  cf_options.OptimizeLevelStyleCompaction(db_opts.memtable_budget);
  // OptimizeLevelStyleCompaction sets no-compression for L0 and
  // L1. Current benchmarks and tests show no benefit to doing this.
  for (int i = 0; i < cf_options.compression_per_level.size(); i++) {
    cf_options.compression_per_level[i] = rocksdb::kSnappyCompression;
  }

  rocksdb::Options options(rocksdb::DBOptions(), cf_options);
  options.allow_os_buffer = db_opts.allow_os_buffer;
  options.comparator = &kComparator;
  options.create_if_missing = true;
  options.info_log.reset(new DBLogger(db_opts.logging_enabled));
  options.merge_operator.reset(new DBMergeOperator);
  options.prefix_extractor.reset(new DBPrefixExtractor);
  options.statistics = rocksdb::CreateDBStatistics();
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  // File size settings: TODO(marc): investigate and determine better long-term settings:
  // https://github.com/cockroachdb/cockroach/issues/5852
  options.target_file_size_base = 64 << 20;
  options.target_file_size_multiplier = 8;
  options.max_bytes_for_level_base = 512 << 20;
  options.max_bytes_for_level_multiplier = 8;

  if (row_cache_size > 0) {
    options.row_cache = rocksdb::NewLRUCache(
        row_cache_size, num_cache_shard_bits);
  }

  // Register listener for tracking RocksDB stats.
  std::shared_ptr<DBEventListener> event_listener(new DBEventListener);
  options.listeners.emplace_back(event_listener);

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
  *db = new DBImpl(db_ptr, memenv.release(), table_options.block_cache, event_listener);
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

DBStatus DBCompact(DBEngine* db) {
  return ToDBStatus(db->rep->CompactRange(rocksdb::CompactRangeOptions(), NULL, NULL));
}

uint64_t DBApproximateSize(DBEngine* db, DBKey start, DBKey end) {
  std::string s = EncodeKey(start);
  std::string e = EncodeKey(end);
  const rocksdb::Range r(s, e);
  uint64_t result;
  db->rep->GetApproximateSizes(&r, 1, &result);
  return result;
}

DBStatus DBImpl::Put(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Put(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBBatch::Put(DBKey key, DBSlice value) {
  ++updates;
  batch.Put(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBSnapshot::Put(DBKey key, DBSlice value) {
  return FmtStatus("unsupported");
}

DBStatus DBPut(DBEngine* db, DBKey key, DBSlice value) {
  return db->Put(key, value);
}

DBStatus DBImpl::Merge(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Merge(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBBatch::Merge(DBKey key, DBSlice value) {
  ++updates;
  batch.Merge(EncodeKey(key), ToSlice(value));
  return kSuccess;
}

DBStatus DBSnapshot::Merge(DBKey key, DBSlice value) {
  return FmtStatus("unsupported");
}

DBStatus DBMerge(DBEngine* db, DBKey key, DBSlice value) {
  return db->Merge(key, value);
}

DBStatus DBImpl::Get(DBKey key, DBString* value) {
  DBGetter base(rep, read_opts, EncodeKey(key));
  return base.Get(value);
}

DBStatus DBBatch::Get(DBKey key, DBString* value) {
  DBGetter base(rep, read_opts, EncodeKey(key));
  if (updates == 0) {
    return base.Get(value);
  }
  std::unique_ptr<rocksdb::WBWIIterator> iter(batch.NewIterator());
  iter->Seek(base.key);
  return ProcessDeltaKey(&base, iter.get(), base.key, value);
}

DBStatus DBSnapshot::Get(DBKey key, DBString* value) {
  DBGetter base(rep, read_opts, EncodeKey(key));
  return base.Get(value);
}

DBStatus DBGet(DBEngine* db, DBKey key, DBString* value) {
  return db->Get(key, value);
}

DBStatus DBImpl::Delete(DBKey key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Delete(options, EncodeKey(key)));
}

DBStatus DBBatch::Delete(DBKey key) {
  ++updates;
  batch.Delete(EncodeKey(key));
  return kSuccess;
}

DBStatus DBSnapshot::Delete(DBKey key) {
  return FmtStatus("unsupported");
}

DBStatus DBDelete(DBEngine *db, DBKey key) {
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

DBIterator* DBImpl::NewIter(DBSlice prefix) {
  DBIterator* iter = new DBIterator(prefix);
  rocksdb::ReadOptions opts = read_opts;
  opts.iterate_upper_bound = iter->upper_bound();
  opts.total_order_seek = iter->upper_bound() == NULL;
  iter->rep.reset(rep->NewIterator(opts));
  return iter;
}

DBIterator* DBBatch::NewIter(DBSlice prefix) {
  DBIterator* iter = new DBIterator(prefix);
  rocksdb::ReadOptions opts = read_opts;
  opts.iterate_upper_bound = iter->upper_bound();
  opts.total_order_seek = iter->upper_bound() == NULL;
  rocksdb::Iterator* base = rep->NewIterator(opts);
  rocksdb::WBWIIterator* delta = batch.NewIterator();
  iter->rep.reset(new BaseDeltaIterator(base, delta, opts.iterate_upper_bound));
  return iter;
}

DBIterator* DBSnapshot::NewIter(DBSlice prefix) {
  DBIterator* iter = new DBIterator(prefix);
  rocksdb::ReadOptions opts = read_opts;
  opts.iterate_upper_bound = iter->upper_bound();
  opts.total_order_seek = iter->upper_bound() == NULL;
  iter->rep.reset(rep->NewIterator(opts));
  return iter;
}

// GetStats retrieves a subset of RocksDB stats that are relevant to
// CockroachDB.
DBStatus DBImpl::GetStats(DBStatsResult* stats) {
  const rocksdb::Options &opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics> &s = opts.statistics;

  std::string memtable_total_size;
  rep->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_total_size);

  std::string table_readers_mem_estimate;
  rep->GetProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem_estimate);

  stats->block_cache_hits = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
  stats->block_cache_misses = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
  stats->block_cache_usage = (int64_t)block_cache->GetUsage();
  stats->block_cache_pinned_usage = (int64_t)block_cache->GetPinnedUsage();
  stats->bloom_filter_prefix_checked =
    (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED);
  stats->bloom_filter_prefix_useful =
    (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL);
  stats->memtable_hits = (int64_t)s->getTickerCount(rocksdb::MEMTABLE_HIT);
  stats->memtable_misses = (int64_t)s->getTickerCount(rocksdb::MEMTABLE_MISS);
  stats->memtable_total_size = std::stoll(memtable_total_size);
  stats->flushes = (int64_t)event_listener->GetFlushes();
  stats->compactions = (int64_t)event_listener->GetCompactions();
  stats->table_readers_mem_estimate = std::stoll(table_readers_mem_estimate);
  return kSuccess;
}

DBStatus DBBatch::GetStats(DBStatsResult* stats) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::GetStats(DBStatsResult* stats) {
  return FmtStatus("unsupported");
}

DBIterator::DBIterator(DBSlice prefix)
    : upper_bound_str(EncodePrefixNextKey(prefix)),
      upper_bound_slice(upper_bound_str) {
}

DBIterator* DBNewIter(DBEngine* db, DBSlice prefix) {
  return db->NewIter(prefix);
}

void DBIterDestroy(DBIterator* iter) {
  delete iter;
}

DBIterState DBIterSeek(DBIterator* iter, DBKey key) {
  iter->rep->Seek(EncodeKey(key));
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

  if (!MergeValues(&meta, update_meta, true, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}

const int64_t kNanosecondPerSecond = 1e9;

inline int64_t age_factor(int64_t fromNS, int64_t toNS) {
  // Careful about implicit conversions here.
  // toNS/1e9 - fromNS/1e9 is not the same since
  // "1e9" is a double.
  return toNS/kNanosecondPerSecond - fromNS/kNanosecondPerSecond;
}

// TODO(tschottdorf): it's unfortunate that this method duplicates the logic
// in (*MVCCStats).AgeTo. Passing now_nanos in is semantically tricky if there
// is a chance that we run into values ahead of now_nanos. Instead, now_nanos
// should be taken as a hint but determined by the max timestamp encountered.
MVCCStatsResult MVCCComputeStats(
    DBIterator* iter, DBKey start, DBKey end, int64_t now_nanos) {
  MVCCStatsResult stats;
  memset(&stats, 0, sizeof(stats));

  rocksdb::Iterator *const iter_rep = iter->rep.get();
  iter_rep->Seek(EncodeKey(start));
  const std::string end_key = EncodeKey(end);

  cockroach::storage::engine::MVCCMetadata meta;
  std::string prev_key;
  bool first = false;

  for (; iter_rep->Valid() && kComparator.Compare(iter_rep->key(), end_key) < 0;
       iter_rep->Next()) {
    const rocksdb::Slice key = iter_rep->key();
    const rocksdb::Slice value = iter_rep->value();

    rocksdb::Slice decoded_key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(key, &decoded_key, &wall_time, &logical)) {
      stats.status = FmtStatus("unable to decode key");
      break;
    }

    const bool isSys = (rocksdb::Slice(decoded_key).compare(kKeyLocalMax) < 0);
    const bool isValue = (wall_time != 0 || logical != 0);
    const bool implicitMeta = isValue && decoded_key != prev_key;
    prev_key.assign(decoded_key.data(), decoded_key.size());

    if (implicitMeta) {
      // No MVCCMetadata entry for this series of keys.
      meta.Clear();
      meta.set_key_bytes(kMVCCVersionTimestampSize);
      meta.set_val_bytes(value.size());
      meta.set_deleted(value.size() == 0);
      meta.mutable_timestamp()->set_wall_time(wall_time);
    }

    if (!isValue || implicitMeta) {
      const int64_t meta_key_size = decoded_key.size() + 1;
      const int64_t meta_val_size = implicitMeta ? 0 : value.size();
      const int64_t total_bytes = meta_key_size + meta_val_size;
      first = true;

      if (!implicitMeta && !meta.ParseFromArray(value.data(), value.size())) {
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
          stats.gc_bytes_age += total_bytes * age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        stats.key_bytes += meta_key_size;
        stats.val_bytes += meta_val_size;
        stats.key_count++;
        if (meta.has_raw_bytes()) {
          stats.val_count++;
        }
      }
      if (!implicitMeta) {
        continue;
      }
    }

    const int64_t total_bytes = value.size() + kMVCCVersionTimestampSize;
    if (isSys) {
      stats.sys_bytes += total_bytes;
    } else {
      if (first) {
        first = false;
        if (!meta.deleted()) {
          stats.live_bytes += total_bytes;
        } else {
          stats.gc_bytes_age += total_bytes * age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        if (meta.has_txn()) {
          stats.intent_bytes += total_bytes;
          stats.intent_count++;
          stats.intent_age += age_factor(meta.timestamp().wall_time(), now_nanos);
        }
        if (meta.key_bytes() != kMVCCVersionTimestampSize) {
          stats.status = FmtStatus("expected mvcc metadata val bytes to equal %d; got %d",
                                   kMVCCVersionTimestampSize, int(meta.key_bytes()));
          break;
        }
        if (meta.val_bytes() != value.size()) {
          stats.status = FmtStatus("expected mvcc metadata val bytes to equal %d; got %d",
                                   int(value.size()), int(meta.val_bytes()));
          break;
        }
      } else {
        stats.gc_bytes_age += total_bytes * age_factor(wall_time, now_nanos);
      }
      stats.key_bytes += kMVCCVersionTimestampSize;
      stats.val_bytes += value.size();
      stats.val_count++;
    }
  }

  stats.last_update_nanos = now_nanos;
  return stats;
}

// DBGetStats queries the given DBEngine for various operational stats and
// write them to the provided DBStatsResult instance.
DBStatus DBGetStats(DBEngine* db, DBStatsResult* stats) {
  return db->GetStats(stats);
}
