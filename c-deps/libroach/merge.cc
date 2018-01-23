// Copyright 2018 The Cockroach Authors.
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

#include "merge.h"
#include <rocksdb/env.h>
#include "db.h"
#include "protos/roachpb/data.pb.h"
#include "protos/roachpb/internal.pb.h"
#include "status.h"

namespace cockroach {

namespace {

const int kChecksumSize = 4;
const int kTagPos = kChecksumSize;
const int kHeaderSize = kTagPos + 1;

rocksdb::Slice ValueDataBytes(const std::string& val) {
  if (val.size() < kHeaderSize) {
    return rocksdb::Slice();
  }
  return rocksdb::Slice(val.data() + kHeaderSize, val.size() - kHeaderSize);
}

cockroach::roachpb::ValueType GetTag(const std::string& val) {
  if (val.size() < kHeaderSize) {
    return cockroach::roachpb::UNKNOWN;
  }
  return cockroach::roachpb::ValueType(val[kTagPos]);
}

void SetTag(std::string* val, cockroach::roachpb::ValueType tag) { (*val)[kTagPos] = tag; }

WARN_UNUSED_RESULT bool ParseProtoFromValue(const std::string& val,
                                            google::protobuf::MessageLite* msg) {
  if (val.size() < kHeaderSize) {
    return false;
  }
  const rocksdb::Slice d = ValueDataBytes(val);
  return msg->ParseFromArray(d.data(), d.size());
}

void SerializeProtoToValue(std::string* val, const google::protobuf::MessageLite& msg) {
  val->resize(kHeaderSize);
  std::fill(val->begin(), val->end(), 0);
  SetTag(val, cockroach::roachpb::BYTES);
  msg.AppendToString(val);
}

// Method used to sort InternalTimeSeriesSamples.
bool TimeSeriesSampleOrdering(const cockroach::roachpb::InternalTimeSeriesSample* a,
                              const cockroach::roachpb::InternalTimeSeriesSample* b) {
  return a->offset() < b->offset();
}

// IsTimeSeriesData returns true if the given protobuffer Value contains a
// TimeSeriesData message.
bool IsTimeSeriesData(const std::string& val) {
  return GetTag(val) == cockroach::roachpb::TIMESERIES;
}

void SerializeTimeSeriesToValue(std::string* val,
                                const cockroach::roachpb::InternalTimeSeriesData& ts) {
  SerializeProtoToValue(val, ts);
  SetTag(val, cockroach::roachpb::TIMESERIES);
}

// MergeTimeSeriesValues attempts to merge two Values which contain
// InternalTimeSeriesData messages. The messages cannot be merged if they have
// different start timestamps or sample durations. Returns true if the merge is
// successful.
WARN_UNUSED_RESULT bool MergeTimeSeriesValues(std::string* left, const std::string& right,
                                              bool full_merge, rocksdb::Logger* logger) {
  // Attempt to parse TimeSeriesData from both Values.
  cockroach::roachpb::InternalTimeSeriesData left_ts;
  cockroach::roachpb::InternalTimeSeriesData right_ts;
  if (!ParseProtoFromValue(*left, &left_ts)) {
    rocksdb::Warn(logger, "left InternalTimeSeriesData could not be parsed from bytes.");
    return false;
  }
  if (!ParseProtoFromValue(right, &right_ts)) {
    rocksdb::Warn(logger, "right InternalTimeSeriesData could not be parsed from bytes.");
    return false;
  }

  // Ensure that both InternalTimeSeriesData have the same timestamp and
  // sample_duration.
  if (left_ts.start_timestamp_nanos() != right_ts.start_timestamp_nanos()) {
    rocksdb::Warn(logger, "TimeSeries merge failed due to mismatched start timestamps");
    return false;
  }
  if (left_ts.sample_duration_nanos() != right_ts.sample_duration_nanos()) {
    rocksdb::Warn(logger, "TimeSeries merge failed due to mismatched sample durations.");
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
                   right_ts.mutable_samples()->pointer_end(), TimeSeriesSampleOrdering);

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
    } else if (left_front->offset() <= right_front->offset()) {
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
// of the value, keeping only the last of samples with duplicate offsets.
// This method is the single-value equivalent of MergeTimeSeriesValues, and is
// used in the case where the first value is merged into the key. Returns true
// if the merge is successful.
WARN_UNUSED_RESULT bool ConsolidateTimeSeriesValue(std::string* val, rocksdb::Logger* logger) {
  // Attempt to parse TimeSeriesData from both Values.
  cockroach::roachpb::InternalTimeSeriesData val_ts;
  if (!ParseProtoFromValue(*val, &val_ts)) {
    rocksdb::Warn(logger, "InternalTimeSeriesData could not be parsed from bytes.");
    return false;
  }

  // Initialize new_ts and its primitive data fields.
  cockroach::roachpb::InternalTimeSeriesData new_ts;
  new_ts.set_start_timestamp_nanos(val_ts.start_timestamp_nanos());
  new_ts.set_sample_duration_nanos(val_ts.sample_duration_nanos());

  // Sort values in the ts value.
  std::stable_sort(val_ts.mutable_samples()->pointer_begin(),
                   val_ts.mutable_samples()->pointer_end(), TimeSeriesSampleOrdering);

  // Consolidate sample values from the ts value with duplicate offsets.
  auto front = val_ts.samples().begin();
  auto end = val_ts.samples().end();

  // Loop until samples have been exhausted.
  while (front != end) {
    // Create an empty sample in the output collection.
    cockroach::roachpb::InternalTimeSeriesSample* ns = new_ts.add_samples();
    ns->set_offset(front->offset());
    while (front != end && front->offset() == ns->offset()) {
      // Only the last sample in the value's repeated samples field with a given
      // offset is kept in the case of multiple samples with identical offsets.
      ns->CopyFrom(*front);
      ++front;
    }
  }

  // Serialize the new TimeSeriesData into the value's byte field.
  SerializeTimeSeriesToValue(val, new_ts);
  return true;
}

class DBMergeOperator : public rocksdb::MergeOperator {
  virtual const char* Name() const { return "cockroach_merge_operator"; }

  virtual bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                         const std::deque<std::string>& operand_list, std::string* new_value,
                         rocksdb::Logger* logger) const WARN_UNUSED_RESULT {
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
    cockroach::storage::engine::enginepb::MVCCMetadata meta;
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

  virtual bool PartialMergeMulti(const rocksdb::Slice& key,
                                 const std::deque<rocksdb::Slice>& operand_list,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const WARN_UNUSED_RESULT {
    cockroach::storage::engine::enginepb::MVCCMetadata meta;

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
  bool MergeOne(cockroach::storage::engine::enginepb::MVCCMetadata* meta,
                const rocksdb::Slice& operand, bool full_merge,
                rocksdb::Logger* logger) const WARN_UNUSED_RESULT {
    cockroach::storage::engine::enginepb::MVCCMetadata operand_meta;
    if (!operand_meta.ParseFromArray(operand.data(), operand.size())) {
      rocksdb::Warn(logger, "corrupted operand value");
      return false;
    }
    return MergeValues(meta, operand_meta, full_merge, logger);
  }
};

}  // namespace

WARN_UNUSED_RESULT bool MergeValues(cockroach::storage::engine::enginepb::MVCCMetadata* left,
                                    const cockroach::storage::engine::enginepb::MVCCMetadata& right,
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
        rocksdb::Warn(logger, "inconsistent value types for merging time "
                              "series data (type(left) != type(right))");
        return false;
      }
      return MergeTimeSeriesValues(left->mutable_raw_bytes(), right.raw_bytes(), full_merge,
                                   logger);
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
      if (!ConsolidateTimeSeriesValue(left->mutable_raw_bytes(), logger)) {
        return false;
      }
    }
    return true;
  }
}

// MergeResult serializes the result MVCCMetadata value into a byte slice.
DBStatus MergeResult(cockroach::storage::engine::enginepb::MVCCMetadata* meta, DBString* result) {
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

rocksdb::MergeOperator* NewMergeOperator() { return new DBMergeOperator; }

}  // namespace cockroach
