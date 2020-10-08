// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "merge.h"
#include <numeric>
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

  // Determine if we are using row or columnar format, by checking if either
  // format has a "last" column.
  bool use_column_format = left_ts.last_size() > 0 || right_ts.last_size() > 0;

  // If only a partial merge, do not sort and combine - instead, just quickly
  // merge the two values together. Values will be processed later after a
  // full merge.
  if (!full_merge) {
    // If using columnar format, convert both operands even in a partial merge.
    // This is necessary to keep the order of merges stable.
    if (use_column_format) {
      convertToColumnar(&left_ts);
      convertToColumnar(&right_ts);
    }
    left_ts.MergeFrom(right_ts);
    SerializeTimeSeriesToValue(left, left_ts);
    return true;
  }

  if (use_column_format) {
    // Convert from row format to column format if necessary.
    convertToColumnar(&left_ts);
    convertToColumnar(&right_ts);

    // Find the minimum offset of the right collection, and find the highest
    // index in the left collection which is greater than or equal to that
    // minimum. This determines how many elements of the left collection will
    // need to be re-sorted and de-duplicated.
    auto min_offset = std::min_element(right_ts.offset().begin(), right_ts.offset().end());
    auto first_unsorted_index = std::distance(
        left_ts.offset().begin(),
        std::lower_bound(left_ts.offset().begin(), left_ts.offset().end(), *min_offset));
    left_ts.MergeFrom(right_ts);
    sortAndDeduplicateColumns(&left_ts, first_unsorted_index);
    SerializeTimeSeriesToValue(left, left_ts);
  } else {
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
  }
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

  // Detect if the value is in columnar or row format. Columnar format is
  // detected by the presence of a non-zero-length offset field.
  if (val_ts.offset_size() > 0) {
    // It's possible that, due to partial merges, the right hand value contain
    // both row-format and column-format data. Convert it all to columnar.
    convertToColumnar(&val_ts);
    sortAndDeduplicateColumns(&val_ts, 0);
  } else {
    std::stable_sort(val_ts.mutable_samples()->pointer_begin(),
                     val_ts.mutable_samples()->pointer_end(), TimeSeriesSampleOrdering);

    // Deduplicate values, keeping only the *last* sample merged with a given index.
    using sample = cockroach::roachpb::InternalTimeSeriesSample;
    auto it =
        std::unique(val_ts.mutable_samples()->rbegin(), val_ts.mutable_samples()->rend(),
                    [](const sample& a, const sample& b) { return a.offset() == b.offset(); });
    val_ts.mutable_samples()->DeleteSubrange(
        0, std::distance(val_ts.mutable_samples()->begin(), it.base()));
  }

  // Serialize the new TimeSeriesData into the value's byte field.
  SerializeTimeSeriesToValue(val, val_ts);
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
    cockroach::storage::enginepb::MVCCMetadata meta;
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
    cockroach::storage::enginepb::MVCCMetadata meta;

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
  bool MergeOne(cockroach::storage::enginepb::MVCCMetadata* meta,
                const rocksdb::Slice& operand, bool full_merge,
                rocksdb::Logger* logger) const WARN_UNUSED_RESULT {
    cockroach::storage::enginepb::MVCCMetadata operand_meta;
    if (!operand_meta.ParseFromArray(operand.data(), operand.size())) {
      rocksdb::Warn(logger, "corrupted operand value");
      return false;
    }
    return MergeValues(meta, operand_meta, full_merge, logger);
  }
};

}  // namespace

// convertToColumnar detects time series data which is in the old row format,
// converting the data within into the new columnar format.
void convertToColumnar(cockroach::roachpb::InternalTimeSeriesData* data) {
  if (data->samples_size() > 0) {
    for (auto sample : data->samples()) {
      // While the row format contains other values (such as min and max), these
      // were not stored in actual usage. Furthermore, the new columnar format
      // has been designed to be "sparse", with high resolutions containing
      // values only for the "offset" and "last" columns. Thus, the other row
      // fields are ignored.
      data->add_offset(sample.offset());
      data->add_last(sample.sum());
    }
    data->clear_samples();
  }
}

// sortAndDeduplicateColumns sorts all column fields of the time series data
// structure according to the values "offset" column. At the same time,
// duplicate offset values are removed - only the last instance of an offset in
// the collection is retained.
//
// "first_unsorted" is an optimization which only sorts data rows with an index
// greater than or equal to the supplied index. This is used because the
// supplied data is often the result of merging an already-sorted collection
// with a smaller unsorted collection, and thus only a portion of the end of the
// data needs to be sorted.
void sortAndDeduplicateColumns(cockroach::roachpb::InternalTimeSeriesData* data,
                               int first_unsorted) {
  // Create an auxiliary array of array indexes, and sort that array according
  // to the corresponding offset value in the data.offset() collection. This
  // yields the permutation of the current array indexes that will place the
  // offsets into sorted order.
  auto order = std::vector<int>(data->offset_size() - first_unsorted);
  std::iota(order.begin(), order.end(), first_unsorted);
  std::stable_sort(order.begin(), order.end(),
                   [&](const int a, const int b) { return data->offset(a) < data->offset(b); });

  // Remove any duplicates from the permutation, keeping the *last* element
  // merged for any given offset. Note the number of duplicates removed so that
  // the columns can be resized later.
  auto it = std::unique(order.rbegin(), order.rend(), [&](const int a, const int b) {
    return data->offset(a) == data->offset(b);
  });
  int duplicates = std::distance(order.begin(), it.base());
  order.erase(order.begin(), it.base());

  // Apply the permutation in the auxiliary array to all of the relevant column
  // arrays in the data set.
  for (int i = 0; i < order.size(); i++) {
    // "dest_idx" is the current index which is being operated on; for each
    // column, we will be replacing the value at this index with the correct
    // sorted-order value for that index.
    //
    // "src_idx" is the current location of the value that is being moved to
    // dest_idx, found by consulting the "order" auxiliary array. Its value
    // will be *swapped* with the current value at "dest_idx".
    //
    // Because we are swapping values, and because we iterate through
    // destinations from front to back, it is possible that value that was
    // originally in "src_idx" has already been swapped to another location;
    // specifically, if "src_idx" is earlier than "dest_idx", then its value is
    // guaranteed to have been swapped. To find its current location, we
    // "follow" the indexes in the order array until we arrive at a src_idx
    // which is greater than the current dest_idx, which will be the correct
    // location of the source value.
    //
    // An example of this situation:
    //
    // initial:
    //    data = [3 1 4 2]
    //    order = [1 3 0 2]
    //
    // dest = 0
    //    src = order[0] // 1
    //    data.swap(dest, src) // 0 <-> 1
    //    data == [1 3 4 2]
    //
    // dest = 1
    //    src = order[1] // 3
    //    data.swap(dest, src) // 1 <-> 3
    //    data == [1 2 4 3]
    //
    // dest = 2
    //    src = order[2] // 0
    //    // src < dest, so follow the trail
    //    src = order[src] // 1
    //    // src < dest, so follow the trail
    //    src = order[src] // 3
    //    data.swap(dest, src) // 2 <-> 3
    //    data == [1 2 3 4]
    int dest_idx = i + first_unsorted;
    int src_idx = order[i];
    while (src_idx < dest_idx) {
      src_idx = order[src_idx - first_unsorted];
    }
    // If the source is equal to the destination, then this value is already
    // at its correct sorted location.
    if (src_idx == dest_idx) {
      continue;
    }

    data->mutable_offset()->SwapElements(src_idx, dest_idx);
    data->mutable_last()->SwapElements(src_idx, dest_idx);

    // These columns are only present at resolutions generated as rollups. We
    // detect this by checking if there are any count columns present (the
    // choice of "count" is arbitrary, all of these columns will be present or
    // not).
    if (data->count_size() > 0) {
      data->mutable_count()->SwapElements(src_idx, dest_idx);
      data->mutable_sum()->SwapElements(src_idx, dest_idx);
      data->mutable_min()->SwapElements(src_idx, dest_idx);
      data->mutable_max()->SwapElements(src_idx, dest_idx);
      data->mutable_first()->SwapElements(src_idx, dest_idx);
      data->mutable_variance()->SwapElements(src_idx, dest_idx);
    }
  }

  // Resize each column to account for any duplicate values which were removed -
  // the swapping algorithm will have moved these to the very end of the
  // collection.
  auto new_size = data->offset_size() - duplicates;
  data->mutable_offset()->Truncate(new_size);
  data->mutable_last()->Truncate(new_size);
  if (data->count_size() > 0) {
    data->mutable_count()->Truncate(new_size);
    data->mutable_sum()->Truncate(new_size);
    data->mutable_min()->Truncate(new_size);
    data->mutable_max()->Truncate(new_size);
    data->mutable_first()->Truncate(new_size);
    data->mutable_variance()->Truncate(new_size);
  }
}

WARN_UNUSED_RESULT bool MergeValues(cockroach::storage::enginepb::MVCCMetadata* left,
                                    const cockroach::storage::enginepb::MVCCMetadata& right,
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
DBStatus MergeResult(cockroach::storage::enginepb::MVCCMetadata* meta, DBString* result) {
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
