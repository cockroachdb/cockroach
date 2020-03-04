// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "table_props.h"
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/types.h>
#include "encoding.h"

namespace cockroach {

namespace {

// This is re-implemented in sst_writer.go and should be kept in sync.
class TimeBoundTblPropCollector : public rocksdb::TablePropertiesCollector {
 public:
  const char* Name() const override { return "TimeBoundTblPropCollector"; }

  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override {
    if (!last_value_.empty()) {
      // Check to see if an intent was the last key in the SSTable. If
      // it was, we need to extract the timestamp from the intent and
      // update the bounds to include that timestamp.
      cockroach::storage::enginepb::MVCCMetadata meta;
      if (!meta.ParseFromArray(last_value_.data(), last_value_.size())) {
        // We're unable to parse the MVCCMetadata. Fail open by not
        // setting the min/max timestamp properties.
        return rocksdb::Status::OK();
      }
      if (meta.has_txn()) {
        // We have an intent, use the intent's timestamp to update the
        // timestamp bounds.
        std::string ts;
        EncodeTimestamp(ts, meta.timestamp().wall_time(), meta.timestamp().logical());
        UpdateBounds(ts);
      }
    }

    *properties = rocksdb::UserCollectedProperties{
        {"crdb.ts.min", ts_min_},
        {"crdb.ts.max", ts_max_},
    };
    return rocksdb::Status::OK();
  }

  rocksdb::Status AddUserKey(const rocksdb::Slice& user_key, const rocksdb::Slice& value,
                             rocksdb::EntryType type, rocksdb::SequenceNumber seq,
                             uint64_t file_size) override {
    rocksdb::Slice unused;
    rocksdb::Slice ts;
    if (!SplitKey(user_key, &unused, &ts)) {
      return rocksdb::Status::OK();
    }

    if (!ts.empty()) {
      last_value_.clear();
      ts.remove_prefix(1);  // The NUL prefix.
      UpdateBounds(ts);
      return rocksdb::Status::OK();
    }

    last_value_.assign(value.data(), value.size());
    return rocksdb::Status::OK();
  }

  virtual rocksdb::UserCollectedProperties GetReadableProperties() const override {
    return rocksdb::UserCollectedProperties{};
  }

 private:
  void UpdateBounds(rocksdb::Slice ts) {
    if (ts_max_.empty() || ts.compare(ts_max_) > 0) {
      ts_max_.assign(ts.data(), ts.size());
    }
    if (ts_min_.empty() || ts.compare(ts_min_) < 0) {
      ts_min_.assign(ts.data(), ts.size());
    }
  }

 private:
  std::string ts_min_;
  std::string ts_max_;
  std::string last_value_;
};

class TimeBoundTblPropCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
 public:
  explicit TimeBoundTblPropCollectorFactory() {}
  virtual rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override {
    return new TimeBoundTblPropCollector();
  }
  const char* Name() const override { return "TimeBoundTblPropCollectorFactory"; }
};

class DeleteRangeTblPropCollector : public rocksdb::TablePropertiesCollector {
 public:
  const char* Name() const override { return "DeleteRangeTblPropCollector"; }

  rocksdb::Status Finish(rocksdb::UserCollectedProperties*) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status AddUserKey(const rocksdb::Slice&, const rocksdb::Slice&, rocksdb::EntryType type,
                             rocksdb::SequenceNumber, uint64_t) override {
    if (type == rocksdb::kEntryRangeDeletion) {
      ntombstones_++;
    }
    return rocksdb::Status::OK();
  }

  virtual rocksdb::UserCollectedProperties GetReadableProperties() const override {
    return rocksdb::UserCollectedProperties{};
  }

  virtual bool NeedCompact() const override {
    // NB: Mark any file containing range deletions as requiring a
    // compaction. This ensures that range deletions are quickly compacted out
    // of existence.
    return ntombstones_ > 0;
  }

 private:
  int ntombstones_ = 0;
};

class DeleteRangeTblPropCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
  virtual rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override {
    return new DeleteRangeTblPropCollector();
  }
  const char* Name() const override { return "DeleteRangeTblPropCollectorFactory"; }
};

}  // namespace

rocksdb::TablePropertiesCollectorFactory* DBMakeTimeBoundCollector() {
  return new TimeBoundTblPropCollectorFactory();
}

rocksdb::TablePropertiesCollectorFactory* DBMakeDeleteRangeCollector() {
  return new DeleteRangeTblPropCollectorFactory();
}

}  // namespace cockroach
