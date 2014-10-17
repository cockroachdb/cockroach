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
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "api.pb.h"
#include "data.pb.h"
#include "db.h"

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
  } else if (rwResp.has_accumulate_ts()) {
    return &rwResp.accumulate_ts().header();
  } else if (rwResp.has_reap_queue()) {
    return &rwResp.reap_queue().header();
  } else if (rwResp.has_enqueue_update()) {
    return &rwResp.enqueue_update().header();
  } else if (rwResp.has_enqueue_message()) {
    return &rwResp.enqueue_message().header();
  } else if (rwResp.has_internal_heartbeat_txn()) {
    return &rwResp.internal_heartbeat_txn().header();
  } else if (rwResp.has_internal_resolve_intent()) {
    return &rwResp.internal_resolve_intent().header();
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
  DBCompactionFilter(const std::string& txn_prefix,
                     const std::string& rcache_prefix,
                     int64_t min_txn_ts,
                     int64_t min_rcache_ts)
      : txn_prefix_(txn_prefix),
        rcache_prefix_(rcache_prefix),
        min_txn_ts_(min_txn_ts),
        min_rcache_ts_(min_rcache_ts) {
  }

  virtual bool Filter(int level,
                      const rocksdb::Slice& key,
                      const rocksdb::Slice& existing_value,
                      std::string* new_value,
                      bool* value_changed) const {
    *value_changed = false;

    // Response cache rows are GC'd if their timestamp is older than the
    // response cache GC timeout.
    if (key.starts_with(rcache_prefix_)) {
      proto::ReadWriteCmdResponse rwResp;
      if (!rwResp.ParseFromArray(existing_value.data(), existing_value.size())) {
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
    } else if (key.starts_with(txn_prefix_)) {
      // Transaction rows are GC'd if their timestamp is older than
      // the system-wide minimum write intent timestamp. This
      // system-wide minimum write intent is periodically computed via
      // map-reduce over all ranges and gossipped.
      proto::Transaction txn;
      if (!txn.ParseFromArray(existing_value.data(), existing_value.size())) {
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
  const std::string txn_prefix_;
  const std::string rcache_prefix_;
  const int64_t min_txn_ts_;
  const int64_t min_rcache_ts_;
};

class DBCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  DBCompactionFilterFactory(void* state,
                            const std::string& txn_prefix,
                            const std::string& rcache_prefix,
                            DBGCTimeoutsFunc f)
      : state_(state),
        txn_prefix_(txn_prefix),
        rcache_prefix_(rcache_prefix),
        func_(f) {
  }

  virtual std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    int64_t min_txn_ts;
    int64_t min_rcache_ts;
    func_(state_, &min_txn_ts, &min_rcache_ts);
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new DBCompactionFilter(txn_prefix_, rcache_prefix_,
                               min_txn_ts, min_rcache_ts));
  }

  virtual const char* Name() const override {
    return "cockroach_compaction_filter_factory";
  }

 private:
  void* const state_;
  const std::string txn_prefix_;
  const std::string rcache_prefix_;
  const DBGCTimeoutsFunc func_;
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

bool MergeValues(proto::Value *left, const proto::Value &right) {
  if (left->has_bytes()) {
    if (right.has_bytes()) {
      *left->mutable_bytes() += right.bytes();
      return true;
    }
  } else if (left->has_integer()) {
    if (right.has_integer()) {
      if (WillOverflow(left->integer(), right.integer())) {
        return false;
      }
      left->set_integer(left->integer() + right.integer());
      return true;
    }
  } else {
    *left = right;
    return true;
  }
  return false;
}

// MergeResult serializes the result Value into a byte slice.
DBStatus MergeResult(proto::Value* value, DBString* result) {
  // TODO(pmattis): Should recompute checksum here. Need a crc32
  // implementation and need to verify the checksumming is identical
  // to what is being done in Go. Zlib's crc32 should be sufficient.
  value->clear_checksum();
  result->len = value->ByteSize();
  result->data = static_cast<char*>(malloc(result->len));
  if (!value->SerializeToArray(result->data, result->len)) {
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

    proto::Value value;
    if (existing_value != NULL) {
      if (!value.ParseFromArray(existing_value->data(), existing_value->size())) {
        // Corrupted existing value.
        rocksdb::Warn(logger, "corrupted existing value");
        return false;
      }
    }

    for (int i = 0; i < operand_list.size(); i++) {
      if (!MergeOne(&value, operand_list[i], logger)) {
        return false;
      }
    }

    if (!value.SerializeToString(new_value)) {
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
    proto::Value value;

    for (int i = 0; i < operand_list.size(); i++) {
      if (!MergeOne(&value, operand_list[i], logger)) {
        return false;
      }
    }

    if (!value.SerializeToString(new_value)) {
      rocksdb::Warn(logger, "serialization error");
      return false;
    }
    return true;
  }

 private:
  bool MergeOne(proto::Value* value,
                const rocksdb::Slice& operand,
                rocksdb::Logger* logger) const {
    proto::Value operand_value;
    if (!operand_value.ParseFromArray(operand.data(), operand.size())) {
      rocksdb::Warn(logger, "corrupted operand value");
      return false;
    }
    return MergeValues(value, operand_value);
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
  rocksdb::Options options;
  options.block_cache = rocksdb::NewLRUCache(db_opts.cache_size);
  options.compaction_filter_factory.reset(
      new DBCompactionFilterFactory(
          db_opts.state,
          ToString(db_opts.txn_prefix),
          ToString(db_opts.rcache_prefix),
          db_opts.gc_timeouts));
  options.create_if_missing = true;
  options.info_log.reset(new DBLogger(db_opts.logger));
  options.merge_operator.reset(new DBMergeOperator);

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

  proto::Value value;
  if (!value.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  proto::Value update_value;
  if (!update_value.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(&value, update_value)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&value, new_value);
}
