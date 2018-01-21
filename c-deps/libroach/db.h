// Copyright 2017 The Cockroach Authors.
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

#pragma once

#include <libroach.h>
#include <memory>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/write_batch_base.h>

struct DBIterator {
  std::unique_ptr<rocksdb::Iterator> rep;
  std::unique_ptr<rocksdb::WriteBatch> kvs;
  std::unique_ptr<rocksdb::WriteBatch> intents;
};

namespace cockroach {

// DBOpenHook is called at the beginning of DBOpen. It can be implemented in CCL code.
rocksdb::Status DBOpenHook(const std::string& db_dir, const DBOptions opts);

// ToDBSlice returns a DBSlice from a rocksdb::Slice
inline DBSlice ToDBSlice(const rocksdb::Slice& s) {
  DBSlice result;
  result.data = const_cast<char*>(s.data());
  result.len = s.size();
  return result;
}

inline DBSlice ToDBSlice(const DBString& s) {
  DBSlice result;
  result.data = s.data;
  result.len = s.len;
  return result;
}

// ToDBString converts a rocksdb::Slice to a DBString.
inline DBString ToDBString(const rocksdb::Slice& s) {
  DBString result;
  result.len = s.size();
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s.data(), s.size());
  return result;
}

// ToString converts a DBSlice/DBString to a C++ string.
inline std::string ToString(DBSlice s) { return std::string(s.data, s.len); }
inline std::string ToString(DBString s) { return std::string(s.data, s.len); }

// ToSlice converts a DBSlice/DBString to a rocksdb::Slice.
inline rocksdb::Slice ToSlice(DBSlice s) { return rocksdb::Slice(s.data, s.len); }
inline rocksdb::Slice ToSlice(DBString s) { return rocksdb::Slice(s.data, s.len); }

// MVCC keys are encoded as <key>[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k);

// CockroachComparator returns CockroachDB's custom mvcc-aware RocksDB
// comparator. The caller does not assume ownership.
const ::rocksdb::Comparator* CockroachComparator();

// GetDBBatchInserter returns a WriteBatch::Handler that operates on a
// WriteBatchBase. The caller assumes ownership of the returned handler.
::rocksdb::WriteBatch::Handler* GetDBBatchInserter(::rocksdb::WriteBatchBase* batch);

// MVCCComputeStatsInternal returns the mvcc stats of the data in an iterator.
// Stats are only computed for keys between the given range.
MVCCStatsResult MVCCComputeStatsInternal(::rocksdb::Iterator* const iter_rep, DBKey start,
                                         DBKey end, int64_t now_nanos);

}  // namespace cockroach
