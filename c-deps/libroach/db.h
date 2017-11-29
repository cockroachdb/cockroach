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

#include <libroach.h>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/write_batch_base.h>

const DBStatus kSuccess = {NULL, 0};

struct DBIterator {
  std::unique_ptr<rocksdb::Iterator> rep;
};

bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, int64_t* wall_time, int32_t* logical);

// ToString returns a c++ string with the contents of a DBSlice.
std::string ToString(DBSlice s);

rocksdb::Slice ToSlice(DBSlice s);

// ToDBString converts a slice to a data+len DBString.
DBString ToDBString(const rocksdb::Slice& s);

DBSlice ToDBSlice(const rocksdb::Slice& s);

// MVCC keys are encoded as <key>[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
std::string EncodeKey(DBKey k);

// ToDBStatus converts a rocksdb Status to a DBStatus.
DBStatus ToDBStatus(const rocksdb::Status& status);

// FmtStatus formats the given arguments printf-style into a DBStatus.
DBStatus FmtStatus(const char* fmt, ...);

// CockroachComparator returns CockroachDB's custom mvcc-aware RocksDB
// comparator. The caller does not assume ownership.
const ::rocksdb::Comparator* CockroachComparator();

// GetDBBatchInserter returns a WriteBatch::Handler that operates on a
// WriteBatchBase. The caller assumes ownership of the returned handler.
::rocksdb::WriteBatch::Handler* GetDBBatchInserter(::rocksdb::WriteBatchBase* batch);

// MVCCComputeStatsInternal returns the mvcc stats of the data in an iterator.
// Stats are only computed for keys between the given range.
MVCCStatsResult MVCCComputeStatsInternal(::rocksdb::Iterator* const iter_rep, DBKey start, DBKey end,
                                         int64_t now_nanos);

// DBSstFileWriterAddRaw is used internally and in ccl -- DBSstFileWriterAdd is
// the preferred function for Go callers.
DBStatus DBSstFileWriterAddRaw(DBSstFileWriter* fw, rocksdb::Slice key, rocksdb::Slice val);

extern "C" {
char* __attribute__((weak)) prettyPrintKey(DBKey);
}  // extern "C"

const char* DBIterAdvance(DBIterator* iter, bool skip_current_key_versions);
