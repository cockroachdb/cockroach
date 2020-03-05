// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <memory>
#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/metadata.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

namespace cockroach {

struct EnvManager;

typedef rocksdb::Status(DBOpenHook)(std::shared_ptr<rocksdb::Logger> info_log,
                                    const std::string& db_dir, const DBOptions opts,
                                    EnvManager* env_mgr);

DBOpenHook DBOpenHookOSS;

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

// ToDBKey converts a rocksb::Slice to a DBKey.
DBKey ToDBKey(const rocksdb::Slice& s);

// ToString converts a DBSlice/DBString to a C++ string.
inline std::string ToString(DBSlice s) { return std::string(s.data, s.len); }
inline std::string ToString(DBString s) { return std::string(s.data, s.len); }

// ToSlice converts a DBSlice/DBString to a rocksdb::Slice.
// Unfortunately, rocksdb::Slice represents empty slice with an
// empty string (rocksdb::Slice("", 0)), as opposed to nullptr.
// This is problematic since rocksdb has various
// assertions checking for slice.data != nullptr.
inline rocksdb::Slice ToSlice(DBSlice s) { return rocksdb::Slice(s.data == nullptr ? "" : s.data, s.len); }
inline rocksdb::Slice ToSlice(DBString s) { return rocksdb::Slice(s.data == nullptr ? "" : s.data, s.len); }

// MVCCComputeStatsInternal returns the mvcc stats of the data in an iterator.
// Stats are only computed for keys between the given range.
MVCCStatsResult MVCCComputeStatsInternal(::rocksdb::Iterator* const iter_rep, DBKey start,
                                         DBKey end, int64_t now_nanos);

// ScopedStats wraps an iterator and, if that iterator has the stats
// member populated, aggregates a subset of the RocksDB perf counters
// into it (while the ScopedStats is live).
class ScopedStats {
 public:
  ScopedStats(DBIterator*);
  ~ScopedStats();

 private:
  DBIterator* const iter_;
  uint64_t internal_delete_skipped_count_base_;
};

// BatchSStables batches the supplied sstable metadata into chunks of
// sstables that are target_size. An empty start or end key indicates
// that the a compaction from the beginning (or end) of the key space
// should be provided. The sstable metadata must already be sorted by
// smallest key.
void BatchSSTablesForCompaction(const std::vector<rocksdb::SstFileMetaData>& sst,
                                rocksdb::Slice start_key, rocksdb::Slice end_key,
                                uint64_t target_size, std::vector<rocksdb::Range>* ranges);

}  // namespace cockroach
