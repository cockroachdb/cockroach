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

#ifndef ROACHLIB_DB_H
#define ROACHLIB_DB_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// A DBSlice contains read-only data that does not need to be freed.
typedef struct {
  char* data;
  int len;
} DBSlice;

// A DBString is structurally identical to a DBSlice, but the data it
// contains must be freed via a call to free().
typedef struct {
  char* data;
  int len;
} DBString;

typedef struct {
  DBSlice key;
  int64_t wall_time;
  int32_t logical;
} DBKey;

typedef struct {
  bool valid;
  DBKey key;
  DBSlice value;
} DBIterState;

// A DBStatus is an alias for DBString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If DBStatus.data == NULL the operation succeeded.
typedef DBString DBStatus;

typedef struct DBEngine DBEngine;
typedef struct DBIterator DBIterator;

// DBOptions contains local database options.
typedef struct {
  uint64_t cache_size;
  uint64_t memtable_budget;
  bool allow_os_buffer;
  bool logging_enabled;
} DBOptions;

// Opens the database located in "dir", creating it if it doesn't
// exist.
DBStatus DBOpen(DBEngine **db, DBSlice dir, DBOptions options);

// Destroys the database located in "dir". As the name implies, this
// operation is destructive. Use with caution.
DBStatus DBDestroy(DBSlice dir);

// Closes the database, freeing memory and other resources.
void DBClose(DBEngine* db);

// Flushes all mem-table data to disk, blocking until the operation is
// complete.
DBStatus DBFlush(DBEngine* db);

// Forces an immediate compaction over all keys.
DBStatus DBCompact(DBEngine* db);

// Returns the approximate file system spaced used by keys in the
// range [start,end].
uint64_t DBApproximateSize(DBEngine* db, DBKey start, DBKey end);

// Sets the database entry for "key" to "value".
DBStatus DBPut(DBEngine* db, DBKey key, DBSlice value);

// Merge the database entry (if any) for "key" with "value".
DBStatus DBMerge(DBEngine* db, DBKey key, DBSlice value);

// Retrieves the database entry for "key".
DBStatus DBGet(DBEngine* db, DBKey key, DBString* value);

// Deletes the database entry for "key".
DBStatus DBDelete(DBEngine* db, DBKey key);

// Applies a batch of operations (puts, merges and deletes) to the
// database atomically. It is only valid to call this function on an
// engine created by DBNewBatch.
DBStatus DBWriteBatch(DBEngine* db);

// Creates a new snapshot of the database for use in DBGet() and
// DBNewIter(). It is the caller's responsibility to call DBClose().
DBEngine* DBNewSnapshot(DBEngine* db);

// Creates a new batch for performing a series of operations
// atomically. Use DBWriteBatch() on the returned engine to apply the
// batch to the database. It is the caller's responsibility to call
// DBClose().
DBEngine* DBNewBatch(DBEngine *db);

// Creates a new database iterator. When prefix is not empty, Seek
// will use the user-key prefix of the supplied MVCC key to restrict
// which sstables are searched, but iteration (using Next) over keys
// without the same user-key prefix will not work correctly (keys may
// be skipped). It is the caller's responsibility to call
// DBIterDestroy().
DBIterator* DBNewIter(DBEngine* db, DBSlice prefix);

// Destroys an iterator, freeing up any associated memory.
void DBIterDestroy(DBIterator* iter);

// Positions the iterator at the first key that is >= "key".
DBIterState DBIterSeek(DBIterator* iter, DBKey key);

// Positions the iterator at the first key in the database.
DBIterState DBIterSeekToFirst(DBIterator* iter);

// Positions the iterator at the last key in the database.
DBIterState DBIterSeekToLast(DBIterator* iter);

// Advances the iterator to the next key. After this call,
// DBIterValid() returns 1 iff the iterator was not positioned at the
// last key.
DBIterState DBIterNext(DBIterator* iter);

// Moves the iterator back to the previous key. After this call,
// DBIterValid() returns 1 iff the iterator was not positioned at the
// first key.
DBIterState DBIterPrev(DBIterator* iter);

// Returns any error associated with the iterator.
DBStatus DBIterError(DBIterator* iter);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

typedef struct {
  DBStatus status;
  int64_t live_bytes;
  int64_t key_bytes;
  int64_t val_bytes;
  int64_t intent_bytes;
  int64_t live_count;
  int64_t key_count;
  int64_t val_count;
  int64_t intent_count;
  int64_t intent_age;
  int64_t gc_bytes_age;
  int64_t sys_bytes;
  int64_t sys_count;
  int64_t last_update_nanos;
} MVCCStatsResult;

MVCCStatsResult MVCCComputeStats(DBIterator* iter, DBKey start, DBKey end, int64_t now_nanos);

// DBStatsResult contains various runtime stats for RocksDB.
typedef struct {
  int64_t block_cache_hits;
  int64_t block_cache_misses;
  size_t  block_cache_usage;
  size_t  block_cache_pinned_usage;
  int64_t bloom_filter_prefix_checked;
  int64_t bloom_filter_prefix_useful;
  int64_t memtable_hits;
  int64_t memtable_misses;
  int64_t memtable_total_size;
  int64_t flushes;
  int64_t compactions;
  int64_t table_readers_mem_estimate;
} DBStatsResult;

DBStatus DBGetStats(DBEngine* db, DBStatsResult* stats);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIB_DB_H

// local variables:
// mode: c++
// end:
