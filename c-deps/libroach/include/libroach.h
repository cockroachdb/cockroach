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

#pragma once

#include <stdbool.h>
#include <stddef.h>
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

// A DBStatus is an alias for DBString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If DBStatus.data == NULL the operation succeeded.
typedef DBString DBStatus;

typedef struct {
  DBSlice key;
  int64_t wall_time;
  int32_t logical;
} DBKey;

typedef struct {
  int64_t wall_time;
  int32_t logical;
} DBTimestamp;

typedef struct {
  bool valid;
  DBKey key;
  DBSlice value;
  DBStatus status;
} DBIterState;

typedef struct DBCache DBCache;
typedef struct DBEngine DBEngine;
typedef struct DBIterator DBIterator;

// DBOptions contains local database options.
typedef struct {
  DBCache* cache;
  bool logging_enabled;
  int num_cpu;
  int max_open_files;
  bool use_switching_env;
  bool must_exist;
  bool read_only;
  DBSlice rocksdb_options;
  DBSlice extra_options;
} DBOptions;

// Create a new cache with the specified size.
DBCache* DBNewCache(uint64_t size);

// Add a reference to an existing cache. Note that the underlying
// RocksDB cache is shared between the original and new reference.
DBCache* DBRefCache(DBCache* cache);

// Release a cache, decrementing the reference count on the underlying
// RocksDB cache. Note that the RocksDB cache will not be freed until
// all of the references have been released.
void DBReleaseCache(DBCache* cache);

// Opens the database located in "dir", creating it if it doesn't
// exist.
DBStatus DBOpen(DBEngine** db, DBSlice dir, DBOptions options);

// Destroys the database located in "dir". As the name implies, this
// operation is destructive. Use with caution.
DBStatus DBDestroy(DBSlice dir);

// Closes the database, freeing memory and other resources.
DBStatus DBClose(DBEngine* db);

// Flushes all mem-table data to disk, blocking until the operation is
// complete.
DBStatus DBFlush(DBEngine* db);

// Syncs the RocksDB WAL ensuring all data is persisted to
// disk. Blocks until the operation is complete.
DBStatus DBSyncWAL(DBEngine* db);

// Forces an immediate compaction over all keys.
DBStatus DBCompact(DBEngine* db);

// Forces an immediate compaction over keys in the specified range.
// Note that if start is empty, it indicates the start of the database.
// If end is empty, it indicates the end of the database.
DBStatus DBCompactRange(DBEngine* db, DBSlice start, DBSlice end, bool force_bottommost);

// Stores the approximate on-disk size of the given key range into the
// supplied uint64.
DBStatus DBApproximateDiskBytes(DBEngine* db, DBKey start, DBKey end, uint64_t* size);

// Sets the database entry for "key" to "value".
DBStatus DBPut(DBEngine* db, DBKey key, DBSlice value);

// Merge the database entry (if any) for "key" with "value".
DBStatus DBMerge(DBEngine* db, DBKey key, DBSlice value);

// Retrieves the database entry for "key".
DBStatus DBGet(DBEngine* db, DBKey key, DBString* value);

// Deletes the database entry for "key".
DBStatus DBDelete(DBEngine* db, DBKey key);

// Deletes a range of keys from start (inclusive) to end (exclusive).
DBStatus DBDeleteRange(DBEngine* db, DBKey start, DBKey end);

// Deletes a range of keys from start (inclusive) to end
// (exclusive). Unlike DBDeleteRange, this function finds the keys to
// delete by iterating over the supplied iterator and creating
// tombstones for the individual keys.
DBStatus DBDeleteIterRange(DBEngine* db, DBIterator* iter, DBKey start, DBKey end);

// Applies a batch of operations (puts, merges and deletes) to the
// database atomically and closes the batch. It is only valid to call
// this function on an engine created by DBNewBatch. If an error is
// returned, the batch is not closed and it is the caller's
// responsibility to call DBClose.
DBStatus DBCommitAndCloseBatch(DBEngine* db, bool sync);

// ApplyBatchRepr applies a batch of mutations encoded using that
// batch representation returned by DBBatchRepr(). It is only valid to
// call this function on an engine created by DBOpen() or DBNewBatch()
// (i.e. not a snapshot).
DBStatus DBApplyBatchRepr(DBEngine* db, DBSlice repr, bool sync);

// Returns the internal batch representation. The returned value is
// only valid until the next call to a method using the DBEngine and
// should thus be copied immediately. It is only valid to call this
// function on an engine created by DBNewBatch.
DBSlice DBBatchRepr(DBEngine* db);

// Creates a new snapshot of the database for use in DBGet() and
// DBNewIter(). It is the caller's responsibility to call DBClose().
DBEngine* DBNewSnapshot(DBEngine* db);

// Creates a new batch for performing a series of operations
// atomically. Use DBCommitBatch() on the returned engine to apply the
// batch to the database. The writeOnly parameter controls whether the
// batch can be used for reads or only for writes. A writeOnly batch
// does not need to index keys for reading and can be faster if the
// number of keys is large (and reads are not necessary). It is the
// caller's responsibility to call DBClose().
DBEngine* DBNewBatch(DBEngine* db, bool writeOnly);

// Creates a new database iterator. When prefix is true, Seek will use
// the user-key prefix of the key supplied to DBIterSeek() to restrict
// which sstables are searched, but iteration (using Next) over keys
// without the same user-key prefix will not work correctly (keys may
// be skipped). It is the callers responsibility to call
// DBIterDestroy().
DBIterator* DBNewIter(DBEngine* db, bool prefix);

DBIterator* DBNewTimeBoundIter(DBEngine* db, DBTimestamp min_ts, DBTimestamp max_ts);

// Destroys an iterator, freeing up any associated memory.
void DBIterDestroy(DBIterator* iter);

// Positions the iterator at the first key that is >= "key".
DBIterState DBIterSeek(DBIterator* iter, DBKey key);

// Positions the iterator at the first key in the database.
DBIterState DBIterSeekToFirst(DBIterator* iter);

// Positions the iterator at the last key in the database.
DBIterState DBIterSeekToLast(DBIterator* iter);

// Advances the iterator to the next key. If skip_current_key_versions
// is true, any remaining versions for the current key are
// skipped. After this call, DBIterValid() returns 1 iff the iterator
// was not positioned at the last key.
DBIterState DBIterNext(DBIterator* iter, bool skip_current_key_versions);

// Moves the iterator back to the previous key. If
// skip_current_key_versions is true, any remaining versions for the
// current key are skipped. After this call, DBIterValid() returns 1
// iff the iterator was not positioned at the first key.
DBIterState DBIterPrev(DBIterator* iter, bool skip_current_key_versions);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

// NB: The function (cStatsToGoStats) that converts these to the go
// representation is unfortunately duplicated in engine and engineccl. If this
// struct is changed, both places need to be updated.
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

bool MVCCIsValidSplitKey(DBSlice key, bool allow_meta2_splits);
DBStatus MVCCFindSplitKey(DBIterator* iter, DBKey start, DBKey end, DBKey min_split,
                          int64_t target_size, bool allow_meta2_splits, DBString* split_key);

// DBTxn contains the fields from a roachpb.Transaction that are
// necessary for MVCC Get and Scan operations. Note that passing a
// serialized roachpb.Transaction appears to be a non-starter as an
// alternative due to the performance overhead.
//
// TODO(peter): We could investigate using
// https://github.com/petermattis/cppgo to generate C++ code that can
// read the Go roachpb.Transaction structure.
typedef struct {
  DBSlice id;
  uint32_t epoch;
  DBTimestamp max_timestamp;
} DBTxn;

typedef struct {
  DBSlice* bufs;
  // len is the number of DBSlices in bufs.
  int32_t len;
  // count is the number of key/value pairs in bufs.
  int32_t count;
} DBChunkedBuffer;

// DBScanResults contains the key/value pairs and intents encoded
// using the RocksDB batch repr format.
typedef struct {
  DBStatus status;
  DBChunkedBuffer data;
  DBSlice intents;
  DBTimestamp uncertainty_timestamp;
} DBScanResults;

DBScanResults MVCCGet(DBIterator* iter, DBSlice key, DBTimestamp timestamp, DBTxn txn,
                      bool consistent, bool tombstones);
DBScanResults MVCCScan(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp,
                       int64_t max_keys, DBTxn txn, bool consistent, bool reverse, bool tombstones);

// DBStatsResult contains various runtime stats for RocksDB.
typedef struct {
  int64_t block_cache_hits;
  int64_t block_cache_misses;
  size_t block_cache_usage;
  size_t block_cache_pinned_usage;
  int64_t bloom_filter_prefix_checked;
  int64_t bloom_filter_prefix_useful;
  int64_t memtable_total_size;
  int64_t flushes;
  int64_t compactions;
  int64_t table_readers_mem_estimate;
  int64_t pending_compaction_bytes_estimate;
} DBStatsResult;

DBStatus DBGetStats(DBEngine* db, DBStatsResult* stats);
DBString DBGetCompactionStats(DBEngine* db);

typedef struct {
  int level;
  uint64_t size;
  DBKey start_key;
  DBKey end_key;
} DBSSTable;

// Retrieve stats about all of the live sstables. Note that the tables
// array must be freed along with the start_key and end_key of each
// table.
DBSSTable* DBGetSSTables(DBEngine* db, int* n);

// DBGetUserProperties fetches the user properties stored in each sstable's
// metadata. These are returned as a serialized SSTUserPropertiesCollection
// proto.
DBString DBGetUserProperties(DBEngine* db);

// Bulk adds the file at the given path to a database. See the RocksDB
// documentation on `IngestExternalFile` for the various restrictions on what
// can be added. If move_file is true, the file will be moved instead of copied.
// If allow_file_modification is false, RocksDB will return an error if it would
// have tried to modify the file's sequence number rather than editing the file
// in place.
DBStatus DBIngestExternalFile(DBEngine* db, DBSlice path, bool move_file,
                              bool allow_file_modification);

typedef struct DBSstFileWriter DBSstFileWriter;

// Creates a new SstFileWriter with the default configuration.
DBSstFileWriter* DBSstFileWriterNew();

// Opens an in-memory file for output of an sstable.
DBStatus DBSstFileWriterOpen(DBSstFileWriter* fw);

// Adds a kv entry to the sstable being built. An error is returned if it is
// not greater than any previously added entry (according to the comparator
// configured during writer creation). `Open` must have been called. `Close`
// cannot have been called.
DBStatus DBSstFileWriterAdd(DBSstFileWriter* fw, DBKey key, DBSlice val);

// Finalizes the writer and stores the constructed file's contents in *data. At
// least one kv entry must have been added. May only be called once.
DBStatus DBSstFileWriterFinish(DBSstFileWriter* fw, DBString* data);

// Closes the writer and frees memory and other resources. May only be called
// once.
void DBSstFileWriterClose(DBSstFileWriter* fw);

void DBRunLDB(int argc, char** argv);

// DBEnvWriteFile writes the given data as a new "file" in the given engine.
DBStatus DBEnvWriteFile(DBEngine* db, DBSlice path, DBSlice contents);

// DBFileLock contains various parameters set during DBLockFile and required for DBUnlockFile.
typedef void* DBFileLock;

// DBLockFile sets a lock on the specified file using RocksDB's file locking interface.
DBStatus DBLockFile(DBSlice filename, DBFileLock* lock);

// DBUnlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for
// the lock.
DBStatus DBUnlockFile(DBFileLock lock);

#ifdef __cplusplus
}  // extern "C"
#endif
