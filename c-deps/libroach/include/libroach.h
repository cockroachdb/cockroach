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
  bool prefix;
  DBKey lower_bound;
  DBKey upper_bound;
  bool with_stats;
  DBTimestamp min_timestamp_hint;
  DBTimestamp max_timestamp_hint;
} DBIterOptions;

typedef struct {
  bool valid;
  DBKey key;
  DBSlice value;
  DBStatus status;
} DBIterState;

typedef struct DBCache DBCache;
typedef struct DBEngine DBEngine;
typedef struct DBIterator DBIterator;
typedef void* DBWritableFile;

// DBOptions contains local database options.
typedef struct {
  DBCache* cache;
  int num_cpu;
  int max_open_files;
  bool use_file_registry;
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

// Creates a RocksDB checkpoint in the specified directory (which must not exist).
// A checkpoint is a logical copy of the database, though it will hardlink the
// SSTs references by it (when possible), thus avoiding duplication of any of
// the actual data.
DBStatus DBCreateCheckpoint(DBEngine* db, DBSlice dir);

// Set a callback to be invoked during DBOpen that can make changes to RocksDB
// initialization. Used by CCL code to install additional features.
//
// The callback must be a pointer to a C++ function of type DBOpenHook. The type
// is declared in db.cc. It cannot be part of the public C API as it refers to
// C++ types.
void DBSetOpenHook(void* hook);

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

// Disable/enable automatic compactions. Automatic compactions are
// enabled by default. Disabling is provided for testing purposes so
// that automatic compactions do not interfere with test expectations.
DBStatus DBDisableAutoCompaction(DBEngine* db);
DBStatus DBEnableAutoCompaction(DBEngine* db);

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

// Deletes the most recent database entry for "key". See the following
// documentation for details on the subtleties of this operation:
// https://github.com/facebook/rocksdb/wiki/Single-Delete.
DBStatus DBSingleDelete(DBEngine* db, DBKey key);

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

// Creates a new database iterator.
//
// When iter_options.prefix is true, Seek will use the user-key prefix of the
// key supplied to DBIterSeek() to restrict which sstables are searched, but
// iteration (using Next) over keys without the same user-key prefix will not
// work correctly (keys may be skipped).
//
// When iter_options.upper_bound is non-nil, the iterator will become invalid
// after seeking past the provided upper bound. This can drastically improve
// performance when seeking within a region covered by range deletion
// tombstones. See #24029 for discussion.
//
// When iter_options.with_stats is true, the iterator will collect RocksDB
// performance counters which can be retrieved via `DBIterStats`.
//
// It is the caller's responsibility to call DBIterDestroy().
DBIterator* DBNewIter(DBEngine* db, DBIterOptions iter_options);

// Destroys an iterator, freeing up any associated memory.
void DBIterDestroy(DBIterator* iter);

// Positions the iterator at the first key that is >= "key".
DBIterState DBIterSeek(DBIterator* iter, DBKey key);

typedef struct {
  uint64_t internal_delete_skipped_count;
  // the number of SSTables touched (only for time bound iterators).
  // This field is populated from the table filter, not from the
  // RocksDB perf counters.
  //
  // TODO(tschottdorf): populate this field for all iterators.
  uint64_t timebound_num_ssts;
  // New fields added here must also be added in various other places;
  // just grep the repo for internal_delete_skipped_count. Sorry.
} IteratorStats;

IteratorStats DBIterStats(DBIterator* iter);

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

// DBIterSetLowerBound updates this iterator's lower bound.
void DBIterSetLowerBound(DBIterator* iter, DBKey key);

// DBIterSetUpperBound updates this iterator's upper bound.
void DBIterSetUpperBound(DBIterator* iter, DBKey key);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

// Implements the partial merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from Go code.
DBStatus DBPartialMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

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

bool MVCCIsValidSplitKey(DBSlice key);
DBStatus MVCCFindSplitKey(DBIterator* iter, DBKey start, DBKey end, DBKey min_split,
                          int64_t target_size, DBString* split_key);

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
  int32_t sequence;
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
  DBSlice resume_key;
} DBScanResults;

DBScanResults MVCCGet(DBIterator* iter, DBSlice key, DBTimestamp timestamp, DBTxn txn,
                      bool inconsistent, bool tombstones, bool ignore_sequence);
DBScanResults MVCCScan(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp,
                       int64_t max_keys, DBTxn txn, bool inconsistent, bool reverse,
                       bool tombstones, bool ignore_sequence);

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
  int64_t l0_file_count;
} DBStatsResult;

typedef struct {
  DBString name;
  uint64_t value;
} TickerInfo;

typedef struct {
  DBString name;
  double mean;
  double p50;
  double p95;
  double p99;
  double max;
  uint64_t count;
  uint64_t sum;
} HistogramInfo;

typedef struct {
  TickerInfo* tickers;
  size_t tickers_len;
  HistogramInfo* histograms;
  size_t histograms_len;
} DBTickersAndHistogramsResult;

// DBEnvStatsResult contains Env stats (filesystem layer).
typedef struct {
  // Basic file encryption stats:
  // Files/bytes across all rocksdb files.
  uint64_t total_files;
  uint64_t total_bytes;
  // Files/bytes using the active data key.
  uint64_t active_key_files;
  uint64_t active_key_bytes;
  // Enum of the encryption algorithm in use.
  int32_t encryption_type;
  // encryption status (CCL only).
  // This is a serialized enginepbccl/stats.proto:EncryptionStatus
  DBString encryption_status;
} DBEnvStatsResult;

// DBEncryptionRegistries contains file and key registries.
typedef struct {
  // File registry.
  DBString file_registry;
  // Key registry (with actual keys scrubbed).
  DBString key_registry;
} DBEncryptionRegistries;

DBStatus DBGetStats(DBEngine* db, DBStatsResult* stats);
DBStatus DBGetTickersAndHistograms(DBEngine* db, DBTickersAndHistogramsResult* stats);
DBString DBGetCompactionStats(DBEngine* db);
DBStatus DBGetEnvStats(DBEngine* db, DBEnvStatsResult* stats);
DBStatus DBGetEncryptionRegistries(DBEngine* db, DBEncryptionRegistries* result);

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

typedef struct {
  uint64_t log_number;
  uint64_t size;
} DBWALFile;

// Retrieve information about all of the write-ahead log files in order from
// oldest to newest. The files array must be freed.
DBStatus DBGetSortedWALFiles(DBEngine* db, DBWALFile** files, int* n);

// DBGetUserProperties fetches the user properties stored in each sstable's
// metadata. These are returned as a serialized SSTUserPropertiesCollection
// proto.
DBString DBGetUserProperties(DBEngine* db);

// Bulk adds the files at the given paths to a database, all atomically. See the
// RocksDB documentation on `IngestExternalFile` for the various restrictions on
// what can be added. If move_files is true, the files will be moved instead of
// copied. If allow_file_modifications is false, RocksDB will return an error if
// it would have tried to modify any of the files' sequence numbers rather than
// editing the files in place. If write_global_seqno is false, it will skip
// writing the seq_no to the SSTs -- this is only safe if this will only ever be
// read by Rocks version >= 5.16 as older versions would still be looking for
// the seqno.
DBStatus DBIngestExternalFiles(DBEngine* db, char** paths, size_t len, bool move_files,
                               bool write_global_seqno, bool allow_file_modifications);

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

// Adds a deletion tombstone to the sstable being built. See DBSstFileWriterAdd for more.
DBStatus DBSstFileWriterDelete(DBSstFileWriter* fw, DBKey key);

// Finalizes the writer and stores the constructed file's contents in *data. At
// least one kv entry must have been added. May only be called once.
DBStatus DBSstFileWriterFinish(DBSstFileWriter* fw, DBString* data);

// Closes the writer and frees memory and other resources. May only be called
// once.
void DBSstFileWriterClose(DBSstFileWriter* fw);

void DBRunLDB(int argc, char** argv);
void DBRunSSTDump(int argc, char** argv);

// DBEnvWriteFile writes the given data as a new "file" in the given engine.
DBStatus DBEnvWriteFile(DBEngine* db, DBSlice path, DBSlice contents);

// DBEnvOpenFile opens a DBWritableFile as a new "file" in the given engine.
DBStatus DBEnvOpenFile(DBEngine* db, DBSlice path, DBWritableFile* file);

// DBEnvReadFile reads the file with the given path in the given engine.
DBStatus DBEnvReadFile(DBEngine* db, DBSlice path, DBSlice* contents);

// DBEnvAppendFile appends the given data to the given DBWritableFile in the
// given engine.
DBStatus DBEnvAppendFile(DBEngine* db, DBWritableFile file, DBSlice contents);

// DBEnvSyncFile synchronously writes the data of the file to the disk.
DBStatus DBEnvSyncFile(DBEngine* db, DBWritableFile file);

// DBEnvCloseFile closes the given DBWritableFile in the given engine.
DBStatus DBEnvCloseFile(DBEngine* db, DBWritableFile file);

// DBEnvDeleteFile deletes the file with the given filename in the given engine.
DBStatus DBEnvDeleteFile(DBEngine* db, DBSlice path);

// DBEnvDeleteDirAndFiles deletes the directory with the given dir name and any
// files it contains but not subdirectories in the given engine.
DBStatus DBEnvDeleteDirAndFiles(DBEngine* db, DBSlice dir);

// DBEnvLinkFile creates 'newname' as a hard link to 'oldname using the given engine.
DBStatus DBEnvLinkFile(DBEngine* db, DBSlice oldname, DBSlice newname);

// DBFileLock contains various parameters set during DBLockFile and required for DBUnlockFile.
typedef void* DBFileLock;

// DBLockFile sets a lock on the specified file using RocksDB's file locking interface.
DBStatus DBLockFile(DBSlice filename, DBFileLock* lock);

// DBUnlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for
// the lock.
DBStatus DBUnlockFile(DBFileLock lock);

// DBExportToSst exports changes over the keyrange and time interval between the
// start and end DBKeys to an SSTable using an IncrementalIterator.
DBStatus DBExportToSst(DBKey start, DBKey end, bool export_all_revisions, DBIterOptions iter_opts,
                       DBEngine* engine, DBString* data, int64_t* entries, int64_t* data_size,
                       DBString* write_intent);

#ifdef __cplusplus
}  // extern "C"
#endif
