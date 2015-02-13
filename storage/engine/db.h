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

#ifndef ROACHLIB_DB_H
#define ROACHLIB_DB_H

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

typedef struct DBBatch DBBatch;
typedef struct DBEngine DBEngine;
typedef struct DBIterator DBIterator;
typedef struct DBSnapshot DBSnapshot;

typedef void (*DBLoggerFunc)(void* state, const char* str, int len);

// DBOptions contains local database options.
typedef struct {
  int64_t cache_size;
  int allow_os_buffer;
  // A function pointer to direct log messages to.
  DBLoggerFunc logger;
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

// Sets GC timeouts.
void DBSetGCTimeouts(DBEngine * db, int64_t min_txn_ts, int64_t min_rcache_ts);

// Compacts the underlying storage for the key range
// [start,end]. start==NULL is treated as a key before all keys in the
// database. end==NULL is treated as a key after all keys in the
// database. DBCompactRange(db, NULL, NULL) will compact the entire
// database.
DBStatus DBCompactRange(DBEngine* db, DBSlice* start, DBSlice* end);

// Returns the approximate file system spaced used by keys in the
// range [start,end].
uint64_t DBApproximateSize(DBEngine* db, DBSlice start, DBSlice end);

// Sets the database entry for "key" to "value".
DBStatus DBPut(DBEngine* db, DBSlice key, DBSlice value);

// Merge the database entry (if any) for "key" with "value".
DBStatus DBMerge(DBEngine* db, DBSlice key, DBSlice value);

// Retrieves the database entry for "key" at the specified
// snapshot. If snapshot==NULL retrieves the current database entry
// for "key".
DBStatus DBGet(DBEngine* db, DBSnapshot* snapshot, DBSlice key, DBString* value);

// Deletes the database entry for "key".
DBStatus DBDelete(DBEngine* db, DBSlice key);

// Applies a batch of operations (puts, merges and deletes) to the
// database atomically.
DBStatus DBWrite(DBEngine* db, DBBatch *batch);

// Creates a new snapshot of the database for use in DBGet() and
// DBNewIter(). It is the callers responsibility to call
// DBSnapshotRelease().
DBSnapshot* DBNewSnapshot(DBEngine* db);

// Releases a snapshot, freeing up any associated memory and other
// resources.
void DBSnapshotRelease(DBSnapshot* snapshot);

// Creates a new database iterator. If snapshot==NULL the iterator
// will iterate over the current state of the database. It is the
// callers responsibility to call DBIterDestroy().
DBIterator* DBNewIter(DBEngine* db, DBSnapshot* snapshot);

// Destroys an iterator, freeing up any associated memory.
void DBIterDestroy(DBIterator* iter);

// Positions the iterator at the first key that is >= "key".
void DBIterSeek(DBIterator* iter, DBSlice key);

// Positions the iterator at the first key in the database.
void DBIterSeekToFirst(DBIterator* iter);

// Positions the iterator at the last key in the database.
void DBIterSeekToLast(DBIterator* iter);

// Returns 1 if the iterator is positioned at a valid key/value pair
// and 0 otherwise.
int  DBIterValid(DBIterator* iter);

// Advances the iterator to the next key. After this call,
// DBIterValid() returns 1 iff the iterator was not positioned at the
// last key.
void DBIterNext(DBIterator* iter);

// Returns the key at the current iterator position. Note that a slice
// is returned and the memory does not have to be freed.
DBSlice DBIterKey(DBIterator* iter);

// Returns the value at the current iterator position. Note that a
// slice is returned and the memory does not have to be freed.
DBSlice DBIterValue(DBIterator* iter);

// Returns any error associated with the iterator.
DBStatus DBIterError(DBIterator* iter);

// Creates a new batch for performing a series of operations
// atomically. Use DBWrite() to apply the batch to a database.
DBBatch* DBNewBatch();

// Destroys a batch, freeing any associated memory.
void DBBatchDestroy(DBBatch* batch);

// Sets the database entry for "key" to "value".
void DBBatchPut(DBBatch* batch, DBSlice key, DBSlice value);

// Merge the database entry (if any) for "key" with "value".
void DBBatchMerge(DBBatch* batch, DBSlice key, DBSlice value);

// Deletes the database entry for "key".
void DBBatchDelete(DBBatch* batch, DBSlice key);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIB_DB_H

// local variables:
// mode: c++
// end:
