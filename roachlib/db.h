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
typedef void (*DBGCTimeoutsFunc)(void* state, int64_t* min_txn_ts, int64_t* min_rcache_ts);

typedef struct {
  int64_t cache_size;
  DBSlice txn_prefix;
  DBSlice rcache_prefix;
  DBLoggerFunc logger;
  DBGCTimeoutsFunc gc_timeouts;
  void* state;
} DBOptions;

DBStatus DBOpen(DBEngine **db, DBSlice dir, DBOptions options);
DBStatus DBDestroy(DBSlice dir);
void DBClose(DBEngine* db);
DBStatus DBFlush(DBEngine* db);
DBStatus DBCompactRange(DBEngine* db, DBSlice* start, DBSlice* end);
uint64_t DBApproximateSize(DBEngine* db, DBSlice start, DBSlice end);
DBStatus DBPut(DBEngine* db, DBSlice key, DBSlice value);
DBStatus DBMerge(DBEngine* db, DBSlice key, DBSlice value);
DBStatus DBGet(DBEngine* db, DBSnapshot* snapshot, DBSlice key, DBString* value);
DBStatus DBDelete(DBEngine* db, DBSlice key);
DBStatus DBWrite(DBEngine* db, DBBatch *batch);

DBSnapshot* DBNewSnapshot(DBEngine* db);
void DBSnapshotRelease(DBSnapshot* snapshot);

DBIterator* DBNewIter(DBEngine* db, DBSnapshot* snapshot);
void DBIterDestroy(DBIterator* iter);
void DBIterSeek(DBIterator* iter, DBSlice key);
void DBIterSeekToFirst(DBIterator* iter);
void DBIterSeekToLast(DBIterator* iter);
int  DBIterValid(DBIterator* iter);
void DBIterNext(DBIterator* iter);
DBSlice DBIterKey(DBIterator* iter);
DBSlice DBIterValue(DBIterator* iter);
DBStatus DBIterError(DBIterator* iter);

DBBatch* DBNewBatch();
void DBBatchDestroy(DBBatch* batch);
void DBBatchPut(DBBatch* batch, DBSlice key, DBSlice value);
void DBBatchMerge(DBBatch* batch, DBSlice key, DBSlice value);
void DBBatchDelete(DBBatch* batch, DBSlice key);

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIB_DB_H

// local variables:
// mode: c++
// end:
