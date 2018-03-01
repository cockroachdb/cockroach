// Copyright 2018 The Cockroach Authors.
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

#include <atomic>
#include <libroach.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/statistics.h>
#include "eventlistener.h"

struct DBEngine {
  rocksdb::DB* const rep;
  std::atomic<int64_t>* iters;

  DBEngine(rocksdb::DB* r, std::atomic<int64_t>* iters) : rep(r), iters(iters) {}
  virtual ~DBEngine();

  virtual DBStatus AssertPreClose();
  virtual DBStatus Put(DBKey key, DBSlice value) = 0;
  virtual DBStatus Merge(DBKey key, DBSlice value) = 0;
  virtual DBStatus Delete(DBKey key) = 0;
  virtual DBStatus DeleteRange(DBKey start, DBKey end) = 0;
  virtual DBStatus CommitBatch(bool sync) = 0;
  virtual DBStatus ApplyBatchRepr(DBSlice repr, bool sync) = 0;
  virtual DBSlice BatchRepr() = 0;
  virtual DBStatus Get(DBKey key, DBString* value) = 0;
  virtual DBIterator* NewIter(rocksdb::ReadOptions*) = 0;
  virtual DBStatus GetStats(DBStatsResult* stats) = 0;
  virtual DBString GetCompactionStats() = 0;
  virtual DBStatus EnvWriteFile(DBSlice path, DBSlice contents) = 0;

  DBSSTable* GetSSTables(int* n);
  DBString GetUserProperties();
};

namespace cockroach {

struct DBImpl : public DBEngine {
  std::unique_ptr<rocksdb::Env> switching_env;
  std::unique_ptr<rocksdb::Env> memenv;
  std::unique_ptr<rocksdb::DB> rep_deleter;
  std::shared_ptr<rocksdb::Cache> block_cache;
  std::shared_ptr<DBEventListener> event_listener;
  std::atomic<int64_t> iters_count;

  // Construct a new DBImpl from the specified DB.
  // The DB and passed Envs will be deleted when the DBImpl is deleted.
  // Either env can be NULL.
  DBImpl(rocksdb::DB* r, rocksdb::Env* m, std::shared_ptr<rocksdb::Cache> bc,
         std::shared_ptr<DBEventListener> event_listener, rocksdb::Env* s_env);
  virtual ~DBImpl();

  virtual DBStatus AssertPreClose();
  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus DeleteRange(DBKey start, DBKey end);
  virtual DBStatus CommitBatch(bool sync);
  virtual DBStatus ApplyBatchRepr(DBSlice repr, bool sync);
  virtual DBSlice BatchRepr();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(rocksdb::ReadOptions*);
  virtual DBStatus GetStats(DBStatsResult* stats);
  virtual DBString GetCompactionStats();
  virtual DBStatus EnvWriteFile(DBSlice path, DBSlice contents);
};

}  // namespace cockroach
