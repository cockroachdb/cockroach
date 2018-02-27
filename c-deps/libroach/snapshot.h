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

#include <libroach.h>
#include <rocksdb/db.h>
#include "engine.h"

namespace cockroach {

struct DBSnapshot : public DBEngine {
  const rocksdb::Snapshot* snapshot;

  DBSnapshot(DBEngine* db) : DBEngine(db->rep, db->iters), snapshot(db->rep->GetSnapshot()) {}
  virtual ~DBSnapshot();

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
