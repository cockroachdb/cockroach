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
#include <rocksdb/utilities/write_batch_with_index.h>
#include "engine.h"

namespace cockroach {

struct DBBatch : public DBEngine {
  int updates;
  bool has_delete_range;
  rocksdb::WriteBatchWithIndex batch;

  DBBatch(DBEngine* db);
  virtual ~DBBatch();

  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus SingleDelete(DBKey key);
  virtual DBStatus DeleteRange(DBKey start, DBKey end);
  virtual DBStatus CommitBatch(bool sync);
  virtual DBStatus ApplyBatchRepr(DBSlice repr, bool sync);
  virtual DBSlice BatchRepr();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(DBIterOptions);
  virtual DBStatus GetStats(DBStatsResult* stats);
  virtual DBStatus GetTickersAndHistograms(DBTickersAndHistogramsResult* stats);
  virtual DBString GetCompactionStats();
  virtual DBStatus GetEnvStats(DBEnvStatsResult* stats);
  virtual DBStatus GetEncryptionRegistries(DBEncryptionRegistries* result);
  virtual DBStatus EnvWriteFile(DBSlice path, DBSlice contents);
  virtual DBStatus EnvOpenFile(DBSlice path, rocksdb::WritableFile** file);
  virtual DBStatus EnvReadFile(DBSlice path, DBSlice* contents);
  virtual DBStatus EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents);
  virtual DBStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvDeleteFile(DBSlice path);
  virtual DBStatus EnvDeleteDirAndFiles(DBSlice dir);
  virtual DBStatus EnvLinkFile(DBSlice oldname, DBSlice newname);
};

struct DBWriteOnlyBatch : public DBEngine {
  int updates;
  rocksdb::WriteBatch batch;

  DBWriteOnlyBatch(DBEngine* db);
  virtual ~DBWriteOnlyBatch();

  virtual DBStatus Put(DBKey key, DBSlice value);
  virtual DBStatus Merge(DBKey key, DBSlice value);
  virtual DBStatus Delete(DBKey key);
  virtual DBStatus SingleDelete(DBKey key);
  virtual DBStatus DeleteRange(DBKey start, DBKey end);
  virtual DBStatus CommitBatch(bool sync);
  virtual DBStatus ApplyBatchRepr(DBSlice repr, bool sync);
  virtual DBSlice BatchRepr();
  virtual DBStatus Get(DBKey key, DBString* value);
  virtual DBIterator* NewIter(DBIterOptions);
  virtual DBStatus GetStats(DBStatsResult* stats);
  virtual DBStatus GetTickersAndHistograms(DBTickersAndHistogramsResult* stats);
  virtual DBString GetCompactionStats();
  virtual DBString GetEnvStats(DBEnvStatsResult* stats);
  virtual DBStatus GetEncryptionRegistries(DBEncryptionRegistries* result);
  virtual DBStatus EnvWriteFile(DBSlice path, DBSlice contents);
  virtual DBStatus EnvOpenFile(DBSlice path, rocksdb::WritableFile** file);
  virtual DBStatus EnvReadFile(DBSlice path, DBSlice* contents);
  virtual DBStatus EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents);
  virtual DBStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvDeleteFile(DBSlice path);
  virtual DBStatus EnvDeleteDirAndFiles(DBSlice dir);
  virtual DBStatus EnvLinkFile(DBSlice oldname, DBSlice newname);
};

// GetDBBatchInserter returns a WriteBatch::Handler that operates on a
// WriteBatchBase. The caller assumes ownership of the returned handler.
::rocksdb::WriteBatch::Handler* GetDBBatchInserter(::rocksdb::WriteBatchBase* batch);

}  // namespace cockroach
