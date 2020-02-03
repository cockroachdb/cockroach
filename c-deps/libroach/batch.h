// Copyright 2018 The Cockroach Authors.
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
  virtual DBStatus EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file);
  virtual DBStatus EnvReadFile(DBSlice path, DBSlice* contents);
  virtual DBStatus EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents);
  virtual DBStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvDeleteFile(DBSlice path);
  virtual DBStatus EnvDeleteDirAndFiles(DBSlice dir);
  virtual DBStatus EnvLinkFile(DBSlice oldname, DBSlice newname);
  virtual DBStatus EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file);
  virtual DBStatus EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer, int64_t offset,
                                 int* n);
  virtual DBStatus EnvCloseReadableFile(rocksdb::RandomAccessFile* file);
  virtual DBStatus EnvOpenDirectory(DBSlice path, rocksdb::Directory** file);
  virtual DBStatus EnvSyncDirectory(rocksdb::Directory* file);
  virtual DBStatus EnvCloseDirectory(rocksdb::Directory* file);
  virtual DBStatus EnvRenameFile(DBSlice oldname, DBSlice newname);
  virtual DBStatus EnvCreateDir(DBSlice name);
  virtual DBStatus EnvDeleteDir(DBSlice name);
  virtual DBStatus EnvListDir(DBSlice name, std::vector<std::string>* result);
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
  virtual DBStatus EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file);
  virtual DBStatus EnvReadFile(DBSlice path, DBSlice* contents);
  virtual DBStatus EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents);
  virtual DBStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual DBStatus EnvDeleteFile(DBSlice path);
  virtual DBStatus EnvDeleteDirAndFiles(DBSlice dir);
  virtual DBStatus EnvLinkFile(DBSlice oldname, DBSlice newname);
  virtual DBStatus EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file);
  virtual DBStatus EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer, int64_t offset,
                                 int* n);
  virtual DBStatus EnvCloseReadableFile(rocksdb::RandomAccessFile* file);
  virtual DBStatus EnvOpenDirectory(DBSlice path, rocksdb::Directory** file);
  virtual DBStatus EnvSyncDirectory(rocksdb::Directory* file);
  virtual DBStatus EnvCloseDirectory(rocksdb::Directory* file);
  virtual DBStatus EnvRenameFile(DBSlice oldname, DBSlice newname);
  virtual DBStatus EnvCreateDir(DBSlice name);
  virtual DBStatus EnvDeleteDir(DBSlice name);
  virtual DBStatus EnvListDir(DBSlice name, std::vector<std::string>* result);
};

// GetDBBatchInserter returns a WriteBatch::Handler that operates on a
// WriteBatchBase. The caller assumes ownership of the returned handler.
::rocksdb::WriteBatch::Handler* GetDBBatchInserter(::rocksdb::WriteBatchBase* batch);

}  // namespace cockroach
