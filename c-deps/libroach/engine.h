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
  virtual DBStatus SingleDelete(DBKey key) = 0;
  virtual DBStatus DeleteRange(DBKey start, DBKey end) = 0;
  virtual DBStatus CommitBatch(bool sync) = 0;
  virtual DBStatus ApplyBatchRepr(DBSlice repr, bool sync) = 0;
  virtual DBSlice BatchRepr() = 0;
  virtual DBStatus Get(DBKey key, DBString* value) = 0;
  virtual DBIterator* NewIter(DBIterOptions) = 0;
  virtual DBStatus GetStats(DBStatsResult* stats) = 0;
  virtual DBStatus GetTickersAndHistograms(DBTickersAndHistogramsResult* stats) = 0;
  virtual DBString GetCompactionStats() = 0;
  virtual DBString GetEnvStats(DBEnvStatsResult* stats) = 0;
  virtual DBStatus GetEncryptionRegistries(DBEncryptionRegistries* result) = 0;
  virtual DBStatus EnvWriteFile(DBSlice path, DBSlice contents) = 0;
  virtual DBStatus EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file) = 0;
  virtual DBStatus EnvReadFile(DBSlice path, DBSlice* contents) = 0;
  virtual DBStatus EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) = 0;
  virtual DBStatus EnvSyncFile(rocksdb::WritableFile* file) = 0;
  virtual DBStatus EnvCloseFile(rocksdb::WritableFile* file) = 0;
  virtual DBStatus EnvDeleteFile(DBSlice path) = 0;
  virtual DBStatus EnvDeleteDirAndFiles(DBSlice dir) = 0;
  virtual DBStatus EnvLinkFile(DBSlice oldname, DBSlice newname) = 0;
  virtual DBStatus EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file) = 0;
  virtual DBStatus EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer, int64_t offset,
                                 int* n) = 0;
  virtual DBStatus EnvCloseReadableFile(rocksdb::RandomAccessFile* file) = 0;
  virtual DBStatus EnvOpenDirectory(DBSlice path, rocksdb::Directory** file) = 0;
  virtual DBStatus EnvSyncDirectory(rocksdb::Directory* file) = 0;
  virtual DBStatus EnvCloseDirectory(rocksdb::Directory* file) = 0;
  virtual DBStatus EnvRenameFile(DBSlice oldname, DBSlice newname) = 0;
  virtual DBStatus EnvCreateDir(DBSlice name) = 0;
  virtual DBStatus EnvDeleteDir(DBSlice name) = 0;
  virtual DBStatus EnvListDir(DBSlice name, std::vector<std::string>* result) = 0;
  
  DBSSTable* GetSSTables(int* n);
  DBStatus GetSortedWALFiles(DBWALFile** out_files, int* n);
  DBString GetUserProperties();
};

namespace cockroach {

struct EnvManager;

struct DBImpl : public DBEngine {
  std::unique_ptr<EnvManager> env_mgr;
  std::unique_ptr<rocksdb::DB> rep_deleter;
  std::shared_ptr<rocksdb::Cache> block_cache;
  std::shared_ptr<DBEventListener> event_listener;
  std::atomic<int64_t> iters_count;

  // Construct a new DBImpl from the specified DB.
  // The DB and passed Envs will be deleted when the DBImpl is deleted.
  // Either env can be NULL.
  DBImpl(rocksdb::DB* r, std::unique_ptr<EnvManager> e, std::shared_ptr<rocksdb::Cache> bc,
         std::shared_ptr<DBEventListener> event_listener);
  virtual ~DBImpl();

  virtual DBStatus AssertPreClose();
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

}  // namespace cockroach
