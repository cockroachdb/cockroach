// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "snapshot.h"
#include "encoding.h"
#include "getter.h"
#include "iterator.h"
#include "status.h"

namespace cockroach {

DBSnapshot::~DBSnapshot() { rep->ReleaseSnapshot(snapshot); }

DBStatus DBSnapshot::Put(DBKey key, DBSlice value) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::Merge(DBKey key, DBSlice value) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::Get(DBKey key, DBString* value) {
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  DBGetter base(rep, read_opts, EncodeKey(key));
  return base.Get(value);
}

DBStatus DBSnapshot::Delete(DBKey key) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::SingleDelete(DBKey key) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::DeleteRange(DBKey start, DBKey end) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::CommitBatch(bool sync) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::ApplyBatchRepr(DBSlice repr, bool sync) { return FmtStatus("unsupported"); }

DBSlice DBSnapshot::BatchRepr() { return ToDBSlice("unsupported"); }

DBIterator* DBSnapshot::NewIter(DBIterOptions iter_options) {
  DBIterator* iter = new DBIterator(iters, iter_options);
  iter->read_opts.snapshot = snapshot;
  iter->rep.reset(rep->NewIterator(iter->read_opts));
  return iter;
}

DBStatus DBSnapshot::GetStats(DBStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::GetTickersAndHistograms(DBTickersAndHistogramsResult* stats) {
  return FmtStatus("unsupported");
}

DBString DBSnapshot::GetCompactionStats() { return ToDBString("unsupported"); }

DBStatus DBSnapshot::GetEnvStats(DBEnvStatsResult* stats) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::GetEncryptionRegistries(DBEncryptionRegistries* result) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvWriteFile(DBSlice path, DBSlice contents) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvOpenFile(DBSlice path, uint64_t bytes_per_sync, rocksdb::WritableFile** file) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvReadFile(DBSlice path, DBSlice* contents) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvCloseFile(rocksdb::WritableFile* file) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::EnvSyncFile(rocksdb::WritableFile* file) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvDeleteFile(DBSlice path) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::EnvDeleteDirAndFiles(DBSlice dir) { return FmtStatus("unsupported"); }

DBStatus DBSnapshot::EnvLinkFile(DBSlice oldname, DBSlice newname) {
  return FmtStatus("unsupported");
}

DBStatus DBSnapshot::EnvOpenReadableFile(DBSlice path, rocksdb::RandomAccessFile** file) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvReadAtFile(rocksdb::RandomAccessFile* file, DBSlice buffer, int64_t offset,
                                   int* n) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvCloseReadableFile(rocksdb::RandomAccessFile* file) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvOpenDirectory(DBSlice path, rocksdb::Directory** file) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvSyncDirectory(rocksdb::Directory* file) { return FmtStatus("unsupported"); }
DBStatus DBSnapshot::EnvCloseDirectory(rocksdb::Directory* file) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvRenameFile(DBSlice oldname, DBSlice newname) {
  return FmtStatus("unsupported");
}
DBStatus DBSnapshot::EnvCreateDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBSnapshot::EnvDeleteDir(DBSlice name) { return FmtStatus("unsupported"); }
DBStatus DBSnapshot::EnvListDir(DBSlice name, std::vector<std::string>* result) {
  return FmtStatus("unsupported");
}

}  // namespace cockroach
