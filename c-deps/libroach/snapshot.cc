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

DBStatus DBSnapshot::EnvOpenFile(DBSlice path, rocksdb::WritableFile** file) {
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

}  // namespace cockroach
