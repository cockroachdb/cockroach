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

#include "engine.h"
#include "db.h"
#include "encoding.h"
#include "env_manager.h"
#include "fmt.h"
#include "getter.h"
#include "iterator.h"
#include "protos/storage/engine/enginepb/rocksdb.pb.h"
#include "status.h"

using namespace cockroach;

DBEngine::~DBEngine() {}

DBStatus DBEngine::AssertPreClose() { return kSuccess; }

DBSSTable* DBEngine::GetSSTables(int* n) {
  std::vector<rocksdb::LiveFileMetaData> metadata;
  rep->GetLiveFilesMetaData(&metadata);
  *n = metadata.size();
  // We malloc the result so it can be deallocated by the caller using free().
  const int size = metadata.size() * sizeof(DBSSTable);
  DBSSTable* tables = reinterpret_cast<DBSSTable*>(malloc(size));
  memset(tables, 0, size);
  for (int i = 0; i < metadata.size(); i++) {
    tables[i].level = metadata[i].level;
    tables[i].size = metadata[i].size;

    rocksdb::Slice tmp;
    if (DecodeKey(metadata[i].smallestkey, &tmp, &tables[i].start_key.wall_time,
                  &tables[i].start_key.logical)) {
      // This is a bit ugly because we want DBKey.key to be copied and
      // not refer to the memory in metadata[i].smallestkey.
      DBString str = ToDBString(tmp);
      tables[i].start_key.key = DBSlice{str.data, str.len};
    }
    if (DecodeKey(metadata[i].largestkey, &tmp, &tables[i].end_key.wall_time,
                  &tables[i].end_key.logical)) {
      DBString str = ToDBString(tmp);
      tables[i].end_key.key = DBSlice{str.data, str.len};
    }
  }
  return tables;
}

DBString DBEngine::GetUserProperties() {
  rocksdb::TablePropertiesCollection props;
  rocksdb::Status status = rep->GetPropertiesOfAllTables(&props);

  cockroach::storage::engine::enginepb::SSTUserPropertiesCollection all;
  if (!status.ok()) {
    all.set_error(status.ToString());
    return ToDBString(all.SerializeAsString());
  }

  for (auto i = props.begin(); i != props.end(); i++) {
    cockroach::storage::engine::enginepb::SSTUserProperties* sst = all.add_sst();
    sst->set_path(i->first);
    auto userprops = i->second->user_collected_properties;

    auto ts_min = userprops.find("crdb.ts.min");
    if (ts_min != userprops.end() && !ts_min->second.empty()) {
      if (!DecodeTimestamp(rocksdb::Slice(ts_min->second), sst->mutable_ts_min())) {
        fmt::SStringPrintf(
            all.mutable_error(), "unable to decode crdb.ts.min value '%s' in table %s",
            rocksdb::Slice(ts_min->second).ToString(true).c_str(), sst->path().c_str());
        break;
      }
    }

    auto ts_max = userprops.find("crdb.ts.max");
    if (ts_max != userprops.end() && !ts_max->second.empty()) {
      if (!DecodeTimestamp(rocksdb::Slice(ts_max->second), sst->mutable_ts_max())) {
        fmt::SStringPrintf(
            all.mutable_error(), "unable to decode crdb.ts.max value '%s' in table %s",
            rocksdb::Slice(ts_max->second).ToString(true).c_str(), sst->path().c_str());
        break;
      }
    }
  }
  return ToDBString(all.SerializeAsString());
}

namespace cockroach {

DBImpl::DBImpl(rocksdb::DB* r, std::unique_ptr<EnvManager> e, std::shared_ptr<rocksdb::Cache> bc,
               std::shared_ptr<DBEventListener> event_listener)
    : DBEngine(r, &iters_count),
      env_mgr(std::move(e)),
      rep_deleter(r),
      block_cache(bc),
      event_listener(event_listener),
      iters_count(0) {}

DBImpl::~DBImpl() {
  const rocksdb::Options& opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics>& s = opts.statistics;
  rocksdb::Info(opts.info_log, "bloom filter utility:    %0.1f%%",
                (100.0 * s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL)) /
                    s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED));
}

DBStatus DBImpl::AssertPreClose() {
  const int64_t n = iters_count.load();
  if (n == 0) {
    return kSuccess;
  }
  return FmtStatus("%" PRId64 " leaked iterators", n);
}

DBStatus DBImpl::Put(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Put(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBImpl::Merge(DBKey key, DBSlice value) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Merge(options, EncodeKey(key), ToSlice(value)));
}

DBStatus DBImpl::Get(DBKey key, DBString* value) {
  rocksdb::ReadOptions read_opts;
  DBGetter base(rep, read_opts, EncodeKey(key));
  return base.Get(value);
}

DBStatus DBImpl::Delete(DBKey key) {
  rocksdb::WriteOptions options;
  return ToDBStatus(rep->Delete(options, EncodeKey(key)));
}

DBStatus DBImpl::DeleteRange(DBKey start, DBKey end) {
  rocksdb::WriteOptions options;
  return ToDBStatus(
      rep->DeleteRange(options, rep->DefaultColumnFamily(), EncodeKey(start), EncodeKey(end)));
}

DBStatus DBImpl::CommitBatch(bool sync) { return FmtStatus("unsupported"); }

DBStatus DBImpl::ApplyBatchRepr(DBSlice repr, bool sync) {
  rocksdb::WriteBatch batch(ToString(repr));
  rocksdb::WriteOptions options;
  options.sync = sync;
  return ToDBStatus(rep->Write(options, &batch));
}

DBSlice DBImpl::BatchRepr() { return ToDBSlice("unsupported"); }

DBIterator* DBImpl::NewIter(rocksdb::ReadOptions* read_opts) {
  DBIterator* iter = new DBIterator(iters);
  iter->rep.reset(rep->NewIterator(*read_opts));
  return iter;
}

// GetStats retrieves a subset of RocksDB stats that are relevant to
// CockroachDB.
DBStatus DBImpl::GetStats(DBStatsResult* stats) {
  const rocksdb::Options& opts = rep->GetOptions();
  const std::shared_ptr<rocksdb::Statistics>& s = opts.statistics;

  uint64_t memtable_total_size;
  rep->GetIntProperty("rocksdb.cur-size-all-mem-tables", &memtable_total_size);

  uint64_t table_readers_mem_estimate;
  rep->GetIntProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem_estimate);

  uint64_t pending_compaction_bytes_estimate;
  rep->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                      &pending_compaction_bytes_estimate);

  stats->block_cache_hits = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
  stats->block_cache_misses = (int64_t)s->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
  stats->block_cache_usage = (int64_t)block_cache->GetUsage();
  stats->block_cache_pinned_usage = (int64_t)block_cache->GetPinnedUsage();
  stats->bloom_filter_prefix_checked =
      (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED);
  stats->bloom_filter_prefix_useful =
      (int64_t)s->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL);
  stats->memtable_total_size = memtable_total_size;
  stats->flushes = (int64_t)event_listener->GetFlushes();
  stats->compactions = (int64_t)event_listener->GetCompactions();
  stats->table_readers_mem_estimate = table_readers_mem_estimate;
  stats->pending_compaction_bytes_estimate = pending_compaction_bytes_estimate;
  return kSuccess;
}

DBString DBImpl::GetCompactionStats() {
  std::string tmp;
  rep->GetProperty("rocksdb.cfstats-no-file-histogram", &tmp);
  return ToDBString(tmp);
}

DBStatus DBImpl::GetEnvStats(DBEnvStatsResult* stats) {
  // Always initialize the fields.
  stats->encryption_status = DBString();
  stats->total_files = stats->total_bytes = stats->active_key_files = stats->active_key_bytes = 0;

  if (env_mgr->env_stats_handler == nullptr || env_mgr->file_registry == nullptr) {
    // We can't compute these if we don't have a file registry or stats handler.
    // This happens in OSS mode or when encryption has not been turned on.
    return kSuccess;
  }

  // Get encryption status.
  std::string encryption_status;
  auto status = env_mgr->env_stats_handler->GetEncryptionStats(&encryption_status);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  stats->encryption_status = ToDBString(encryption_status);

  // Get file statistics.
  // We compute the total number of files and total file size and break it down by
  // whether the file is encrypted using the active data key or not.
  //
  // Only files accounted for by rocksdb are included. This will ignore files multiple types
  // of files:
  // - files that are no longer live (eg: older OPTIONS files, files pending deletion)
  // - files created by libroach (eg: file/key registries)
  // - files created by cockroach (eg: working directory, side loading, etc...)
  //
  // We use the file size reported by rocksdb when available and stat other files ourselves.
  //
  // There is no synchronization between the various rocksdb calls and the file registry. We also
  // do not call DisableFileDeletions. This means that some files may be listed by rocksdb but not
  // present in the file registry (deleted between calls). Such files will be considered
  // "plaintext".
  //
  // TODO(mberhault): to obtain accurate statistics, we need at least:
  // - FileRegistry entries for all plaintext files
  // - Scan files in the FileRegistry
  // - Disable deletions in rocksdb during the scan
  // - Report "unknown" files (no registry entry) to avoid false positives.

  struct FileStat {
    FileStat() : has_size(false), size(0), entry(nullptr) {}
    bool has_size;
    uint64_t size;
    std::unique_ptr<enginepb::FileEntry> entry;
  };

  std::unordered_map<std::string, FileStat> file_list;

  // List of all live files. Filename only.
  // Contains SSTs, miscellaneous files (eg: CURRENT, MANIFEST, OPTIONS)
  std::vector<std::string> files;
  uint64_t manifest_size = 0;
  status = rep->GetLiveFiles(files, &manifest_size, false /* flush_memtable */);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  for (auto it = files.begin(); it != files.end(); ++it) {
    file_list[*it];
  }

  // List of WAL files. Filename and size.
  // Contains log files only.
  rocksdb::VectorLogPtr wal_files;
  status = rep->GetSortedWalFiles(wal_files);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  for (auto it = wal_files.begin(); it != wal_files.end(); ++it) {
    file_list[(*it)->PathName()].has_size = true;
    file_list[(*it)->PathName()].size = (*it)->SizeFileBytes();
  }

  // Metadata about live files. Filename and size.
  // Contains SSTs only.
  std::vector<rocksdb::LiveFileMetaData> live_files;
  rep->GetLiveFilesMetaData(&live_files);

  for (auto it = live_files.begin(); it != live_files.end(); ++it) {
    file_list[it->name].has_size = true;
    file_list[it->name].size = it->size;
  }

  uint64_t total_files = 0, total_bytes = 0, active_key_files = 0, active_key_bytes = 0;

  // Run through the list of found files.
  auto active_key_id = env_mgr->env_stats_handler->GetActiveDataKeyID();

  for (auto it = file_list.begin(); it != file_list.end(); ++it) {
    it->second.entry = env_mgr->file_registry->GetFileEntry(it->first, true /* relative */);

    if (!it->second.has_size) {
      // Unknown file size: stat it.
      // Ignore all errors. We can't filter for "not found" as that returns an I/O error.
      uint64_t size;
      status = env_mgr->db_env->GetFileSize(env_mgr->file_registry->db_dir() + it->first, &size);
      if (status.ok()) {
        it->second.has_size = true;
        it->second.size = size;
      }
    }

    // Check whether the file is using the active key.
    std::string id;
    status = env_mgr->env_stats_handler->GetFileEntryKeyID(it->second.entry.get(), &id);
    if (!status.ok()) {
      return ToDBStatus(status);
    }

    if (id == active_key_id) {
      active_key_files++;
      active_key_bytes += it->second.size;
    }

    total_files++;
    total_bytes += it->second.size;
  }

  stats->total_files = total_files;
  stats->total_bytes = total_bytes;
  stats->active_key_files = active_key_files;
  stats->active_key_bytes = active_key_bytes;

  return kSuccess;
}

// EnvWriteFile writes the given data as a new "file" in the given engine.
DBStatus DBImpl::EnvWriteFile(DBSlice path, DBSlice contents) {
  rocksdb::Status s;

  const rocksdb::EnvOptions soptions;
  rocksdb::unique_ptr<rocksdb::WritableFile> destfile;
  s = this->rep->GetEnv()->NewWritableFile(ToString(path), &destfile, soptions);
  if (!s.ok()) {
    return ToDBStatus(s);
  }

  s = destfile->Append(ToSlice(contents));
  if (!s.ok()) {
    return ToDBStatus(s);
  }

  return kSuccess;
}

// EnvOpenFile opens a new file in the given engine.
DBStatus DBImpl::EnvOpenFile(DBSlice path, rocksdb::WritableFile** file) {
  rocksdb::Status status;
  const rocksdb::EnvOptions soptions;
  rocksdb::unique_ptr<rocksdb::WritableFile> rocksdb_file;

  // Create the file.
  status = this->rep->GetEnv()->NewWritableFile(ToString(path), &rocksdb_file, soptions);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *file = rocksdb_file.release();
  return kSuccess;
}

// EnvReadFile reads the content of the given filename.
DBStatus DBImpl::EnvReadFile(DBSlice path, DBSlice* contents) {
  rocksdb::Status status;
  std::string data;

  status = ReadFileToString(this->rep->GetEnv(), ToString(path), &data);
  if (!status.ok()) {
    if (status.IsNotFound()) {
      return FmtStatus("No such file or directory");
    }
    return ToDBStatus(status);
  }
  contents->data = static_cast<char*>(malloc(data.size()));
  contents->len = data.size();
  memcpy(contents->data, data.c_str(), data.size());
  return kSuccess;
}

// CloseFile closes the given file in the given engine.
DBStatus DBImpl::EnvCloseFile(rocksdb::WritableFile* file) {
  rocksdb::Status status = file->Close();
  delete file;
  return ToDBStatus(status);
}

// EnvAppendFile appends the given data to the file in the given engine.
DBStatus DBImpl::EnvAppendFile(rocksdb::WritableFile* file, DBSlice contents) {
  rocksdb::Status status = file->Append(ToSlice(contents));
  return ToDBStatus(status);
}

// EnvSyncFile synchronously writes the data of the file to the disk.
DBStatus DBImpl::EnvSyncFile(rocksdb::WritableFile* file) {
  rocksdb::Status status = file->Sync();
  return ToDBStatus(status);
}

// EnvDeleteFile deletes the file with the given filename.
DBStatus DBImpl::EnvDeleteFile(DBSlice path) {
  rocksdb::Status status = this->rep->GetEnv()->DeleteFile(ToString(path));
  if (status.IsNotFound()) {
    return FmtStatus("No such file or directory");
  }
  return ToDBStatus(status);
}

// EnvDeleteDirAndFiles deletes the directory with the given dir name and any
// files it contains but not subdirectories.
DBStatus DBImpl::EnvDeleteDirAndFiles(DBSlice dir) {
  rocksdb::Status status;

  std::vector<std::string> files;
  this->rep->GetEnv()->GetChildren(ToString(dir), &files);
  for (auto& file : files) {
    if (file != "." && file != "..") {
      this->rep->GetEnv()->DeleteFile(ToString(dir) + "/" + file);
    }
  }

  status = this->rep->GetEnv()->DeleteDir(ToString(dir));
  if (status.IsNotFound()) {
    return FmtStatus("No such file or directory");
  }
  return ToDBStatus(status);
}

}  // namespace cockroach
