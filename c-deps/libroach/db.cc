// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "db.h"
#include <algorithm>
#include <iostream>
#include <rocksdb/convenience.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#include <stdarg.h>
#include "batch.h"
#include "cache.h"
#include "comparator.h"
#include "defines.h"
#include "encoding.h"
#include "engine.h"
#include "env_manager.h"
#include "eventlistener.h"
#include "fmt.h"
#include "getter.h"
#include "godefs.h"
#include "incremental_iterator.h"
#include "iterator.h"
#include "merge.h"
#include "options.h"
#include "protos/roachpb/errors.pb.h"
#include "row_counter.h"
#include "snapshot.h"
#include "stack_trace.h"
#include "status.h"
#include "table_props.h"
#include "timestamp.h"

using namespace cockroach;

namespace cockroach {

DBKey ToDBKey(const rocksdb::Slice& s) {
  DBKey key;
  memset(&key, 0, sizeof(key));
  rocksdb::Slice tmp;
  if (DecodeKey(s, &tmp, &key.wall_time, &key.logical)) {
    key.key = ToDBSlice(tmp);
  }
  return key;
}

ScopedStats::ScopedStats(DBIterator* iter)
    : iter_(iter),
      internal_delete_skipped_count_base_(
          rocksdb::get_perf_context()->internal_delete_skipped_count) {
  if (iter_->stats != nullptr) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
  }
}
ScopedStats::~ScopedStats() {
  if (iter_->stats != nullptr) {
    iter_->stats->internal_delete_skipped_count +=
        (rocksdb::get_perf_context()->internal_delete_skipped_count -
         internal_delete_skipped_count_base_);
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  }
}

void BatchSSTablesForCompaction(const std::vector<rocksdb::SstFileMetaData>& sst,
                                rocksdb::Slice start_key, rocksdb::Slice end_key,
                                uint64_t target_size, std::vector<rocksdb::Range>* ranges) {
  int prev = -1;  // index of the last compacted sst
  uint64_t size = 0;
  for (int i = 0; i < sst.size(); ++i) {
    size += sst[i].size;
    if (size < target_size && (i + 1) < sst.size()) {
      // We haven't reached the target size or the end of the sstables
      // to compact.
      continue;
    }

    rocksdb::Slice start;
    if (prev == -1) {
      // This is the first compaction.
      start = start_key;
    } else {
      // This is a compaction in the middle or end of the requested
      // key range. The start key for the compaction is the largest
      // key from the previous compacted.
      start = rocksdb::Slice(sst[prev].largestkey);
    }

    rocksdb::Slice end;
    if ((i + 1) == sst.size()) {
      // This is the last compaction.
      end = end_key;
    } else {
      // This is a compaction at the start or in the middle of the
      // requested key range. The end key is the largest key in the
      // current sstable.
      end = rocksdb::Slice(sst[i].largestkey);
    }

    ranges->emplace_back(rocksdb::Range(start, end));

    prev = i;
    size = 0;
  }
}

}  // namespace cockroach

namespace {

DBIterState DBIterGetState(DBIterator* iter) {
  DBIterState state = {};
  state.valid = iter->rep->Valid();
  state.status = ToDBStatus(iter->rep->status());

  if (state.valid) {
    rocksdb::Slice key;
    state.valid = DecodeKey(iter->rep->key(), &key, &state.key.wall_time, &state.key.logical);
    if (state.valid) {
      state.key.key = ToDBSlice(key);
      state.value = ToDBSlice(iter->rep->value());
    }
  }

  return state;
}
}  // namespace

namespace cockroach {

// DBOpenHookOSS mode only verifies that no extra options are specified.
rocksdb::Status DBOpenHookOSS(std::shared_ptr<rocksdb::Logger> info_log, const std::string& db_dir,
                              const DBOptions db_opts, EnvManager* env_mgr) {
  if (db_opts.extra_options.len != 0) {
    return rocksdb::Status::InvalidArgument("encryption options are not supported in OSS builds");
  }
  return rocksdb::Status::OK();
}

}  // namespace cockroach

static DBOpenHook* db_open_hook = DBOpenHookOSS;

void DBSetOpenHook(void* hook) { db_open_hook = (DBOpenHook*)hook; }

DBStatus DBOpen(DBEngine** db, DBSlice dir, DBOptions db_opts) {
  rocksdb::Options options = DBMakeOptions(db_opts);

  const std::string additional_options = ToString(db_opts.rocksdb_options);
  if (!additional_options.empty()) {
    // TODO(peter): Investigate using rocksdb::LoadOptionsFromFile if
    // "additional_options" starts with "@". The challenge is that
    // LoadOptionsFromFile gives us a DBOptions and
    // ColumnFamilyOptions with no ability to supply "base" options
    // and no ability to determine what options were specified in the
    // file which could cause "defaults" to override the options
    // returned by DBMakeOptions. We might need to fix this upstream.
    rocksdb::Status status = rocksdb::GetOptionsFromString(options, additional_options, &options);
    if (!status.ok()) {
      return ToDBStatus(status);
    }
  }

  const std::string db_dir = ToString(dir);

  // Make the default options.env the default. It points to Env::Default which does not
  // need to be deleted.
  std::unique_ptr<cockroach::EnvManager> env_mgr(new cockroach::EnvManager(options.env));

  if (dir.len == 0) {
    // In-memory database: use a MemEnv as the base Env.
    auto memenv = rocksdb::NewMemEnv(rocksdb::Env::Default());
    // Register it for deletion.
    env_mgr->TakeEnvOwnership(memenv);
    // Create a root directory to suppress error messages that RocksDB would
    // print if it had to create the DB directory itself.
    memenv->CreateDir("/");
    // Make it the env that all other Envs must wrap.
    env_mgr->base_env = memenv;
    // Make it the env for rocksdb.
    env_mgr->db_env = memenv;
  }

  // Create the file registry. It uses the base_env to access the registry file.
  auto file_registry =
      std::unique_ptr<FileRegistry>(new FileRegistry(env_mgr->base_env, db_dir, db_opts.read_only));

  if (db_opts.use_file_registry) {
    // We're using the file registry.
    auto status = file_registry->Load();
    if (!status.ok()) {
      return ToDBStatus(status);
    }

    status = file_registry->CheckNoRegistryFile();
    if (!status.ok()) {
      // We have a file registry, this means we've used encryption flags before
      // and are tracking all files on disk. Running without encryption (extra_options empty)
      // will bypass the file registry and lose changes.
      // In this case, we have multiple possibilities:
      // - no extra_options: this fails here
      // - extra_options:
      //   - OSS: this fails in the OSS hook (OSS does not understand extra_options)
      //   - CCL: fails if the options do not parse properly
      if (db_opts.extra_options.len == 0) {
        return ToDBStatus(rocksdb::Status::InvalidArgument(
            "encryption was used on this store before, but no encryption flags specified. You need "
            "a CCL build and must fully specify the --enterprise-encryption flag"));
      }
    }

    // EnvManager takes ownership of the file registry.
    env_mgr->file_registry.swap(file_registry);
  } else {
    // File registry format not enabled: check whether we have a registry file (we shouldn't).
    // The file_registry is not passed to anyone, it is deleted when it goes out of scope.
    auto status = file_registry->CheckNoRegistryFile();
    if (!status.ok()) {
      return ToDBStatus(status);
    }
  }

  // Call hooks to handle db_opts.extra_options.
  auto hook_status = db_open_hook(options.info_log, db_dir, db_opts, env_mgr.get());
  if (!hook_status.ok()) {
    return ToDBStatus(hook_status);
  }

  // Register listener for tracking RocksDB stats.
  std::shared_ptr<DBEventListener> event_listener(new DBEventListener);
  options.listeners.emplace_back(event_listener);

  // Point rocksdb to the env to use.
  options.env = env_mgr->db_env;

  rocksdb::DB* db_ptr;
  rocksdb::Status status;
  if (db_opts.read_only) {
    status = rocksdb::DB::OpenForReadOnly(options, db_dir, &db_ptr);
  } else {
    status = rocksdb::DB::Open(options, db_dir, &db_ptr);
  }

  if (!status.ok()) {
    return ToDBStatus(status);
  }
  *db = new DBImpl(db_ptr, std::move(env_mgr),
                   db_opts.cache != nullptr ? db_opts.cache->rep : nullptr, event_listener);
  return kSuccess;
}

DBStatus DBCreateCheckpoint(DBEngine* db, DBSlice dir) {
  const std::string cp_dir = ToString(dir);

  rocksdb::Checkpoint* cp_ptr;
  auto status = rocksdb::Checkpoint::Create(db->rep, &cp_ptr);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  // NB: passing 0 for log_size_for_flush forces a WAL sync, i.e. makes sure
  // that the checkpoint is up to date.
  status = cp_ptr->CreateCheckpoint(cp_dir, 0 /* log_size_for_flush */);
  delete (cp_ptr);
  return ToDBStatus(status);
}

DBStatus DBDestroy(DBSlice dir) {
  rocksdb::Options options;
  return ToDBStatus(rocksdb::DestroyDB(ToString(dir), options));
}

DBStatus DBClose(DBEngine* db) {
  DBStatus status = db->AssertPreClose();
  if (status.data == nullptr) {
    delete db;
  }
  return status;
}

DBStatus DBFlush(DBEngine* db) {
  rocksdb::FlushOptions options;
  options.wait = true;
  return ToDBStatus(db->rep->Flush(options));
}

DBStatus DBSyncWAL(DBEngine* db) {
#ifdef _WIN32
  // On Windows, DB::SyncWAL() is not implemented due to fact that
  // `WinWritableFile` is not thread safe. To get around that, the only other
  // methods that can be used to ensure that a sync is triggered is to either
  // flush the memtables or perform a write with `WriteOptions.sync=true`. See
  // https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ for more details.
  // Please also see #17442 for more discussion on the topic.

  // In order to force a sync we issue a write-batch containing
  // LogData with 'sync=true'. The LogData forces a write to the WAL
  // but otherwise doesn't add anything to the memtable or sstables.
  rocksdb::WriteBatch batch;
  batch.PutLogData("");
  rocksdb::WriteOptions options;
  options.sync = true;
  return ToDBStatus(db->rep->Write(options, &batch));
#else
  return ToDBStatus(db->rep->FlushWAL(true /* sync */));
#endif
}

DBStatus DBCompact(DBEngine* db) {
  return DBCompactRange(db, DBSlice(), DBSlice(), true /* force_bottommost */);
}

DBStatus DBCompactRange(DBEngine* db, DBSlice start, DBSlice end, bool force_bottommost) {
  rocksdb::CompactRangeOptions options;
  // By default, RocksDB doesn't recompact the bottom level (unless
  // there is a compaction filter, which we don't use). However,
  // recompacting the bottom layer is necessary to pick up changes to
  // settings like bloom filter configurations, and to fully reclaim
  // space after dropping, truncating, or migrating tables.
  if (force_bottommost) {
    options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
  }
  // By default, RocksDB treats manual compaction requests as
  // operating exclusively, preventing normal automatic compactions
  // from running. This can block writes to the database, as L0
  // SSTables will become full without being allowed to compact to L1.
  options.exclusive_manual_compaction = false;

  // Compacting the entire database in a single-shot can use a
  // significant amount of additional (temporary) disk space. Instead,
  // we loop over the sstables in the lowest level and initiate
  // compactions on smaller ranges of keys. The resulting compacted
  // database is the same size, but the temporary disk space needed
  // for the compaction is dramatically reduced.
  std::vector<rocksdb::LiveFileMetaData> all_metadata;
  std::vector<rocksdb::LiveFileMetaData> metadata;
  db->rep->GetLiveFilesMetaData(&all_metadata);

  const std::string start_key(ToString(start));
  const std::string end_key(ToString(end));

  int max_level = 0;
  for (int i = 0; i < all_metadata.size(); i++) {
    // Skip any SSTables which fall outside the specified range, if a
    // range was specified.
    if ((!start_key.empty() && all_metadata[i].largestkey < start_key) ||
        (!end_key.empty() && all_metadata[i].smallestkey >= end_key)) {
      continue;
    }
    if (max_level < all_metadata[i].level) {
      max_level = all_metadata[i].level;
    }
    // Gather the set of SSTables to compact.
    metadata.push_back(all_metadata[i]);
  }
  all_metadata.clear();

  if (max_level != db->rep->NumberLevels() - 1) {
    // There are no sstables at the lowest level, so just compact the
    // specified key span, wholesale. Due to the
    // level_compaction_dynamic_level_bytes setting, this will only
    // happen on spans containing very little data.
    const rocksdb::Slice start_slice(start_key);
    const rocksdb::Slice end_slice(end_key);
    return ToDBStatus(db->rep->CompactRange(options, !start_key.empty() ? &start_slice : nullptr,
                                            !end_key.empty() ? &end_slice : nullptr));
  }

  // A naive approach to selecting ranges to compact would be to
  // compact the ranges specified by the smallest and largest key in
  // each sstable of the bottom-most level. Unfortunately, the
  // sstables in the bottom-most level have vastly different
  // sizes. For example, starting with the following set of bottom-most
  // sstables:
  //
  //   100M[16] 89M 70M 66M 56M 54M 38M[2] 36M 23M 20M 17M 8M 6M 5M 2M 2K[4]
  //
  // If we compact the entire database in one call we can end up with:
  //
  //   100M[22] 77M 76M 50M
  //
  // If we use the naive approach (compact the range specified by
  // the smallest and largest keys):
  //
  //   100M[18] 92M 68M 62M 61M 50M 45M 39M 31M 29M[2] 24M 23M 18M 9M 8M[2] 7M
  //   2K[4]
  //
  // With the approach below:
  //
  //   100M[19] 80M 68M[2] 62M 61M 53M 45M 36M 31M
  //
  // The approach below is to loop over the bottom-most sstables in
  // sorted order and initiate a compact range every 128MB of data.

  // Gather up the bottom-most sstable metadata.
  std::vector<rocksdb::SstFileMetaData> sst;
  for (int i = 0; i < metadata.size(); i++) {
    if (metadata[i].level != max_level) {
      continue;
    }
    sst.push_back(metadata[i]);
  }
  // Sort the metadata by smallest key.
  std::sort(sst.begin(), sst.end(),
            [](const rocksdb::SstFileMetaData& a, const rocksdb::SstFileMetaData& b) -> bool {
              return a.smallestkey < b.smallestkey;
            });

  // Batch the bottom-most sstables into compactions of ~128MB.
  const uint64_t target_size = 128 << 20;
  std::vector<rocksdb::Range> ranges;
  BatchSSTablesForCompaction(sst, start_key, end_key, target_size, &ranges);

  for (auto r : ranges) {
    rocksdb::Status status = db->rep->CompactRange(options, r.start.empty() ? nullptr : &r.start,
                                                   r.limit.empty() ? nullptr : &r.limit);
    if (!status.ok()) {
      return ToDBStatus(status);
    }
  }

  return kSuccess;
}

DBStatus DBDisableAutoCompaction(DBEngine* db) {
  auto status = db->rep->SetOptions({{"disable_auto_compactions", "true"}});
  return ToDBStatus(status);
}

DBStatus DBEnableAutoCompaction(DBEngine* db) {
  auto status = db->rep->EnableAutoCompaction({db->rep->DefaultColumnFamily()});
  return ToDBStatus(status);
}

DBStatus DBApproximateDiskBytes(DBEngine* db, DBKey start, DBKey end, uint64_t* size) {
  const std::string start_key(EncodeKey(start));
  const std::string end_key(EncodeKey(end));
  const rocksdb::Range r(start_key, end_key);
  const uint8_t flags = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES;

  db->rep->GetApproximateSizes(&r, 1, size, flags);
  return kSuccess;
}

DBStatus DBPut(DBEngine* db, DBKey key, DBSlice value) { return db->Put(key, value); }

DBStatus DBMerge(DBEngine* db, DBKey key, DBSlice value) { return db->Merge(key, value); }

DBStatus DBGet(DBEngine* db, DBKey key, DBString* value) { return db->Get(key, value); }

DBStatus DBDelete(DBEngine* db, DBKey key) { return db->Delete(key); }

DBStatus DBSingleDelete(DBEngine* db, DBKey key) { return db->SingleDelete(key); }

DBStatus DBDeleteRange(DBEngine* db, DBKey start, DBKey end) { return db->DeleteRange(start, end); }

DBStatus DBDeleteIterRange(DBEngine* db, DBIterator* iter, DBKey start, DBKey end) {
  rocksdb::Iterator* const iter_rep = iter->rep.get();
  iter_rep->Seek(EncodeKey(start));
  const std::string end_key = EncodeKey(end);
  for (; iter_rep->Valid() && kComparator.Compare(iter_rep->key(), end_key) < 0; iter_rep->Next()) {
    DBStatus status = db->Delete(ToDBKey(iter_rep->key()));
    if (status.data != NULL) {
      return status;
    }
  }
  return kSuccess;
}

DBStatus DBCommitAndCloseBatch(DBEngine* db, bool sync) {
  DBStatus status = db->CommitBatch(sync);
  if (status.data == NULL) {
    DBClose(db);
  }
  return status;
}

DBStatus DBApplyBatchRepr(DBEngine* db, DBSlice repr, bool sync) {
  return db->ApplyBatchRepr(repr, sync);
}

DBSlice DBBatchRepr(DBEngine* db) { return db->BatchRepr(); }

DBEngine* DBNewSnapshot(DBEngine* db) { return new DBSnapshot(db); }

DBEngine* DBNewBatch(DBEngine* db, bool writeOnly) {
  if (writeOnly) {
    return new DBWriteOnlyBatch(db);
  }
  return new DBBatch(db);
}

DBStatus DBEnvWriteFile(DBEngine* db, DBSlice path, DBSlice contents) {
  return db->EnvWriteFile(path, contents);
}

DBStatus DBEnvOpenFile(DBEngine* db, DBSlice path,  uint64_t bytes_per_sync,
                       DBWritableFile* file) {
  return db->EnvOpenFile(path, bytes_per_sync, (rocksdb::WritableFile**)file);
}

DBStatus DBEnvReadFile(DBEngine* db, DBSlice path, DBSlice* contents) {
  return db->EnvReadFile(path, contents);
}

DBStatus DBEnvCloseFile(DBEngine* db, DBWritableFile file) {
  return db->EnvCloseFile((rocksdb::WritableFile*)file);
}

DBStatus DBEnvSyncFile(DBEngine* db, DBWritableFile file) {
  return db->EnvSyncFile((rocksdb::WritableFile*)file);
}

DBStatus DBEnvAppendFile(DBEngine* db, DBWritableFile file, DBSlice contents) {
  return db->EnvAppendFile((rocksdb::WritableFile*)file, contents);
}

DBStatus DBEnvDeleteFile(DBEngine* db, DBSlice path) { return db->EnvDeleteFile(path); }

DBStatus DBEnvDeleteDirAndFiles(DBEngine* db, DBSlice dir) { return db->EnvDeleteDirAndFiles(dir); }

DBStatus DBEnvLinkFile(DBEngine* db, DBSlice oldname, DBSlice newname) {
  return db->EnvLinkFile(oldname, newname);
}

DBIterState DBCheckForKeyCollisions(DBIterator* existingIter, DBIterator* sstIter,
                                    MVCCStatsResult* skippedKVStats, DBString* write_intent) {
  DBIterState state = {};
  memset(skippedKVStats, 0, sizeof(*skippedKVStats));

  while (existingIter->rep->Valid() && sstIter->rep->Valid()) {
    rocksdb::Slice sstKey;
    rocksdb::Slice existingKey;
    DBTimestamp existing_ts = kZeroTimestamp;
    DBTimestamp sst_ts = kZeroTimestamp;
    if (!DecodeKey(sstIter->rep->key(), &sstKey, &sst_ts) ||
        !DecodeKey(existingIter->rep->key(), &existingKey, &existing_ts)) {
      state.valid = false;
      state.status = FmtStatus("unable to decode key");
      return state;
    }

    // Encountered an inline value or a write intent.
    if (existing_ts == kZeroTimestamp) {
      cockroach::storage::enginepb::MVCCMetadata meta;
      if (!meta.ParseFromArray(existingIter->rep->value().data(),
                               existingIter->rep->value().size())) {
        state.status = FmtStatus("failed to parse meta");
        state.valid = false;
        return state;
      }

      // Check for an inline value, as these are only used in non-user data.
      // This method is currently used by AddSSTable when performing an IMPORT
      // INTO. We do not expect to encounter any inline values, and thus we
      // report an error.
      if (meta.has_raw_bytes()) {
        state.status = FmtStatus("InlineError");
      } else if (meta.has_txn()) {
        // Check for a write intent.
        //
        // TODO(adityamaru): Currently, we raise a WriteIntentError on
        // encountering all intents. This is because, we do not expect to
        // encounter many intents during IMPORT INTO as we lock the key space we
        // are importing into. Older write intents could however be found in the
        // target key space, which will require appropriate resolution logic.
        cockroach::roachpb::WriteIntentError err;
        cockroach::roachpb::Intent* intent = err.add_intents();
        intent->mutable_single_key_span()->set_key(existingIter->rep->key().data(),
                                                   existingIter->rep->key().size());
        intent->mutable_txn()->CopyFrom(meta.txn());

        *write_intent = ToDBString(err.SerializeAsString());
        state.status = FmtStatus("WriteIntentError");
      } else {
        state.status = FmtStatus("intent without transaction");
      }

      state.valid = false;
      return state;
    }

    DBKey targetKey;
    memset(&targetKey, 0, sizeof(targetKey));
    int compare = kComparator.Compare(existingKey, sstKey);
    if (compare == 0) {
      // If the colliding key is a tombstone in the existing data, and the
      // timestamp of the sst key is greater than or equal to the timestamp of
      // the tombstone, then this is not considered a collision. We move the
      // iterator over the existing data to the next potentially colliding key
      // (skipping all versions of the deleted key), and resume iteration.
      //
      // If the ts of the sst key is less than that of the tombstone it is
      // changing existing data, and we treat this as a collision.
      if (existingIter->rep->value().empty() && sst_ts >= existing_ts) {
        DBIterNext(existingIter, true /* skip_current_key_versions */);
        continue;
      }

      // If the ingested KV has an identical timestamp and value as the existing
      // data then we do not consider it to be a collision. We move the iterator
      // over the existing data to the next potentially colliding key (skipping
      // all versions of the current key), and resume iteration.
      bool has_equal_timestamp = existing_ts == sst_ts;
      bool has_equal_value =
          kComparator.Compare(existingIter->rep->value(), sstIter->rep->value()) == 0;
      if (has_equal_timestamp && has_equal_value) {
        // Even though we skip over the KVs described above, their stats have
        // already been accounted for resulting in a problem of double-counting.
        // To solve this we send back the stats of these skipped KVs so that we
        // can subtract them later. This enables us to construct accurate
        // MVCCStats and prevents expensive recomputation in the future.
        const int64_t meta_key_size = sstKey.size() + 1;
        const int64_t meta_val_size = 0;
        int64_t total_bytes = meta_key_size + meta_val_size;

        // Update the skipped stats to account fot the skipped meta key.
        skippedKVStats->live_bytes += total_bytes;
        skippedKVStats->live_count++;
        skippedKVStats->key_bytes += meta_key_size;
        skippedKVStats->val_bytes += meta_val_size;
        skippedKVStats->key_count++;

        // Update the stats to account for the skipped versioned key/value.
        total_bytes = sstIter->rep->value().size() + kMVCCVersionTimestampSize;
        skippedKVStats->live_bytes += total_bytes;
        skippedKVStats->key_bytes += kMVCCVersionTimestampSize;
        skippedKVStats->val_bytes += sstIter->rep->value().size();
        skippedKVStats->val_count++;

        DBIterNext(existingIter, true /* skip_current_key_versions */);
        continue;
      }

      state.valid = false;
      state.key.key = ToDBSlice(sstKey);
      state.status = FmtStatus("key collision");
      return state;
    } else if (compare < 0) {
      targetKey.key = ToDBSlice(sstKey);
      DBIterSeek(existingIter, targetKey);
    } else if (compare > 0) {
      targetKey.key = ToDBSlice(existingKey);
      DBIterSeek(sstIter, targetKey);
    }
  }

  state.valid = true;
  return state;
}

DBIterator* DBNewIter(DBEngine* db, DBIterOptions iter_options) {
  return db->NewIter(iter_options);
}

void DBIterDestroy(DBIterator* iter) { delete iter; }

IteratorStats DBIterStats(DBIterator* iter) {
  IteratorStats stats = {};
  if (iter->stats != nullptr) {
    stats = *iter->stats;
  }
  return stats;
}

DBIterState DBIterSeek(DBIterator* iter, DBKey key) {
  ScopedStats stats(iter);
  iter->rep->Seek(EncodeKey(key));
  return DBIterGetState(iter);
}

DBIterState DBIterSeekForPrev(DBIterator* iter, DBKey key) {
  ScopedStats stats(iter);
  iter->rep->SeekForPrev(EncodeKey(key));
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToFirst(DBIterator* iter) {
  ScopedStats stats(iter);
  iter->rep->SeekToFirst();
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToLast(DBIterator* iter) {
  ScopedStats stats(iter);
  iter->rep->SeekToLast();
  return DBIterGetState(iter);
}

DBIterState DBIterNext(DBIterator* iter, bool skip_current_key_versions) {
  ScopedStats stats(iter);
  // If we're skipping the current key versions, remember the key the
  // iterator was pointing out.
  std::string old_key;
  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (!SplitKey(iter->rep->key(), &key, &ts)) {
      DBIterState state = {0};
      state.valid = false;
      state.status = FmtStatus("failed to split mvcc key");
      return state;
    }
    old_key = key.ToString();
  }

  iter->rep->Next();

  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (!SplitKey(iter->rep->key(), &key, &ts)) {
      DBIterState state = {0};
      state.valid = false;
      state.status = FmtStatus("failed to split mvcc key");
      return state;
    }
    if (old_key == key) {
      // We're pointed at a different version of the same key. Fall
      // back to seeking to the next key.
      old_key.append("\0", 1);
      DBKey db_key;
      db_key.key = ToDBSlice(old_key);
      db_key.wall_time = 0;
      db_key.logical = 0;
      iter->rep->Seek(EncodeKey(db_key));
    }
  }

  return DBIterGetState(iter);
}

DBIterState DBIterPrev(DBIterator* iter, bool skip_current_key_versions) {
  ScopedStats stats(iter);
  // If we're skipping the current key versions, remember the key the
  // iterator was pointed out.
  std::string old_key;
  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (SplitKey(iter->rep->key(), &key, &ts)) {
      old_key = key.ToString();
    }
  }

  iter->rep->Prev();

  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (SplitKey(iter->rep->key(), &key, &ts)) {
      if (old_key == key) {
        // We're pointed at a different version of the same key. Fall
        // back to seeking to the prev key. In this case, we seek to
        // the "metadata" key and that back up the iterator.
        DBKey db_key;
        db_key.key = ToDBSlice(old_key);
        db_key.wall_time = 0;
        db_key.logical = 0;
        iter->rep->Seek(EncodeKey(db_key));
        if (iter->rep->Valid()) {
          iter->rep->Prev();
        }
      }
    }
  }

  return DBIterGetState(iter);
}

void DBIterSetLowerBound(DBIterator* iter, DBKey key) { iter->SetLowerBound(key); }
void DBIterSetUpperBound(DBIterator* iter, DBKey key) { iter->SetUpperBound(key); }

DBStatus DBMerge(DBSlice existing, DBSlice update, DBString* new_value, bool full_merge) {
  new_value->len = 0;

  cockroach::storage::enginepb::MVCCMetadata meta;
  if (!meta.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  cockroach::storage::enginepb::MVCCMetadata update_meta;
  if (!update_meta.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(&meta, update_meta, full_merge, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  return DBMerge(existing, update, new_value, true);
}

DBStatus DBPartialMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  return DBMerge(existing, update, new_value, false);
}

// DBGetStats queries the given DBEngine for various operational stats and
// write them to the provided DBStatsResult instance.
DBStatus DBGetStats(DBEngine* db, DBStatsResult* stats) {
  return db->GetStats(stats);
}

// `DBGetTickersAndHistograms` retrieves maps of all RocksDB tickers and histograms.
// It differs from `DBGetStats` by getting _every_ ticker and histogram, and by not
// getting anything else (DB properties, for example).
//
// In addition to freeing the `DBString`s in the result, the caller is also
// responsible for freeing `DBTickersAndHistogramsResult::tickers` and
// `DBTickersAndHistogramsResult::histograms`.
DBStatus DBGetTickersAndHistograms(DBEngine* db, DBTickersAndHistogramsResult* stats) {
  return db->GetTickersAndHistograms(stats);
}

DBString DBGetCompactionStats(DBEngine* db) { return db->GetCompactionStats(); }

DBStatus DBGetEnvStats(DBEngine* db, DBEnvStatsResult* stats) { return db->GetEnvStats(stats); }

DBStatus DBGetEncryptionRegistries(DBEngine* db, DBEncryptionRegistries* result) {
  return db->GetEncryptionRegistries(result);
}

DBSSTable* DBGetSSTables(DBEngine* db, int* n) { return db->GetSSTables(n); }

DBStatus DBGetSortedWALFiles(DBEngine* db, DBWALFile** files, int* n) {
  return db->GetSortedWALFiles(files, n);
}

DBString DBGetUserProperties(DBEngine* db) { return db->GetUserProperties(); }

DBStatus DBIngestExternalFiles(DBEngine* db, char** paths, size_t len, bool move_files) {
  std::vector<std::string> paths_vec;
  for (size_t i = 0; i < len; i++) {
    paths_vec.push_back(paths[i]);
  }

  rocksdb::IngestExternalFileOptions ingest_options;
  // If move_files is true and the env supports it, RocksDB will hard link.
  // Otherwise, it will copy.
  ingest_options.move_files = move_files;
  // If snapshot_consistency is true and there is an outstanding RocksDB
  // snapshot, a global sequence number is forced (see the allow_global_seqno
  // option).
  ingest_options.snapshot_consistency = true;
  // If a file is ingested over existing data (including the range tombstones
  // used by range snapshots) or if a RocksDB snapshot is outstanding when this
  // ingest runs, then after moving/copying the file, historically RocksDB would
  // edit it (overwrite some of the bytes) to have a global sequence number.
  // After https://github.com/facebook/rocksdb/pull/4172 this can be disabled
  // (with the mutable manifest/metadata tracking that instead). However it is
  // only safe to disable the seqno write if older versions of RocksDB (<5.16)
  // will not be used to read these SSTs; luckily we no longer need to
  // interoperate with such older versions.
  ingest_options.write_global_seqno = false;
  // RocksDB checks the option allow_global_seqno and, if it is false, returns
  // an error instead of ingesting a file that would require one. However it
  // does this check *even if it is not planning on writing seqno* at all (and
  // we're not planning on writing any as per write_global_seqno above), so we
  // need to set allow_global_seqno to true.
  ingest_options.allow_global_seqno = true;
  // If there are mutations in the memtable for the keyrange covered by the file
  // being ingested, this option is checked. If true, the memtable is flushed
  // using a blocking, write-stalling flush and the ingest run. If false, an
  // error is returned.
  //
  // We want to ingest, but we do not want a write-stall, so we initially set it
  // to false -- if our ingest fails, we'll do a manual, no-stall flush and wait
  // for it to finish before trying the ingest again.
  ingest_options.allow_blocking_flush = false;

  rocksdb::Status status = db->rep->IngestExternalFile(paths_vec, ingest_options);
  if (status.IsInvalidArgument()) {
    // TODO(dt): inspect status to see if it has the message
    //          `External file requires flush`
    //           since the move_file and other errors also use kInvalidArgument.

    // It is possible we failed because the memtable required a flush but in the
    // options above, we set "allow_blocking_flush = false" preventing ingest
    // from running flush with allow_write_stall = true and halting foreground
    // traffic. Now that we know we need to flush, let's do one ourselves, with
    // allow_write_stall = false and wait for it. After it finishes we can retry
    // the ingest.
    rocksdb::FlushOptions flush_options;
    flush_options.allow_write_stall = false;
    flush_options.wait = true;

    rocksdb::Status flush_status = db->rep->Flush(flush_options);
    if (!flush_status.ok()) {
      return ToDBStatus(flush_status);
    }

    // Hopefully on this second attempt we will not need to flush at all, but
    // just in case we do, we'll allow the write stall this time -- that way we
    // can ensure we actually get the ingestion done and move on. A stalling
    // flush is be less than ideal, but since we just flushed, a) this shouldn't
    // happen often and b) if it does, it should be small and quick.
    ingest_options.allow_blocking_flush = true;
    status = db->rep->IngestExternalFile(paths_vec, ingest_options);
  }

  if (!status.ok()) {
    return ToDBStatus(status);
  }

  return kSuccess;
}

struct DBSstFileWriter {
  std::unique_ptr<rocksdb::Options> options;
  std::unique_ptr<rocksdb::Env> memenv;
  rocksdb::SstFileWriter rep;

  DBSstFileWriter(rocksdb::Options* o, rocksdb::Env* m)
      : options(o), memenv(m), rep(rocksdb::EnvOptions(), *o, o->comparator) {}
  virtual ~DBSstFileWriter() {}
};

DBSstFileWriter* DBSstFileWriterNew() {
  // TODO(dan): Right now, backup is the only user of this code, so that's what
  // the options are tuned for. If something else starts using it, we'll likely
  // have to add some configurability.

  rocksdb::BlockBasedTableOptions table_options;
  // Larger block size (4kb default) means smaller file at the expense of more
  // scanning during lookups.
  table_options.block_size = 32 * 1024;
  // The original LevelDB compatible format. We explicitly set the checksum too
  // to guard against the silent version upconversion. See
  // https://github.com/facebook/rocksdb/blob/972f96b3fbae1a4675043bdf4279c9072ad69645/include/rocksdb/table.h#L198
  table_options.format_version = 0;
  table_options.checksum = rocksdb::kCRC32c;
  table_options.whole_key_filtering = false;
  // This makes the sstables produced by Pebble and RocksDB byte-by-byte identical, which is
  // useful for testing.
  table_options.index_shortening =
      rocksdb::BlockBasedTableOptions::IndexShorteningMode::kShortenSeparatorsAndSuccessor;

  rocksdb::Options* options = new rocksdb::Options();
  options->comparator = &kComparator;
  options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  // Use the TablePropertiesCollector hook to store the min and max MVCC
  // timestamps present in each sstable in the metadata for that sstable. Used
  // by the time bounded iterator optimization.
  options->table_properties_collector_factories.emplace_back(DBMakeTimeBoundCollector());
  // Automatically request compactions whenever an SST contains too many range
  // deletions.
  options->table_properties_collector_factories.emplace_back(DBMakeDeleteRangeCollector());

  std::unique_ptr<rocksdb::Env> memenv;
  memenv.reset(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  options->env = memenv.get();

  return new DBSstFileWriter(options, memenv.release());
}

DBStatus DBSstFileWriterOpen(DBSstFileWriter* fw) {
  rocksdb::Status status = fw->rep.Open("sst");
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

namespace {
DBStatus DBSstFileWriterAddRaw(DBSstFileWriter* fw, const rocksdb::Slice key,
                               const rocksdb::Slice val) {
  rocksdb::Status status = fw->rep.Put(key, val);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  return kSuccess;
}
}  // namespace

DBStatus DBSstFileWriterAdd(DBSstFileWriter* fw, DBKey key, DBSlice val) {
  return DBSstFileWriterAddRaw(fw, EncodeKey(key), ToSlice(val));
}

DBStatus DBSstFileWriterDelete(DBSstFileWriter* fw, DBKey key) {
  rocksdb::Status status = fw->rep.Delete(EncodeKey(key));
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

DBStatus DBSstFileWriterDeleteRange(DBSstFileWriter* fw, DBKey start, DBKey end) {
  rocksdb::Status status = fw->rep.DeleteRange(EncodeKey(start), EncodeKey(end));
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

DBStatus DBSstFileWriterCopyData(DBSstFileWriter* fw, DBString* data) {
  uint64_t file_size;
  rocksdb::Status status = fw->memenv->GetFileSize("sst", &file_size);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  if (file_size == 0) {
    return kSuccess;
  }

  const rocksdb::EnvOptions soptions;
  std::unique_ptr<rocksdb::SequentialFile> sst;
  status = fw->memenv->NewSequentialFile("sst", &sst, soptions);
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  // scratch is eventually returned as the array part of data and freed by the
  // caller.
  char* scratch = static_cast<char*>(malloc(file_size));

  rocksdb::Slice sst_contents;
  status = sst->Read(file_size, &sst_contents, scratch);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  if (sst_contents.size() != file_size) {
        return FmtStatus("expected to read %" PRIu64 " bytes but got %zu", file_size,
                     sst_contents.size());
  }

  // The contract of the SequentialFile.Read call above is that it _might_ use
  // scratch as the backing data for sst_contents, but it also _might not_. If
  // it didn't, copy sst_contents into scratch, so we can unconditionally return
  // a DBString backed by scratch (which can then always be freed by the
  // caller). Note that this means the data is always copied exactly once,
  // either by Read or here.
  if (sst_contents.data() != scratch) {
    memcpy(scratch, sst_contents.data(), sst_contents.size());
  }
  data->data = scratch;
  data->len = sst_contents.size();

  return kSuccess;
}

DBStatus DBSstFileWriterTruncate(DBSstFileWriter* fw, DBString* data) {
  DBStatus status = DBSstFileWriterCopyData(fw, data);
  if (status.data != NULL) {
    return status;
  }
  return ToDBStatus(fw->memenv->Truncate("sst", 0));
}

DBStatus DBSstFileWriterFinish(DBSstFileWriter* fw, DBString* data) {
  rocksdb::Status status = fw->rep.Finish();
  if (!status.ok()) {
    return ToDBStatus(status);
  }

  return DBSstFileWriterCopyData(fw, data);
}

void DBSstFileWriterClose(DBSstFileWriter* fw) { delete fw; }

DBStatus DBLockFile(DBSlice filename, DBFileLock* lock) {
  return ToDBStatus(
      rocksdb::Env::Default()->LockFile(ToString(filename), (rocksdb::FileLock**)lock));
}

DBStatus DBUnlockFile(DBFileLock lock) {
  return ToDBStatus(rocksdb::Env::Default()->UnlockFile((rocksdb::FileLock*)lock));
}

DBStatus DBExportToSst(DBKey start, DBKey end, bool export_all_revisions,
                       uint64_t target_size, uint64_t max_size,
                       DBIterOptions iter_opts, DBEngine* engine, DBString* data,
                       DBString* write_intent, DBString* summary, DBString* resume) {
  DBSstFileWriter* writer = DBSstFileWriterNew();
  DBStatus status = DBSstFileWriterOpen(writer);
  if (status.data != NULL) {
    return status;
  }

  DBIncrementalIterator iter(engine, iter_opts, start, end, write_intent);

  roachpb::BulkOpSummary bulkop_summary;
  RowCounter row_counter(&bulkop_summary);

  bool skip_current_key_versions = !export_all_revisions;
  DBIterState state;
  const std::string end_key = EncodeKey(end);
  // cur_key is used when paginated is true and export_all_revisions is
  // true. If we're exporting all revisions and we're returning a paginated
  // SST then we need to keep track of when we've finished adding all of the
  // versions of a key to the writer.
  const bool paginated = target_size > 0;
  std::string cur_key;
  std::string resume_key;
  // Seek to the MVCC metadata key for the provided start key and let the
  // incremental iterator find the appropriate version.
  const DBKey seek_key = {.key = start.key};
  for (state = iter.seek(seek_key);; state = iter.next(skip_current_key_versions)) {
    if (state.status.data != NULL) {
      DBSstFileWriterClose(writer);
      return state.status;
    } else if (!state.valid || kComparator.Compare(iter.key(), end_key) >= 0) {
      break;
    }
    rocksdb::Slice decoded_key;
    int64_t wall_time = 0;
    int32_t logical_time = 0;

    if (!DecodeKey(iter.key(), &decoded_key, &wall_time, &logical_time)) {
      DBSstFileWriterClose(writer);
      return ToDBString("Unable to decode key");
    }

    const bool is_new_key = !export_all_revisions || decoded_key.compare(cur_key) != 0;
    if (paginated && export_all_revisions && is_new_key) {
      // Reuse the underlying buffer in cur_key.
      cur_key.clear();
      cur_key.reserve(decoded_key.size());
      cur_key.assign(decoded_key.data(), decoded_key.size());
    }

    // Skip tombstone (len=0) records when start time is zero (non-incremental)
    // and we are not exporting all versions.
    const bool is_skipping_deletes =
        start.wall_time == 0 && start.logical == 0 && !export_all_revisions;
    if (is_skipping_deletes && iter.value().size() == 0) {
      continue;
    }

    // Check to see if this is the first version of key and adding it would
    // put us over the limit (we might already be over the limit).
    const int64_t cur_size = bulkop_summary.data_size();
    const bool reached_target_size = cur_size > 0 && cur_size >= target_size;
    if (paginated && is_new_key && reached_target_size) {
      resume_key.reserve(decoded_key.size());
      resume_key.assign(decoded_key.data(), decoded_key.size());
      break;
    }

    // Insert key into sst and update statistics.
    status = DBSstFileWriterAddRaw(writer, iter.key(), iter.value());
    if (status.data != NULL) {
      DBSstFileWriterClose(writer);
      return status;
    }

    if (!row_counter.Count(iter.key())) {
      return ToDBString("Error in row counter");
    }
    const int64_t new_size = cur_size + decoded_key.size() + iter.value().size();
    if (max_size > 0 && new_size > max_size) {
      return FmtStatus("export size (%" PRIi64 " bytes) exceeds max size (%" PRIi64 " bytes)",
                       new_size, max_size);
    }
    bulkop_summary.set_data_size(new_size);
  }
  *summary = ToDBString(bulkop_summary.SerializeAsString());

  if (bulkop_summary.data_size() == 0) {
    DBSstFileWriterClose(writer);
    return kSuccess;
  }

  auto res = DBSstFileWriterFinish(writer, data);
  DBSstFileWriterClose(writer);

  // If we're not returning an error, check to see if we need to return the resume key.
  if (res.data == NULL && resume_key.length() > 0) {
    *resume = ToDBString(resume_key);
  }

  return res;
}

DBStatus DBEnvOpenReadableFile(DBEngine* db, DBSlice path, DBReadableFile* file) {
  return db->EnvOpenReadableFile(path, (rocksdb::RandomAccessFile**)file);
}

DBStatus DBEnvReadAtFile(DBEngine* db, DBReadableFile file, DBSlice buffer, int64_t offset,
                         int* n) {
  return db->EnvReadAtFile((rocksdb::RandomAccessFile*)file, buffer, offset, n);
}

DBStatus DBEnvCloseReadableFile(DBEngine* db, DBReadableFile file) {
  return db->EnvCloseReadableFile((rocksdb::RandomAccessFile*)file);
}

DBStatus DBEnvOpenDirectory(DBEngine* db, DBSlice path, DBDirectory* file) {
  return db->EnvOpenDirectory(path, (rocksdb::Directory**)file);
}

DBStatus DBEnvSyncDirectory(DBEngine* db, DBDirectory file) {
  return db->EnvSyncDirectory((rocksdb::Directory*)file);
}

DBStatus DBEnvCloseDirectory(DBEngine* db, DBDirectory file) {
  return db->EnvCloseDirectory((rocksdb::Directory*)file);
}

DBStatus DBEnvRenameFile(DBEngine* db, DBSlice oldname, DBSlice newname) {
  return db->EnvRenameFile(oldname, newname);
}

DBStatus DBEnvCreateDir(DBEngine* db, DBSlice name) {
  return db->EnvCreateDir(name);
}

DBStatus DBEnvDeleteDir(DBEngine* db, DBSlice name) {
  return db->EnvDeleteDir(name);
}

DBListDirResults DBEnvListDir(DBEngine* db, DBSlice name) {
  DBListDirResults result;
  std::vector<std::string> contents;
  result.status = db->EnvListDir(name, &contents);
  result.n = contents.size();
  // We malloc the names so it can be deallocated by the caller using free().
  const int size = contents.size() * sizeof(DBString);
  result.names = reinterpret_cast<DBString*>(malloc(size));
  memset(result.names, 0, size);
  for (int i = 0; i < contents.size(); i++) {
    result.names[i] = ToDBString(rocksdb::Slice(contents[i].data(), contents[i].size()));
  }
  return result;
}

DBString DBDumpThreadStacks() {
  return ToDBString(DumpThreadStacks());
}
