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
#include "fmt.h"
#include "getter.h"
#include "iterator.h"
#include "protos/storage/engine/enginepb/rocksdb.pb.h"
#include "status.h"

using namespace cockroach;

DBEngine::~DBEngine() {}

DBStatus DBEngine::AssertPreClose() {
  return kSuccess;
}

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

DBImpl::DBImpl(rocksdb::DB* r, rocksdb::Env* m, std::shared_ptr<rocksdb::Cache> bc,
               std::shared_ptr<DBEventListener> event_listener, rocksdb::Env* s_env)
    : DBEngine(r, &iters_count),
      switching_env(s_env),
      memenv(m),
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

}  // namespace cockroach
