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

#include "options.h"
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include "cache.h"
#include "comparator.h"
#include "encoding.h"
#include "godefs.h"
#include "merge.h"
#include "protos/util/log/log.pb.h"
#include "table_props.h"

namespace cockroach {

namespace {

class DBPrefixExtractor : public rocksdb::SliceTransform {
 public:
  DBPrefixExtractor() {}

  virtual const char* Name() const { return "cockroach_prefix_extractor"; }

  // MVCC keys are encoded as <user-key>/<timestamp>. Extract the <user-key>
  // prefix which will allow for more efficient iteration over the keys
  // matching a particular <user-key>. Specifically, the <user-key> will be
  // added to the per table bloom filters and will be used to skip tables
  // which do not contain the <user-key>.
  virtual rocksdb::Slice Transform(const rocksdb::Slice& src) const { return KeyPrefix(src); }

  virtual bool InDomain(const rocksdb::Slice& src) const { return true; }
};

// The DBLogger is a rocksdb::Logger that calls back into Go code for formatted logging.
class DBLogger : public rocksdb::Logger {
 public:
  DBLogger(int info_verbosity) : info_verbosity_(info_verbosity) {}

  virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format,
                    va_list ap) override {
    int go_log_level = util::log::Severity::UNKNOWN;  // compiler tells us to initialize it
    switch (log_level) {
      case rocksdb::DEBUG_LEVEL:
        // There is no DEBUG severity. Just give it INFO severity, then.
        go_log_level = util::log::Severity::INFO;
        break;
      case rocksdb::INFO_LEVEL:
        go_log_level = util::log::Severity::INFO;
        break;
      case rocksdb::WARN_LEVEL:
        go_log_level = util::log::Severity::WARNING;
        break;
      case rocksdb::ERROR_LEVEL:
        go_log_level = util::log::Severity::ERROR;
        break;
      case rocksdb::FATAL_LEVEL:
        go_log_level = util::log::Severity::FATAL;
        break;
      case rocksdb::HEADER_LEVEL:
        // There is no HEADER severity. Just give it INFO severity, then.
        go_log_level = util::log::Severity::INFO;
        break;
      case rocksdb::NUM_INFO_LOG_LEVELS:
        assert(false);
        return;
    }
    if (!rocksDBV(go_log_level, info_verbosity_)) {
      return;
    }

    // First try with a small fixed size buffer.
    char space[1024];

    // It's possible for methods that use a va_list to invalidate the data in
    // it upon use. The fix is to make a copy of the structure before using it
    // and use that copy instead.
    va_list backup_ap;
    va_copy(backup_ap, ap);
    int result = vsnprintf(space, sizeof(space), format, backup_ap);
    va_end(backup_ap);

    if ((result >= 0) && (result < sizeof(space))) {
      rocksDBLog(go_log_level, space, result);
      return;
    }

    // Repeatedly increase buffer size until it fits.
    int length = sizeof(space);
    while (true) {
      if (result < 0) {
        // Older behavior: just try doubling the buffer size.
        length *= 2;
      } else {
        // We need exactly "result+1" characters.
        length = result + 1;
      }
      char* buf = new char[length];

      // Restore the va_list before we use it again
      va_copy(backup_ap, ap);
      result = vsnprintf(buf, length, format, backup_ap);
      va_end(backup_ap);

      if ((result >= 0) && (result < length)) {
        // It fit
        rocksDBLog(go_log_level, buf, result);
        delete[] buf;
        return;
      }
      delete[] buf;
    }
  }

  virtual void Logv(const char* format, va_list ap) override {
    // The RocksDB API tries to force us to separate the severity check (above function)
    // from the actual logging (this function) by making this function pure virtual.
    // However, when calling into Go, we need to provide severity level to both the severity
    // level check function (`rocksDBV`) and the actual logging function (`rocksDBLog`). So,
    // we do all the work in the function that has severity level and then expect this
    // function to never be called.
    assert(false);
  }

 private:
  const int info_verbosity_;
};

}  // namespace

rocksdb::Logger* NewDBLogger(int info_verbosity) { return new DBLogger(info_verbosity); }

rocksdb::Options DBMakeOptions(DBOptions db_opts) {
  // Use the rocksdb options builder to configure the base options
  // using our memtable budget.
  rocksdb::Options options;
  // Increase parallelism for compactions and flushes based on the
  // number of cpus. Always use at least 2 threads, otherwise
  // compactions and flushes may fight with each other.
  options.IncreaseParallelism(std::max(db_opts.num_cpu, 2));
  // Disable subcompactions since they're a less stable feature, and not
  // necessary for our workload, where frequent fsyncs naturally prevent
  // foreground writes from getting too far ahead of compactions.
  options.max_subcompactions = 1;
  options.comparator = &kComparator;
  options.create_if_missing = !db_opts.must_exist;
  options.info_log.reset(NewDBLogger(kDefaultVerbosityForInfoLogging));
  options.merge_operator.reset(NewMergeOperator());
  options.prefix_extractor.reset(new DBPrefixExtractor);
  options.statistics = rocksdb::CreateDBStatistics();
  options.max_open_files = db_opts.max_open_files;
  options.compaction_pri = rocksdb::kMinOverlappingRatio;
  // Periodically sync both the WAL and SST writes to smooth out disk
  // usage. Not performing such syncs can be faster but can cause
  // performance blips when the OS decides it needs to flush data.
  options.wal_bytes_per_sync = 512 << 10;  // 512 KB
  options.bytes_per_sync = 512 << 10;      // 512 KB

  // The size reads should be performed in for compaction. The
  // internets claim this can speed up compactions, though RocksDB
  // docs say it is only useful on spinning disks. Experimentally it
  // has had no effect.
  // options.compaction_readahead_size = 2 << 20;

  // Do not create bloom filters for the last level (i.e. the largest
  // level which contains data in the LSM store). Setting this option
  // reduces the size of the bloom filters by 10x. This is significant
  // given that bloom filters require 1.25 bytes (10 bits) per key
  // which can translate into gigabytes of memory given typical key
  // and value sizes. The downside is that bloom filters will only be
  // usable on the higher levels, but that seems acceptable. We
  // typically see read amplification of 5-6x on clusters (i.e. there
  // are 5-6 levels of sstables) which means we'll achieve 80-90% of
  // the benefit of having bloom filters on every level for only 10%
  // of the memory cost.
  options.optimize_filters_for_hits = true;

  // We periodically report stats ourselves and by default the info
  // logger swallows log messages.
  options.stats_dump_period_sec = 0;

  // Use the TablePropertiesCollector hook to store the min and max MVCC
  // timestamps present in each sstable in the metadata for that sstable.
  options.table_properties_collector_factories.emplace_back(DBMakeTimeBoundCollector());

  // Automatically request compactions whenever an SST contains too many range
  // deletions.
  options.table_properties_collector_factories.emplace_back(DBMakeDeleteRangeCollector());

  // The write buffer size is the size of the in memory structure that
  // will be flushed to create L0 files.
  options.write_buffer_size = 64 << 20;  // 64 MB
  // How much memory should be allotted to memtables? Note that this
  // is a peak setting, steady state should be lower. We set this
  // relatively high to account for bursts of writes (e.g. due to a
  // deletion of a large range of keys). In particular, we want this
  // to be somewhat larger than than typical range size so that
  // deletion of a range worth of keys does not cause write stalls.
  options.max_write_buffer_number = 4;
  // Number of files to trigger L0 compaction. We set this low so that
  // we quickly move files out of L0 as each L0 file increases read
  // amplification.
  options.level0_file_num_compaction_trigger = 2;
  // Soft limit on number of L0 files. Writes are slowed down when
  // this number is reached. Bulk-ingestion can add lots of files
  // suddenly, so setting this much higher should avoid spurious
  // slowdowns to writes.
  // TODO(dt): if/when we dynamically tune for bulk-ingestion, we
  // could leave this at 20 and only raise it during ingest jobs.
  options.level0_slowdown_writes_trigger = 200;
  // Maximum number of L0 files. Writes are stopped at this
  // point. This is set significantly higher than
  // level0_slowdown_writes_trigger to avoid completely blocking
  // writes.
  // TODO(dt): if/when we dynamically tune for bulk-ingestion, we
  // could leave this at 30 and only raise it during ingest.
  options.level0_stop_writes_trigger = 400;
  // Maximum estimated pending compaction bytes before slowing writes.
  // Default is 64gb but that can be hit during bulk-ingestion since it
  // is based on assumptions about relative level sizes that do not hold
  // during bulk-ingestion.
  // TODO(dt): if/when we dynamically tune for bulk-ingestion, we
  // could leave these as-is and only raise / disable them during ingest.
  options.soft_pending_compaction_bytes_limit = 256 * 1073741824ull;
  options.hard_pending_compaction_bytes_limit = 512 * 1073741824ull;
  // Flush write buffers to L0 as soon as they are full. A higher
  // value could be beneficial if there are duplicate records in each
  // of the individual write buffers, but perf testing hasn't shown
  // any benefit so far.
  options.min_write_buffer_number_to_merge = 1;
  // Enable dynamic level sizing which reduces both size and write
  // amplification. This causes RocksDB to pick the target size of
  // each level dynamically.
  options.level_compaction_dynamic_level_bytes = true;
  // Follow the RocksDB recommendation to configure the size of L1 to
  // be the same as the estimated size of L0.
  options.max_bytes_for_level_base = 64 << 20;  // 64 MB
  options.max_bytes_for_level_multiplier = 10;
  // Target the base file size (L1) as 4 MB. Each additional level
  // grows the file size by 2. With max_bytes_for_level_base set to 64
  // MB, this translates into the following target level and file
  // sizes for each level:
  //
  //       level-size  file-size  max-files
  //   L1:      64 MB       4 MB         16
  //   L2:     640 MB       8 MB         80
  //   L3:    6.25 GB      16 MB        400
  //   L4:    62.5 GB      32 MB       2000
  //   L5:     625 GB      64 MB      10000
  //   L6:     6.1 TB     128 MB      50000
  //
  // Due to the use of level_compaction_dynamic_level_bytes most data
  // will be in L6. The number of files will be approximately
  // total-data-size / 128 MB.
  //
  // We don't want the target file size to be too large, otherwise
  // individual compactions become more expensive. We don't want the
  // target file size to be too small or else we get an overabundance
  // of sstables.
  options.target_file_size_base = 4 << 20;  // 4 MB
  options.target_file_size_multiplier = 2;
  options.manual_wal_flush = true;

  // Because we open a long running rocksdb instance, we do not want the
  // manifest file to grow unbounded. Assuming each manifest entry is about 1
  // KB, this allows for 128 K entries. This could account for several hours to
  // few months of runtime without rolling based on the workload.
  options.max_manifest_file_size = 128 << 20;  // 128 MB

  rocksdb::BlockBasedTableOptions table_options;
  if (db_opts.cache != nullptr) {
    table_options.block_cache = db_opts.cache->rep;

    // Reserve 1 memtable worth of memory from the cache. Under high
    // load situations we'll be using somewhat more than 1 memtable,
    // but usually not significantly more unless there is an I/O
    // throughput problem.
    //
    // We ensure that at least 1MB is allocated for the block cache.
    // Some unit tests expect to see a non-zero block cache hit rate,
    // but they use a cache that is small enough that all of it would
    // otherwise be reserved for the memtable.
    std::lock_guard<std::mutex> guard(db_opts.cache->mu);
    const int64_t capacity = db_opts.cache->rep->GetCapacity();
    const int64_t new_capacity = std::max<int64_t>(1 << 20, capacity - options.write_buffer_size);
    db_opts.cache->rep->SetCapacity(new_capacity);
  }

  // Pass false for use_blocked_base_builder creates a per file
  // (sstable) filter instead of a per-block filter. The per file
  // filter can be consulted before going to the index which saves an
  // index lookup. The cost is an 4-bytes per key in memory during
  // compactions, which seems a small price to pay.
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false /* !block_based */));
  table_options.format_version = 2;

  // Increasing block_size decreases memory usage at the cost of
  // increased read amplification. When reading a key-value pair from
  // a table file, RocksDB loads an entire block into memory. The
  // RocksDB default is 4KB. This sets it to 32KB.
  table_options.block_size = 32 << 10;
  // Disable whole_key_filtering which adds a bloom filter entry for
  // the "whole key", doubling the size of our bloom filters. This is
  // used to speed up Get operations which we don't use.
  table_options.whole_key_filtering = false;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  return options;
}

}  // namespace cockroach
