// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "../db.h"
#include <iostream>
#include <libroachccl.h>
#include <memory>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include "../protosccl/ccl/baseccl/encryption_options.pb.h"
#include "key_manager.h"

const DBStatus kSuccess = {NULL, 0};

rocksdb::Status parse_extra_options(const DBSlice s) {
  if (s.len == 0) {
    return rocksdb::Status::OK();
  }

  cockroach::ccl::baseccl::EncryptionOptions opts;
  if (!opts.ParseFromArray(s.data, s.len)) {
    return rocksdb::Status::InvalidArgument("failed to parse extra options");
  }

  if (opts.key_source() != cockroach::ccl::baseccl::KeyFiles) {
    return rocksdb::Status::InvalidArgument("unknown encryption key source");
  }

  std::cout << "found encryption options:\n"
            << "  active key: " << opts.key_files().current_key() << "\n"
            << "  old key: " << opts.key_files().old_key() << "\n"
            << "  rotation duration: " << opts.data_key_rotation_period() << "\n";

  std::shared_ptr<FileKeyManager> key_manager(
      new FileKeyManager(rocksdb::Env::Default(), opts.key_files().current_key(), opts.key_files().old_key()));

  rocksdb::Status status = key_manager->LoadKeys();
  if (!status.ok()) {
    return status;
  }

  return rocksdb::Status::InvalidArgument("encryption is not supported");
}

// DBOpenHook parses the extra_options field of DBOptions.
rocksdb::Status DBOpenHook(const DBOptions opts) { return parse_extra_options(opts.extra_options); }

DBStatus DBBatchReprVerify(DBSlice repr, DBKey start, DBKey end, int64_t now_nanos, MVCCStatsResult* stats) {
  const rocksdb::Comparator* kComparator = CockroachComparator();

  // TODO(dan): Inserting into a batch just to iterate over it is unfortunate.
  // Consider replacing this with WriteBatch's Iterate/Handler mechanism and
  // computing MVCC stats on the post-ApplyBatchRepr engine. splitTrigger does
  // the latter and it's a headache for propEvalKV, so wait to see how that
  // settles out before doing it that way.
  rocksdb::WriteBatchWithIndex batch(kComparator, 0, true);
  rocksdb::WriteBatch b(ToString(repr));
  std::unique_ptr<rocksdb::WriteBatch::Handler> inserter(GetDBBatchInserter(&batch));
  rocksdb::Status status = b.Iterate(inserter.get());
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  std::unique_ptr<rocksdb::Iterator> iter;
  iter.reset(batch.NewIteratorWithBase(rocksdb::NewEmptyIterator()));

  iter->SeekToFirst();
  if (iter->Valid() && kComparator->Compare(iter->key(), EncodeKey(start)) < 0) {
    return FmtStatus("key not in request range");
  }
  iter->SeekToLast();
  if (iter->Valid() && kComparator->Compare(iter->key(), EncodeKey(end)) >= 0) {
    return FmtStatus("key not in request range");
  }

  *stats = MVCCComputeStatsInternal(iter.get(), start, end, now_nanos);

  return kSuccess;
}
