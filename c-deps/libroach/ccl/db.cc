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
#include "../batch.h"
#include "../comparator.h"
#include "../status.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "key_manager.h"

using namespace cockroach;

namespace cockroach {

// DBOpenHook parses the extra_options field of DBOptions and initializes encryption objects if
// needed.
rocksdb::Status DBOpenHook(const std::string& db_dir, const DBOptions db_opts) {
  DBSlice options = db_opts.extra_options;
  if (options.len == 0) {
    return rocksdb::Status::OK();
  }

  cockroach::ccl::baseccl::EncryptionOptions opts;
  if (!opts.ParseFromArray(options.data, options.len)) {
    return rocksdb::Status::InvalidArgument("failed to parse extra options");
  }

  if (opts.key_source() != cockroach::ccl::baseccl::KeyFiles) {
    return rocksdb::Status::InvalidArgument("unknown encryption key source");
  }

  std::cout << "found encryption options:\n"
            << "  active key: " << opts.key_files().current_key() << "\n"
            << "  old key: " << opts.key_files().old_key() << "\n"
            << "  rotation duration: " << opts.data_key_rotation_period() << "\n";

  // Initialize store key manager.
  std::shared_ptr<FileKeyManager> store_key_manager(new FileKeyManager(
      rocksdb::Env::Default(), opts.key_files().current_key(), opts.key_files().old_key()));
  rocksdb::Status status = store_key_manager->LoadKeys();
  if (!status.ok()) {
    return status;
  }

  // Initialize data key manager.
  std::shared_ptr<DataKeyManager> data_key_manager(
      new DataKeyManager(rocksdb::Env::Default(), db_dir, opts.data_key_rotation_period()));
  status = data_key_manager->LoadKeys();
  if (!status.ok()) {
    return status;
  }

  // Generate a new data key if needed by giving the active store key info to the data key manager.
  std::unique_ptr<enginepbccl::KeyInfo> store_key = store_key_manager->CurrentKeyInfo();
  assert(store_key != nullptr);
  status = data_key_manager->SetActiveStoreKey(std::move(store_key));
  if (!status.ok()) {
    return status;
  }

  return rocksdb::Status::InvalidArgument("encryption is not supported");
}

}  // namespace cockroach

DBStatus DBBatchReprVerify(DBSlice repr, DBKey start, DBKey end, int64_t now_nanos,
                           MVCCStatsResult* stats) {
  // TODO(dan): Inserting into a batch just to iterate over it is unfortunate.
  // Consider replacing this with WriteBatch's Iterate/Handler mechanism and
  // computing MVCC stats on the post-ApplyBatchRepr engine. splitTrigger does
  // the latter and it's a headache for propEvalKV, so wait to see how that
  // settles out before doing it that way.
  rocksdb::WriteBatchWithIndex batch(&kComparator, 0, true);
  rocksdb::WriteBatch b(ToString(repr));
  std::unique_ptr<rocksdb::WriteBatch::Handler> inserter(GetDBBatchInserter(&batch));
  rocksdb::Status status = b.Iterate(inserter.get());
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  std::unique_ptr<rocksdb::Iterator> iter;
  iter.reset(batch.NewIteratorWithBase(rocksdb::NewEmptyIterator()));

  iter->SeekToFirst();
  if (iter->Valid() && kComparator.Compare(iter->key(), EncodeKey(start)) < 0) {
    return FmtStatus("key not in request range");
  }
  iter->SeekToLast();
  if (iter->Valid() && kComparator.Compare(iter->key(), EncodeKey(end)) >= 0) {
    return FmtStatus("key not in request range");
  }

  *stats = MVCCComputeStatsInternal(iter.get(), start, end, now_nanos);

  return kSuccess;
}
