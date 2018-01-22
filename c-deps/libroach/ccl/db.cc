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
#include "../env_manager.h"
#include "../status.h"
#include "../switching_provider.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "ctr_stream.h"
#include "key_manager.h"

using namespace cockroach;

namespace cockroach {

// DBOpenHook parses the extra_options field of DBOptions and initializes encryption objects if
// needed.
rocksdb::Status DBOpenHook(const std::string& db_dir, const DBOptions db_opts,
                           EnvManager* env_ctx) {
  DBSlice options = db_opts.extra_options;
  if (options.len == 0) {
    return rocksdb::Status::OK();
  }

  // The Go code sets the "switching_env" storage version if we specified encryption flags,
  // but let's double check anyway.
  if (!db_opts.use_switching_env) {
    return rocksdb::Status::InvalidArgument(
        "on-disk version does not support encryption, but we found encryption flags");
  }

  // Switching env enabled means we have a switching provider.
  assert(env_ctx->switching_provider != nullptr);

  // Parse extra_options.
  cockroach::ccl::baseccl::EncryptionOptions opts;
  if (!opts.ParseFromArray(options.data, options.len)) {
    return rocksdb::Status::InvalidArgument("failed to parse extra options");
  }

  if (opts.key_source() != cockroach::ccl::baseccl::KeyFiles) {
    return rocksdb::Status::InvalidArgument("unknown encryption key source");
  }

  // Initialize store key manager.
  // NOTE: FileKeyManager uses the default env as the MemEnv can never have pre-populated files.
  FileKeyManager* store_key_manager = new FileKeyManager(
      rocksdb::Env::Default(), opts.key_files().current_key(), opts.key_files().old_key());
  rocksdb::Status status = store_key_manager->LoadKeys();
  if (!status.ok()) {
    delete store_key_manager;
    return status;
  }

  // Register a CTR stream creator at the store level. It uses (and owns) the store key manager.
  // The switching provider owns the stream creator.
  env_ctx->switching_provider->RegisterCipherStreamCreator(
      enginepb::Store, new CTRCipherStreamCreator(store_key_manager));

  // Construct an EncryptedEnv pointing at the "store" level.
  // It wraps the base_env (Default or Mem).
  // It is owned by the env context. It is an intermediate, so is not used as the db_env.
  rocksdb::Env* store_keyed_env =
      NewEncryptedEnv(env_ctx->base_env, env_ctx->switching_provider.get(), enginepb::Store);
  env_ctx->TakeEnvOwnership(store_keyed_env);

  // Initialize data key manager using the stored-keyed-env.
  DataKeyManager* data_key_manager =
      new DataKeyManager(store_keyed_env, db_dir, opts.data_key_rotation_period());
  status = data_key_manager->LoadKeys();
  if (!status.ok()) {
    delete data_key_manager;
    return status;
  }

  // Register a CTR stream creator at the data level. It uses (and owns) the data key manager.
  // The switching provider owns the stream creator.
  env_ctx->switching_provider->RegisterCipherStreamCreator(
      enginepb::Data, new CTRCipherStreamCreator(data_key_manager));

  // Construct an EncryptedEnv pointing to the DataKeyManager as a key source.
  // It wraps the base_env (Default or Mem).
  // It is owned by the env context, and is used as the db_env.
  rocksdb::Env* data_keyed_env =
      NewEncryptedEnv(env_ctx->base_env, env_ctx->switching_provider.get(), enginepb::Data);
  env_ctx->TakeEnvOwnership(data_keyed_env);
  env_ctx->db_env = data_keyed_env;

  // Fetch the current store key info.
  std::unique_ptr<enginepbccl::KeyInfo> store_key = store_key_manager->CurrentKeyInfo();
  assert(store_key != nullptr);

  // Generate a new data key if needed by giving the active store key info to the data key manager.
  status = data_key_manager->SetActiveStoreKey(std::move(store_key));
  if (!status.ok()) {
    return status;
  }

  // TODO(mberhault): enable at some point. We still want to make sure people do not use it.
  return rocksdb::Status::InvalidArgument("encryption is not supported");
}

}  // namespace cockroach

DBStatus DBBatchReprVerify(DBSlice repr, DBKey start, DBKey end, int64_t now_nanos,
                           MVCCStatsResult* stats) {
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
