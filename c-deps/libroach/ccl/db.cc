// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "db.h"
#include <iostream>
#include <libroachccl.h>
#include <memory>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include "../batch.h"
#include "../comparator.h"
#include "../encoding.h"
#include "../env_manager.h"
#include "../options.h"
#include "../rocksdbutils/env_encryption.h"
#include "../status.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "ccl/storageccl/engineccl/enginepbccl/stats.pb.h"
#include "crypto_utils.h"
#include "ctr_stream.h"
#include "key_manager.h"

using namespace cockroach;

namespace cockroach {

class CCLEnvStatsHandler : public EnvStatsHandler {
 public:
  explicit CCLEnvStatsHandler(DataKeyManager* data_key_manager)
      : data_key_manager_(data_key_manager) {}
  virtual ~CCLEnvStatsHandler() {}

  virtual rocksdb::Status GetEncryptionStats(std::string* serialized_stats) override {
    if (data_key_manager_ == nullptr) {
      return rocksdb::Status::OK();
    }

    enginepbccl::EncryptionStatus enc_status;

    // GetActiveStoreKeyInfo returns a unique_ptr containing a copy. Transfer ownership from the
    // unique_ptr to the proto. set_allocated_active_store deletes the existing field, if any.
    enc_status.set_allocated_active_store_key(data_key_manager_->GetActiveStoreKeyInfo().release());

    // CurrentKeyInfo returns a unique_ptr containing a copy. Transfer ownership from the
    // unique_ptr to the proto. set_allocated_active_store deletes the existing field, if any.
    enc_status.set_allocated_active_data_key(data_key_manager_->CurrentKeyInfo().release());

    if (!enc_status.SerializeToString(serialized_stats)) {
      return rocksdb::Status::InvalidArgument("failed to serialize encryption status");
    }
    return rocksdb::Status::OK();
  }

  virtual rocksdb::Status GetEncryptionRegistry(std::string* serialized_registry) override {
    if (data_key_manager_ == nullptr) {
      return rocksdb::Status::OK();
    }

    auto key_registry = data_key_manager_->GetScrubbedRegistry();
    if (key_registry == nullptr) {
      return rocksdb::Status::OK();
    }

    if (!key_registry->SerializeToString(serialized_registry)) {
      return rocksdb::Status::InvalidArgument("failed to serialize data keys registry");
    }

    return rocksdb::Status::OK();
  }

  virtual std::string GetActiveDataKeyID() override {
    // Look up the current data key.
    if (data_key_manager_ == nullptr) {
      // No data key manager: plaintext.
      return kPlainKeyID;
    }

    auto active_key_info = data_key_manager_->CurrentKeyInfo();
    if (active_key_info == nullptr) {
      // Plaintext.
      return kPlainKeyID;
    }
    return active_key_info->key_id();
  }

  virtual int32_t GetActiveStoreKeyType() override {
    if (data_key_manager_ == nullptr) {
      return enginepbccl::Plaintext;
    }

    auto store_key_info = data_key_manager_->GetActiveStoreKeyInfo();
    if (store_key_info == nullptr) {
      return enginepbccl::Plaintext;
    }

    return store_key_info->encryption_type();
  }

  virtual rocksdb::Status GetFileEntryKeyID(const enginepb::FileEntry* entry,
                                            std::string* id) override {
    if (entry == nullptr) {
      // No file entry: file written in plaintext before the file registry was used.
      *id = kPlainKeyID;
      return rocksdb::Status::OK();
    }

    enginepbccl::EncryptionSettings enc_settings;
    if (!enc_settings.ParseFromString(entry->encryption_settings())) {
      return rocksdb::Status::InvalidArgument("failed to parse encryption settings");
    }

    if (enc_settings.encryption_type() == enginepbccl::Plaintext) {
      *id = kPlainKeyID;
    } else {
      *id = enc_settings.key_id();
    }

    return rocksdb::Status::OK();
  }

 private:
  // The DataKeyManager is needed to get key information but is not owned by the StatsHandler.
  DataKeyManager* data_key_manager_;
};

// DBOpenHookCCL parses the extra_options field of DBOptions and initializes
// encryption objects if needed.
rocksdb::Status DBOpenHookCCL(std::shared_ptr<rocksdb::Logger> info_log, const std::string& db_dir,
                              const DBOptions db_opts, EnvManager* env_mgr) {
  DBSlice options = db_opts.extra_options;
  if (options.len == 0) {
    return rocksdb::Status::OK();
  }

  // The Go code sets the "file_registry" storage version if we specified encryption flags,
  // but let's double check anyway.
  if (!db_opts.use_file_registry) {
    return rocksdb::Status::InvalidArgument(
        "on-disk version does not support encryption, but we found encryption flags");
  }

  // We log to the primary CockroachDB log for encryption status
  // instead of the RocksDB specific log. This should only be used to
  // occasional logging (eg: key loading and rotation).
  std::shared_ptr<rocksdb::Logger> logger(NewDBLogger(true /* use_primary_log */));

  // We have encryption options. Check whether the AES instruction set is supported.
  if (!UsesAESNI()) {
    rocksdb::Warn(
        logger, "*** WARNING*** Encryption requested, but no AES instruction set detected: expect "
                "significant performance degradation!");
  }

  // Attempt to disable core dumps.
  auto status = DisableCoreFile();
  if (!status.ok()) {
    rocksdb::Warn(logger,
                  "*** WARNING*** Encryption requested, but could not disable core dumps: %s. Keys "
                  "may be leaked in core dumps!",
                  status.getState());
  }

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
      rocksdb::Env::Default(), logger, opts.key_files().current_key(), opts.key_files().old_key());
  status = store_key_manager->LoadKeys();
  if (!status.ok()) {
    delete store_key_manager;
    return status;
  }

  // Create a cipher stream creator using the store_key_manager.
  auto store_stream = new CTRCipherStreamCreator(store_key_manager, enginepb::Store);

  // Construct an EncryptedEnv using this stream creator and the base_env (Default or Mem).
  // It takes ownership of the stream.
  rocksdb::Env* store_keyed_env =
      rocksdb_utils::NewEncryptedEnv(env_mgr->base_env, env_mgr->file_registry.get(), store_stream);
  // Transfer ownership to the env manager.
  env_mgr->TakeEnvOwnership(store_keyed_env);

  // Initialize data key manager using the stored-keyed-env.
  DataKeyManager* data_key_manager = new DataKeyManager(
      store_keyed_env, logger, db_dir, opts.data_key_rotation_period(), db_opts.read_only);
  status = data_key_manager->LoadKeys();
  if (!status.ok()) {
    delete data_key_manager;
    return status;
  }

  // Create a cipher stream creator using the data_key_manager.
  auto data_stream = new CTRCipherStreamCreator(data_key_manager, enginepb::Data);

  // Construct an EncryptedEnv using this stream creator and the base_env (Default or Mem).
  // It takes ownership of the stream.
  rocksdb::Env* data_keyed_env =
      rocksdb_utils::NewEncryptedEnv(env_mgr->base_env, env_mgr->file_registry.get(), data_stream);
  // Transfer ownership to the env manager and make it as the db_env.
  env_mgr->TakeEnvOwnership(data_keyed_env);
  env_mgr->db_env = data_keyed_env;

  // Fetch the current store key info.
  std::unique_ptr<enginepbccl::KeyInfo> store_key = store_key_manager->CurrentKeyInfo();
  assert(store_key != nullptr);

  if (!db_opts.read_only) {
    // Generate a new data key if needed by giving the active store key info to the data key
    // manager.
    status = data_key_manager->SetActiveStoreKeyInfo(std::move(store_key));
    if (!status.ok()) {
      return status;
    }
  }

  // Everything's ok: initialize a stats handler.
  env_mgr->SetStatsHandler(new CCLEnvStatsHandler(data_key_manager));

  return rocksdb::Status::OK();
}

}  // namespace cockroach

void* DBOpenHookCCL = (void*)cockroach::DBOpenHookCCL;

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
