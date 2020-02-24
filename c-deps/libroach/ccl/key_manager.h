// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <mutex>
#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <string>
#include "ccl/storageccl/engineccl/enginepbccl/key_registry.pb.h"

namespace enginepbccl = cockroach::ccl::storageccl::engineccl::enginepbccl;

// Constants must not be changed, they are used to build the persisted key registry.
// The key ID is 32 bytes (the length of the longest possible AES key). This is to easily
// tell if a store key file has the right length.
static const size_t kKeyIDLength = 32;
static const std::string kPlainKeyID = "plain";
static const std::string kKeyRegistryFilename = "COCKROACHDB_DATA_KEYS";

// Helpers are in a separate namespace to avoid pollution.
// These should only be used by the KeyManager and its tests.
namespace KeyManagerUtils {

// Read key from a file.
rocksdb::Status KeyFromFile(rocksdb::Env* env, const std::string& path,
                            enginepbccl::SecretKey* key);

// Generate a key based on the characteristics of `store_info`.
rocksdb::Status KeyFromKeyInfo(rocksdb::Env* env, const enginepbccl::KeyInfo& store_info,
                               enginepbccl::SecretKey* key);

// Validate a registry. This should be called before using a registry.
rocksdb::Status ValidateRegistry(enginepbccl::DataKeysRegistry* registry);

// Generate a new data key based on the currently active store key.
rocksdb::Status GenerateDataKey(rocksdb::Env* env, enginepbccl::DataKeysRegistry* reg);

};  // namespace KeyManagerUtils

// KeyManager is the basic implementation for a key manager.
// Specific subclasses will provide different methods of accessing keys.
class KeyManager {
 public:
  virtual ~KeyManager();

  // CurrentKeyInfo returns the KeyInfo about the active key.
  // It does NOT include the key itself and can be logged, displayed, and stored.
  std::unique_ptr<enginepbccl::KeyInfo> CurrentKeyInfo() {
    auto key = CurrentKey();
    if (key == nullptr) {
      return nullptr;
    }
    return std::unique_ptr<enginepbccl::KeyInfo>(new enginepbccl::KeyInfo(key->info()));
  }

  // CurrentKey returns the key itself.
  // **WARNING**: this must not be logged, displayed, or stored outside the key registry.
  virtual std::shared_ptr<enginepbccl::SecretKey> CurrentKey() = 0;

  // GetKeyInfo returns the KeyInfo about the key the requested `id`.
  // It does NOT include the key itself and can be logged, displayed, and stored.
  std::unique_ptr<enginepbccl::KeyInfo> GetKeyInfo(const std::string& id) {
    auto key = GetKey(id);
    if (key == nullptr) {
      return nullptr;
    }
    return std::unique_ptr<enginepbccl::KeyInfo>(new enginepbccl::KeyInfo(key->info()));
  }

  // GetKey returns the key with requested `id`.
  // **WARNING**: this must not be logged, displayed, or stored outside the key registry.
  virtual std::shared_ptr<enginepbccl::SecretKey> GetKey(const std::string& id) = 0;
};

// FileKeyManager loads raw keys from files.
// Keys are only loaded at startup after object construction, but before use.
class FileKeyManager : public KeyManager {
 public:
  // `env` is owned by the caller.
  explicit FileKeyManager(rocksdb::Env* env, std::shared_ptr<rocksdb::Logger> logger,
                          const std::string& active_key_path, const std::string& old_key_path);

  virtual ~FileKeyManager();

  // LoadKeys tells the key manager to read and validate the key files.
  // On error, existing keys held by the object are not overwritten.
  rocksdb::Status LoadKeys();

  virtual std::shared_ptr<enginepbccl::SecretKey> CurrentKey() override;
  virtual std::shared_ptr<enginepbccl::SecretKey> GetKey(const std::string& id) override;

 private:
  rocksdb::Env* env_;
  std::shared_ptr<rocksdb::Logger> logger_;
  std::string active_key_path_;
  std::string old_key_path_;
  // TODO(mberhault): protect keys by a mutex if we allow reload.
  std::shared_ptr<enginepbccl::SecretKey> active_key_;
  std::shared_ptr<enginepbccl::SecretKey> old_key_;
};

// DataKeyManager generates and handles data keys and persists them to disk.
// The Env passed should be an encrypted env using store keys.
class DataKeyManager : public KeyManager {
 public:
  // `env` is owned by the caller and should be an encrypted Env.
  // `db_dir` is the rocksdb directory.
  // If read-only is true, the DataKeyManager can be used to lookup keys but cannot
  // change any keys (eg: SetActiveStoreKeyInfo will fail).
  explicit DataKeyManager(rocksdb::Env* env, std::shared_ptr<rocksdb::Logger> logger,
                          const std::string& db_dir, int64_t rotation_period, bool read_only);

  virtual ~DataKeyManager();

  // LoadKeys tells the key manager to read and validate the key files.
  // On error, existing keys held by the object are not overwritten.
  rocksdb::Status LoadKeys();

  // SetActiveStoreKeyInfo takes the KeyInfo of the active store key
  // and adds it to the registry. A new data key is generated if needed.
  rocksdb::Status SetActiveStoreKeyInfo(std::unique_ptr<enginepbccl::KeyInfo> store_key);

  // GetActiveStoreKeyInfo returns the KeyInfo for the active store key.
  // The data key registry keeps the active store key information (but not the key) from
  // the first time the active key was seen. Fields like creation_time will only
  // be accurate here.
  std::unique_ptr<enginepbccl::KeyInfo> GetActiveStoreKeyInfo();

  // GetScrubbedRegistry returns a copy of the key registry with key contents scrubbed.
  std::unique_ptr<enginepbccl::DataKeysRegistry> GetScrubbedRegistry() const;

  virtual std::shared_ptr<enginepbccl::SecretKey> CurrentKey() override;
  virtual std::shared_ptr<enginepbccl::SecretKey> GetKey(const std::string& id) override;

 private:
  // Lookup registry_->active_data_key_id in the map. This should only be done after
  // changing the registry, otherwise use current_key_.
  std::shared_ptr<enginepbccl::SecretKey> CurrentKeyLocked();
  rocksdb::Status LoadKeysHelper(enginepbccl::DataKeysRegistry* registry);
  // Write the given registry to disk. If successful, update registry_ and current_key_.
  rocksdb::Status PersistRegistryLocked(std::unique_ptr<enginepbccl::DataKeysRegistry> reg);
  // MaybeRotateKeyLocked generates a new data key if the active one has expired.
  rocksdb::Status MaybeRotateKeyLocked();

  // These do not change after initialization. env_ is thread safe.
  rocksdb::Env* env_;
  std::shared_ptr<rocksdb::Logger> logger_;
  std::unique_ptr<rocksdb::Directory> registry_dir_;
  std::string registry_path_;
  int64_t rotation_period_;
  bool read_only_;

  // The registry is read-only and can only be swapped for another one, it cannot be mutated in
  // place. mu_ must be held for any registry access.
  // current_key_ is the active data key (or nullptr if none present yet) and is updated every time
  // registry_ is modified.
  // TODO(mberhault): use a shared_mutex for multiple read-only holders.
  mutable std::mutex mu_;
  std::unique_ptr<enginepbccl::DataKeysRegistry> registry_;
  std::shared_ptr<enginepbccl::SecretKey> current_key_;
};
