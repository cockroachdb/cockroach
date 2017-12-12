// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <string>
#include "key.h"

// KeyManager is the basic implementation for a key manager.
// Specific subclasses will provide different methods of accessing keys.
class KeyManager {
 public:
  // Get the current key.
  virtual const EncryptionKey* CurrentKey() = 0;

  // Get a key by its ID. Null if not found.
  // PLAIN keys do not have an ID an cannot be retrieved.
  virtual const EncryptionKey* GetKey(const KeyID id) = 0;
};

// FileKeyManager loads raw keys from files.
// Keys are only loaded at startup after object construction, but before use.
class FileKeyManager : public KeyManager {
 public:
  // `env` is owned by the caller.
  explicit FileKeyManager(rocksdb::Env* env, const std::string& active_key_path, const std::string& old_key_path)
      : env_(env), active_key_path_(active_key_path), old_key_path_(old_key_path) {}

  // LoadKeys tells the key manager to read and validate the key files.
  // On error, existing keys held by the object are not overwritten.
  rocksdb::Status LoadKeys();

  virtual const EncryptionKey* CurrentKey() override { return active_key_.get(); }

  virtual const EncryptionKey* GetKey(const KeyID id) override {
    if (id == "") {
      return nullptr;
    }
    if (active_key_.get() != nullptr && active_key_->id == id) {
      return active_key_.get();
    }
    if (old_key_.get() != nullptr && old_key_->id == id) {
      return old_key_.get();
    }
    return nullptr;
  }

 private:
  rocksdb::Env* env_;
  std::string active_key_path_;
  std::string old_key_path_;
  std::unique_ptr<EncryptionKey> active_key_;
  std::unique_ptr<EncryptionKey> old_key_;
};
