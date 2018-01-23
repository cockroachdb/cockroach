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

#pragma once

#include <rocksdb/env.h>
#include "file_registry.h"
#include "rocksdbutils/env_encryption.h"

// CipherStreamCreator is the abstract class passed to SwitchingProvider.
// It performs two functions:
// * initializes encryption settings and create a cipher stream with them
// * creates a cipher stream given encryption settings
class CipherStreamCreator {
 public:
  virtual ~CipherStreamCreator() {}
  // Create new encryption settings and a cipher stream using them.
  // Assigns new objects to 'settings' and 'result'.
  // If plaintext, settings will be nullptr.
  virtual rocksdb::Status InitSettingsAndCreateCipherStream(
      std::string* settings, std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) = 0;

  // Create a cipher stream given encryption settings.
  virtual rocksdb::Status CreateCipherStreamFromSettings(
      const std::string& settings,
      std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) = 0;

  // Return the EnvLevel for this stream creator. It should match files being operated on.
  virtual enginepb::EnvLevel GetEnvLevel() = 0;
};

// SwitchingProvider implement EncryptionProvider.
//
// It can have a number of cipher creators registered, each with a corresponding Env level.
// Env level denotes which level of key source we are using:
// - 0: plaintext
// - 1: store-level keys
// - 2: data-level keys
//
// For example: with encryption enabled, we have the following Envs being used:
//   db_env: level 2, pointing to a cipher stream creator backed by data-level keys
//   data_env: level 1, pointing to a cipher stream creater backed by store-level keys
//   store_env: level 0, pointing to the base env (default or in memory env)
//
// The env level is stored in the file registry to ensure that:
// 1) we have the necessary envs (eg: we can't stop specifying encryption flags if we have encrypted
// files) 2) we know which env is responsible for which file
//
// When an Env requests a cipher stream, it specifies the env level, filename,
// and whether the file is new.
class SwitchingProvider final : public rocksdb_utils::EncryptionProvider {
 public:
  SwitchingProvider(std::unique_ptr<FileRegistry> registry) : registry_(std::move(registry)) {}
  virtual ~SwitchingProvider();

  // Register a CipherStreamCreator. This must be done at initialization time (before any calls to
  // CreateCipherStream) and is not safe for concurrent use.
  // This returns an error if a cipher stream creator with the same GetEnvLevel() is registered.
  // Takes ownership of the creator.
  rocksdb::Status RegisterCipherStreamCreator(std::unique_ptr<CipherStreamCreator> creator);

  // Verifies that all env levels described by the file registry have a registered
  // CipherStreamCreator.
  rocksdb::Status CheckEnvLevels();

  // The following implement EncryptionProvider.
  virtual rocksdb::Status
  CreateCipherStream(CipherStreamCreator* creator, const std::string& fname, bool new_file,
                     std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

  virtual rocksdb::Status DeleteFile(const std::string& fname) override;
  virtual rocksdb::Status RenameFile(const std::string& src, const std::string& target) override;
  virtual rocksdb::Status LinkFile(const std::string& src, const std::string& target) override;

 private:
  std::unique_ptr<FileRegistry> registry_;

  // Delete a file registry entry if is exists, and persist the file.
  rocksdb::Status MaybeDeleteEntry(const std::string& fname);

  // Initialize encryption settings, maybe persist, and create cipher stream.
  // This may modify the registry.
  rocksdb::Status
  InitAndCreateCipherStream(CipherStreamCreator* creator, const std::string& fname,
                            std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result);

  // Lookup encryption settings, maybe persist, and create cipher stream.
  // This never modifies the registry.
  rocksdb::Status
  LookupAndCreateCipherStream(CipherStreamCreator* creator, const std::string& fname,
                              std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result);

  typedef std::unordered_map<int, std::unique_ptr<CipherStreamCreator>> creator_map;
  creator_map creators_;
};
