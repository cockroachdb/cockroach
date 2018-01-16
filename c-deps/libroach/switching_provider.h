// Copyright 2017 The Cockroach Authors.
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

class SwitchingProvider;

// EnvContext manages all created Envs, as well as the switching provider.
// All envs not owned by rocksdb (eg: all but Env::Default) should be added to envs for
// deletion.
//
// Some useful envs are keps:
// base_env: the env that all other envs must wrap. Usually a Env::Default or MemEnv.
// db_env: the env used by rocksdb. This can be an EncryptedEnv.
struct EnvContext {
  EnvContext(rocksdb::Env* env) : base_env(env), db_env(env) {}
  ~EnvContext() {}

  void TakeEnvOwnership(rocksdb::Env* env) { envs.push_back(std::unique_ptr<rocksdb::Env>(env)); }

  rocksdb::Env* base_env;
  rocksdb::Env* db_env;
  std::unique_ptr<SwitchingProvider> switching_provider;
  std::vector<std::unique_ptr<rocksdb::Env>> envs;
};

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
};

// SwitchingProvider implement EncryptionProvider.
//
// It can have a number of cipher creators registered, each with a corresponding Env level.
// When an Env requests a cipher stream, it specifies the env level, filename,
// and whether the file is new.
//
// The 0 env level (for plaintext) is always present.
//
// The base_env should be a plain rocksdb::Env, either Env::Default(), or
// an in-memory env.
class SwitchingProvider final : public rocksdb_utils::EncryptionProvider {
 public:
  SwitchingProvider(std::unique_ptr<FileRegistry> registry) : registry_(std::move(registry)) {}
  virtual ~SwitchingProvider();

  // Register a CipherStreamCreator. This must be done at initialization time (before any calls to
  // CreateCipherStream) and is not safe for concurrent use.
  // Returns an error if a creator is already registered for 'env_level'.
  rocksdb::Status RegisterCipherStreamCreator(int env_level, CipherStreamCreator* creator);

  // Verifies that all env levels described by the file registry have a registered
  // CipherStreamCreator.
  rocksdb::Status CheckEnvLevels();

  // The following implement EncryptionProvider.
  virtual rocksdb::Status
  CreateCipherStream(int env_level, const std::string& fname, bool new_file,
                     std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

  virtual rocksdb::Status DeleteFile(const std::string& fname) override;
  virtual rocksdb::Status RenameFile(const std::string& src, const std::string& target) override;

 private:
  std::unique_ptr<FileRegistry> registry_;

  // Delete a file registry entry if is exists, and persist the file.
  rocksdb::Status MaybeDeleteEntry(const std::string& fname);

  // Initialize encryption settings, maybe persist, and create cipher stream.
  // This may modify the registry.
  rocksdb::Status
  InitAndCreateCipherStream(int env_level, const std::string& fname,
                            std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result);

  // Lookup encryption settings, maybe persist, and create cipher stream.
  // This never modifies the registry.
  rocksdb::Status
  LookupAndCreateCipherStream(int env_level, const std::string& fname,
                              std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result);

  typedef std::unordered_map<int, std::unique_ptr<CipherStreamCreator>> creator_map;
  creator_map creators_;
};
