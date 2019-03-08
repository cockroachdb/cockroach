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
#include "../file_registry.h"
#include "rocksdbutils/env_encryption.h"

namespace cockroach {

// EnvStatsHandler provides an interface to generate Env-specific stats.
class EnvStatsHandler {
 public:
  virtual ~EnvStatsHandler() {}

  // Get serialized encryption stats.
  virtual rocksdb::Status GetEncryptionStats(std::string* stats) = 0;
  // Get a serialized encryption registry (scrubbed of key contents).
  virtual rocksdb::Status GetEncryptionRegistry(std::string* registry) = 0;
  // Get the ID of the active data key, or "plain" if none.
  virtual std::string GetActiveDataKeyID() = 0;
  // Get the enum value of the encryption type.
  virtual int32_t GetActiveStoreKeyType() = 0;
  // Get the key ID in use by this file, or "plain" if none.
  virtual rocksdb::Status GetFileEntryKeyID(const enginepb::FileEntry* entry, std::string* id) = 0;
};

// EnvManager manages all created Envs, as well as the file registry.
// Rocksdb owns Env::Default (global static). All other envs are owned by EnvManager.
//
// Some useful envs are kept:
// base_env: the env that all other envs must wrap. Usually a Env::Default or MemEnv.
// db_env: the env used by rocksdb. This can be an EncryptedEnv.
struct EnvManager {
  EnvManager(rocksdb::Env* env) : base_env(env), db_env(env) {}
  ~EnvManager() {}

  // Set the stats handler implementing GetEncryptionStats to fill in env-related stats.
  // It does not have called, leaving env_stats_handler nil.
  void SetStatsHandler(EnvStatsHandler* stats_handler) { env_stats_handler.reset(stats_handler); }
  void TakeEnvOwnership(rocksdb::Env* env) { envs.push_back(std::unique_ptr<rocksdb::Env>(env)); }

  rocksdb::Env* base_env;
  rocksdb::Env* db_env;
  std::unique_ptr<EnvStatsHandler> env_stats_handler;
  std::unique_ptr<FileRegistry> file_registry;
  std::vector<std::unique_ptr<rocksdb::Env>> envs;
};

}  // namespace cockroach
