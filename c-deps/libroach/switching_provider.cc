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

#include "switching_provider.h"
#include "../fmt.h"
#include "plaintext_stream.h"

SwitchingProvider::~SwitchingProvider() {}

rocksdb::Status SwitchingProvider::RegisterCipherStreamCreator(enginepb::EnvLevel env_level,
                                                               CipherStreamCreator* creator) {
  auto res =
      creators_.insert(std::make_pair(env_level, std::unique_ptr<CipherStreamCreator>(creator)));
  if (res.second != true) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("double registration of cipher creator at env_level %d", env_level));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status SwitchingProvider::CheckEnvLevels() {
  auto levels = registry_->GetUsedEnvLevels();
  for (auto it : levels) {
    if (creators_.find(it) == creators_.cend()) {
      return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
          "registry has encrypted files at env_level %d, but we do not have a registered "
          "CipherStreamCreator for it, did you specify all encryption options?",
          it));
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status SwitchingProvider::CreateCipherStream(
    enginepb::EnvLevel env_level, const std::string& fname, bool new_file,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
  if (new_file) {
    return InitAndCreateCipherStream(env_level, fname, result);
  } else {
    return LookupAndCreateCipherStream(env_level, fname, result);
  }
}

rocksdb::Status SwitchingProvider::LookupAndCreateCipherStream(
    enginepb::EnvLevel env_level, const std::string& fname,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {

  // Look for file in registry.
  auto file_entry = registry_->GetFileEntry(fname);
  if (file_entry == nullptr) {
    // No entry: plaintext.
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
    return rocksdb::Status::OK();
  }

  // File entry exists: check that the env_level corresponds to the requested one.
  if (file_entry->env_level() != env_level) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s was written using env_level %d, but we are trying to read it "
                          "using env_level %d: problems will occur",
                          fname.c_str(), file_entry->env_level(), env_level));
  }

  if (env_level == 0) {
    // No encryption settings: plaintext.
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
    return rocksdb::Status::OK();
  }

  // Look for a creator at the requested 'env_level'.
  creator_map::const_iterator it = creators_.find(env_level);
  if (it == creators_.cend()) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf("unknown env_level: %d", env_level));
  }

  auto creator = it->second.get();
  return creator->CreateCipherStreamFromSettings(file_entry->encryption_settings(), result);
}

rocksdb::Status SwitchingProvider::InitAndCreateCipherStream(
    enginepb::EnvLevel env_level, const std::string& fname,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {

  // Look for file in registry.
  auto file_entry = registry_->GetFileEntry(fname);
  if (file_entry != nullptr && file_entry->env_level() != env_level) {
    // File entry exists but the env_level does not match the requested one.
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s was written using env_level %d, but we are overwriting it "
                          "using env_level %d: problems will occur",
                          fname.c_str(), file_entry->env_level(), env_level));
  }

  std::string encryption_settings;
  if (env_level == 0) {
    // Plaintext. Leave encryption_settings blank.
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
  } else {
    // Look for a creator at the requested 'env_level'.
    creator_map::const_iterator it = creators_.find(env_level);
    if (it == creators_.cend()) {
      return rocksdb::Status::InvalidArgument(
          fmt::StringPrintf("unknown env_level: %d", env_level));
    }

    auto creator = it->second.get();
    auto status = creator->InitSettingsAndCreateCipherStream(&encryption_settings, result);
    if (!status.ok()) {
      return status;
    }
  }

  if (encryption_settings.size() == 0) {
    // Plaintext: delete registry entry if is exists.
    return registry_->MaybeDeleteEntry(fname);
  } else {
    // Encryption settings specified: create a FileEntry and save it, overwriting any existing
    // one.
    std::unique_ptr<enginepb::FileEntry> new_entry(new enginepb::FileEntry());
    new_entry->set_env_level(env_level);
    new_entry->set_encryption_settings(encryption_settings);
    return registry_->SetFileEntry(fname, std::move(new_entry));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status SwitchingProvider::DeleteFile(const std::string& fname) {
  return registry_->MaybeDeleteEntry(fname);
}

rocksdb::Status SwitchingProvider::RenameFile(const std::string& src, const std::string& target) {
  return registry_->MaybeRenameEntry(src, target);
}

rocksdb::Status SwitchingProvider::LinkFile(const std::string& src, const std::string& target) {
  return registry_->MaybeLinkEntry(src, target);
}
