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

#include "switching_provider.h"
#include "../fmt.h"
#include "plaintext_stream.h"

SwitchingProvider::~SwitchingProvider() {}

rocksdb::Status
SwitchingProvider::RegisterCipherStreamCreator(std::unique_ptr<CipherStreamCreator> creator) {
  auto res = creators_.insert(std::make_pair(creator->GetEnvLevel(), std::move(creator)));
  if (res.second != true) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
        "double registration of cipher creator at env_level %d", creator->GetEnvLevel()));
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
    CipherStreamCreator* creator, const std::string& fname, bool new_file,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
  if (new_file) {
    return InitAndCreateCipherStream(creator, fname, result);
  } else {
    return LookupAndCreateCipherStream(creator, fname, result);
  }
}

rocksdb::Status SwitchingProvider::LookupAndCreateCipherStream(
    CipherStreamCreator* creator, const std::string& fname,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {

  // Look for file in registry.
  auto file_entry = registry_->GetFileEntry(fname);
  if (file_entry == nullptr) {
    // No entry: plaintext.
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
    return rocksdb::Status::OK();
  }

  // File entry exists: check that the env_level corresponds to the requested one.
  if (file_entry->env_level() != creator->GetEnvLevel()) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s was written using env_level %d, but we are trying to read it "
                          "using env_level %d: problems will occur",
                          fname.c_str(), file_entry->env_level(), creator->GetEnvLevel()));
  }

  return creator->CreateCipherStreamFromSettings(file_entry->encryption_settings(), result);
}

rocksdb::Status SwitchingProvider::InitAndCreateCipherStream(
    CipherStreamCreator* creator, const std::string& fname,
    std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {

  // Look for file in registry.
  auto file_entry = registry_->GetFileEntry(fname);
  if (file_entry != nullptr && file_entry->env_level() != creator->GetEnvLevel()) {
    // File entry exists but the env_level does not match the requested one.
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s was written using env_level %d, but we are overwriting it "
                          "using env_level %d: problems will occur",
                          fname.c_str(), file_entry->env_level(), creator->GetEnvLevel()));
  }

  std::string encryption_settings;
  auto status = creator->InitSettingsAndCreateCipherStream(&encryption_settings, result);
  if (!status.ok()) {
    return status;
  }

  if (encryption_settings.size() == 0) {
    // Plaintext: delete registry entry if is exists.
    return registry_->MaybeDeleteEntry(fname);
  } else {
    // Encryption settings specified: create a FileEntry and save it, overwriting any existing
    // one.
    std::unique_ptr<enginepb::FileEntry> new_entry(new enginepb::FileEntry());
    new_entry->set_env_level(creator->GetEnvLevel());
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
