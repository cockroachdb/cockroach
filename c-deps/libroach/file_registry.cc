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

#include "file_registry.h"
#include "../fmt.h"
#include "../utils.h"

using namespace cockroach;

namespace cockroach {

FileRegistry::FileRegistry(rocksdb::Env* env, const std::string& db_dir)
    : env_(env), db_dir_(db_dir), registry_path_(db_dir_ + "/" + kFileRegistryFilename) {}

rocksdb::Status FileRegistry::CheckNoRegistryFile() {
  rocksdb::Status status = env_->FileExists(registry_path_);
  if (status.ok()) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("registry file %s exists", registry_path_.c_str()));
  } else if (!status.IsNotFound()) {
    return status;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status FileRegistry::Load() {
  rocksdb::Status status = env_->FileExists(registry_path_);
  if (status.IsNotFound()) {
    registry_.reset(new enginepb::FileRegistry());
    return rocksdb::Status::OK();
  } else if (!status.ok()) {
    return status;
  }

  std::string contents;
  status = rocksdb::ReadFileToString(env_, registry_path_, &contents);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<enginepb::FileRegistry> registry(new enginepb::FileRegistry());
  if (!registry->ParseFromString(contents)) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("failed to parse file registry %s", registry_path_.c_str()));
  }

  // Make it the active registry.
  registry_.swap(registry);

  return rocksdb::Status::OK();
}

std::unordered_set<int> FileRegistry::GetUsedEnvTypes() {
  std::unordered_set<int> ret;

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  for (auto it : registry_->files()) {
    ret.insert(it.second.env_type());
  }

  return ret;
}

std::string FileRegistry::TransformPath(const std::string& filename) {
  // Check for the rare case when we're referring to the db directory itself (without slash).
  if (filename == db_dir_) {
    return "";
  }

  // The db_dir usually does not include a trailing slash. We add it here.
  auto prefixLength = db_dir_.size();

  std::string db_dir(db_dir_);
  if (prefixLength == 0 || db_dir[prefixLength - 1] != '/') {
    db_dir.append("/");
    prefixLength++;
  }

  auto nameLength = filename.size();
  if (nameLength < prefixLength) {
    // Shorter than db_dir.
    return filename;
  }

  if (filename.compare(0, prefixLength, db_dir) != 0) {
    // Does not start with db_dir.
    return filename;
  }

  // Matched rocksdbdir prefix. Check if there's a double slash (it happens):
  if ((nameLength > prefixLength) && (filename[prefixLength] == '/')) {
    prefixLength++;
  }

  if (nameLength == prefixLength) {
    // Exact match.
    return "";
  }

  return filename.substr(prefixLength);
}

std::unique_ptr<enginepb::FileEntry> FileRegistry::GetFileEntry(const std::string& filename) {
  std::string newName = TransformPath(filename);

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  auto key = registry_->files().find(newName);
  if (key == registry_->files().cend()) {
    return nullptr;
  }
  return std::unique_ptr<enginepb::FileEntry>(new enginepb::FileEntry(key->second));
}

rocksdb::Status FileRegistry::SetFileEntry(const std::string& filename,
                                           std::unique_ptr<enginepb::FileEntry> entry) {
  std::string newName = TransformPath(filename);

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  // Make a copy of the registry and insert entry.
  auto new_registry =
      std::unique_ptr<enginepb::FileRegistry>(new enginepb::FileRegistry(*registry_));
  (*new_registry->mutable_files())[newName] = *entry;

  return PersistRegistryLocked(std::move(new_registry));
}

rocksdb::Status FileRegistry::MaybeDeleteEntry(const std::string& filename) {
  std::string newName = TransformPath(filename);

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  // It's cheaper to check first rather than copy and check existence after deletion.
  auto key = registry_->files().find(newName);
  if (key == registry_->files().cend()) {
    // Entry does not exist: do nothing.
    return rocksdb::Status::OK();
  }

  // Make a copy of the registry and delete entry.
  auto new_registry =
      std::unique_ptr<enginepb::FileRegistry>(new enginepb::FileRegistry(*registry_));
  new_registry->mutable_files()->erase(newName);

  return PersistRegistryLocked(std::move(new_registry));
}

// Move the entry under 'src' to 'target' if 'src' exists, and persist the registry.
rocksdb::Status FileRegistry::MaybeRenameEntry(const std::string& src, const std::string& target) {
  std::string newSrc = TransformPath(src);
  std::string newTarget = TransformPath(target);

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  // It's cheaper to check first rather than copy-then-check.
  auto src_key = registry_->files().find(newSrc);
  auto target_key = registry_->files().find(newTarget);
  if ((src_key == registry_->files().cend()) && (target_key == registry_->files().cend())) {
    // Entries do not exist: do nothing.
    return rocksdb::Status::OK();
  }

  // Make a copy of the registry.
  auto new_registry =
      std::unique_ptr<enginepb::FileRegistry>(new enginepb::FileRegistry(*registry_));

  if (src_key != registry_->files().cend()) {
    // Source key exists: copy to target and delete source.
    auto new_entry = std::unique_ptr<enginepb::FileEntry>(new enginepb::FileEntry(src_key->second));
    (*new_registry->mutable_files())[newTarget] = *new_entry;
    new_registry->mutable_files()->erase(newSrc);
  } else {
    // Source doesn't exist, this means target does: delete it.
    new_registry->mutable_files()->erase(newTarget);
  }

  return PersistRegistryLocked(std::move(new_registry));
}

// Copy the entry under 'src' to 'target' if 'src' exists, and persist the registry.
rocksdb::Status FileRegistry::MaybeLinkEntry(const std::string& src, const std::string& target) {
  std::string newSrc = TransformPath(src);
  std::string newTarget = TransformPath(target);

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  // It's cheaper to check first rather than copy and check existence after deletion.
  auto src_key = registry_->files().find(newSrc);
  auto target_key = registry_->files().find(newTarget);
  if ((src_key == registry_->files().cend()) && (target_key == registry_->files().cend())) {
    // Entries do not exist: do nothing.
  }

  // Make a copy of the registry.
  auto new_registry =
      std::unique_ptr<enginepb::FileRegistry>(new enginepb::FileRegistry(*registry_));

  if (src_key != registry_->files().cend()) {
    // Source key exists: copy to target.
    auto new_entry = std::unique_ptr<enginepb::FileEntry>(new enginepb::FileEntry(src_key->second));
    (*new_registry->mutable_files())[newTarget] = *new_entry;
  } else {
    // Source doesn't exist, this means target does: delete it.
    new_registry->mutable_files()->erase(newTarget);
  }

  return PersistRegistryLocked(std::move(new_registry));
}

rocksdb::Status FileRegistry::PersistRegistryLocked(std::unique_ptr<enginepb::FileRegistry> reg) {
  // Serialize and write to file.
  std::string contents;
  if (!reg->SerializeToString(&contents)) {
    return rocksdb::Status::InvalidArgument("failed to serialize key registry");
  }

  auto status = SafeWriteStringToFile(env_, registry_path_, contents);
  if (!status.ok()) {
    return status;
  }

  // Swap registry.
  registry_.swap(reg);

  return rocksdb::Status::OK();
}

}  // namespace cockroach
