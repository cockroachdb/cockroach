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

#include <mutex>
#include <rocksdb/env.h>
#include <string>
#include "protos/storage/engine/enginepb/file_registry.pb.h"

namespace enginepb = cockroach::storage::engine::enginepb;

namespace cockroach {

// Name of the registry file.
static const std::string kFileRegistryFilename = "COCKROACHDB_REGISTRY";

// FileRegistry keeps track of encryption information per file.
// Plaintext files are not registered for two reasons:
// - they may be missing if transitioning from a non-registry version
// - we don't want to add the extra file rewrite in non-encrypted setups
//
// All paths given to FileRegistry should be absolute paths. It converts
// paths within `db_dir` to relative paths internally.
// Paths outside `db_dir` remain absolute.
// If read-only is set, the registry can be used for a read-only DB, but cannot be modified.
class FileRegistry {
 public:
  FileRegistry(rocksdb::Env* env, const std::string& db_dir, bool read_only);
  ~FileRegistry() {}

  // Returns OK if the registry file does not exist.
  // An error could mean the file exists, or I/O error.
  rocksdb::Status CheckNoRegistryFile();

  // Load the file registry. Errors on file reading/parsing issues.
  // OK if the file does not exist or is empty.
  rocksdb::Status Load();

  // Returns a hash-set of all used env types.
  std::unordered_set<int> GetUsedEnvTypes();

  // Returns the FileEntry for the specified file.
  std::unique_ptr<enginepb::FileEntry> GetFileEntry(const std::string& filename);

  // Insert 'entry' under 'filename' and persist the registry.
  rocksdb::Status SetFileEntry(const std::string& filename,
                               std::unique_ptr<enginepb::FileEntry> entry);

  // Delete the entry for 'filename' if it exists and persist the registry.
  rocksdb::Status MaybeDeleteEntry(const std::string& filename);

  // Move the entry under 'src' to 'target' if 'src' exists, and persist the registry.
  // If 'src' does not exist (probably plaintext) but 'target' does, 'target' is deleted.
  rocksdb::Status MaybeRenameEntry(const std::string& src, const std::string& target);

  // Copy the entry under 'src' to 'target' if 'src' exists, and persist the registry.
  // If 'src' does not exist (probably plaintext) but 'target' does, 'target' is deleted.
  rocksdb::Status MaybeLinkEntry(const std::string& src, const std::string& target);

  // TransformPath takes an absolute path. If 'path' is inside 'db_dir_', return the relative
  // path, else return 'path'.
  // This assumes sanitized paths (no symlinks, no relative paths) and uses a naive
  // prefix removal.
  std::string TransformPath(const std::string& path);

 private:
  rocksdb::Env* env_;
  std::string db_dir_;
  bool read_only_;
  std::string registry_path_;

  // Write 'reg' to the registry file, and swap with registry_ if successful. mu_ is held.
  rocksdb::Status PersistRegistryLocked(std::unique_ptr<enginepb::FileRegistry> reg);

  // The registry is read-only and can only be swapped for another one, it cannot be mutated in
  // place. mu_ must be held for any registry access.
  // TODO(mberhault): use a shared_mutex for multiple read-only holders.
  std::mutex mu_;
  std::unique_ptr<enginepb::FileRegistry> registry_;
};

}  // namespace cockroach
