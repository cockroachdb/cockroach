// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <string>
#include <unordered_map>
#include "protos/storage/enginepb/file_registry.pb.h"

namespace enginepb = cockroach::storage::enginepb;

namespace cockroach {

struct EnvManager;

// Name of the registry file.
static const std::string kFileRegistryFilename = "COCKROACHDB_REGISTRY";

// Class to compute file stats.
class FileStats;

// FileRegistry keeps track of encryption information per file.
// Plaintext files created before use of the file registry will not have an entry.
// All files created after enabling the file registry are guaranteed to have an entry.
//
// All paths given to FileRegistry should be absolute paths. It converts
// paths within `db_dir` to relative paths internally.
// Paths outside `db_dir` remain absolute.
// If read-only is set, the registry can be used for a read-only DB, but cannot be modified.
class FileRegistry {
 public:
  FileRegistry(rocksdb::Env* env, const std::string& db_dir, bool read_only);
  ~FileRegistry() {}

  const std::string db_dir() const { return db_dir_; }

  // Returns OK if the registry file does not exist.
  // An error could mean the file exists, or I/O error.
  rocksdb::Status CheckNoRegistryFile() const;

  // Load the file registry. Errors on file reading/parsing issues.
  // OK if the file does not exist or is empty.
  // This does **not** create a file registry.
  rocksdb::Status Load();

  // Returns a hash-set of all used env types.
  std::unordered_set<int> GetUsedEnvTypes() const;

  // Returns the FileEntry for the specified file.
  // If 'relative' is true, the path is relative to 'db_dir'.
  // A 'nullptr' result indicates a plaintext file.
  std::unique_ptr<enginepb::FileEntry> GetFileEntry(const std::string& filename,
                                                    bool relative = false) const;

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
  std::string TransformPath(const std::string& path) const;

  // Returns a shared pointer to the FileRegistry proto.
  std::shared_ptr<const enginepb::FileRegistry> GetFileRegistry() const;

 private:
  rocksdb::Env* const env_;
  const std::string db_dir_;
  const bool read_only_;
  const std::string registry_path_;

  std::unique_ptr<rocksdb::Directory> registry_dir_;

  // Write 'reg' to the registry file, and swap with registry_ if successful. mu_ is held.
  rocksdb::Status PersistRegistryLocked(std::unique_ptr<enginepb::FileRegistry> reg);

  // The registry is read-only and can only be swapped for another one, it cannot be mutated in
  // place. mu_ must be held for any registry access.
  // TODO(mberhault): use a shared_mutex for multiple read-only holders.
  mutable std::mutex mu_;
  std::shared_ptr<const enginepb::FileRegistry> registry_;
};

// FileStats builds statistics about file counts and sizes.
//
// WARNING: It is not thread-safe and should not be reused.
//
// It looks up files in use by merging files reported by RocksDB with
// our own file registry. Files that do not have a known size from the
// listing helpers will be stated.
//
// It can then serve basic statictics about file/byte count matching
// the current encryption key.
//
// Some caveats:
// - files created before using the file registry will not be found
//   in the registry and will reported in "total".
// - RocksDB keeps older copies of files around (eg: OPTIONS file from
//   previous run) which may not have the latest key even if the current
//   file is properly encrypted.
//
// TODO(mberhault): we can improve usability/accuracy by:
// - having a notion of "unknown" to cover files not in the registry
// - use knowledge of file nomenclature to omit older files that
//   are not longer live (better would be to force them to be rewritten/deleted)
// TODO(mberhault): cache file information (especially stated size)
// if stats computation becomes frequent.
class FileStats {
 public:
  FileStats(EnvManager* env_mgr);
  ~FileStats();

  // Get the list of files from RocksDB and our own file registry.
  // Lookup encryption settings and size for all files.
  rocksdb::Status GetFiles(rocksdb::DB* const rep);

  // Fill in basic stats for files of a given EnvType and KeyID.
  // This fills in the count of files and bytes for:
  // - total: env as requested, or no file entry
  // - active_key: active key is the requested one
  // TODO(mberhault): introduce a concept of "unknown" to distinguish
  // between "no file entry" and "this env type but not this key".
  rocksdb::Status GetStatsForEnvAndKey(enginepb::EnvType env_type, const std::string& active_key_id,
                                       DBEnvStatsResult* result);

 private:
  // Called by GetRocksdDBFiles with RocksDB deletions disabled.
  rocksdb::Status GetFilesInternal(rocksdb::DB* const rep);

  // Various helpers to get file lists.
  rocksdb::Status GetLiveFiles(rocksdb::DB* const rep);
  rocksdb::Status GetWalFiles(rocksdb::DB* const rep);
  void GetLiveFilesMetadata(rocksdb::DB* const rep);
  rocksdb::Status FillRegistryEntries();
  void StatFilesForSize();
  std::string FixRocksDBPath(const std::string& filename);

  struct FileInfo {
    FileInfo() : has_size(false), size(0), has_entry(false) {}
    bool has_size;
    uint64_t size;
    bool has_entry;
    enginepb::FileEntry entry;
  };

  EnvManager* env_mgr_;
  const std::string db_dir_;
  std::unordered_map<std::string, FileInfo> files_;
};

}  // namespace cockroach
