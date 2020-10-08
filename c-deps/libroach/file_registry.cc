// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "file_registry.h"
#include "env_manager.h"
#include "fmt.h"
#include "options.h"
#include "utils.h"

using namespace cockroach;

namespace cockroach {

FileRegistry::FileRegistry(rocksdb::Env* env, const std::string& db_dir, bool read_only)
    : env_(env),
      db_dir_(db_dir),
      read_only_(read_only),
      registry_path_(PathAppend(db_dir_, kFileRegistryFilename)) {
  auto status = env_->NewDirectory(db_dir_, &registry_dir_);
  if (!status.ok()) {
    std::shared_ptr<rocksdb::Logger> logger(NewDBLogger(true /* use_primary_log */));
    rocksdb::Fatal(logger, "unable to open directory %s to sync: %s", db_dir.c_str(), status.ToString().c_str());
  }
}

rocksdb::Status FileRegistry::CheckNoRegistryFile() const {
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
  registry_.reset(registry.release());

  return rocksdb::Status::OK();
}

std::unordered_set<int> FileRegistry::GetUsedEnvTypes() const {
  std::unordered_set<int> ret;

  std::unique_lock<std::mutex> l(mu_);
  assert(registry_ != nullptr);

  for (auto it : registry_->files()) {
    ret.insert(it.second.env_type());
  }

  return ret;
}

std::string FileRegistry::TransformPath(const std::string& filename) const {
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

std::shared_ptr<const enginepb::FileRegistry> FileRegistry::GetFileRegistry() const {
  std::unique_lock<std::mutex> l(mu_);
  return registry_;
}
std::unique_ptr<enginepb::FileEntry> FileRegistry::GetFileEntry(const std::string& filename,
                                                                bool relative) const {
  std::string newName =
      relative ? TransformPath(PathAppend(db_dir_, filename)) : TransformPath(filename);

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

  if (newSrc == newTarget) {
    return rocksdb::Status::OK();
  }

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
    return rocksdb::Status::OK();
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
  if (read_only_) {
    return rocksdb::Status::InvalidArgument(
        "file registry is read-only but registry modification attempted");
  }

  // Serialize and write to file.
  std::string contents;
  if (!reg->SerializeToString(&contents)) {
    return rocksdb::Status::InvalidArgument("failed to serialize key registry");
  }

  auto status = SafeWriteStringToFile(env_, registry_dir_.get(), registry_path_, contents);
  if (!status.ok()) {
    return status;
  }

  // Swap registry.
  registry_.reset(reg.release());

  return rocksdb::Status::OK();
}

FileStats::FileStats(EnvManager* env_mgr)
    : env_mgr_(env_mgr), db_dir_(env_mgr->file_registry->db_dir()) {}
FileStats::~FileStats() {}

std::string FileStats::FixRocksDBPath(const std::string& filename) {
  return env_mgr_->file_registry->TransformPath(PathAppend(db_dir_, filename));
}

rocksdb::Status FileStats::GetFiles(rocksdb::DB* const rep) {
  // Prevent deletions by rocksdb.
  auto status = rep->DisableFileDeletions();
  if (!status.ok()) {
    return status;
  }

  status = GetFilesInternal(rep);

  // Always re-enable deletions.
  auto status_enable = rep->EnableFileDeletions(false /* force */);

  if (!status.ok()) {
    // The bad status from GetFilesInternal takes precedence.
    return status;
  }

  return status_enable;
}

rocksdb::Status FileStats::GetStatsForEnvAndKey(enginepb::EnvType env_type,
                                                const std::string& active_key_id,
                                                DBEnvStatsResult* result) {
  uint64_t total_files = 0, total_bytes = 0, active_key_files = 0, active_key_bytes = 0;

  for (const auto& it : files_) {
    if (it.second.has_entry) {
      if (it.second.entry.env_type() != env_type) {
        // Different env type, don't include in total.
        continue;
      }

      std::string key_id;
      auto status = env_mgr_->env_stats_handler->GetFileEntryKeyID(&(it.second.entry), &key_id);
      if (!status.ok()) {
        return status;
      }

      if (key_id == active_key_id) {
        active_key_files++;
        active_key_bytes += it.second.size;
      }
    }

    total_files++;
    total_bytes += it.second.size;
  }

  result->total_files = total_files;
  result->total_bytes = total_bytes;
  result->active_key_files = active_key_files;
  result->active_key_bytes = active_key_bytes;
  return rocksdb::Status::OK();
}

rocksdb::Status FileStats::GetFilesInternal(rocksdb::DB* const rep) {
  // Fetch all the information we can from RocksDB.
  auto status = GetLiveFiles(rep);
  if (!status.ok()) {
    return status;
  }

  status = GetWalFiles(rep);
  if (!status.ok()) {
    return status;
  }

  GetLiveFilesMetadata(rep);

  // Populate files list with entries from the registry.
  status = FillRegistryEntries();
  if (!status.ok()) {
    return status;
  }

  // Stat files for which we do not know the size.
  StatFilesForSize();

  return rocksdb::Status::OK();
}

rocksdb::Status FileStats::GetLiveFiles(rocksdb::DB* const rep) {
  // List of all live files. Filename only.
  // Contains SSTs, miscellaneous files (eg: CURRENT, MANIFEST, OPTIONS)
  std::vector<std::string> live_files;
  uint64_t manifest_size = 0;
  auto status = rep->GetLiveFiles(live_files, &manifest_size, false /* flush_memtable */);
  if (!status.ok()) {
    return status;
  }

  for (const auto& f : live_files) {
    files_[FixRocksDBPath(f)];
  }

  return rocksdb::Status::OK();
}

rocksdb::Status FileStats::GetWalFiles(rocksdb::DB* const rep) {
  // List of WAL files. Filename and size.
  // Contains log files only.
  rocksdb::VectorLogPtr wal_files;
  auto status = rep->GetSortedWalFiles(wal_files);
  if (!status.ok()) {
    return status;
  }

  for (const auto& wf : wal_files) {
    auto path = FixRocksDBPath(wf->PathName());
    files_[path].has_size = true;
    files_[path].size = wf->SizeFileBytes();
  }

  return rocksdb::Status::OK();
}

void FileStats::GetLiveFilesMetadata(rocksdb::DB* const rep) {
  // Metadata about live files. Filename and size.
  // Contains SSTs only.
  std::vector<rocksdb::LiveFileMetaData> live_files;
  rep->GetLiveFilesMetaData(&live_files);

  for (const auto& lf : live_files) {
    auto path = FixRocksDBPath(lf.name);
    files_[path].has_size = true;
    files_[path].size = lf.size;
  }
}

rocksdb::Status FileStats::FillRegistryEntries() {
  auto registry = env_mgr_->file_registry->GetFileRegistry();
  if (registry == nullptr) {
    return rocksdb::Status::InvalidArgument("cannot compute stats, file registry not loaded yet");
  }

  for (const auto& it : registry->files()) {
    files_[it.first].has_entry = true;
    files_[it.first].entry = it.second;
  }
  return rocksdb::Status::OK();
}

void FileStats::StatFilesForSize() {
  for (auto it = files_.begin(); it != files_.end(); ++it) {
    if (it->second.has_size) {
      continue;
    }
    // Unknown file size: stat it.
    // Ignore all errors. We can't filter for "not found" as that returns an I/O error.
    uint64_t size;
    auto status =
        env_mgr_->db_env->GetFileSize(env_mgr_->file_registry->db_dir() + "/" + it->first, &size);
    if (status.ok()) {
      it->second.has_size = true;
      it->second.size = size;
    }
  }
}

}  // namespace cockroach
