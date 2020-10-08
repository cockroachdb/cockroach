// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "key_manager.h"
#include "../fmt.h"
#include "../utils.h"
#include "crypto_utils.h"

static const std::string kFilenamePlain = "plain";

namespace KeyManagerUtils {

rocksdb::Status KeyFromFile(rocksdb::Env* env, const std::string& path,
                            enginepbccl::SecretKey* key) {
  int64_t now;
  auto status = env->GetCurrentTime(&now);
  if (!status.ok()) {
    return status;
  }

  auto info = key->mutable_info();
  if (path == kFilenamePlain) {
    key->set_key("");
    info->set_encryption_type(enginepbccl::Plaintext);
    info->set_key_id(kPlainKeyID);
    info->set_creation_time(now);
    info->set_source(kFilenamePlain);
    return rocksdb::Status::OK();
  }

  // Real file, try to read it.
  std::string contents;
  status = rocksdb::ReadFileToString(env, path, &contents);
  if (!status.ok()) {
    return status;
  }

  // Check that the length is valid for AES.
  size_t key_length = contents.size() - kKeyIDLength;
  switch (key_length) {
  case 16:
    info->set_encryption_type(enginepbccl::AES128_CTR);
    break;
  case 24:
    info->set_encryption_type(enginepbccl::AES192_CTR);
    break;
  case 32:
    info->set_encryption_type(enginepbccl::AES256_CTR);
    break;
  default:
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("file %s is %zu bytes long, it must be <key ID length (%zu)> + <key "
                          "size (16, 24, or 32)> long",
                          path.c_str(), contents.size(), kKeyIDLength));
  }

  // Fill in the key and ID: first kKeyIDLength are the ID, the rest are the key.
  info->set_key_id(HexString(contents.substr(0, kKeyIDLength)));
  key->set_key(contents.substr(kKeyIDLength, contents.size() - kKeyIDLength));
  info->set_creation_time(now);
  info->set_source(path);

  return rocksdb::Status::OK();
}

rocksdb::Status KeyFromKeyInfo(rocksdb::Env* env, const enginepbccl::KeyInfo& store_info,
                               enginepbccl::SecretKey* key) {
  int64_t now;
  auto status = env->GetCurrentTime(&now);
  if (!status.ok()) {
    return status;
  }

  auto info = key->mutable_info();
  info->set_encryption_type(store_info.encryption_type());
  info->set_creation_time(now);
  info->set_source("data key manager");
  info->set_parent_key_id(store_info.key_id());

  if (store_info.encryption_type() == enginepbccl::Plaintext) {
    key->set_key("");
    info->set_key_id(kPlainKeyID);
    info->set_was_exposed(true);
    return rocksdb::Status::OK();
  }

  // AES encryption.
  size_t length;
  switch (store_info.encryption_type()) {
  case enginepbccl::AES128_CTR:
    length = 16;
    break;
  case enginepbccl::AES192_CTR:
    length = 24;
    break;
  case enginepbccl::AES256_CTR:
    length = 32;
    break;
  default:
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("unknown encryption type %d for key ID %s", store_info.encryption_type(),
                          store_info.key_id().c_str()));
  }
  key->set_key(RandomBytes(length));
  // Assign a random ID to the key.
  info->set_key_id(HexString(RandomBytes(kKeyIDLength)));
  info->set_was_exposed(false);  // For completeness only.
  return rocksdb::Status::OK();
}

rocksdb::Status ValidateRegistry(enginepbccl::DataKeysRegistry* registry) {
  // Make sure active keys exist if set.
  if (registry->active_store_key_id() != "" &&
      registry->store_keys().find(registry->active_store_key_id()) ==
          registry->store_keys().cend()) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
        "active store key %s not found", registry->active_store_key_id().c_str()));
  }

  if (registry->active_data_key_id() != "" &&
      registry->data_keys().find(registry->active_data_key_id()) == registry->data_keys().cend()) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("active data key %s not found", registry->active_data_key_id().c_str()));
  }

  return rocksdb::Status::OK();
}

// GenerateDataKey takes a pointer to a registry (should be a temporary registry, not the
// key manager's private registry) and generates a data key based on the active store key.
rocksdb::Status GenerateDataKey(rocksdb::Env* env, enginepbccl::DataKeysRegistry* reg) {
  // Get the active store key.
  auto iter = reg->store_keys().find(reg->active_store_key_id());
  // The caller should have either called ValidateRegistry or set a store key.
  assert(iter != reg->store_keys().cend());

  // Generate a new key.
  enginepbccl::SecretKey new_key;
  auto status = KeyManagerUtils::KeyFromKeyInfo(env, iter->second, &new_key);
  if (!status.ok()) {
    return status;
  }

  // Store new data key and mark as active.
  (*reg->mutable_data_keys())[new_key.info().key_id()] = new_key;
  reg->set_active_data_key_id(new_key.info().key_id());

  return rocksdb::Status::OK();
}

std::string AlgorithmEnumToString(const enginepbccl::EncryptionType t) {
  switch (t) {
  case enginepbccl::AES128_CTR:
    return "AES128-CTR";
    break;
  case enginepbccl::AES192_CTR:
    return "AES192-CTR";
    break;
  case enginepbccl::AES256_CTR:
    return "AES256-CTR";
    break;
  default:
    return fmt::StringPrintf("unknown EncryptionType: %d", t);
  }
}

std::string StoreKeyInfoSummary(const enginepbccl::KeyInfo& info) {
  if (info.encryption_type() == enginepbccl::Plaintext) {
    return "plain";
  }
  return fmt::StringPrintf("ID: %s, Type: %s, Source: %s", info.key_id().c_str(),
                           AlgorithmEnumToString(info.encryption_type()).c_str(),
                           info.source().c_str());
}

std::string DataKeyInfoSummary(const enginepbccl::KeyInfo& info) {
  if (info.encryption_type() == enginepbccl::Plaintext) {
    return "plain";
  }
  return fmt::StringPrintf("ID: %s, Type: %s, Parent Key ID: %s", info.key_id().c_str(),
                           AlgorithmEnumToString(info.encryption_type()).c_str(),
                           info.parent_key_id().c_str());
}

};  // namespace KeyManagerUtils

KeyManager::~KeyManager() {}

FileKeyManager::FileKeyManager(rocksdb::Env* env, std::shared_ptr<rocksdb::Logger> logger,
                               const std::string& active_key_path, const std::string& old_key_path)
    : env_(env), logger_(logger), active_key_path_(active_key_path), old_key_path_(old_key_path) {}

FileKeyManager::~FileKeyManager() {}

rocksdb::Status FileKeyManager::LoadKeys() {
  std::shared_ptr<enginepbccl::SecretKey> active(new enginepbccl::SecretKey());
  rocksdb::Status status = KeyManagerUtils::KeyFromFile(env_, active_key_path_, active.get());
  if (!status.ok()) {
    return status;
  }

  std::shared_ptr<enginepbccl::SecretKey> old(new enginepbccl::SecretKey());
  status = KeyManagerUtils::KeyFromFile(env_, old_key_path_, old.get());
  if (!status.ok()) {
    return status;
  }

  rocksdb::Info(logger_, "loaded active store key: %s, old store key: %s",
                KeyManagerUtils::StoreKeyInfoSummary(active->info()).c_str(),
                KeyManagerUtils::StoreKeyInfoSummary(old->info()).c_str());

  active_key_.swap(active);
  old_key_.swap(old);
  return rocksdb::Status::OK();
}

std::shared_ptr<enginepbccl::SecretKey> FileKeyManager::CurrentKey() { return active_key_; }

std::shared_ptr<enginepbccl::SecretKey> FileKeyManager::GetKey(const std::string& id) {
  if (active_key_ != nullptr && active_key_->info().key_id() == id) {
    return active_key_;
  }
  if (old_key_ != nullptr && old_key_->info().key_id() == id) {
    return old_key_;
  }
  return nullptr;
}

DataKeyManager::~DataKeyManager() {}

DataKeyManager::DataKeyManager(rocksdb::Env* env, std::shared_ptr<rocksdb::Logger> logger,
                               const std::string& db_dir, int64_t rotation_period, bool read_only)
    : env_(env),
      logger_(logger),
      registry_path_(db_dir + "/" + kKeyRegistryFilename),
      rotation_period_(rotation_period),
      read_only_(read_only) {
  auto status = env_->NewDirectory(db_dir, &registry_dir_);
  if (!status.ok()) {
    rocksdb::Fatal(logger_, "unable to open directory %s to sync: %s", db_dir.c_str(), status.ToString().c_str());
  }
}

rocksdb::Status DataKeyManager::LoadKeysHelper(enginepbccl::DataKeysRegistry* registry) {
  rocksdb::Status status = env_->FileExists(registry_path_);
  if (status.IsNotFound()) {
    // First run: we'll write the file soon enough.
    return rocksdb::Status::OK();
  } else if (!status.ok()) {
    return status;
  }

  std::string contents;
  status = rocksdb::ReadFileToString(env_, registry_path_, &contents);
  if (!status.ok()) {
    return status;
  }

  if (!registry->ParseFromString(contents)) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("failed to parse key registry %s", registry_path_.c_str()));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status DataKeyManager::LoadKeys() {
  std::unique_lock<std::mutex> l(mu_);

  // We should never have loaded keys before.
  assert(current_key_ == nullptr);
  assert(registry_ == nullptr);

  std::unique_ptr<enginepbccl::DataKeysRegistry> registry(new enginepbccl::DataKeysRegistry());
  auto status = LoadKeysHelper(registry.get());
  if (!status.ok()) {
    return status;
  }

  status = KeyManagerUtils::ValidateRegistry(registry.get());
  if (!status.ok()) {
    return status;
  }

  registry_.swap(registry);
  current_key_ = CurrentKeyLocked();

  if (current_key_ == nullptr) {
    rocksdb::Info(logger_, "no active data key yet");
  } else {
    rocksdb::Info(logger_, "loaded active data key: %s",
                  KeyManagerUtils::DataKeyInfoSummary(current_key_->info()).c_str());
  }

  return rocksdb::Status::OK();
}

std::shared_ptr<enginepbccl::SecretKey> DataKeyManager::CurrentKey() {
  std::unique_lock<std::mutex> l(mu_);
  auto status = MaybeRotateKeyLocked();
  if (!status.ok()) {
    rocksdb::Error(logger_, "error while attempting to rotate data key: %s", status.getState());
  }
  return current_key_;
}

std::shared_ptr<enginepbccl::SecretKey> DataKeyManager::CurrentKeyLocked() {
  assert(registry_ != nullptr);
  if (registry_->active_data_key_id() == "") {
    return nullptr;
  }

  auto iter = registry_->data_keys().find(registry_->active_data_key_id());

  // Any modification of the registry should have called Validate.
  assert(iter != registry_->data_keys().cend());
  return std::shared_ptr<enginepbccl::SecretKey>(new enginepbccl::SecretKey(iter->second));
}

std::shared_ptr<enginepbccl::SecretKey> DataKeyManager::GetKey(const std::string& id) {
  std::unique_lock<std::mutex> l(mu_);

  assert(registry_ != nullptr);

  if (id == registry_->active_data_key_id()) {
    // Shortcut: this is the current key.
    return current_key_;
  }

  auto key = registry_->data_keys().find(id);
  if (key == registry_->data_keys().cend()) {
    return nullptr;
  }
  return std::shared_ptr<enginepbccl::SecretKey>(new enginepbccl::SecretKey(key->second));
}

std::unique_ptr<enginepbccl::KeyInfo> DataKeyManager::GetActiveStoreKeyInfo() {
  std::unique_lock<std::mutex> l(mu_);

  assert(registry_ != nullptr);
  if (registry_->active_store_key_id() == "") {
    return nullptr;
  }

  auto iter = registry_->store_keys().find(registry_->active_store_key_id());

  // Any modification of the registry should have called Validate.
  assert(iter != registry_->store_keys().cend());
  return std::unique_ptr<enginepbccl::KeyInfo>(new enginepbccl::KeyInfo(iter->second));
}

std::unique_ptr<enginepbccl::DataKeysRegistry> DataKeyManager::GetScrubbedRegistry() const {
  std::unique_lock<std::mutex> l(mu_);
  if (registry_ == nullptr) {
    return nullptr;
  }

  auto new_registry =
      std::unique_ptr<enginepbccl::DataKeysRegistry>(new enginepbccl::DataKeysRegistry(*registry_));
  auto keys = new_registry->mutable_data_keys();
  for (auto key_iter = keys->begin(); key_iter != keys->end(); ++key_iter) {
    key_iter->second.clear_key();
  }

  return new_registry;
}

rocksdb::Status DataKeyManager::MaybeRotateKeyLocked() {
  assert(registry_ != nullptr);

  if (registry_->active_store_key_id() == "" || registry_->active_data_key_id() == "") {
    return rocksdb::Status::InvalidArgument(
        "MaybeRotateKey called before SetActiveStoreKeyInfo: there is no key to rotate");
  }

  assert(current_key_ != nullptr);

  if (current_key_->info().encryption_type() == enginepbccl::Plaintext) {
    // There's no point in rotating plaintext.
    return rocksdb::Status::OK();
  }

  int64_t now;
  auto status = env_->GetCurrentTime(&now);
  if (!status.ok()) {
    return status;
  }

  if ((now - current_key_->info().creation_time()) < rotation_period_) {
    return rocksdb::Status::OK();
  }

  // We need a new key. Copy the registry first.
  auto new_registry =
      std::unique_ptr<enginepbccl::DataKeysRegistry>(new enginepbccl::DataKeysRegistry(*registry_));

  // Generate and store a new data key.
  status = KeyManagerUtils::GenerateDataKey(env_, new_registry.get());
  if (!status.ok()) {
    return status;
  }

  status = PersistRegistryLocked(std::move(new_registry));
  if (!status.ok()) {
    return status;
  }

  assert(current_key_ != nullptr);
  rocksdb::Info(logger_, "rotated to new active data key: %s",
                KeyManagerUtils::DataKeyInfoSummary(current_key_->info()).c_str());

  return rocksdb::Status::OK();
}

rocksdb::Status
DataKeyManager::SetActiveStoreKeyInfo(std::unique_ptr<enginepbccl::KeyInfo> store_info) {
  std::unique_lock<std::mutex> l(mu_);

  assert(registry_ != nullptr);
  if (registry_->active_store_key_id() == store_info->key_id()) {
    // We currently have this store key marked active: check if we need a refresh.
    return MaybeRotateKeyLocked();
  }

  if (store_info->encryption_type() != enginepbccl::Plaintext) {
    // Make sure the key doesn't exist yet for keys other than plaintext.
    // If we are not currently using plaintext, we're ok overwriting an older "plain" key.
    // TODO(mberhault): Are there cases we may want to allow?
    if (registry_->store_keys().find(store_info->key_id()) != registry_->store_keys().cend()) {
      return rocksdb::Status::InvalidArgument(
          fmt::StringPrintf("new active store key ID %s already exists as an inactive key. This "
                            "is really dangerous.",
                            store_info->key_id().c_str()));
    }
  }

  // Make a copy of the registry, add store key info to the list of store keys, and mark as
  // active.
  auto new_registry =
      std::unique_ptr<enginepbccl::DataKeysRegistry>(new enginepbccl::DataKeysRegistry(*registry_));
  (*new_registry->mutable_store_keys())[store_info->key_id()] = *store_info;
  new_registry->set_active_store_key_id(store_info->key_id());

  if (store_info->encryption_type() == enginepbccl::Plaintext) {
    // This is a plaintext store key: mark all data keys as exposed.
    for (auto it = new_registry->mutable_data_keys()->begin();
         it != new_registry->mutable_data_keys()->end(); ++it) {
      it->second.mutable_info()->set_was_exposed(true);
    }
  }

  // Generate and store a new data key.
  auto status = KeyManagerUtils::GenerateDataKey(env_, new_registry.get());
  if (!status.ok()) {
    return status;
  }

  status = PersistRegistryLocked(std::move(new_registry));
  if (!status.ok()) {
    return status;
  }

  assert(current_key_ != nullptr);
  rocksdb::Info(logger_, "generated new active data key: %s",
                KeyManagerUtils::DataKeyInfoSummary(current_key_->info()).c_str());

  return rocksdb::Status::OK();
}

rocksdb::Status
DataKeyManager::PersistRegistryLocked(std::unique_ptr<enginepbccl::DataKeysRegistry> reg) {
  if (read_only_) {
    return rocksdb::Status::InvalidArgument("key manager is read-only, keys cannot be rotated");
  }

  // Validate before writing.
  auto status = KeyManagerUtils::ValidateRegistry(reg.get());
  if (!status.ok()) {
    return status;
  }

  // Serialize and write to file.
  std::string contents;
  if (!reg->SerializeToString(&contents)) {
    return rocksdb::Status::InvalidArgument("failed to serialize key registry");
  }

  status = SafeWriteStringToFile(env_, registry_dir_.get(), registry_path_, contents);
  if (!status.ok()) {
    return status;
  }

  // Swap registry.
  registry_.swap(reg);
  current_key_ = CurrentKeyLocked();

  return rocksdb::Status::OK();
}
