// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "testutils.h"
#include <experimental/filesystem>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <string.h>
#include "crypto_utils.h"

namespace testutils {

enginepbccl::SecretKey* MakeAES128Key(rocksdb::Env* env) {
  int64_t now;
  env->GetCurrentTime(&now);

  auto key = new enginepbccl::SecretKey();
  // Random key.
  key->set_key(RandomBytes(16));

  auto info = key->mutable_info();
  info->set_encryption_type(enginepbccl::AES128_CTR);
  info->set_creation_time(now);
  // Random key ID.
  info->set_key_id(HexString(RandomBytes(kKeyIDLength)));

  return key;
}

MemKeyManager::~MemKeyManager() {}

std::unique_ptr<enginepbccl::SecretKey> MemKeyManager::CurrentKey() {
  if (key_ != nullptr) {
    return std::unique_ptr<enginepbccl::SecretKey>(new enginepbccl::SecretKey(*key_.get()));
  }
  return nullptr;
}

std::unique_ptr<enginepbccl::SecretKey> MemKeyManager::GetKey(const std::string& id) {
  if (key_ != nullptr && key_->info().key_id() == id) {
    return std::unique_ptr<enginepbccl::SecretKey>(new enginepbccl::SecretKey(*key_.get()));
  }
  return nullptr;
}

void MemKeyManager::set_key(enginepbccl::SecretKey* key) { key_.reset(key); }

TempDirHandler::TempDirHandler() {}

TempDirHandler::~TempDirHandler() {
  if (tmp_dir_ == "") {
    return;
  }
  std::experimental::filesystem::remove_all(tmp_dir_);
}

bool TempDirHandler::Init() {
  auto fs_dir = std::experimental::filesystem::temp_directory_path() / "tmpccl.XXXXXX";

  // mkdtemp needs a []char to modify.
  char* tmpl = new char[strlen(fs_dir.c_str()) + 1];
  tmpl[strlen(fs_dir.c_str())] = '\0'; /* stupid null-terminated string */
  strcpy(tmpl, fs_dir.c_str());

  auto tmp_c_dir = mkdtemp(tmpl);
  if (tmp_c_dir != NULL) {
    tmp_dir_ = std::string(tmp_c_dir);
  } else {
    std::cerr << "Error creating temp directory" << std::endl;
  }

  delete[] tmpl;
  return (tmp_c_dir != NULL);
}

std::string TempDirHandler::Path(const std::string& subpath) {
  auto fullpath = std::experimental::filesystem::path(tmp_dir_) / subpath;
  return fullpath.string();
}

}  // namespace testutils
