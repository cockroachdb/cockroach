// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "testutils.h"
#include <gtest/gtest.h>
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

}  // namespace testutils
