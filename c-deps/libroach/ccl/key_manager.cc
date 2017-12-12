// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "key_manager.h"
#include <aes.h>  // CryptoPP
#include <google/protobuf/stubs/stringprintf.h>
#include <modes.h>  // CryptoPP

using google::protobuf::StringPrintf;

static std::string kFilenamePlain = "plain";

static rocksdb::Status KeyFromFile(rocksdb::Env* env, const std::string& path, EncryptionKey* key) {
  if (path == kFilenamePlain) {
    // plaintext placeholder.
    key->type = PLAIN;
    key->id = "";
    key->key = "";
    key->source = kFilenamePlain;
    return rocksdb::Status::OK();
  }

  // Real file, try to read it.
  std::string contents;
  auto status = rocksdb::ReadFileToString(env, path, &contents);
  if (!status.ok()) {
    return status;
  }

  // Check that the length is valid for AES.
  auto key_length = contents.size();
  if (key_length != 16 && key_length != 24 && key_length != 32) {
    return rocksdb::Status::InvalidArgument(
        StringPrintf("key in file %s has length %llu, want 16, 24, or 32", path.c_str(), key_length));
  }

  // Let's make sure CryptoPP::AES is ok with it, this returns a valid key length based on the
  // passed-in one (eg: 32 for 31).
  size_t accepted = CryptoPP::AES::Encryption::StaticGetValidKeyLength(key_length);
  if (accepted != key_length) {
    return rocksdb::Status::InvalidArgument(
        StringPrintf("key in file %s has length %llu, but CryptoPP::AES says the nearest acceptable is %llu",
                     path.c_str(), key_length, accepted));
  }

  // Fill in the key.
  key->type = AES;
  key->id = KeyHash(contents);
  key->key = contents;
  key->source = path;

  return rocksdb::Status::OK();
}

rocksdb::Status FileKeyManager::LoadKeys() {
  std::unique_ptr<EncryptionKey> active(new EncryptionKey());
  rocksdb::Status status = KeyFromFile(env_, active_key_path_, active.get());
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<EncryptionKey> old(new EncryptionKey());
  status = KeyFromFile(env_, old_key_path_, old.get());
  if (!status.ok()) {
    return status;
  }

  active_key_.swap(active);
  old_key_.swap(old);
  return rocksdb::Status::OK();
}
