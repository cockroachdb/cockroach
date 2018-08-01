// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <string>
#include "../rocksdbutils/env_encryption.h"
#include "key_manager.h"

namespace cockroach {

// CTRCipherStreamCreator creates a CTRCipherStream using a KeyManager.
// Takes ownership of the KeyManager.
class CTRCipherStreamCreator final : public rocksdb_utils::CipherStreamCreator {
 public:
  CTRCipherStreamCreator(KeyManager* key_mgr, enginepb::EnvType env_type)
      : key_manager_(key_mgr), env_type_(env_type) {}
  virtual ~CTRCipherStreamCreator();

  // Initialize 'settings' based on the current encryption algorithm and key
  // and assign a new cipher stream to 'result'.
  virtual rocksdb::Status InitSettingsAndCreateCipherStream(
      std::string* settings,
      std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

  // Create a cipher stream given encryption settings.
  virtual rocksdb::Status CreateCipherStreamFromSettings(
      const std::string& settings,
      std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

  virtual enginepb::EnvType GetEnvType() override;

 private:
  std::unique_ptr<KeyManager> key_manager_;
  enginepb::EnvType env_type_;
};

class CTRCipherStream final : public rocksdb_utils::BlockAccessCipherStream {
 public:
  // Create a CTR cipher stream given:
  // - a block cipher (takes ownership)
  // - nonce of size 'cipher.BlockSize - sizeof(counter)' (eg: 16-4 = 12 bytes for AES)
  // - counter
  CTRCipherStream(std::shared_ptr<enginepbccl::SecretKey> key, const std::string& nonce,
                  uint32_t counter);
  virtual ~CTRCipherStream();

 protected:
  // Initialize a new cipher object. A Cipher is not thread-safe but can be used for any
  // number of EncryptBlock/DecryptBlock calls.
  virtual rocksdb::Status
  InitCipher(std::unique_ptr<rocksdb_utils::BlockCipher>* cipher) const override;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status EncryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const override;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status DecryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const override;

 private:
  const std::shared_ptr<enginepbccl::SecretKey> key_;
  const std::string nonce_;
  const uint32_t counter_;
};

}  // namespace cockroach
