// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <string>
#include "../rocksdbutils/env_encryption.h"
#include "../switching_provider.h"
#include "key_manager.h"

// CTRCipherStreamCreator creates a CTRCipherStream using a KeyManager.
// Takes ownership of the KeyManager.
class CTRCipherStreamCreator final : public CipherStreamCreator {
 public:
  CTRCipherStreamCreator(KeyManager* key_mgr) : key_manager_(key_mgr) {}
  virtual ~CTRCipherStreamCreator();

  virtual rocksdb::Status InitSettingsAndCreateCipherStream(
      std::string* settings,
      std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

  // Create a cipher stream given encryption settings.
  virtual rocksdb::Status CreateCipherStreamFromSettings(
      const std::string& settings,
      std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) override;

 private:
  std::unique_ptr<KeyManager> key_manager_;
};

class CTRCipherStream final : public rocksdb_utils::BlockAccessCipherStream {
 public:
  // Create a CTR cipher stream given:
  // - a block cipher (takes ownership)
  // - nonce of size 'cipher.BlockSize - sizeof(counter)' (eg: 16-4 = 12 bytes for AES)
  // - counter
  CTRCipherStream(rocksdb_utils::BlockCipher* c, const std::string& nonce, uint32_t counter);
  virtual ~CTRCipherStream();

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() override;

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) override;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status EncryptBlock(uint64_t blockIndex, char* data, char* scratch) override;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status DecryptBlock(uint64_t blockIndex, char* data, char* scratch) override;

 private:
  std::unique_ptr<rocksdb_utils::BlockCipher> cipher_;
  std::string nonce_;
  uint32_t counter_;
};
