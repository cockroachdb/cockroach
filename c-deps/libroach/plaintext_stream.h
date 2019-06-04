// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "rocksdbutils/env_encryption.h"

namespace cockroach {

// PlaintextStream is a no-op stream cipher used for plaintext content.
class PlaintextStream final : public rocksdb_utils::BlockAccessCipherStream {
 public:
  PlaintextStream() {}
  virtual ~PlaintextStream() {}

  virtual rocksdb::Status Encrypt(uint64_t fileOffset, char* data, size_t dataSize) const override {
    return rocksdb::Status::OK();
  }
  virtual rocksdb::Status Decrypt(uint64_t fileOffset, char* data, size_t dataSize) const override {
    return rocksdb::Status::OK();
  }

 protected:
  virtual rocksdb::Status
  InitCipher(std::unique_ptr<rocksdb_utils::BlockCipher>* cipher) const override {
    return rocksdb::Status::InvalidArgument("InitCipher cannot be called on a PlaintextStream");
  }

  virtual rocksdb::Status EncryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const override {
    return rocksdb::Status::InvalidArgument("EncryptBlock cannot be called on a PlaintextStream");
  }
  virtual rocksdb::Status DecryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const override {
    return rocksdb::Status::InvalidArgument("DecryptBlock cannot be called on a PlaintextStream");
  }
};

}  // namespace cockroach
