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
