// Copyright 2017 The Cockroach Authors.
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

#ifndef ROACHLIB_PREAMBLE_H
#define ROACHLIB_PREAMBLE_H

#include <rocksdb/env.h>
#include <rocksdb/env_encryption.h>

// Preamble length.
// WARNING: changing this will result in incompatible on-disk format.
// The preamble length must fit in a uint16_t.
const static size_t defaultPreambleLength = 4096;

class PreambleHandler : rocksdb::EncryptionProvider {
 public:

  PreambleHandler() {}
  virtual ~PreambleHandler() {}

  // GetEnv returns an EncryptionEnv wrapped around base_env.
  rocksdb::Env* GetEnv(rocksdb::Env* base_env);

  // GetPrefixLength returns the preamble length.
  virtual size_t GetPrefixLength() override;

  // CreateNewPrefix initializes an allocated block of prefix memory  for a new file.
  virtual rocksdb::Status CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) override;

  // CreateCipherStream creates a block access cipher stream for a file given name and options.
  virtual rocksdb::Status CreateCipherStream(const std::string& fname, const rocksdb::EnvOptions& options,
    rocksdb::Slice& prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result) override;

 private:
  rocksdb::EncryptionProvider* passthrough_provider_;
};

// Blocksize for the plaintext cipher stream.
// TODO(mberhault): we can pick anything we like. What's good?
const static size_t plaintextBlockSize = 4096;

// PlaintextCipherStream implements BlockAccessCipherStream with
// no-op encrypt/decrypt operations.
class PlaintextCipherStream final : public rocksdb::BlockAccessCipherStream {
 public:
  PlaintextCipherStream() {}
  virtual ~PlaintextCipherStream() {}

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() override { return plaintextBlockSize; }

  // Encrypt blocks of data. This is a noop.
  virtual rocksdb::Status Encrypt(uint64_t fileOffset, char *data, size_t dataSize) override {
    return rocksdb::Status::OK();
  }

  // Decrypt blocks of data. This is a noop.
  virtual rocksdb::Status Decrypt(uint64_t fileOffset, char *data, size_t dataSize) override {
    return rocksdb::Status::OK();
  }
 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) override {}

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status EncryptBlock(uint64_t blockIndex, char *data, char* scratch) override {
		return rocksdb::Status::OK();
  }

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status DecryptBlock(uint64_t blockIndex, char *data, char* scratch) override {
		return rocksdb::Status::OK();
  }
};

#endif // ROACHLIB_PREAMBLE_H
