//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace rocksdb_utils {

class EncryptionProvider;

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
rocksdb::Env* NewEncryptedEnv(rocksdb::Env* base_env, EncryptionProvider* provider, int env_level);

// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other blocks).
// E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream {
 public:
  virtual ~BlockAccessCipherStream() {}

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual rocksdb::Status Encrypt(uint64_t fileOffset, char* data, size_t dataSize);

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual rocksdb::Status Decrypt(uint64_t fileOffset, char* data, size_t dataSize);

 protected:
  // Allocate scratch space which is passed to EncryptBlock/DecryptBlock.
  virtual void AllocateScratch(std::string&) = 0;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status EncryptBlock(uint64_t blockIndex, char* data, char* scratch) = 0;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to BlockSize();
  virtual rocksdb::Status DecryptBlock(uint64_t blockIndex, char* data, char* scratch) = 0;
};

// BlockCipher
class BlockCipher {
 public:
  virtual ~BlockCipher() {}

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() = 0;

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual rocksdb::Status Encrypt(char* data) = 0;

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  virtual rocksdb::Status Decrypt(char* data) = 0;
};

// The encryption provider is used to create a cipher stream for a specific file.
// The returned cipher stream will be used for actual encryption/decryption
// actions.
class EncryptionProvider {
 public:
  virtual ~EncryptionProvider() {}

  virtual rocksdb::Status CreateCipherStream(int env_level, const std::string& fname, bool new_file,
                                             std::unique_ptr<BlockAccessCipherStream>* result) = 0;

  virtual rocksdb::Status DeleteFile(const std::string& fname) = 0;
  virtual rocksdb::Status RenameFile(const std::string& src, const std::string& target) = 0;
};

}  // namespace rocksdb_utils
