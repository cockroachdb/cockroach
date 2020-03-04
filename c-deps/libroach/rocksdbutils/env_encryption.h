// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
//
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found at
//  https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
//  and Apache 2.0 License (found in licenses/APL.txt in the root
//  of this repository).

#pragma once

#include <string>

#include "../file_registry.h"
#include "../protos/storage/enginepb/file_registry.pb.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace enginepb = cockroach::storage::enginepb;

namespace rocksdb_utils {

class CipherStreamCreator;

// Returns an Env that encrypts data when stored on disk and decrypts data when
// read from disk.
// The env takes ownership of the CipherStreamCreator.
rocksdb::Env* NewEncryptedEnv(rocksdb::Env* base_env, cockroach::FileRegistry* file_registry,
                              CipherStreamCreator* creator);

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

// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other blocks).
// E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream {
 public:
  virtual ~BlockAccessCipherStream() {}

  // Encrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual rocksdb::Status Encrypt(uint64_t fileOffset, char* data, size_t dataSize) const;

  // Decrypt one or more (partial) blocks of data at the file offset.
  // Length of data is given in dataSize.
  virtual rocksdb::Status Decrypt(uint64_t fileOffset, char* data, size_t dataSize) const;

 protected:
  // Initialize a new cipher object. A Cipher is not thread-safe but can be used for any
  // number of EncryptBlock/DecryptBlock calls.
  virtual rocksdb::Status InitCipher(std::unique_ptr<rocksdb_utils::BlockCipher>* cipher) const = 0;

  // Encrypt a block of data at the given block index.
  // Length of data is equal to cipher.BlockSize();
  virtual rocksdb::Status EncryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const = 0;

  // Decrypt a block of data at the given block index.
  // Length of data is equal to cipher.BlockSize();
  virtual rocksdb::Status DecryptBlock(rocksdb_utils::BlockCipher* cipher, uint64_t blockIndex,
                                       char* data, char* scratch) const = 0;
};

// CipherStreamCreator is the abstract class used by EncryptedEnv.
// It performs two functions:
// * initializes encryption settings and create a cipher stream with them
// * creates a cipher stream given encryption settings
class CipherStreamCreator {
 public:
  virtual ~CipherStreamCreator() {}
  // Create new encryption settings and a cipher stream using them.
  // Assigns new objects to 'settings' and 'result'.
  // If plaintext, settings will be nullptr.
  virtual rocksdb::Status
  InitSettingsAndCreateCipherStream(std::string* settings,
                                    std::unique_ptr<BlockAccessCipherStream>* result) = 0;

  // Create a cipher stream given encryption settings.
  virtual rocksdb::Status
  CreateCipherStreamFromSettings(const std::string& settings,
                                 std::unique_ptr<BlockAccessCipherStream>* result) = 0;

  // Return the EnvType for this stream creator. It should match files being operated on.
  virtual enginepb::EnvType GetEnvType() = 0;
};

}  // namespace rocksdb_utils
