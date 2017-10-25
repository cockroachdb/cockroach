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

#include <rocksdb/env.h>
#include "encryption.h"

#include <iostream>
#include <osrng.h>
#include <modes.h>
#include <filters.h>
#include <sha.h>
#include <hex.h>

size_t AESCipher::BlockSize() {
  return CryptoPP::AES::BLOCKSIZE;
}

rocksdb::Status AESCipher::Encrypt(char *data) {
  CryptoPP::ECB_Mode<CryptoPP::AES>::Encryption enc((byte *)key_, CryptoPP::AES::DEFAULT_KEYLENGTH);
  std::string output;
  output.reserve(CryptoPP::AES::BLOCKSIZE);
  enc.ProcessData((byte *)(output.data()), (const byte *)data, CryptoPP::AES::BLOCKSIZE);
  memmove(data, output.data(), CryptoPP::AES::BLOCKSIZE);

  return rocksdb::Status::OK();
};

rocksdb::Status AESCipher::Decrypt(char *data) {
  CryptoPP::ECB_Mode<CryptoPP::AES>::Decryption dec((byte *)key_, CryptoPP::AES::DEFAULT_KEYLENGTH);
  std::string output;
  output.reserve(CryptoPP::AES::BLOCKSIZE);
  dec.ProcessData((byte *)(output.data()), (const byte *)data, CryptoPP::AES::BLOCKSIZE);
  memmove(data, output.data(), CryptoPP::AES::BLOCKSIZE);

  return rocksdb::Status::OK();
};

CipherList ciphers;
rocksdb::Env* GetEncryptedEnv(rocksdb::Env* base_env) {
  // Make a few hard-coded keys and corresponding ciphers.
  char *key1 = new char[CryptoPP::AES::DEFAULT_KEYLENGTH];
  memset(key1, 0x01, CryptoPP::AES::DEFAULT_KEYLENGTH);
  char *key2 = new char[CryptoPP::AES::DEFAULT_KEYLENGTH];
  memset(key2, 0x02, CryptoPP::AES::DEFAULT_KEYLENGTH);

  ciphers[1] = new AESCipher(key1);
  ciphers[2] = new AESCipher(key2);
  rocksdb::EncryptionProvider* provider = new CTREncryptionProviderWithKey(ciphers);
  return rocksdb::NewEncryptedEnv(base_env ? base_env : rocksdb::Env::Default(), provider);
}

inline uint64_t DecodeFixed64(const char* ptr) {
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

inline void EncodeFixed64(char* ptr, uint64_t entry) {
    memcpy(ptr, &entry, sizeof(entry));
}

// GetPrefixLength returns the length of the prefix that is added to every file
// and used for storing encryption options.
// For optimal performance, the prefix length should be a multiple of 
// the a page size.
size_t CTREncryptionProviderWithKey::GetPrefixLength() {
  return defaultPrefixLength;
}

// encodeCTRParameters encodes the parameters into the prefix and returns the pseudo-random IV and counter.
static void encodeCTRParameters(char *prefix, size_t blockSize, uint64_t keyID, uint64_t &initialCounter, rocksdb::Slice &iv) {
  // Zero-out the prefix.
  memset(prefix, 0, defaultPrefixLength);

  // Skip the first 64 bits, just zeros for now.
  auto offset = sizeof(keyID);
  // Second set of 64 bits is the key ID.
  EncodeFixed64(prefix + offset, keyID);
  // Add 2*blocksize worth of pseudo-random data.
  offset += sizeof(keyID);
  CryptoPP::OS_GenerateRandomBlock(false, (byte *)(prefix + offset), blockSize * 2);
  // Extract the block-wise long initial counter.
  initialCounter = DecodeFixed64(prefix + offset);
  // Followed by a blockSize-long IV.
  offset += blockSize;
  iv = rocksdb::Slice(prefix + offset, blockSize);
}

// CreateNewPrefix initialized an allocated block of prefix memory 
// for a new file.
rocksdb::Status CTREncryptionProviderWithKey::CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) {
  std::cout << "#### Creating new file: " << fname << " with active_key_: " << active_key_ << "\n";

  // Take random data to extract initial counter & IV
  auto blockSize = ciphers_[active_key_]->BlockSize();
  uint64_t initialCounter;
  rocksdb::Slice prefixIV;
  encodeCTRParameters(prefix, blockSize, active_key_, initialCounter, prefixIV);

  return rocksdb::Status::OK();
}

rocksdb::Status CTREncryptionProviderWithKey::CreateCipherStream(const std::string& fname, const rocksdb::EnvOptions& options, rocksdb::Slice &prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result) {
  // Read plain text part of prefix.
  uint64_t keyID, initialCounter;
  rocksdb::Slice iv;

  // Skip the 64 bits, just zeros for now.
  auto offset = sizeof(keyID);
  // Second set of 64 bits if the key ID.
  keyID = DecodeFixed64(prefix.data() + offset);
  // Then comes a blockSize-long initial counter (but we just want 64 bits).
  offset += sizeof(keyID);
  initialCounter = DecodeFixed64(prefix.data() + offset);
  // Followed by a blockSize-long IV.
  // TODO(marc): fail on missing key.
  auto blockSize = ciphers_[keyID]->BlockSize();
  offset += blockSize;
  iv = rocksdb::Slice(prefix.data() + offset, blockSize);

  std::cout << "#### Opening file: " << fname << " with key: " << keyID << "\n";

  // Create cipher stream 
  return CreateCipherStreamFromPrefix(fname, options, keyID, initialCounter, iv, prefix, result);
}

// CreateCipherStreamFromPrefix creates a block access cipher stream for a file given
// given name and options. The given prefix is already decrypted.
rocksdb::Status CTREncryptionProviderWithKey::CreateCipherStreamFromPrefix(const std::string& fname, const rocksdb::EnvOptions& options,
    uint64_t keyID, uint64_t initialCounter, const rocksdb::Slice& iv, const rocksdb::Slice& prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result) {
  (*result) = std::unique_ptr<rocksdb::BlockAccessCipherStream>(new rocksdb::CTRCipherStream(*ciphers_[keyID], iv.data(), initialCounter));
  return rocksdb::Status::OK();
}
