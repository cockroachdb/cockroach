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

#include <arpa/inet.h>
#include "preamble.h"
#include "protos/storage/preamble.pb.h"

// Preamble length.
// WARNING: changing this will result in incompatible on-disk format.
// The preamble length must fit in a uint16_t.
const static size_t kPreambleLength = 4096;

// Blocksize for the plaintext cipher stream.
// TODO(mberhault): we need to benchmark this for a good value, but for now we use the AES::BlockSize.
const static size_t kPlaintextBlockSize = 16;

// PlaintextCipherStream implements BlockAccessCipherStream with
// no-op encrypt/decrypt operations.
class PlaintextCipherStream final : public rocksdb::BlockAccessCipherStream {
 public:
  PlaintextCipherStream() {}
  virtual ~PlaintextCipherStream() {}

  // BlockSize returns the size of each block supported by this cipher stream.
  virtual size_t BlockSize() override { return kPlaintextBlockSize; }

  // Encrypt blocks of data. This is a noop.
  virtual rocksdb::Status Encrypt(uint64_t fileOffset, char *data, size_t dataSize) override {
    return rocksdb::Status::OK();
  }

  // Decrypt blocks of data. This is a noop.
  virtual rocksdb::Status Decrypt(uint64_t fileOffset, char *data, size_t dataSize) override {
    return rocksdb::Status::OK();
  }
 protected:
  // No-op required methods.
  virtual void AllocateScratch(std::string&) override {}
  virtual rocksdb::Status EncryptBlock(uint64_t blockIndex, char *data, char* scratch) override {
    return rocksdb::Status::OK();
  }
  virtual rocksdb::Status DecryptBlock(uint64_t blockIndex, char *data, char* scratch) override {
    return rocksdb::Status::OK();
  }
};

size_t PreambleHandler::GetPrefixLength() {
  return kPreambleLength;
}

rocksdb::Env* PreambleHandler::GetEnv(rocksdb::Env* base_env) {
  return rocksdb::NewEncryptedEnv(base_env ? base_env : rocksdb::Env::Default(), this);
}

rocksdb::Status PreambleHandler::CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) {
  // Zero-out the prefix.
  memset(prefix, 0, prefixLength);

  // Create a preamble proto with encryption settings.
  cockroach::storage::Preamble preamble;
  // Everything is plaintext for now.
  preamble.set_encryption_type(cockroach::storage::Plaintext);

  // Check the byte size before encoding.
  int byte_size = preamble.ByteSize();

  // Determine the serialized length and size of the length prefix.
  assert(byte_size < UINT16_MAX);
  uint16_t encoded_size = htons(byte_size);
  auto num_length_bytes = sizeof(encoded_size);

  if ((byte_size + num_length_bytes) > prefixLength ) {
    return rocksdb::Status::Corruption("new preamble exceeds max preamble length");
  }

  // Write length prefix.
  memcpy(prefix, &encoded_size, num_length_bytes);

  // Write it to the prefix.
  if (!preamble.SerializeToArray(prefix + num_length_bytes, byte_size)) {
    return rocksdb::Status::Corruption("unable to write prefix");
  }

  return rocksdb::Status::OK();
}

rocksdb::Status PreambleHandler::CreateCipherStream(const std::string& fname, const rocksdb::EnvOptions& options, rocksdb::Slice &prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result) {
  // Read length prefix.
  uint16_t encoded_size;
  auto num_length_bytes = sizeof(encoded_size);
  memcpy(&encoded_size, prefix.data(), num_length_bytes);

  // Convert length prefix from network byte order.
  int byte_size = ntohs(encoded_size);

  // Parse prefix
  cockroach::storage::Preamble preamble;
  if (!preamble.ParseFromArray(prefix.data() + num_length_bytes, byte_size)) {
    return rocksdb::Status::Corruption("unable to parse prefix");
  }

  if (preamble.encryption_type() == cockroach::storage::Plaintext) {
    (*result) = std::unique_ptr<rocksdb::BlockAccessCipherStream>(new PlaintextCipherStream());
  } else {
    return rocksdb::Status::NotSupported("unknown encryption type");
  }

  return rocksdb::Status::OK();
}
