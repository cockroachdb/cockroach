// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
//  (found in the LICENSE.Apache file in the root directory).

#include "ctr_stream.h"
#include <arpa/inet.h>
#include "../fmt.h"
#include "../plaintext_stream.h"
#include "crypto_utils.h"

using namespace cockroach;

namespace cockroach {

rocksdb::Status BuildCipherStream(const enginepbccl::EncryptionSettings& settings,
                                  const enginepbccl::SecretKey* key,
                                  std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
  // We should not be getting called for plaintext, and we only have AES.
  if (settings.encryption_type() != enginepbccl::AES128_CTR &&
      settings.encryption_type() != enginepbccl::AES192_CTR &&
      settings.encryption_type() != enginepbccl::AES256_CTR) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("unknown encryption type %d", settings.encryption_type()));
  }

  (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(
      new CTRCipherStream(new AESCipher(key->key()), settings.nonce(), settings.counter()));

  return rocksdb::Status::OK();
}

CTRCipherStreamCreator::~CTRCipherStreamCreator() {}

rocksdb::Status CTRCipherStreamCreator::InitSettingsAndCreateCipherStream(
    std::string* settings, std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
  auto key = key_manager_->CurrentKey();
  if (key == nullptr || key->info().encryption_type() == enginepbccl::Plaintext) {
    // Plaintext: don't set "settings".
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
    return rocksdb::Status::OK();
  }

  // Create the settings.
  enginepbccl::EncryptionSettings enc_settings;
  enc_settings.set_encryption_type(key->info().encryption_type());
  enc_settings.set_key_id(key->info().key_id());

  // Let's get 16 random bytes. 12 for the nonce, 4 for the counter.
  std::string random_bytes = RandomBytes(16);
  assert(random_bytes.size() == 16);

  // First 12 bytes for the nonce.
  enc_settings.set_nonce(random_bytes.substr(0, 12));
  // Last 4 as an unsigned int32 for the counter.
  uint32_t counter;
  memcpy(&counter, random_bytes.data() + 12, 4);
  enc_settings.set_counter(counter);

  // Serialize enc_settings directly into the passed settings pointer. This will be ignored
  // on error.
  if (!enc_settings.SerializeToString(settings)) {
    return rocksdb::Status::InvalidArgument("failed to serialize encryption settings");
  }

  return BuildCipherStream(enc_settings, key.get(), result);
}

// Create a cipher stream given encryption settings.
rocksdb::Status CTRCipherStreamCreator::CreateCipherStreamFromSettings(
    const std::string& settings, std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>* result) {
  enginepbccl::EncryptionSettings enc_settings;
  if (!enc_settings.ParseFromString(settings)) {
    return rocksdb::Status::InvalidArgument("failed to parse encryption settings");
  }

  if (settings.size() == 0 || enc_settings.encryption_type() == enginepbccl::Plaintext) {
    // Plaintext.
    (*result) = std::unique_ptr<rocksdb_utils::BlockAccessCipherStream>(new PlaintextStream());
    return rocksdb::Status::OK();
  }

  // Get the key from the manager.
  auto key = key_manager_->GetKey(enc_settings.key_id());
  if (key == nullptr) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
        "key_manager does not have a key with ID %s", enc_settings.key_id().c_str()));
  }

  return BuildCipherStream(enc_settings, key.get(), result);
}

enginepb::EnvType CTRCipherStreamCreator::GetEnvType() { return env_type_; }

CTRCipherStream::CTRCipherStream(rocksdb_utils::BlockCipher* c, const std::string& nonce,
                                 uint32_t counter)
    : cipher_(c), nonce_(nonce), counter_(counter) {
  assert(iv_.size() == (cipher_->BlockSize() - 4));
}

CTRCipherStream::~CTRCipherStream() {}

size_t CTRCipherStream::BlockSize() { return cipher_->BlockSize(); }

void CTRCipherStream::AllocateScratch(std::string& scratch) {
  auto blockSize = cipher_->BlockSize();
  scratch.reserve(blockSize);
}

rocksdb::Status CTRCipherStream::EncryptBlock(uint64_t blockIndex, char* data, char* scratch) {
  // Create IV = nonce + counter
  auto blockSize = cipher_->BlockSize();
  auto nonce_size = blockSize - 4;
  // Write the nonce at the beginning of the scratch space.
  memcpy(scratch, nonce_.data(), nonce_size);

  // Counter value for this block, converted to network byte order.
  uint32_t block_counter = htonl(counter_ + blockIndex);
  // Write after the nonce.
  memcpy(scratch + nonce_size, &block_counter, 4);

  // Encrypt nonce+counter
  auto status = cipher_->Encrypt(scratch);
  if (!status.ok()) {
    return status;
  }

  // XOR data with ciphertext.
  // TODO(mberhault): this is not an efficient XOR. Instead, we could move
  // this into the cipher and use something like CryptoPP::ProcessAndXorBlock.
  for (size_t i = 0; i < blockSize; i++) {
    data[i] = data[i] ^ scratch[i];
  }
  return rocksdb::Status::OK();
}

// Decrypt a block of data at the given block index.
// Length of data is equal to BlockSize();
rocksdb::Status CTRCipherStream::DecryptBlock(uint64_t blockIndex, char* data, char* scratch) {
  // For CTR decryption & encryption are the same
  return EncryptBlock(blockIndex, data, scratch);
}

}  // namespace cockroach
