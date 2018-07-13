// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "crypto_utils.h"
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>
#include <cryptopp/osrng.h>
#include <cryptopp/sha.h>
#include "../fmt.h"

#ifndef _WIN32
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#endif

std::string HexString(const std::string& s) {
  std::string value;

  CryptoPP::StringSource ss(
      s, true /* PumpAll */,
      new CryptoPP::HexEncoder(new CryptoPP::StringSink(value), false /* uppercase */));

  return value;
}

std::string RandomBytes(size_t length) {
  CryptoPP::SecByteBlock data(length);
  CryptoPP::OS_GenerateRandomBlock(false /* blocking */, data, length);
  return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

class AESEncryptCipher : public rocksdb_utils::BlockCipher {
 public:
  // The key must have a valid length (16/24/32 bytes) or CryptoPP will fail.
  AESEncryptCipher(const std::string& key) : enc_((byte*)key.data(), key.size()) {}

  ~AESEncryptCipher() {}

  // Blocksize is fixed for AES.
  size_t BlockSize() { return CryptoPP::AES::BLOCKSIZE; }

  // Encrypt a block of data.
  // Length of data is equal to BlockSize().
  rocksdb::Status Encrypt(char* data) {
    enc_.ProcessBlock((byte*)data);
    return rocksdb::Status::OK();
  }

  // Decrypt a block of data.
  // Length of data is equal to BlockSize().
  // NOT IMPLEMENTED: this is not needed for CTR mode so remains unimplemented.
  rocksdb::Status Decrypt(char* data) {
    return rocksdb::Status::NotSupported("this is an encrypt-only cipher");
  }

 private:
  CryptoPP::AES::Encryption enc_;
};

rocksdb_utils::BlockCipher* NewAESEncryptCipher(const enginepbccl::SecretKey* key) {
  return new AESEncryptCipher(key->key());
}

bool UsesAESNI() { return CryptoPP::UsesAESNI(); }

rocksdb::Status DisableCoreFile() {
#ifdef _WIN32
  return rocksdb::Status::NotSupported("preventing crash reports is not supported on Windows");
#else
  // We can't use prlimit on OSX. Use setrlimit and getrlimit instead.
  rlimit lim;
  int ret = getrlimit(RLIMIT_CORE, &lim);
  if (ret != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to get core size rlimit: %s", strerror(errno)));
  }

  if (lim.rlim_cur == 0 && lim.rlim_max == 0) {
    return rocksdb::Status::OK();
  }

  rlimit new_lim = {0, 0};
  ret = setrlimit(RLIMIT_CORE, &new_lim);
  if (ret != 0) {
    return rocksdb::Status::NotSupported(
        fmt::StringPrintf("failed to set core size rlimit: %s", strerror(errno)));
  }

  return rocksdb::Status::OK();
#endif
}
