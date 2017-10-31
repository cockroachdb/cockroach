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

#ifndef ROACHLIB_ENCRYPTION_H
#define ROACHLIB_ENCRYPTION_H

#include <map>
#include <aes.h>
#include <modes.h>
#include <rocksdb/env_encryption.h>

const static size_t defaultPrefixLength = 4096;

rocksdb::Env* GetEncryptedEnv(rocksdb::Env* base_env);

// Implements a BlockCipher using AES-128.
class AESCipher : public rocksdb::BlockCipher {
    private:
      size_t blockSize_;
      CryptoPP::AES::Encryption enc_;
      CryptoPP::AES::Decryption dec_;
    public:
      AESCipher(char *key) :
        enc_((byte*)key, CryptoPP::AES::DEFAULT_KEYLENGTH),
        dec_((byte*)key, CryptoPP::AES::DEFAULT_KEYLENGTH) {
      }
      virtual ~AESCipher() {}

      // BlockSize returns the size of each block supported by this cipher stream.
      virtual size_t BlockSize() override {
        return CryptoPP::AES::BLOCKSIZE;
      }

      // Encrypt a block of data.
      // Length of data is equal to BlockSize().
      virtual rocksdb::Status Encrypt(char *data) override {
        enc_.ProcessBlock((byte *)data);
        return rocksdb::Status::OK();
      }

      // Decrypt a block of data.
      // Length of data is equal to BlockSize().
      virtual rocksdb::Status Decrypt(char *data) override {
        dec_.ProcessBlock((byte *)data);
        return rocksdb::Status::OK();
      }
};

typedef std::map<uint64_t, rocksdb::BlockCipher*> CipherList;

class CTREncryptionProviderWithKey : public rocksdb::EncryptionProvider {
  private:
      CipherList& ciphers_;
      uint64_t active_key_;

  public:
     CTREncryptionProviderWithKey(CipherList& ciphers)
        : ciphers_(ciphers) {
          auto iter = ciphers_.rbegin();
          if (iter == ciphers_.rend()) {
            active_key_ = 0;
          } else {
            active_key_ = iter->first;
          }
        };
    virtual ~CTREncryptionProviderWithKey() {}

    // GetPrefixLength returns the length of the prefix that is added to every file
    // and used for storing encryption options.
    // For optimal performance, the prefix length should be a multiple of 
    // the a page size.
    virtual size_t GetPrefixLength() override;

    // CreateNewPrefix initialized an allocated block of prefix memory 
    // for a new file.
    virtual rocksdb::Status CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) override;

    // CreateCipherStream creates a block access cipher stream for a file given
    // given name and options.
    virtual rocksdb::Status CreateCipherStream(const std::string& fname, const rocksdb::EnvOptions& options,
      rocksdb::Slice& prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result) override;
  protected:
    // CreateCipherStreamFromPrefix creates a block access cipher stream for a file given
    // given name and options. The given prefix is already decrypted.
    virtual rocksdb::Status CreateCipherStreamFromPrefix(const std::string& fname, const rocksdb::EnvOptions& options,
      uint64_t keyID, uint64_t initialCounter, const rocksdb::Slice& iv, const rocksdb::Slice& prefix, std::unique_ptr<rocksdb::BlockAccessCipherStream>* result);
};

#endif // ROACHLIB_ENCRYPTION_H
