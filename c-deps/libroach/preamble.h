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
};

#endif // ROACHLIB_PREAMBLE_H
