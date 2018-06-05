// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#pragma once

#include <rocksdb/env.h>
#include <string>
#include "ccl/storageccl/engineccl/enginepbccl/key_registry.pb.h"
#include "key_manager.h"

namespace enginepbccl = cockroach::ccl::storageccl::engineccl::enginepbccl;

namespace testutils {

// MakeAES<size>Key creates a SecretKeyObject with a key of the specified size.
// It needs an Env for the current time.
enginepbccl::SecretKey* MakeAES128Key(rocksdb::Env* env);

// MemKeyManager is a simple key manager useful for tests.
// It holds a single key. ie: there is only an active key, no old keys.
class MemKeyManager : public KeyManager {
 public:
  explicit MemKeyManager(enginepbccl::SecretKey* key) : key_(key) {}
  virtual ~MemKeyManager();

  virtual std::unique_ptr<enginepbccl::SecretKey> CurrentKey() override;
  virtual std::unique_ptr<enginepbccl::SecretKey> GetKey(const std::string& id) override;

 private:
  std::unique_ptr<enginepbccl::SecretKey> key_;
};

// TempDirHandler will create a temporary directory at initialization time
// and destroy it and all its contents at destruction time.
class TempDirHandler {
 public:
  // Directory name will be /tmp/tmp-ccl-XXXXXX
  TempDirHandler();
  ~TempDirHandler();

  // Initialize the temp directory. Returns true on success.
  // Must be called and checked before any other uses of this class.
  bool Init();

  // Path takes a file or directory name and returns its full path
  // inside the tmp directory.
  std::string Path(const std::string& subpath);

 private:
  std::string tmp_dir_;
};

}  // namespace testutils
