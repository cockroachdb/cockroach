// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <rocksdb/env.h>
#include <vector>
#include "../testutils.h"
#include "key_manager.h"

TEST(FileKeyManager, ReadKeyFiles) {
  // Use a memenv for testing.
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  // Write a few keys.
  struct KeyFiles {
    std::string filename;
    std::string contents;
  };

  std::vector<KeyFiles> keys = {
      {"empty.key", ""},
      {"8.key", "12345678"},
      {"16.key", "1234567890123456"},
      {"24.key", "123456789012345678901234"},
      {"32.key", "12345678901234567890123456789012"},
      {"64.key", "1234567890123456789012345678901234567890123456789012345678901234"},
  };
  for (auto k = keys.cbegin(); k != keys.cend(); ++k) {
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), k->contents, k->filename));
  }

  struct TestCase {
    std::string new_key;
    std::string old_key;
    std::string error;
  };

  std::vector<TestCase> test_cases = {
      {"", "", ": File not found"},
      {"missing_new.key", "missing_old.key", "missing_new.key: File not found"},
      {"plain", "missing_old.key", "missing_old.key: File not found"},
      {"plain", "plain", ""},
      {"empty.key", "plain", "key in file empty.key has length 0, want 16, 24, or 32"},
      {"8.key", "plain", "key in file 8.key has length 8, want 16, 24, or 32"},
      {"16.key", "plain", ""},
      {"24.key", "plain", ""},
      {"32.key", "plain", ""},
      {"64.key", "plain", "key in file 64.key has length 64, want 16, 24, or 32"},
      {"16.key", "8.key", "key in file 8.key has length 8, want 16, 24, or 32"},
      {"16.key", "32.key", ""},
  };

  for (auto t = test_cases.cbegin(); t != test_cases.cend(); ++t) {
    FileKeyManager fkm(env.get(), t->new_key, t->old_key);
    auto status = fkm.LoadKeys();
    EXPECT_ERR(status, t->error);
  }
}

TEST(FileKeyManager, BuildKeyFromFile) {
  // Use a memenv for testing.
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  // Write a few keys.
  ASSERT_OK(rocksdb::WriteStringToFile(env.get(), "1234567890123456", "16.key"));
  ASSERT_OK(rocksdb::WriteStringToFile(env.get(), "12345678901234567890123456789012", "32.key"));

  // We manually compute the key shas with: `echo -n <key contents> | sha256sum`
  std::string key16_sha = "7a51d064a1a216a692f753fcdab276e4ff201a01d8b66f56d50d4d719fd0dc87";
  std::string key32_sha = "e1b85b27d6bcb05846c18e6a48f118e89f0c0587140de9fb3359f8370d0dba08";

  // Check plain keys.
  FileKeyManager fkm_plain(env.get(), "plain", "plain");
  ASSERT_OK(fkm_plain.LoadKeys());
  auto k = fkm_plain.CurrentKey();
  ASSERT_NE(k, nullptr);
  EXPECT_EQ(k->type, PLAIN);
  EXPECT_EQ(k->id, "");
  EXPECT_EQ(k->key, "");
  EXPECT_EQ(k->source, "plain");

  ASSERT_EQ(fkm_plain.GetKey(""), nullptr);

  FileKeyManager fkm(env.get(), "16.key", "32.key");
  ASSERT_OK(fkm.LoadKeys());
  k = fkm.CurrentKey();
  ASSERT_NE(k, nullptr);
  EXPECT_EQ(k->type, AES);
  EXPECT_EQ(k->id, key16_sha);
  EXPECT_EQ(k->key, "1234567890123456");
  EXPECT_EQ(k->source, "16.key");

  ASSERT_EQ(fkm.GetKey("something"), nullptr);

  k = fkm.GetKey(key16_sha);
  ASSERT_NE(k, nullptr);
  EXPECT_EQ(k->type, AES);
  EXPECT_EQ(k->id, key16_sha);
  EXPECT_EQ(k->key, "1234567890123456");
  EXPECT_EQ(k->source, "16.key");

  k = fkm.GetKey(key32_sha);
  ASSERT_NE(k, nullptr);
  EXPECT_EQ(k->type, AES);
  EXPECT_EQ(k->id, key32_sha);
  EXPECT_EQ(k->key, "12345678901234567890123456789012");
  EXPECT_EQ(k->source, "32.key");
}
