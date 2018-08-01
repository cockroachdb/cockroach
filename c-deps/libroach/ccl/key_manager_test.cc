// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <rocksdb/env.h>
#include <vector>
#include "../fmt.h"
#include "../testutils.h"
#include "crypto_utils.h"
#include "key_manager.h"

// A few fixed keys and simple IDs (the first 16 bytes).
// key_file is the file contents for store keys: includes a key ID and key.
// key is the key.
// key_id is the ID (Hex of the first part of key_file)
const std::string key_file_128 = "111111111111111111111111111111111234567890123456";
const std::string key_file_192 = "22222222222222222222222222222222123456789012345678901234";
const std::string key_file_256 = "3333333333333333333333333333333312345678901234567890123456789012";
const std::string key_128 = "1234567890123456";
const std::string key_192 = "123456789012345678901234";
const std::string key_256 = "12345678901234567890123456789012";

// Hex of the binary value of the first kKeyIDLength of key_file.
const std::string key_id_128 = "3131313131313131313131313131313131313131313131313131313131313131";
const std::string key_id_192 = "3232323232323232323232323232323232323232323232323232323232323232";
const std::string key_id_256 = "3333333333333333333333333333333333333333333333333333333333333333";

TEST(FileKeyManager, ReadKeyFiles) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  // Write a few keys.
  struct KeyFiles {
    std::string filename;
    std::string contents;
  };

  std::vector<KeyFiles> keys = {
      {"empty.key", ""},
      {"noid_8.key", "12345678"},
      {"noid_16.key", "1234567890123456"},
      {"noid_24.key", "123456789012345678901234"},
      {"noid_32.key", "12345678901234567890123456789012"},
      {"16.key", key_file_128},
      {"24.key", key_file_192},
      {"32.key", key_file_256},
  };
  for (auto k : keys) {
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), k.contents, k.filename));
  }

  struct TestCase {
    std::string active_file;
    std::string old_file;
    std::string error;
  };

  std::vector<TestCase> test_cases = {
      {"", "", ": File not found"},
      {"missing_new.key", "missing_old.key", "missing_new.key: File not found"},
      {"plain", "missing_old.key", "missing_old.key: File not found"},
      {"plain", "plain", ""},
      {"empty.key", "plain",
       R"(file empty.key is 0 bytes long, it must be <key ID length \(32\)> \+ <key size \(16, 24, or 32\)> long)"},
      {"noid_8.key", "plain", "file noid_8.key is 8 bytes long, it must be .*"},
      {"noid_16.key", "plain", "file noid_16.key is 16 bytes long, it must be .*"},
      {"noid_24.key", "plain", "file noid_24.key is 24 bytes long, it must be .*"},
      {"noid_32.key", "plain", "file noid_32.key is 32 bytes long, it must be .*"},
      {"16.key", "plain", ""},
      {"24.key", "plain", ""},
      {"32.key", "plain", ""},
      {"16.key", "noid_8.key", "file noid_8.key is 8 bytes long, it must be .*"},
      {"16.key", "32.key", ""},
  };

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    FileKeyManager fkm(env.get(), nullptr, t.active_file, t.old_file);
    auto status = fkm.LoadKeys();
    EXPECT_ERR(status, t.error);
  }
}

struct testKey {
  std::string id;
  std::string key;
  enginepbccl::EncryptionType type;
  int64_t approximate_timestamp;  // We check that the creation time is within 5s of this timestamp.
  std::string source;
  bool was_exposed;
  std::string parent_key_id;

  // Constructors with various fields specified.
  testKey() : testKey("") {}
  // Copy constructor.
  testKey(const testKey& k)
      : testKey(k.id, k.key, k.type, k.approximate_timestamp, k.source, k.was_exposed,
                k.parent_key_id) {}
  // Key ID only (to test key existence).
  testKey(std::string id) : testKey(id, "", enginepbccl::Plaintext, 0, "") {}
  testKey(std::string id, std::string key, enginepbccl::EncryptionType type, int64_t timestamp,
          std::string source)
      : testKey(id, key, type, timestamp, source, false, "") {}
  testKey(std::string id, std::string key, enginepbccl::EncryptionType type, int64_t timestamp,
          std::string source, bool exposed, std::string parent)
      : id(id),
        key(key),
        type(type),
        approximate_timestamp(timestamp),
        source(source),
        was_exposed(exposed),
        parent_key_id(parent) {}
};

enginepbccl::KeyInfo keyInfoFromTestKey(const testKey& k) {
  enginepbccl::KeyInfo ki;
  ki.set_encryption_type(k.type);
  ki.set_key_id(k.id);
  ki.set_creation_time(k.approximate_timestamp);
  ki.set_source(k.source);
  return ki;
}

enginepbccl::SecretKey secretKeyFromTestKey(const testKey& k) {
  enginepbccl::SecretKey sk;
  sk.set_key(k.key);
  sk.set_allocated_info(new enginepbccl::KeyInfo(keyInfoFromTestKey(k)));
  return sk;
}

rocksdb::Status compareNonRandomKeyInfo(const enginepbccl::KeyInfo& actual,
                                        const testKey& expected) {
  if (actual.encryption_type() != expected.type) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
        "actual type %d does not match expected type %d", actual.encryption_type(), expected.type));
  }

  auto diff = actual.creation_time() - expected.approximate_timestamp;
  if (diff > 5 || diff < -5) {
    return rocksdb::Status::InvalidArgument(fmt::StringPrintf(
        "actual creation time %" PRId64 " does not match expected timestamp %" PRId64,
        actual.creation_time(), expected.approximate_timestamp));
  }

  if (actual.source() != expected.source) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("actual source \"%s\" does not match expected source \"%s\"",
                          actual.source().c_str(), expected.source.c_str()));
  }

  if (actual.was_exposed() != expected.was_exposed) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("actual was_exposed %d does not match expected was_exposed %d",
                          actual.was_exposed(), expected.was_exposed));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status compareKeyInfo(const enginepbccl::KeyInfo& actual, const testKey& expected) {
  auto status = compareNonRandomKeyInfo(actual, expected);
  if (!status.ok()) {
    return status;
  }

  if (actual.key_id() != expected.id) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("actual key_id \"%s\" does not match expected key_id \"%s\"",
                          actual.key_id().c_str(), expected.id.c_str()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status compareKey(const enginepbccl::SecretKey& actual, const testKey& expected) {
  if (actual.key() != expected.key) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("actual key \"%s\" does not match expected key \"%s\"",
                          actual.key().c_str(), expected.key.c_str()));
  }
  return compareKeyInfo(actual.info(), expected);
}

void getInfoFromVec(std::vector<testKey> infos, const std::string& id, testKey* out) {
  for (auto i : infos) {
    if (i.id == id) {
      *out = i;
      return;
    }
  }
  FAIL() << "key id " << id << " not found in info vector";
}

TEST(FileKeyManager, BuildKeyFromFile) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  // Write a few keys.
  ASSERT_OK(rocksdb::WriteStringToFile(env.get(), key_file_128, "16.key"));
  ASSERT_OK(rocksdb::WriteStringToFile(env.get(), key_file_192, "24.key"));
  ASSERT_OK(rocksdb::WriteStringToFile(env.get(), key_file_256, "32.key"));

  int64_t now;
  ASSERT_OK(env->GetCurrentTime(&now));
  struct TestCase {
    std::string active_file;
    std::string old_file;
    std::string active_id;
    std::vector<testKey> keys;
  };

  std::vector<TestCase> test_cases = {
      {"plain", "plain", "plain", {testKey("plain", "", enginepbccl::Plaintext, now, "plain")}},
      {"16.key",
       "32.key",
       key_id_128,
       {testKey(key_id_128, key_128, enginepbccl::AES128_CTR, now, "16.key"),
        testKey(key_id_256, key_256, enginepbccl::AES256_CTR, now, "32.key")}},
      {
          "24.key",
          "plain",
          key_id_192,
          {testKey(key_id_192, key_192, enginepbccl::AES192_CTR, now, "24.key"),
           testKey("plain", "", enginepbccl::Plaintext, now, "plain")},
      },
  };

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    FileKeyManager fkm(env.get(), nullptr, t.active_file, t.old_file);
    ASSERT_OK(fkm.LoadKeys());

    testKey ki;
    getInfoFromVec(t.keys, t.active_id, &ki);

    auto secret_key = fkm.CurrentKey();
    EXPECT_EQ(secret_key->key(), ki.key);
    EXPECT_OK(compareKeyInfo(secret_key->info(), ki));

    auto key_info = fkm.CurrentKeyInfo();
    EXPECT_OK(compareKeyInfo(*key_info, ki));

    for (auto ki_iter : t.keys) {
      if (ki_iter.id == "") {
        continue;
      }
      secret_key = fkm.GetKey(ki_iter.id);
      ASSERT_NE(secret_key, nullptr);
      EXPECT_EQ(secret_key->key(), ki_iter.key);
      EXPECT_OK(compareKeyInfo(secret_key->info(), ki_iter));

      key_info = fkm.GetKeyInfo(ki_iter.id);
      ASSERT_NE(key_info, nullptr);
      EXPECT_OK(compareKeyInfo(*key_info, ki_iter));
    }
  }
}

TEST(DataKeyManager, LoadKeys) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  const std::string registry_path = "/" + kKeyRegistryFilename;

  // Test a missing file first.
  {
    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_OK(dkm.LoadKeys());
    ASSERT_EQ(dkm.CurrentKey(), nullptr);
  }

  // Now a file with random data.
  {
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), "blah blah", registry_path));
    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_ERR(dkm.LoadKeys(), "failed to parse key registry " + registry_path);
    ASSERT_OK(env->DeleteFile(registry_path));
  }

  // Empty file.
  {
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), "", registry_path));
    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_OK(dkm.LoadKeys());
    ASSERT_OK(env->DeleteFile(registry_path));
  }

  // Empty protobuf.
  {
    enginepbccl::DataKeysRegistry registry;
    std::string contents;
    ASSERT_TRUE(registry.SerializeToString(&contents));
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), contents, registry_path));
    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_OK(dkm.LoadKeys());
    ASSERT_OK(env->DeleteFile(registry_path));
  }

  // Active store key not found.
  {
    enginepbccl::DataKeysRegistry registry;
    registry.set_active_store_key_id("foobar");

    std::string contents;
    ASSERT_TRUE(registry.SerializeToString(&contents));
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), contents, registry_path));

    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_ERR(dkm.LoadKeys(), "active store key foobar not found");
    ASSERT_OK(env->DeleteFile(registry_path));
  }

  // Active data key not found.
  {
    enginepbccl::DataKeysRegistry registry;
    registry.set_active_data_key_id("foobar");

    std::string contents;
    ASSERT_TRUE(registry.SerializeToString(&contents));
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), contents, registry_path));

    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_ERR(dkm.LoadKeys(), "active data key foobar not found");
    ASSERT_OK(env->DeleteFile(registry_path));
  }

  // Both active keys exist.
  {
    enginepbccl::DataKeysRegistry registry;
    (*registry.mutable_store_keys())["foo"] = keyInfoFromTestKey(testKey("foo"));
    (*registry.mutable_store_keys())["bar"] = keyInfoFromTestKey(testKey("bar"));
    registry.set_active_store_key_id("foo");

    testKey foo2 = testKey("foo2");
    (*registry.mutable_data_keys())["foo2"] = secretKeyFromTestKey(foo2);
    testKey bar2 = testKey("bar2");
    (*registry.mutable_data_keys())["bar2"] = secretKeyFromTestKey(bar2);
    registry.set_active_data_key_id("bar2");

    std::string contents;
    ASSERT_TRUE(registry.SerializeToString(&contents));
    ASSERT_OK(rocksdb::WriteStringToFile(env.get(), contents, registry_path));
    DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
    EXPECT_OK(dkm.LoadKeys());

    auto k = dkm.CurrentKey();
    ASSERT_NE(k, nullptr);
    EXPECT_OK(compareKeyInfo(k->info(), bar2));

    auto ki = dkm.CurrentKeyInfo();
    ASSERT_NE(ki, nullptr);
    EXPECT_OK(compareKeyInfo(*ki, bar2));

    k = dkm.GetKey("foo2");
    ASSERT_NE(k, nullptr);
    EXPECT_OK(compareKeyInfo(k->info(), foo2));

    ki = dkm.GetKeyInfo("foo2");
    ASSERT_NE(ki, nullptr);
    EXPECT_OK(compareKeyInfo(*ki, foo2));

    ASSERT_OK(env->DeleteFile(registry_path));
  }
}

TEST(DataKeyManager, KeyFromKeyInfo) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  int64_t now;
  ASSERT_OK(env->GetCurrentTime(&now));

  struct TestCase {
    testKey store_info;
    std::string error;
    testKey new_key;
  };

  std::vector<TestCase> test_cases = {
      {testKey("bad", "", enginepbccl::EncryptionType_INT_MIN_SENTINEL_DO_NOT_USE_, 0, ""),
       "unknown encryption type .* for key ID bad",
       {}},
      {testKey("plain", "", enginepbccl::Plaintext, now, "plain"), "",
       testKey("plain", "", enginepbccl::Plaintext, now, "data key manager", true, "plain")},
      {testKey(key_id_128, "", enginepbccl::AES128_CTR, now - 86400, "128.key"), "",
       testKey("someid", "somekey", enginepbccl::AES128_CTR, now, "data key manager", false,
               key_id_128)},
      {testKey(key_id_192, "", enginepbccl::AES192_CTR, now + 86400, "192.key"), "",
       testKey("someid", "somekey", enginepbccl::AES192_CTR, now, "data key manager", false,
               key_id_192)},
      {testKey(key_id_256, "", enginepbccl::AES256_CTR, 0, "256.key"), "",
       testKey("someid", "somekey", enginepbccl::AES256_CTR, now, "data key manager", false,
               key_id_256)},
  };

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    enginepbccl::SecretKey new_key;
    auto status =
        KeyManagerUtils::KeyFromKeyInfo(env.get(), keyInfoFromTestKey(t.store_info), &new_key);
    EXPECT_ERR(status, t.error);
    if (!status.ok()) {
      continue;
    }

    // Check only the non-random bits (everything but key and ID).
    EXPECT_OK(compareNonRandomKeyInfo(new_key.info(), t.new_key));

    size_t expected_length;
    switch (t.new_key.type) {
    case enginepbccl::Plaintext:
      expected_length = 0;
      break;
    case enginepbccl::AES128_CTR:
      expected_length = 16;
      break;
    case enginepbccl::AES192_CTR:
      expected_length = 24;
      break;
    case enginepbccl::AES256_CTR:
      expected_length = 32;
      break;
    default:
      FAIL() << "unknown encryption type: " << t.new_key.type;
    }

    EXPECT_EQ(new_key.key().size(), expected_length);
    if (t.new_key.type == enginepbccl::Plaintext) {
      EXPECT_EQ(new_key.key(), "");
    } else {
      EXPECT_EQ(new_key.info().key_id().size(), kKeyIDLength * 2 /* Hex doubles the size) */);
    }
  }
}

TEST(DataKeyManager, SetStoreKey) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  int64_t now;
  ASSERT_OK(env->GetCurrentTime(&now));

  struct TestCase {
    testKey store_info;
    std::string error;
    testKey active_key;
  };

  // Test cases are incremental: the key manager is not reset for each key.
  // If error is empty, the active data key should match the "active key".
  // The active key is then added to the list of keys.
  std::vector<testKey> active_keys;
  std::vector<TestCase> test_cases = {
      {
          testKey("plain", "", enginepbccl::Plaintext, now, "plain"),
          "",
          testKey("plain", "", enginepbccl::Plaintext, now, "data key manager", true, "plain"),
      },
      {
          testKey(key_id_128, "", enginepbccl::AES128_CTR, now - 86400, "128.key"),
          "",
          testKey(key_id_128, "", enginepbccl::AES128_CTR, now, "data key manager", false,
                  key_id_128),
      },
      {
          // Setting the same store key does nothing.
          testKey(key_id_128, "", enginepbccl::AES128_CTR, now - 86400, "128.key"),
          "",
          // We add it again because we don't have a "skip" thing. It's find if it's there twice.
          testKey(key_id_128, "", enginepbccl::AES128_CTR, now, "data key manager", false,
                  key_id_128),
      },
      {
          testKey(key_id_256, "", enginepbccl::AES256_CTR, now - 86400, "256.key"),
          "",
          testKey(key_id_256, "", enginepbccl::AES256_CTR, now, "data key manager", false,
                  key_id_256),
      },
      {
          testKey(key_id_128, "", enginepbccl::AES128_CTR, now - 86400, "128.key"),
          fmt::StringPrintf("new active store key ID %s already exists as an inactive key. This is "
                            "really dangerous.",
                            key_id_128.c_str()),
          {},
      },
      {
          // Switch to plain: this marks all keys as exposed.
          testKey("plain", "", enginepbccl::Plaintext, now, "plain"),
          "",
          testKey("plain", "", enginepbccl::Plaintext, now, "data key manager", true, "plain"),
      }};

  DataKeyManager dkm(env.get(), nullptr, "", 0, false /* read-only */);
  ASSERT_OK(dkm.LoadKeys());

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    // Set new active store key.
    auto store_info = std::unique_ptr<enginepbccl::KeyInfo>(
        new enginepbccl::KeyInfo(keyInfoFromTestKey(t.store_info)));
    auto status = dkm.SetActiveStoreKeyInfo(std::move(store_info));
    EXPECT_ERR(status, t.error);
    if (status.ok()) {
      // New key generated. Check active data key.
      auto active_info = dkm.CurrentKey();
      ASSERT_NE(active_info, nullptr);
      EXPECT_OK(compareNonRandomKeyInfo(active_info->info(), t.active_key));

      if (t.store_info.type == enginepbccl::Plaintext) {
        // Plaintext store info: mark all existing keys as exposed.
        for (auto ki_iter = active_keys.begin(); ki_iter != active_keys.end(); ++ki_iter) {
          ki_iter->was_exposed = true;
        }
      }

      // Insert new key with filled-in ID and key.
      auto new_key = testKey(t.active_key);
      new_key.id = active_info->info().key_id();
      new_key.key = active_info->key();
      active_keys.push_back(new_key);
    }

    // Check all expected keys.
    for (auto ki_iter : active_keys) {
      auto ki = dkm.GetKey(ki_iter.id);
      ASSERT_NE(ki, nullptr);
      EXPECT_OK(compareKey(*ki, ki_iter));
    }

    // Initialize a new data key manager to load the file.
    DataKeyManager tmp_dkm(env.get(), nullptr, "", 0, false /* read-only */);
    ASSERT_OK(tmp_dkm.LoadKeys());

    if (status.ok()) {
      // Check active data key.
      auto active_info = tmp_dkm.CurrentKey();
      ASSERT_NE(active_info, nullptr);
      EXPECT_OK(compareNonRandomKeyInfo(active_info->info(), t.active_key));
    }

    // Check all expected keys.
    for (auto ki_iter : active_keys) {
      auto ki = tmp_dkm.GetKey(ki_iter.id);
      ASSERT_NE(ki, nullptr);
      EXPECT_OK(compareKey(*ki, ki_iter));
    }
  }
}

TEST(DataKeyManager, RotateKeyAtStartup) {
  // MemEnv returns a MockEnv, but use `new mockEnv` to access FakeSleepForMicroseconds.
  // We need to wrap it around a memenv for memory files.
  std::unique_ptr<rocksdb::Env> memenv(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  std::unique_ptr<testutils::FakeTimeEnv> env(new testutils::FakeTimeEnv(memenv.get()));

  struct TestCase {
    testKey store_info;    // Active store key info.
    int64_t current_time;  // We update the fake env time before the call to SetActiveStoreKeyInfo.
    testKey active_key;    // We call compareNonRandomKeyInfo against the active data key.
  };

  // Test cases are incremental: the key manager is not reset for each key.
  std::vector<TestCase> test_cases = {
      {
          testKey("plain", "", enginepbccl::Plaintext, 0, "plain"),
          0,
          testKey("plain", "", enginepbccl::Plaintext, 0, "data key manager", true, "plain"),
      },
      {
          // Plain keys are not rotated.
          testKey("plain", "", enginepbccl::Plaintext, 0, "plain"),
          20,
          testKey("plain", "", enginepbccl::Plaintext, 0, "data key manager", true, "plain"),
      },
      {
          // New key on store key change.
          testKey(key_id_128, "", enginepbccl::AES128_CTR, 0, "128.key"),
          20,
          testKey("not checked", "not checked", enginepbccl::AES128_CTR, 20, "data key manager",
                  false, key_id_128),
      },
      {
          // Less than 10 seconds: no change.
          testKey(key_id_128, "", enginepbccl::AES128_CTR, 0, "128.key"),
          29,
          testKey("not checked", "not checked", enginepbccl::AES128_CTR, 20, "data key manager",
                  false, key_id_128),
      },
      {
          // Exactly 10 seconds: new key.
          testKey(key_id_128, "", enginepbccl::AES128_CTR, 0, "128.key"),
          30,
          testKey("not checked", "not checked", enginepbccl::AES128_CTR, 30, "data key manager",
                  false, key_id_128),
      },
  };

  DataKeyManager dkm(env.get(), nullptr, "", 10 /* 10 second rotation period */,
                     false /* read-only */);
  ASSERT_OK(dkm.LoadKeys());

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));
    env->SetCurrentTime(t.current_time);

    // Set new active store key.
    auto store_info = std::unique_ptr<enginepbccl::KeyInfo>(
        new enginepbccl::KeyInfo(keyInfoFromTestKey(t.store_info)));
    auto status = dkm.SetActiveStoreKeyInfo(std::move(store_info));
    ASSERT_OK(status);

    auto active_info = dkm.CurrentKey();
    ASSERT_NE(active_info, nullptr);
    EXPECT_OK(compareNonRandomKeyInfo(active_info->info(), t.active_key));
  }

  {
    // Now try a read-only data key manager.
    DataKeyManager ro_dkm(env.get(), nullptr, "", 10, true);
    ASSERT_OK(ro_dkm.LoadKeys());
    // Verify that the key matches the last test case.
    auto tc = test_cases.back();
    auto active_info = ro_dkm.CurrentKey();
    ASSERT_NE(active_info, nullptr);
    EXPECT_OK(compareNonRandomKeyInfo(active_info->info(), tc.active_key));

    // Increase time, check rotation failure, and check key again.
    env->SetCurrentTime(tc.current_time + 100);
    auto store_info = std::unique_ptr<enginepbccl::KeyInfo>(
        new enginepbccl::KeyInfo(keyInfoFromTestKey(tc.store_info)));
    EXPECT_ERR(ro_dkm.SetActiveStoreKeyInfo(std::move(store_info)),
               "key manager is read-only, keys cannot be rotated");
    active_info = ro_dkm.CurrentKey();
    ASSERT_NE(active_info, nullptr);
    EXPECT_OK(compareNonRandomKeyInfo(active_info->info(), tc.active_key));
  }
}

TEST(DataKeyManager, RotateKeyWhileRunning) {
  // MemEnv returns a MockEnv, but use `new mockEnv` to access FakeSleepForMicroseconds.
  // We need to wrap it around a memenv for memory files.
  std::unique_ptr<rocksdb::Env> memenv(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  std::unique_ptr<testutils::FakeTimeEnv> env(new testutils::FakeTimeEnv(memenv.get()));

  DataKeyManager dkm(env.get(), nullptr, "", 10 /* 10 second rotation period */,
                     false /* read-only */);
  ASSERT_OK(dkm.LoadKeys());

  // Set new active store key.
  auto plain_key = testKey("plain", "", enginepbccl::Plaintext, 0, "plain");
  auto plain_info = std::unique_ptr<enginepbccl::KeyInfo>(
      new enginepbccl::KeyInfo(keyInfoFromTestKey(plain_key)));
  ASSERT_OK(dkm.SetActiveStoreKeyInfo(std::move(plain_info)));

  auto key1 = dkm.CurrentKeyInfo();
  EXPECT_EQ(plain_key.id, key1->key_id());

  // Try as we might, we don't rotate plaintext.
  env->IncCurrentTime(60);
  auto key2 = dkm.CurrentKeyInfo();
  EXPECT_EQ(key1->key_id(), key2->key_id());
  EXPECT_EQ(key1->creation_time(), key2->creation_time());

  // Switch to actual encryption.
  auto aes_key = testKey(key_id_128, "", enginepbccl::AES128_CTR, 0, "128.key");
  auto aes_info =
      std::unique_ptr<enginepbccl::KeyInfo>(new enginepbccl::KeyInfo(keyInfoFromTestKey(aes_key)));
  ASSERT_OK(dkm.SetActiveStoreKeyInfo(std::move(aes_info)));

  auto aes_key1 = dkm.CurrentKeyInfo();
  EXPECT_EQ(aes_key.id, aes_key1->parent_key_id());
  EXPECT_EQ(aes_key1->encryption_type(), enginepbccl::AES128_CTR);

  // Let's grab the key a few times.
  for (int i = 0; i < 9; i++) {
    env->IncCurrentTime(1);
    auto aes_key2 = dkm.CurrentKeyInfo();
    EXPECT_EQ(aes_key1->key_id(), aes_key2->key_id());
    EXPECT_EQ(aes_key1->creation_time(), aes_key2->creation_time());
    EXPECT_EQ(aes_key2->encryption_type(), enginepbccl::AES128_CTR);
  }

  // Go over the 10s lifetime.
  env->IncCurrentTime(2);
  auto aes_key2 = dkm.CurrentKeyInfo();
  EXPECT_NE(aes_key1->key_id(), aes_key2->key_id());
  EXPECT_GT(aes_key2->creation_time(), aes_key1->creation_time());
  EXPECT_EQ(aes_key2->encryption_type(), enginepbccl::AES128_CTR);

  // And again.
  env->IncCurrentTime(11);
  auto aes_key3 = dkm.CurrentKeyInfo();
  EXPECT_NE(aes_key3->key_id(), aes_key2->key_id());
  EXPECT_NE(aes_key3->key_id(), aes_key1->key_id());
  EXPECT_GT(aes_key3->creation_time(), aes_key2->creation_time());
  EXPECT_EQ(aes_key3->encryption_type(), enginepbccl::AES128_CTR);
}
