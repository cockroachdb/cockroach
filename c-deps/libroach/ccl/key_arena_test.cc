// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "../fmt.h"
#include "../testutils.h"
#include "key_arena.h"

#include "ccl/storageccl/engineccl/enginepbccl/key_registry.pb.h"
namespace enginepbccl = cockroach::ccl::storageccl::engineccl::enginepbccl;

bool IsZeroMemory(const char* addr, size_t size) {
  if (addr[0] != 0) {
    return false;
  }
  return (memcmp(addr, addr + 1, size - 1) == 0);
}

TEST(KeyArena, TestPageSupport) { ASSERT_OK(CanLockPages()); }

TEST(KeyArena, TestArenaAllocation) {
  google::protobuf::Arena* arena = NewKeyArena();

  // Allocate a few different types of objects.
  // SecretKey: the one we really care about.
  enginepbccl::SecretKey* secret_key =
      google::protobuf::Arena::CreateMessage<enginepbccl::SecretKey>(arena);
  EXPECT_TRUE(IsLocked((size_t)secret_key, 1));

  // KeyInfo: same proto file so also available for arena allocation.
  enginepbccl::KeyInfo* key_info =
      google::protobuf::Arena::CreateMessage<enginepbccl::KeyInfo>(arena);
  EXPECT_TRUE(IsLocked((size_t)key_info, 1));

  // DataKeysRegistry: contains SecretKey objects.
  enginepbccl::DataKeysRegistry* key_registry =
      google::protobuf::Arena::CreateMessage<enginepbccl::DataKeysRegistry>(arena);
  EXPECT_TRUE(IsLocked((size_t)key_registry, 1));

  // Char array.
  char* char_array = google::protobuf::Arena::CreateArray<char>(arena, 128);
  EXPECT_TRUE(IsLocked((size_t)char_array, 128));

  // Multiple pages.
  const size_t size = 16 << 10;
  char* large_char_array = google::protobuf::Arena::CreateArray<char>(arena, size);
  ASSERT_NE(large_char_array, nullptr);
  EXPECT_TRUE(IsLocked((size_t)large_char_array, size));

  delete arena;

  // Verify that all previous areas of memory are unlocked.
  EXPECT_FALSE(IsLocked((size_t)secret_key, 1));
  EXPECT_FALSE(IsLocked((size_t)key_info, 1));
  EXPECT_FALSE(IsLocked((size_t)key_registry, 1));
  EXPECT_FALSE(IsLocked((size_t)char_array, 128));
  EXPECT_FALSE(IsLocked((size_t)large_char_array, size));
}

TEST(KeyArena, TestSecretKeyLocking) {
  const std::string key("foo bar");

  // Create a SecretKey without an arena.
  enginepbccl::SecretKey* secret_key =
      google::protobuf::Arena::CreateMessage<enginepbccl::SecretKey>(nullptr);
  secret_key->mutable_key()->append(key);

  const char* raw_key = secret_key->mutable_key()->data();
  // Raw contents match and memory page is NOT locked.
  EXPECT_FALSE(IsLocked((size_t)secret_key->mutable_key()->data(), 1));
  EXPECT_EQ(0, strncmp(raw_key, key.c_str(), key.size()));

  // Delete string. Raw contents still match.
  delete secret_key;
  EXPECT_FALSE(IsLocked((size_t)raw_key, key.size()));
  EXPECT_EQ(0, strncmp(raw_key, key.c_str(), key.size()));

  // Now create one using an arena.
  google::protobuf::Arena* arena = NewKeyArena();

  secret_key = google::protobuf::Arena::CreateMessage<enginepbccl::SecretKey>(arena);
  // Some random contents:
  secret_key->mutable_key()->append(key);

  // Check contents and that the key bytes are locked.
  raw_key = &(secret_key->mutable_key()->data()[0]);
  EXPECT_STREQ(raw_key, key.c_str());
  EXPECT_TRUE(IsLocked((size_t)raw_key, key.size()));

  // Delete arena. Raw contents are overwritten and page is no longer locked.
  delete arena;
  EXPECT_FALSE(IsLocked((size_t)raw_key, key.size()));
  EXPECT_TRUE(IsZeroMemory(raw_key, key.size()));
}
