// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <thread>
#include "../db.h"
#include "../file_registry.h"
#include "../testutils.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "ccl/storageccl/engineccl/enginepbccl/stats.pb.h"
#include "ctr_stream.h"
#include "testutils.h"

using namespace cockroach;

TEST(LibroachCCL, DBOpenHook) {
  DBOptions db_opts;
  db_opts.use_file_registry = false;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHook(nullptr, "", db_opts, nullptr));

  // Try without file registry enabled and bogus options. We should fail
  // because encryption options without file registry is not allowed.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook(nullptr, "", db_opts, nullptr),
             "on-disk version does not support encryption, but we found encryption flags");

  db_opts.use_file_registry = true;
  // Try with file registry but bogus encryption flags.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook(nullptr, "", db_opts, nullptr), "failed to parse extra options");
}

TEST(LibroachCCL, DBOpen) {
  {
    // Empty options: no encryption.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    EXPECT_STREQ(DBOpen(&db, DBSlice(), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STREQ(stats.encryption_status.data, NULL);

    DBClose(db);
  }
  {
    // Encryption enabled.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    // Enable encryption, but plaintext only, that's enough to get stats going.
    cockroach::ccl::baseccl::EncryptionOptions enc_opts;
    enc_opts.set_key_source(cockroach::ccl::baseccl::KeyFiles);
    enc_opts.mutable_key_files()->set_current_key("plain");
    enc_opts.mutable_key_files()->set_old_key("plain");

    std::string tmpstr;
    ASSERT_TRUE(enc_opts.SerializeToString(&tmpstr));
    db_opts.extra_options = ToDBSlice(tmpstr);

    EXPECT_STREQ(DBOpen(&db, DBSlice(), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STRNE(stats.encryption_status.data, NULL);

    // Now parse the status protobuf.
    cockroach::ccl::storageccl::engineccl::enginepbccl::EncryptionStatus enc_status;
    ASSERT_TRUE(
        enc_status.ParseFromArray(stats.encryption_status.data, stats.encryption_status.len));
    EXPECT_STREQ(enc_status.active_store_key().key_id().c_str(), "plain");
    EXPECT_STREQ(enc_status.active_data_key().key_id().c_str(), "plain");

    DBClose(db);
  }
}

TEST(LibroachCCL, EncryptedEnv) {
  // This test creates a standalone encrypted env (as opposed to a full
  // rocksdb instance) and verifies that what goes in comes out.
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  auto key_manager = new MemKeyManager(MakeAES128Key(env.get()));
  auto stream = new CTRCipherStreamCreator(key_manager, enginepb::Data);

  auto file_registry = std::unique_ptr<FileRegistry>(new FileRegistry(env.get(), "/"));
  EXPECT_OK(file_registry->Load());

  std::unique_ptr<rocksdb::Env> encrypted_env(
      rocksdb_utils::NewEncryptedEnv(env.get(), file_registry.get(), stream));

  std::string filename("/foo");
  std::string contents("this is the string stored inside the file!");
  size_t kContentsLength = 42;
  ASSERT_EQ(kContentsLength, contents.size());

  // Write the file.
  EXPECT_OK(
      rocksdb::WriteStringToFile(encrypted_env.get(), contents, filename, false /* should_sync */));

  // Read the file using the mem env (no encryption).
  std::string result_plain;
  EXPECT_OK(rocksdb::ReadFileToString(env.get(), filename, &result_plain));
  EXPECT_STRNE(contents.c_str(), result_plain.c_str());

  // Read the file back using the encrypted env.
  std::string result_encrypted;
  EXPECT_OK(rocksdb::ReadFileToString(encrypted_env.get(), filename, &result_encrypted));
  EXPECT_STREQ(contents.c_str(), result_encrypted.c_str());

  // Open as a random access file.
  std::unique_ptr<rocksdb::RandomAccessFile> file;
  EXPECT_OK(encrypted_env->NewRandomAccessFile(filename, &file, rocksdb::EnvOptions()));

  // Reader thread. Captures all useful variables.
  auto read_file = [&]() {
    //    ThreadArg* arg = reinterpret_cast<ThreadArg*>(p);

    char scratch[kContentsLength];  // needs to be at least len(contents).
    rocksdb::Slice result_read;

    for (int i = 0; i < 100; i++) {
      EXPECT_OK(file->Read(0, kContentsLength, &result_read, scratch));
      EXPECT_EQ(kContentsLength, result_read.size());
      // We need to go through Slice.ToString as .data() does not have a null terminator.
      EXPECT_STREQ(contents.c_str(), result_read.ToString().c_str());
    }
  };

  // Call it once by itself.
  read_file();

  // Run two at the same time. We're not using rocksdb thread utilities as they don't support
  // lambda functions with variable capture, everything has to done through args.
  auto t1 = std::thread(read_file);
  auto t2 = std::thread(read_file);
  t1.join();
  t2.join();
}
