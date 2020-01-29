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
#include "../utils.h"
#include "ccl/baseccl/encryption_options.pb.h"
#include "ccl/storageccl/engineccl/enginepbccl/stats.pb.h"
#include "ctr_stream.h"
#include "db.h"
#include "testutils.h"

using namespace cockroach;
using namespace testutils;

namespace enginepbccl = cockroach::ccl::storageccl::engineccl::enginepbccl;

#include <libroach.h>
#include "db.h"
class CCLTest : public testing::Test {
 protected:
  void SetUp() override { DBSetOpenHook((void*)DBOpenHookCCL); }
  void TearDown() override { DBSetOpenHook((void*)DBOpenHookOSS); }
};

TEST_F(CCLTest, DBOpenHook) {
  DBOptions db_opts;
  db_opts.use_file_registry = false;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHookCCL(nullptr, "", db_opts, nullptr));

  // Try without file registry enabled and bogus options. We should fail
  // because encryption options without file registry is not allowed.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHookCCL(nullptr, "", db_opts, nullptr),
             "on-disk version does not support encryption, but we found encryption flags");

  db_opts.use_file_registry = true;
  // Try with file registry but bogus encryption flags.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHookCCL(nullptr, "", db_opts, nullptr), "failed to parse extra options");
}

TEST_F(CCLTest, DBOpen) {
  // Use a real directory, we need to create a file_registry.
  TempDirHandler dir;

  {
    // Empty options: no encryption.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STREQ(stats.encryption_status.data, NULL);
    EXPECT_EQ(stats.encryption_type, enginepbccl::Plaintext);
    EXPECT_EQ(stats.total_files, 0);
    EXPECT_EQ(stats.total_bytes, 0);
    EXPECT_EQ(stats.active_key_files, 0);
    EXPECT_EQ(stats.active_key_bytes, 0);

    DBClose(db);
  }
  {
    // No options but file registry exists.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    // Create bogus file registry.
    ASSERT_OK(rocksdb::WriteStringToFile(rocksdb::Env::Default(), "",
                                         dir.Path(kFileRegistryFilename), true));

    auto ret = DBOpen(&db, ToDBSlice(dir.Path("")), db_opts);
    EXPECT_STREQ(std::string(ret.data, ret.len).c_str(),
                 "Invalid argument: encryption was used on this store before, but no encryption "
                 "flags specified. You need a CCL build and must fully specify the "
                 "--enterprise-encryption flag");
    free(ret.data);
    ASSERT_OK(rocksdb::Env::Default()->DeleteFile(dir.Path(kFileRegistryFilename)));
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

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STRNE(stats.encryption_status.data, NULL);
    EXPECT_EQ(stats.encryption_type, enginepbccl::Plaintext);

    // Now parse the status protobuf.
    enginepbccl::EncryptionStatus enc_status;
    ASSERT_TRUE(
        enc_status.ParseFromArray(stats.encryption_status.data, stats.encryption_status.len));
    free(stats.encryption_status.data);
    EXPECT_STREQ(enc_status.active_store_key().key_id().c_str(), "plain");
    EXPECT_STREQ(enc_status.active_data_key().key_id().c_str(), "plain");

    // Make sure the file/bytes stats are non-zero and all marked as using the active key.
    EXPECT_NE(stats.total_files, 0);
    EXPECT_NE(stats.total_bytes, 0);
    EXPECT_NE(stats.active_key_files, 0);
    EXPECT_NE(stats.active_key_bytes, 0);

    EXPECT_EQ(stats.total_files, stats.active_key_files);
    EXPECT_EQ(stats.total_bytes, stats.active_key_bytes);

    DBClose(db);
  }
}

TEST_F(CCLTest, ReadOnly) {
  // We need a real directory.
  TempDirHandler dir;

  {
    // Write/read a single key.
    DBEngine* db;
    DBOptions db_opts = defaultDBOptions();

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    EXPECT_STREQ(DBPut(db, ToDBKey("foo"), ToDBSlice("foo's value")).data, NULL);
    DBString value;
    EXPECT_STREQ(DBGet(db, ToDBKey("foo"), &value).data, NULL);
    EXPECT_STREQ(ToString(value).c_str(), "foo's value");
    free(value.data);

    DBClose(db);
  }

  {
    // Re-open read-only without encryption options.
    DBEngine* db;
    DBOptions db_opts = defaultDBOptions();
    db_opts.read_only = true;

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    // Read the previously-written key.
    DBString ro_value;
    EXPECT_STREQ(DBGet(db, ToDBKey("foo"), &ro_value).data, NULL);
    EXPECT_STREQ(ToString(ro_value).c_str(), "foo's value");
    free(ro_value.data);
    // Try to write it again.
    auto ret = DBPut(db, ToDBKey("foo"), ToDBSlice("foo's value"));
    EXPECT_EQ(ToString(ret), "Not implemented: Not supported operation in read only mode.");
    free(ret.data);

    DBClose(db);
  }

  {
    // Re-open read-only with encryption options (plaintext-only).
    DBEngine* db;
    DBOptions db_opts = defaultDBOptions();
    db_opts.read_only = true;
    db_opts.use_file_registry = true;
    auto extra_opts = MakePlaintextExtraOptions();
    ASSERT_NE(extra_opts, "");
    db_opts.extra_options = ToDBSlice(extra_opts);

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    // Read the previously-written key.
    DBString ro_value;
    EXPECT_STREQ(DBGet(db, ToDBKey("foo"), &ro_value).data, NULL);
    EXPECT_STREQ(ToString(ro_value).c_str(), "foo's value");
    free(ro_value.data);
    // Try to write it again.
    auto ret = DBPut(db, ToDBKey("foo"), ToDBSlice("foo's value"));
    EXPECT_EQ(ToString(ret), "Not implemented: Not supported operation in read only mode.");
    free(ret.data);

    DBClose(db);
  }
}

TEST_F(CCLTest, EncryptionStats) {
  // We need a real directory.
  TempDirHandler dir;

  // Write a key.
  ASSERT_OK(WriteAES128KeyFile(rocksdb::Env::Default(), dir.Path("aes-128.key")));

  {
    // Encryption options specified, but plaintext.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    cockroach::ccl::baseccl::EncryptionOptions enc_opts;
    enc_opts.set_key_source(cockroach::ccl::baseccl::KeyFiles);
    enc_opts.mutable_key_files()->set_current_key("plain");
    enc_opts.mutable_key_files()->set_old_key("plain");

    std::string tmpstr;
    ASSERT_TRUE(enc_opts.SerializeToString(&tmpstr));
    db_opts.extra_options = ToDBSlice(tmpstr);

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STRNE(stats.encryption_status.data, NULL);
    EXPECT_EQ(stats.encryption_type, enginepbccl::Plaintext);

    // Write a key.
    EXPECT_STREQ(DBPut(db, ToDBKey("foo"), ToDBSlice("foo's value")).data, NULL);
    // Force a compaction.
    ASSERT_EQ(DBCompact(db).data, nullptr);

    // Now parse the status protobuf.
    enginepbccl::EncryptionStatus enc_status;
    ASSERT_TRUE(
        enc_status.ParseFromArray(stats.encryption_status.data, stats.encryption_status.len));
    EXPECT_EQ(enc_status.active_store_key().encryption_type(), enginepbccl::Plaintext);
    EXPECT_EQ(enc_status.active_data_key().encryption_type(), enginepbccl::Plaintext);

    // Make sure the file/bytes stats are non-zero and all marked as using the active key.
    EXPECT_NE(stats.total_files, 0);
    EXPECT_NE(stats.total_bytes, 0);
    EXPECT_NE(stats.active_key_files, 0);
    EXPECT_NE(stats.active_key_bytes, 0);

    EXPECT_EQ(stats.total_files, stats.active_key_files);
    EXPECT_EQ(stats.total_bytes, stats.active_key_bytes);

    // Fetch registries and parse.
    DBEncryptionRegistries result;
    EXPECT_STREQ(DBGetEncryptionRegistries(db, &result).data, NULL);

    enginepbccl::DataKeysRegistry key_registry;
    ASSERT_TRUE(key_registry.ParseFromArray(result.key_registry.data, result.key_registry.len));

    enginepb::FileRegistry file_registry;
    ASSERT_TRUE(file_registry.ParseFromArray(result.file_registry.data, result.file_registry.len));

    // Check some registry contents.
    EXPECT_STREQ(key_registry.active_store_key_id().c_str(), "plain");
    EXPECT_STREQ(key_registry.active_data_key_id().c_str(), "plain");
    EXPECT_GT(key_registry.store_keys().size(), 0);
    EXPECT_GT(key_registry.data_keys().size(), 0);
    EXPECT_GT(file_registry.files().size(), 0);

    DBClose(db);
  }

  {
    // Re-open the DB with AES encryption.
    DBOptions db_opts = defaultDBOptions();
    DBEngine* db;
    db_opts.use_file_registry = true;

    cockroach::ccl::baseccl::EncryptionOptions enc_opts;
    enc_opts.set_key_source(cockroach::ccl::baseccl::KeyFiles);
    enc_opts.set_data_key_rotation_period(3600);
    enc_opts.mutable_key_files()->set_current_key(dir.Path("aes-128.key"));
    enc_opts.mutable_key_files()->set_old_key("plain");

    std::string tmpstr;
    ASSERT_TRUE(enc_opts.SerializeToString(&tmpstr));
    db_opts.extra_options = ToDBSlice(tmpstr);

    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STRNE(stats.encryption_status.data, NULL);
    EXPECT_EQ(stats.encryption_type, enginepbccl::AES128_CTR);

    // Now parse the status protobuf.
    enginepbccl::EncryptionStatus enc_status;
    ASSERT_TRUE(
        enc_status.ParseFromArray(stats.encryption_status.data, stats.encryption_status.len));
    EXPECT_EQ(enc_status.active_store_key().encryption_type(), enginepbccl::AES128_CTR);
    EXPECT_EQ(enc_status.active_data_key().encryption_type(), enginepbccl::AES128_CTR);

    // Make sure the file/bytes stats are non-zero.
    EXPECT_NE(stats.total_files, 0);
    EXPECT_NE(stats.total_bytes, 0);
    EXPECT_NE(stats.active_key_files, 0);
    EXPECT_NE(stats.active_key_bytes, 0);

    // However, we won't be at the total as we have the SST from the plaintext run still around.
    EXPECT_NE(stats.total_files, stats.active_key_files);
    EXPECT_NE(stats.total_bytes, stats.active_key_bytes);

    DBClose(db);

    // Sleep for 1 second. Key creation timestamps are in seconds since epoch.
    ASSERT_EQ(0, sleep(1));

    // Re-open the DB with exactly the same options and grab stats in a separate object.
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats2;
    EXPECT_STREQ(DBGetEnvStats(db, &stats2).data, NULL);
    EXPECT_STRNE(stats2.encryption_status.data, NULL);

    // Now parse the status protobuf.
    enginepbccl::EncryptionStatus enc_status2;
    ASSERT_TRUE(
        enc_status2.ParseFromArray(stats2.encryption_status.data, stats2.encryption_status.len));
    EXPECT_EQ(enc_status2.active_store_key().encryption_type(), enginepbccl::AES128_CTR);
    EXPECT_EQ(enc_status2.active_data_key().encryption_type(), enginepbccl::AES128_CTR);

    // Check timestamp equality with the previous stats, we want to make sure we have
    // the time we first saw the store key, not the second start.
    EXPECT_EQ(enc_status2.active_store_key().creation_time(),
              enc_status.active_store_key().creation_time());

    // Fetch registries and parse.
    DBEncryptionRegistries result;
    EXPECT_STREQ(DBGetEncryptionRegistries(db, &result).data, NULL);

    enginepbccl::DataKeysRegistry key_registry;
    ASSERT_TRUE(key_registry.ParseFromArray(result.key_registry.data, result.key_registry.len));
    free(result.key_registry.data);

    enginepb::FileRegistry file_registry;
    ASSERT_TRUE(file_registry.ParseFromArray(result.file_registry.data, result.file_registry.len));
    free(result.file_registry.data);

    // Check some registry contents.
    EXPECT_STRNE(key_registry.active_store_key_id().c_str(), "plain");
    EXPECT_STRNE(key_registry.active_data_key_id().c_str(), "plain");

    auto iter = key_registry.data_keys().find(key_registry.active_data_key_id());
    ASSERT_NE(iter, key_registry.data_keys().end());
    // Make sure the key data was cleared.
    EXPECT_STREQ(iter->second.key().c_str(), "");
    EXPECT_EQ(iter->second.info().encryption_type(), enginepbccl::AES128_CTR);

    auto iter2 = key_registry.store_keys().find(key_registry.active_store_key_id());
    ASSERT_NE(iter2, key_registry.store_keys().end());
    EXPECT_EQ(iter2->second.encryption_type(), enginepbccl::AES128_CTR);

    EXPECT_GT(key_registry.store_keys().size(), 0);
    EXPECT_GT(key_registry.data_keys().size(), 0);
    EXPECT_GT(file_registry.files().size(), 0);

    DBClose(db);
  }
}
