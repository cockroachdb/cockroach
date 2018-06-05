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
using namespace testutils;

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
