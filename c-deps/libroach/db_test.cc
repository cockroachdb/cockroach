// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "db.h"
#include "file_registry.h"
#include "include/libroach.h"
#include "status.h"
#include "testutils.h"

using namespace cockroach;
using namespace testutils;

TEST(Libroach, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHookOSS(nullptr, "", db_opts, nullptr));

  // Try extra_options with anything at all.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHookOSS(nullptr, "", db_opts, nullptr),
             "encryption options are not supported in OSS builds");
}

TEST(Libroach, DBOpen) {
  // Use a real directory, we need to create a file_registry.
  TempDirHandler dir;

  {
    DBOptions db_opts = defaultDBOptions();

    DBEngine* db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBEnvStatsResult stats;
    EXPECT_STREQ(DBGetEnvStats(db, &stats).data, NULL);
    EXPECT_STREQ(stats.encryption_status.data, NULL);
    EXPECT_EQ(stats.encryption_type, 0);
    EXPECT_EQ(stats.total_files, 0);
    EXPECT_EQ(stats.total_bytes, 0);
    EXPECT_EQ(stats.active_key_files, 0);
    EXPECT_EQ(stats.active_key_bytes, 0);

    // Fetch registries, parse, and check that they're empty.
    DBEncryptionRegistries result;
    EXPECT_STREQ(DBGetEncryptionRegistries(db, &result).data, NULL);
    EXPECT_STREQ(result.key_registry.data, NULL);
    EXPECT_STREQ(result.file_registry.data, NULL);

    DBClose(db);
  }
  {
    // We're at storage version FileRegistry, but we don't have one.
    DBOptions db_opts = defaultDBOptions();
    db_opts.use_file_registry = true;

    DBEngine* db;
    EXPECT_STREQ(DBOpen(&db, ToDBSlice(dir.Path("")), db_opts).data, NULL);
    DBClose(db);
  }
  {
    // We're at storage version FileRegistry, and the file_registry file exists:
    // this is not supported in OSS mode, or without encryption options.
    DBOptions db_opts = defaultDBOptions();
    db_opts.use_file_registry = true;

    // Create bogus file registry.
    ASSERT_OK(rocksdb::WriteStringToFile(rocksdb::Env::Default(), "",
                                         dir.Path(kFileRegistryFilename), true));

    DBEngine* db;
    auto ret = DBOpen(&db, ToDBSlice(dir.Path("")), db_opts);
    EXPECT_STREQ(std::string(ret.data, ret.len).c_str(),
                 "Invalid argument: encryption was used on this store before, but no encryption "
                 "flags specified. You need a CCL build and must fully specify the "
                 "--enterprise-encryption flag");
    free(ret.data);
    ASSERT_OK(rocksdb::Env::Default()->DeleteFile(dir.Path(kFileRegistryFilename)));
  }
  {
    // We're at storage version FileRegistry, and the file_registry file exists.
    // We do have encryption options, but those are not supported in OSS builds.
    DBOptions db_opts = defaultDBOptions();
    db_opts.use_file_registry = true;
    db_opts.extra_options = ToDBSlice("blah");

    // Create bogus file registry.
    ASSERT_OK(rocksdb::WriteStringToFile(rocksdb::Env::Default(), "",
                                         dir.Path(kFileRegistryFilename), true));

    DBEngine* db;
    auto ret = DBOpen(&db, ToDBSlice(dir.Path("")), db_opts);
    EXPECT_STREQ(std::string(ret.data, ret.len).c_str(),
                 "Invalid argument: encryption options are not supported in OSS builds");
    free(ret.data);
    ASSERT_OK(rocksdb::Env::Default()->DeleteFile(dir.Path(kFileRegistryFilename)));
  }
}

TEST(Libroach, BatchSSTablesForCompaction) {
  auto toString = [](const std::vector<rocksdb::Range>& ranges) -> std::string {
    std::string res;
    for (auto r : ranges) {
      if (!res.empty()) {
        res.append(",");
      }
      res.append(r.start.data(), r.start.size());
      res.append("-");
      res.append(r.limit.data(), r.limit.size());
    }
    return res;
  };

  auto sst = [](const std::string& smallest, const std::string& largest,
                uint64_t size) -> rocksdb::SstFileMetaData {
    return rocksdb::SstFileMetaData("", "", size, 0, 0, smallest, largest, 0, 0);
  };

  struct TestCase {
    TestCase(const std::vector<rocksdb::SstFileMetaData>& s, const std::string& start,
             const std::string& end, uint64_t target, const std::string& expected)
        : sst(s), start_key(start), end_key(end), target_size(target), expected_ranges(expected) {}
    std::vector<rocksdb::SstFileMetaData> sst;
    std::string start_key;
    std::string end_key;
    uint64_t target_size;
    std::string expected_ranges;
  };

  std::vector<TestCase> testCases = {
      TestCase({sst("a", "b", 10)}, "", "", 10, "-"),
      TestCase({sst("a", "b", 10)}, "a", "", 10, "a-"),
      TestCase({sst("a", "b", 10)}, "", "b", 10, "-b"),
      TestCase({sst("a", "b", 10)}, "a", "b", 10, "a-b"),
      TestCase({sst("c", "d", 10)}, "a", "b", 10, "a-b"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10)}, "a", "c", 10, "a-b,b-c"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10)}, "a", "c", 100, "a-c"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10)}, "", "c", 10, "-b,b-c"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10)}, "a", "", 10, "a-b,b-"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10), sst("c", "d", 10)}, "a", "d", 10,
               "a-b,b-c,c-d"),
      TestCase({sst("a", "b", 10), sst("b", "c", 10), sst("c", "d", 10)}, "a", "d", 20, "a-c,c-d"),
  };
  for (auto c : testCases) {
    std::vector<rocksdb::Range> ranges;
    BatchSSTablesForCompaction(c.sst, c.start_key, c.end_key, c.target_size, &ranges);
    auto result = toString(ranges);
    EXPECT_EQ(c.expected_ranges, result);
  }
}
