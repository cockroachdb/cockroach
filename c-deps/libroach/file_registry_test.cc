// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <vector>
#include "file_registry.h"
#include "fmt.h"
#include "testutils.h"

using namespace cockroach;

TEST(FileRegistry, TransformPath) {
  struct TestCase {
    std::string db_dir;
    std::string input;
    std::string output;
  };

  // The db_dir as sanitized does not have a trailing slash.
  std::vector<TestCase> test_cases = {
      {"/", "/foo", "foo"},
      {"/rocksdir", "/rocksdirfoo", "/rocksdirfoo"},
      {"/rocksdir", "/rocksdir/foo", "foo"},
      // We get the occasional double-slash.
      {"/rocksdir", "/rocksdir//foo", "foo"},
      {"/mydir", "/mydir", ""},
      {"/mydir", "/mydir/", ""},
      {"/mydir", "/mydir//", ""},
      {"/mnt/otherdevice/", "/mnt/otherdevice/myfile", "myfile"},
      {"/mnt/otherdevice/myfile", "/mnt/otherdevice/myfile", ""},
  };

  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));
  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    FileRegistry reg(env.get(), t.db_dir, false /* read-only */);
    auto out = reg.TransformPath(t.input);
    EXPECT_EQ(t.output, out);
  }
}

TEST(FileRegistry, RelativePaths) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  FileRegistry reg(env.get(), "/base", false);
  ASSERT_OK(reg.Load());

  // Create a few file entries.
  std::vector<std::string> files = {
      "/base/foo",
      "/base/dir1/bar",
      "/base/dir1/dir2/baz",
  };
  for (auto f : files) {
    auto entry = std::unique_ptr<enginepb::FileEntry>(new enginepb::FileEntry());
    EXPECT_OK(reg.SetFileEntry(f, std::move(entry)));
    EXPECT_NE(nullptr, reg.GetFileEntry(f));
  }

  struct TestCase {
    std::string path;
    bool relative;
    bool found;
  };

  std::vector<TestCase> test_cases = {
      // Full paths.
      {"/base/foo", false, true},
      {"/base/dir1/bar", false, true},
      {"/base/dir1/dir2/baz", false, true},
      // Relative paths with separator.
      {"/foo", true, true},
      {"/dir1/bar", true, true},
      {"/dir1/dir2/baz", true, true},
      // Relative paths without separator.
      {"foo", true, true},
      {"dir1/bar", true, true},
      {"dir1/dir2/baz", true, true},
      // We don't sanitize paths, so a few things break:
      {"../base/foo", true, false},  // ..
      {"/dir1//bar", true, false},   // multiple separators
  };

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));
    bool found = (reg.GetFileEntry(t.path, t.relative) != nullptr);
    EXPECT_EQ(t.found, found);
  }
}

TEST(FileRegistry, FileOps) {
  std::unique_ptr<rocksdb::Env> env(rocksdb::NewMemEnv(rocksdb::Env::Default()));

  FileRegistry rw_reg(env.get(), "/", false /* read-only */);
  ASSERT_OK(rw_reg.CheckNoRegistryFile());
  ASSERT_OK(rw_reg.Load());

  EXPECT_EQ(nullptr, rw_reg.GetFileEntry("/foo"));
  auto entry = std::unique_ptr<enginepb::FileEntry>(new enginepb::FileEntry());
  EXPECT_OK(rw_reg.SetFileEntry("/foo", std::move(entry)));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/foo"));

  EXPECT_OK(rw_reg.MaybeLinkEntry("/foo", "/bar"));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/foo"));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/bar"));

  EXPECT_OK(rw_reg.MaybeRenameEntry("/bar", "/baz"));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/foo"));
  EXPECT_EQ(nullptr, rw_reg.GetFileEntry("/bar"));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/baz"));

  EXPECT_OK(rw_reg.MaybeDeleteEntry("/baz"));
  EXPECT_NE(nullptr, rw_reg.GetFileEntry("/foo"));
  EXPECT_EQ(nullptr, rw_reg.GetFileEntry("/bar"));
  EXPECT_EQ(nullptr, rw_reg.GetFileEntry("/baz"));

  // Now try with a read-only registry.
  FileRegistry ro_reg(env.get(), "/", true /* read-only */);
  // We have a registry file.
  EXPECT_ERR(ro_reg.CheckNoRegistryFile(), "registry file .* exists");
  ASSERT_OK(ro_reg.Load());

  EXPECT_NE(nullptr, ro_reg.GetFileEntry("/foo"));
  EXPECT_EQ(nullptr, ro_reg.GetFileEntry("/bar"));
  EXPECT_EQ(nullptr, ro_reg.GetFileEntry("/baz"));

  // All mutable operations fail.
  EXPECT_ERR(ro_reg.MaybeLinkEntry("/foo", "/bar"), "file registry is read-only .*");
  EXPECT_ERR(ro_reg.MaybeRenameEntry("/foo", "/baz"), "file registry is read-only .*");
  EXPECT_ERR(ro_reg.MaybeDeleteEntry("/foo"), "file registry is read-only .*");
}
