// Copyright 2018 The Cockroach Authors.
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

#include <vector>
#include "../fmt.h"
#include "../testutils.h"
#include "file_registry.h"

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

  int test_num = 0;
  for (auto t : test_cases) {
    SCOPED_TRACE(fmt::StringPrintf("Testing #%d", test_num++));

    FileRegistry reg(nullptr, t.db_dir);
    auto out = reg.TransformPath(t.input);
    EXPECT_EQ(t.output, out);
  }
}
