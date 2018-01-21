// Copyright 2017 The Cockroach Authors.
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

#include "db.h"
#include "include/libroach.h"
#include "testutils.h"

using namespace cockroach;

TEST(Libroach, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHook("", db_opts));

  // Try extra_options with anything at all.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook("", db_opts),
             "DBOptions has extra_options, but OSS code cannot handle them");
}
