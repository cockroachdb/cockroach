// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "../db.h"
#include "../testutils.h"

using namespace cockroach;

TEST(LibroachCCL, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHook("", db_opts));

  // Try extra_options with bad data.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook("", db_opts), "failed to parse extra options");
}
