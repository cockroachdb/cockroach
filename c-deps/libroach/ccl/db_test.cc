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
  db_opts.use_switching_env = false;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_OK(DBOpenHook("", db_opts, nullptr));

  // Try without switching env enabled and bogus options. We should fail
  // because encryption options without switching env is not allowed.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook("", db_opts, nullptr),
             "on-disk version does not support encryption, but we found encryption flags");

  db_opts.use_switching_env = true;
  // Try with switching env but bogus encryption flags.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_ERR(DBOpenHook("", db_opts, nullptr), "failed to parse extra options");
}
