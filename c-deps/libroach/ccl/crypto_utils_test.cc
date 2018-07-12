// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include "../testutils.h"
#include "crypto_utils.h"

TEST(CryptoUtils, HasAESNI) { EXPECT_TRUE(UsesAESNI()); }

#ifdef _WIN32

TEST(CryptoUtils, DisableCoreDumps) {
  EXPECT_ERR(DisableCoreFile(), ".* not supported on Windows");
}

#else

#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>

TEST(CryptoUtils, DisableCoreDumps) {
  rlimit lim = {1 << 10, 2 << 10};
  ASSERT_EQ(0, setrlimit(RLIMIT_CORE, &lim));

  EXPECT_OK(DisableCoreFile());
  ASSERT_EQ(0, getrlimit(RLIMIT_CORE, &lim));
  EXPECT_EQ(0, lim.rlim_cur);
  EXPECT_EQ(0, lim.rlim_max);
}

#endif
