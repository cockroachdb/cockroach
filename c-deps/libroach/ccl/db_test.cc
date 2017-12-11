#include <gtest/gtest.h>
#include "../db.h"

TEST(LibroachCCL, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_TRUE(DBOpenHook(db_opts).ok());

  // Try extra_options with bad data.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_STREQ("failed to parse extra options", DBOpenHook(db_opts).getState());
}
