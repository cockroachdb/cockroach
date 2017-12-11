#include <gtest/gtest.h>
#include "db.h"
#include "include/libroach.h"

TEST(Libroach, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_TRUE(DBOpenHook(db_opts).ok());

  // Try extra_options with anything at all.
  db_opts.extra_options = ToDBSlice("blah");
  EXPECT_STREQ("DBOptions has extra_options, but OSS code cannot handle them", DBOpenHook(db_opts).getState());
}
