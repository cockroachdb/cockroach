#include <gtest/gtest.h>
#include "../db.h"

TEST(LibroachCCL, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_EQ(NULL, DBOpenHook(db_opts).data);

  // Try extra_options with bad data.
  db_opts.extra_options = ToDBSlice("blah");
  // TODO(mberhault): we need to convert to a std::string because the DBSlice's data
  // is not null-terminated. Switch internal-only data to be "normal" objects.
  EXPECT_EQ(std::string("failed to parse extra options"), ToString(DBOpenHook(db_opts)));
}
