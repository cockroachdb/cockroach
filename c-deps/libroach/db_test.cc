#include <gtest/gtest.h>
#include "db.h"
#include "include/libroach.h"

TEST(Libroach, DBOpenHook) {
  DBOptions db_opts;

  // Try an empty extra_options.
  db_opts.extra_options = ToDBSlice("");
  EXPECT_EQ(NULL, DBOpenHook(db_opts).data);

  // Try extra_options with anything at all.
  db_opts.extra_options = ToDBSlice("blah");
  // TODO(mberhault): we need to convert to a std::string because the DBSlice's data
  // is not null-terminated. Switch internal-only data to be "normal" objects.
  EXPECT_EQ(std::string("DBOPtions has extra_options, but OSS code cannot handle them"), ToString(DBOpenHook(db_opts)));
}
