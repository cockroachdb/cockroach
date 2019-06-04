// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

#include <rocksdb/ldb_tool.h>
#include "db.h"
#include "options.h"

extern "C" {
char* prettyPrintKey(DBKey);
}  // extern "C"

using namespace cockroach;

namespace {

class KeyFormatter : public rocksdb::SliceFormatter {
  std::string Format(const rocksdb::Slice& s) const {
    char* p = prettyPrintKey(ToDBKey(s));
    std::string ret(p);
    free(static_cast<void*>(p));
    return ret;
  }
};

}  // namespace

void DBRunLDB(int argc, char** argv) {
  rocksdb::Options options = DBMakeOptions(DBOptions());
  rocksdb::LDBOptions ldb_options;
  ldb_options.key_formatter.reset(new KeyFormatter);
  rocksdb::LDBTool tool;
  tool.Run(argc, argv, options, ldb_options);
}
