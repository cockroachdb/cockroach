// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
