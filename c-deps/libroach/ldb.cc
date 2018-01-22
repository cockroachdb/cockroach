// Copyright 2018 The Cockroach Authors.
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
