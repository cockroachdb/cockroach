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

#include <rocksdb/sst_dump_tool.h>
#include <libroach.h>

void DBRunSSTDump(int argc, char** argv) {
  rocksdb::SSTDumpTool tool;
  tool.Run(argc, argv);
}
