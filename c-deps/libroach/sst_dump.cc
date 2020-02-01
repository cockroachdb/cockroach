// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <libroach.h>
#include <rocksdb/sst_dump_tool.h>

void DBRunSSTDump(int argc, char** argv) {
  rocksdb::SSTDumpTool tool;
  tool.Run(argc, argv);
}
