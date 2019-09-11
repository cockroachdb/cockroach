// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <rocksdb/options.h>

namespace cockroach {

// Make a new rocksdb::Logger that calls back into Go with a
// translation of RocksDB's log level into a corresponding Go log
// level. If info_logger is not NULL, then info logs are output
// there. Otherwise they are dispatched to the Go logger.
rocksdb::Logger* NewDBLogger(rocksdb::Logger* info_logger);

// DBMakeOptions constructs a rocksdb::Options given a DBOptions.
rocksdb::Options DBMakeOptions(DBOptions db_opts);

}  // namespace cockroach
