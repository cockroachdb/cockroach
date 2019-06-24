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

static const int kDefaultVerbosityForInfoLogging = 3;

// Make a new rocksdb::Logger that calls back into Go with a translation of
// RocksDB's log level into a corresponding Go log level.
// The message is logged if severity is higher than info, or if severity is
// info and glog verbosity is at least `info_verbosity`.
rocksdb::Logger* NewDBLogger(int info_verbosity);

// DBMakeOptions constructs a rocksdb::Options given a DBOptions.
rocksdb::Options DBMakeOptions(DBOptions db_opts);

}  // namespace cockroach
