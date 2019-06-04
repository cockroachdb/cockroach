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
