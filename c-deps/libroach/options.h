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

#pragma once

#include <libroach.h>
#include <rocksdb/options.h>

namespace cockroach {

static const int kDefaultLogLevel = 3;

// Make a new rocksdb::Logger that calls back into Go with log level.
// Most callers should use kDefaultLogLevel to avoid excessive logging from C++ code.
rocksdb::Logger* NewDBLogger(int go_log_level);

// DBMakeOptions constructs a rocksdb::Options given a DBOptions.
rocksdb::Options DBMakeOptions(DBOptions db_opts);

}  // namespace cockroach
