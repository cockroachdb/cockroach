//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "rocksdb/options.h"

namespace rocksdb {
void DumpDBFileSummary(const DBOptions& options, const std::string& dbname);
}  // namespace rocksdb
