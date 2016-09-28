// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_OPTIONS_BUILDER_H_
#define STORAGE_ROCKSDB_INCLUDE_OPTIONS_BUILDER_H_

#include "rocksdb/options.h"

namespace rocksdb {

// Get options based on some guidelines. Now only tune parameter based on
// flush/compaction and fill default parameters for other parameters.
// total_write_buffer_limit: budget for memory spent for mem tables
// read_amplification_threshold: comfortable value of read amplification
// write_amplification_threshold: comfortable value of write amplification.
// target_db_size: estimated total DB size.
extern Options GetOptions(size_t total_write_buffer_limit,
                          int read_amplification_threshold = 8,
                          int write_amplification_threshold = 32,
                          uint64_t target_db_size = 68719476736 /* 64GB */);


}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_OPTIONS_BUILDER_H_
