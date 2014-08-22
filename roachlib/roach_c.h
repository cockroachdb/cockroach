// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

#include "rocksdb/c.h"

struct FilterState {
  // The minimum timestamp for transaction rows. Any transaction with
  // a timestamp prior to min_txn_ts can be discarded.
  int64_t min_txn_ts;
  // The minimum timestamp for response cache rows. Any response cache
  // entry with a timestamp prior to min_rcache_ts can be discarded.
  int64_t min_rcache_ts;
};

#ifdef __cplusplus
extern "C" {
#endif

unsigned char GCCompactionFilter(
    void* state, int level, const char* key,
    size_t key_length,
    const char* existing_value,
    size_t value_length, char** new_value,
    size_t* new_value_length,
    unsigned char* value_changed,
    char** error_msg);

#ifdef __cplusplus
}
#endif
