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

#ifndef ROACHLIB_ROACH_C_H
#define ROACHLIB_ROACH_C_H

#include "rocksdb/c.h"

#ifdef __cplusplus
extern "C" {
#endif

struct FilterState {
  // The key prefix identifying transaction records.
  const char* txn_prefix;
  // The key prefix identifying response cache records.
  const char* rcache_prefix;
  // The minimum timestamp for transaction rows. Any transaction with
  // a timestamp prior to min_txn_ts can be discarded.
  int64_t min_txn_ts;
  // The minimum timestamp for response cache rows. Any response cache
  // entry with a timestamp prior to min_rcache_ts can be discarded.
  int64_t min_rcache_ts;
};

unsigned char GCCompactionFilter(
    void* state, int level, const char* key,
    size_t key_length,
    const char* existing_value,
    size_t value_length, char** new_value,
    size_t* new_value_length,
    unsigned char* value_changed,
    char** error_msg);

char* MergeOne(
    const char* existing, size_t existing_length,
    const char* update, size_t update_length,
    size_t* new_value_length, char** error_msg);
char* MergeOperator(
    const char* key, size_t key_length,
    const char* existing_value,
    size_t existing_value_length,
    const char* const* operands_list,
    const size_t* operands_list_length,
    int num_operands, unsigned char* success,
    size_t* new_value_length);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIB_ROACH_C_H
