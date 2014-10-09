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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Style settings: indent -kr -ci2 -cli2 -i2 -l80 -nut storage/merge.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>             // For memcpy
#include <limits.h>             // For LLONG_{MIN,MAX}
#include <errno.h>
#include "rocksdb/c.h"
#include "rocksdb_merge.h"
#include "roach_c.h"

static void operator_destroy(void* arg)
{
}

static const char* operator_name(void* arg)
{
  return "cockroach.inc_merge_operator";
}

static char* operator_full_merge(void* arg,
                                 const char* key, size_t key_length,
                                 const char* existing_value,
                                 size_t existing_value_length,
                                 const char* const* operands_list,
                                 const size_t* operands_list_length,
                                 int num_operands, unsigned char* success,
                                 size_t* new_value_length)
{
  return MergeOperator(key, key_length,
                       existing_value, existing_value_length,
                       operands_list, operands_list_length,
                       num_operands, success, new_value_length);
}

static char* operator_partial_merge(void* arg,
                                    const char* key, size_t key_length,
                                    const char* const* operands_list,
                                    const size_t* operands_list_length,
                                    int num_operands, unsigned char* success,
                                    size_t* new_value_length)
{
  return MergeOperator(key, key_length,
                       NULL, 0,     // no existing value & length
                       operands_list, operands_list_length,
                       num_operands,
                       success, new_value_length);
}

rocksdb_mergeoperator_t* make_merge_operator()
{
  rocksdb_mergeoperator_t* op =
    rocksdb_mergeoperator_create(NULL, operator_destroy,
                                 operator_full_merge,
                                 operator_partial_merge, NULL,
                                 operator_name);
  return op;
}
