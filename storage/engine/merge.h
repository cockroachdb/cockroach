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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>             // For memcpy
#include <limits.h>             // For LLONG_{MIN,MAX}
#include <errno.h>
#include "rocksdb/c.h"

void IncMergeOperatorDestroy(void *arg);
const char *IncMergeOperatorName(void *arg);
char *IncMergeOperatorFullMerge(void *arg,
                                const char *key, size_t key_length,
                                const char *existing_value,
                                size_t existing_value_length,
                                const char *const *operands_list,
                                const size_t * operands_list_length,
                                int num_operands, unsigned char *success,
                                size_t * new_value_length);

char *IncMergeOperatorPartialMerge(void *arg,
                                   const char *key, size_t key_length,
                                   const char *const *operands_list,
                                   const size_t * operands_list_length,
                                   int num_operands, unsigned char *success,
                                   size_t * new_value_length);
rocksdb_mergeoperator_t *MakeMergeOperator();
