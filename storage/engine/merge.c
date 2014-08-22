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
#include "merge.h"
#include "_cgo_export.h"

void IncMergeOperatorDestroy(void *arg)
{
}

const char *IncMergeOperatorName(void *arg)
{
  return "IncMergeOperator";
}

char *IncMergeOperatorFullMerge(void *arg,
                                const char *key, size_t key_length,
                                const char *existing_value,
                                size_t existing_value_length,
                                const char *const *operands_list,
                                const size_t * operands_list_length,
                                int num_operands, unsigned char *success,
                                size_t * new_value_length)
{
  int i;
  char *result = NULL;
  int64_t resultInt;
  char *endptr;
  struct merge_return current_result;
  if (existing_value != NULL) {
    // The existing value is freed by the caller, so we make our own
    // copy to do with it as we please.
    // Note that the input may be \0-terminated, but we don't care if
    // it is or not.
    current_result.r0 = malloc(existing_value_length);
    memcpy(current_result.r0, existing_value, existing_value_length);
    current_result.r1 = existing_value_length;
  } else {
    // Get the initial value from Go. It will not be garbage collected
    // so it belongs completely to us. The operands list is never empty.
    struct mergeInit_return tmp = mergeInit((char*)operands_list[0], operands_list_length[0]);
    current_result.r0 = tmp.r0;
    current_result.r1 = tmp.r1;
  }

  struct merge_return old_result;
  for (i = 0; i < num_operands; ++i) {
    old_result = current_result;
    // r0 is the char*, r1 the corresponding number of characters.
    current_result =
      merge(old_result.r0, (char *) (operands_list)[i], old_result.r1,
            operands_list_length[i]);

    // Free the (now outdated) previous intermediate result.
    if(old_result.r0 != NULL) {
      free(old_result.r0);
    }
    if (current_result.r0 == NULL) {
      // If no new value is returned, a serious error occurred and
      // the merge operation should propagate that to RocksDB.
      // The actual merge logic should try its best to avoid this.
      goto fail;
      break;
    }
  }
  *success = 1;
  *new_value_length = current_result.r1;
  return current_result.r0;

fail:
  // If we indicate failure (*success=0), then the call to the merger
  // via rocksdb_merge will not return an error, but simply remove or
  // truncate the offending key (at least when the settings specify that
  // missing keys should be created; otherwise a corruption error will
  // be returned, but likely only after the next read of the key).
  // In effect, there is no propagation of error information to the client.
  //
  // RocksDB expects to get a valid char* back no matter what.
  // We should rarely end up here and be tolerant with input instead,
  // at least until we find a way to have RocksDB propagate the error
  // which is likely impossible as the merges are carried out in an
  // asynchronous manner.
  *success = 0;
  *new_value_length = 1;
  result = malloc(1);
  memcpy(result, "0", 1);
  return result;
}

char *IncMergeOperatorPartialMerge(void *arg,
                                   const char *key, size_t key_length,
                                   const char *const *operands_list,
                                   const size_t * operands_list_length,
                                   int num_operands, unsigned char *success,
                                   size_t * new_value_length)
{
  char *result = IncMergeOperatorFullMerge(arg, key, key_length,
                                           NULL, 0,     // no existing value & length
                                           operands_list, operands_list_length,
                                           num_operands,
                                           success, new_value_length);
  return result;
}

rocksdb_mergeoperator_t *MakeMergeOperator()
{
  rocksdb_mergeoperator_t *op =
    rocksdb_mergeoperator_create(NULL, IncMergeOperatorDestroy,
                                 IncMergeOperatorFullMerge,
                                 IncMergeOperatorPartialMerge, NULL,
                                 IncMergeOperatorName);
  return op;
}
