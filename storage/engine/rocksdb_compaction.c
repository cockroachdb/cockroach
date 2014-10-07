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

// Style settings: indent -kr -ci2 -cli2 -i2 -l80 -nut storage/rocksdb_compaction.cc
#include <stdio.h>
#include <stdlib.h>
#include <string.h>             // For memcpy
#include <limits.h>             // For LLONG_{MIN,MAX}
#include <errno.h>
#include "rocksdb/c.h"
#include "rocksdb_compaction.h"
#include "roach_c.h"
#include "_cgo_export.h"

void gc_compaction_filter_destroy(void* state)
{
  free(state);
}

const char* gc_compaction_filter_name(void* state)
{
  return "cockroachdb.gc_compaction_filter";
}

void gc_compaction_filter_factory_destroy(void* state)
{
}

unsigned char gc_compaction_filter_filter(
    void* state, int level, const char* key,
    size_t key_length,
    const char* existing_value,
    size_t value_length, char** new_value,
    size_t* new_value_length,
    unsigned char* value_changed) {
  unsigned char retval;
  char* error_msg;
  retval = GCCompactionFilter(state, level, key, key_length, existing_value, value_length,
                              new_value, new_value_length, value_changed, &error_msg);
  if (error_msg != NULL) {
    reportGCError(error_msg, (char*)key, key_length, (char*)existing_value, value_length);
  }
  return retval;
}

rocksdb_compactionfilter_t* gc_compaction_filter_factory_create_filter(
    void* rocksdb, rocksdb_compactionfiltercontext_t* context) {
  struct getGCPrefixes_return prefixes = getGCPrefixes();
  struct getGCTimeouts_return timeouts = getGCTimeouts(rocksdb);
  struct FilterState* fs = (struct FilterState*)malloc(sizeof(struct FilterState));

  fs->txn_prefix = prefixes.r0;
  fs->rcache_prefix = prefixes.r1;
  fs->min_txn_ts = timeouts.r0;
  fs->min_rcache_ts = timeouts.r1;

  return rocksdb_compactionfilter_create(
      fs, gc_compaction_filter_destroy,
      gc_compaction_filter_filter,
      gc_compaction_filter_name);
}

const char* gc_compaction_filter_factory_name(void* state)
{
  return "cockroachdb.gc_compaction_filter_factory";
}

rocksdb_compactionfilterfactory_t* make_gc_compaction_filter_factory(void* rocksdb)
{
  return rocksdb_compactionfilterfactory_create(
      rocksdb, gc_compaction_filter_factory_destroy,
      gc_compaction_filter_factory_create_filter,
      gc_compaction_filter_factory_name);
}

