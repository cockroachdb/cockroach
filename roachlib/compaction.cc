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
#include <string>
#include "api.pb.h"
#include "data.pb.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"
#include "roach_c.h"

namespace {

// These constants must be kept exactly in sync with their
// counterparts in storage/engine/keys.go. Note that the length must
// be explicitly specified because these keys are prefixed with null
// characters. Take care to update the lengths if the keys change.
static const rocksdb::Slice KeyLocalRangeResponseCachePrefix("\x00\x00\x00respcache-", 13);
static const rocksdb::Slice KeyLocalTransactionPrefix("\x00\x00\x00txn-", 7);

// GetResponseHeader extracts the response header for each type of
// response in the ReadWriteCmdResponse union.
const proto::ResponseHeader* GetResponseHeader(const proto::ReadWriteCmdResponse& rwResp) {
  if (rwResp.has_put()) {
    return &rwResp.put().header();
  } else if (rwResp.has_conditional_put()) {
    return &rwResp.conditional_put().header();
  } else if (rwResp.has_increment()) {
    return &rwResp.increment().header();
  } else if (rwResp.has_delete_()) {
    return &rwResp.delete_().header();
  } else if (rwResp.has_delete_range()) {
    return &rwResp.delete_range().header();
  } else if (rwResp.has_end_transaction()) {
    return &rwResp.end_transaction().header();
  } else if (rwResp.has_accumulate_ts()) {
    return &rwResp.accumulate_ts().header();
  } else if (rwResp.has_reap_queue()) {
    return &rwResp.reap_queue().header();
  } else if (rwResp.has_enqueue_update()) {
    return &rwResp.enqueue_update().header();
  } else if (rwResp.has_enqueue_message()) {
    return &rwResp.enqueue_message().header();
  } else if (rwResp.has_internal_heartbeat_txn()) {
    return &rwResp.internal_heartbeat_txn().header();
  } else if (rwResp.has_internal_resolve_intent()) {
    return &rwResp.internal_resolve_intent().header();
  }
  return NULL;
}

}  // unnamed namespace

// GCCompactionFilter implements our garbage collection policy for
// key/value pairs which can be considered in isolation. This includes:
//
// - Response cache: response cache entries are garbage collected
//   based on their age vs. the current wall time. They're kept for a
//   configurable window.
//
// - Transactions: transaction table entries are garbage collected
//   according to the commit time of the transaction vs. the oldest
//   remaining write intent across the entire system. The oldest write
//   intent is maintained as a low-water mark, updated after polling
//   all ranges in the map.
unsigned char GCCompactionFilter(
    void* state, int level, const char* key,
    size_t key_length,
    const char* existing_value,
    size_t value_length, char** new_value,
    size_t* new_value_length,
    unsigned char* value_changed,
    char** error_msg)
{
  struct FilterState* fs = (struct FilterState*)state;
  const rocksdb::Slice keyS(key, key_length);
  *value_changed = 0;
  *error_msg = NULL;

  // Response cache rows are GC'd if their timestamp is older than the
  // response cache GC timeout.
  if (keyS.starts_with(KeyLocalRangeResponseCachePrefix)) {
    proto::ReadWriteCmdResponse rwResp;
    if (!rwResp.ParseFromArray(existing_value, value_length)) {
      *error_msg = (char*)"failed to parse response cache entry";
      return (unsigned char)0;
    }
    const proto::ResponseHeader* header = GetResponseHeader(rwResp);
    if (header == NULL) {
      *error_msg = (char*)"failed to parse response cache header";
      return (unsigned char)0;
    }
    if (header->timestamp().wall_time() <= fs->min_rcache_ts) {
      return (unsigned char)1;
    }
  } else if (keyS.starts_with(KeyLocalTransactionPrefix)) {
    // Transaction rows are GC'd if their timestamp is older than the
    // system-wide minimum write intent timestamp. This system-wide
    // minimum write intent is periodically computed via map-reduce
    // over all ranges and gossipped.
    proto::Transaction txn;
    if (!txn.ParseFromArray(existing_value, value_length)) {
      *error_msg = (char*)"failed to parse transaction entry";
      return (unsigned char)0;
    }
    if (txn.timestamp().wall_time() <= fs->min_txn_ts) {
      return (unsigned char)1;
    }
  }

  return (unsigned char)0;
}
