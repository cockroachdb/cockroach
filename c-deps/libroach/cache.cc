// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

#include "cache.h"

DBCache* DBNewCache(uint64_t size) {
  const int num_cache_shard_bits = 4;
  DBCache* cache = new DBCache;
  cache->rep = rocksdb::NewLRUCache(size, num_cache_shard_bits);
  return cache;
}

DBCache* DBRefCache(DBCache* cache) {
  DBCache* res = new DBCache;
  res->rep = cache->rep;
  return res;
}

void DBReleaseCache(DBCache* cache) { delete cache; }
