// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License.

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
