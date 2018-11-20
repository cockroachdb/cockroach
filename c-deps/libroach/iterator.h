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

#pragma once

#include <atomic>
#include <memory>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>
#include "chunked_buffer.h"

struct DBIterator {
  DBIterator(std::atomic<int64_t>* iters, DBIterOptions iter_options);
  ~DBIterator();
  void SetLowerBound(DBKey key);
  void SetUpperBound(DBKey key);

  std::atomic<int64_t>* const iters_count;
  std::unique_ptr<rocksdb::Iterator> rep;
  std::unique_ptr<cockroach::chunkedBuffer> kvs;
  std::unique_ptr<rocksdb::WriteBatch> intents;
  std::unique_ptr<IteratorStats> stats;
  std::string rev_resume_key;

  rocksdb::ReadOptions read_opts;
  std::string lower_bound_str;
  std::string upper_bound_str;
  rocksdb::Slice lower_bound;
  rocksdb::Slice upper_bound;
};
