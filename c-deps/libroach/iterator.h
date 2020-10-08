// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
