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

#include <libroach.h>
#include <memory>
#include <mutex>
#include <rocksdb/cache.h>

struct DBCache {
  std::mutex mu;
  std::shared_ptr<rocksdb::Cache> rep;
};
