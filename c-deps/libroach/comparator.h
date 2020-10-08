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

#include <rocksdb/db.h>

namespace cockroach {

class DBComparator : public rocksdb::Comparator {
 public:
  DBComparator() {}

  virtual const char* Name() const override { return "cockroach_comparator"; }

  virtual int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override;
  virtual bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const override;
  virtual void FindShortestSeparator(std::string* start,
                                     const rocksdb::Slice& limit) const override;
  virtual void FindShortSuccessor(std::string* key) const override;
};

const DBComparator kComparator;

}  // namespace cockroach
