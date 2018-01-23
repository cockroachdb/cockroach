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
