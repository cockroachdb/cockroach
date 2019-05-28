// Copyright 2019 The Cockroach Authors.
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

#include <libroach.h>
#include "db.h"
#include "protos/roachpb/api.pb.h"

// MaxReservedDescID is the maximum value of reserved descriptor IDs. Reserved
// IDs are used by namespaces and tables used internally by cockroach.
const int MaxReservedDescID = 49;

// RowCounter counts how many distinct rows appear in the KVs that is is shown
// via `Count`. Note: the `DataSize` field of the BulkOpSummary is *not*
// populated by this and should be set separately.
struct RowCounter {
  bool Count(const rocksdb::Slice& key, cockroach::roachpb::BulkOpSummary* summary);

 private:
  void EnsureSafeSplitKey(rocksdb::Slice* key);
  int GetRowPrefixLength(rocksdb::Slice* key);
  DBString prev_key;
  rocksdb::Slice prev;
};
