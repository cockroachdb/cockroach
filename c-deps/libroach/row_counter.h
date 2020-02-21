// Copyright 2016 The Cockroach Authors.
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
  RowCounter(cockroach::roachpb::BulkOpSummary* summary) : summary(summary) {}
  bool Count(const rocksdb::Slice& key);

 private:
  void EnsureSafeSplitKey(rocksdb::Slice* key);
  int GetRowPrefixLength(rocksdb::Slice* key);
  cockroach::roachpb::BulkOpSummary* summary;
  std::string prev_key;
  rocksdb::Slice prev;
};
