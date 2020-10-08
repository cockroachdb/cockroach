// Copyright 2019 The Cockroach Authors.
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
#include "db.h"
#include "engine.h"
#include "iterator.h"
#include "protos/roachpb/data.pb.h"
#include "status.h"

// DBIncrementalIterator is the C++ implementation of MVCCIncrementalIterator.
// This implementation should be kept in sync with MVCCIncrementalIterator,
// which is used as an oracle to test DBIncrementalIterator. It may be useful to
// revive in the future when Pebble becomes the main storage engine.
// For general documentation around this iterator please refer to the comments
// in mvcc_incremental_iterator.go.
struct DBIncrementalIterator {
  DBIncrementalIterator(DBEngine* engine, DBIterOptions opts, DBKey start, DBKey end,
                        DBString* write_intent);
  ~DBIncrementalIterator();
  DBIterState seek(DBKey key);
  DBIterState next(bool skip_current_versions);
  const rocksdb::Slice key();
  const rocksdb::Slice value();

  std::unique_ptr<DBIterator> iter;

  // The time_bound_iter is used as a performance optimization to potentially
  // allow skipping over a large number of keys. The time bound iterator skips
  // sstables which do not contain keys modified in a certain interval.
  //
  // A time-bound iterator cannot be used by itself due to a bug in the time-
  // bound iterator (#28358). This was historically augmented with an iterator
  // without the time-bound optimization to act as a sanity iterator, but
  // issues remained (#43799), so now the iterator above is the main iterator
  // the timeBoundIter is used to check if any keys can be skipped by the main
  // iterator.
  //
  // Note regarding the correctness of the time-bound iterator optimization:
  //
  // When using (t_s, t_e], say there is a version (committed or provisional)
  // k@t where t is in that interval, that is visible to iter. All sstables
  // containing k@t will be included in timeBoundIter. Note that there may be
  // multiple sequence numbers for the key k@t at the storage layer, say k@t#n1,
  // k@t#n2, where n1 > n2, some of which may be deleted, but the latest
  // sequence number will be visible using iter (since not being visible would be
  // a contradiction of the initial assumption that k@t is visible to iter).
  // Since there is no delete across all sstables that deletes k@t#n1, there is
  // no delete in the subset of sstables used by timeBoundIter that deletes
  // k@t#n1, so the timeBoundIter will see k@t.
  std::unique_ptr<DBIterator> time_bound_iter = nullptr;

  DBEngine* engine;
  DBIterOptions opts;
  bool valid;
  DBStatus status;
  DBKey start, end;
  DBString* write_intent;

 private:
  rocksdb::Slice sanityCheckMetadataKey();
  bool legacyTimestampIsLess(const cockroach::util::hlc::LegacyTimestamp& t1,
                             const cockroach::util::hlc::LegacyTimestamp& t2);
  DBIterState getState();
  void advanceKey();
  void maybeSkipKeys();
  bool extractKey(rocksdb::Slice mvcc_key, rocksdb::Slice* key);

  cockroach::util::hlc::LegacyTimestamp start_time;
  cockroach::util::hlc::LegacyTimestamp end_time;
};
