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

struct DBIncrementalIterator {
  DBIncrementalIterator(DBEngine* engine, DBIterOptions opts, DBKey start, DBKey end,
                        DBString* write_intent);
  ~DBIncrementalIterator();
  DBIterState seek(DBKey key);
  DBIterState next(bool skip_current_versions);
  const rocksdb::Slice key();
  const rocksdb::Slice value();

  std::unique_ptr<DBIterator> iter;
  std::unique_ptr<DBIterator> sanity_iter;

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

  cockroach::util::hlc::LegacyTimestamp start_time;
  cockroach::util::hlc::LegacyTimestamp end_time;
};
