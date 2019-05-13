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
#include "engine.h"
#include "iterator.h"
#include "protos/roachpb/data.pb.h"
#include "status.h"

struct DBIncrementalIterator {
  DBIncrementalIterator(DBEngine* engine, DBIterOptions opts, DBKey start, DBKey end,
                        DBString* write_intent);
  ~DBIncrementalIterator();
  void advanceKey();
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

  cockroach::util::hlc::LegacyTimestamp start_time;
  cockroach::util::hlc::LegacyTimestamp end_time;
};
