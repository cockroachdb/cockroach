// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "../protos/roachpb/errors.pb.h"
#include "../protos/storage/engine/enginepb/mvcc.pb.h"

#include <libroachccl.h>
#include "../db.h"

DBStatus DBBatchReprVerify(
  DBSlice repr, DBKey start, DBKey end, int64_t now_nanos, MVCCStatsResult* stats
) {
  const rocksdb::Comparator* kComparator = CockroachComparator();

  // TODO(dan): Inserting into a batch just to iterate over it is unfortunate.
  // Consider replacing this with WriteBatch's Iterate/Handler mechanism and
  // computing MVCC stats on the post-ApplyBatchRepr engine. splitTrigger does
  // the latter and it's a headache for propEvalKV, so wait to see how that
  // settles out before doing it that way.
  rocksdb::WriteBatchWithIndex batch(kComparator, 0, true);
  rocksdb::WriteBatch b(ToString(repr));
  std::unique_ptr<rocksdb::WriteBatch::Handler> inserter(GetDBBatchInserter(&batch));
  rocksdb::Status status = b.Iterate(inserter.get());
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  std::unique_ptr<rocksdb::Iterator> iter;
  iter.reset(batch.NewIteratorWithBase(rocksdb::NewEmptyIterator()));

  iter->SeekToFirst();
  if (iter->Valid() && kComparator->Compare(iter->key(), EncodeKey(start)) < 0) {
    return FmtStatus("key not in request range");
  }
  iter->SeekToLast();
  if (iter->Valid() && kComparator->Compare(iter->key(), EncodeKey(end)) >= 0) {
    return FmtStatus("key not in request range");
  }

  *stats = MVCCComputeStatsInternal(iter.get(), start, end, now_nanos);

  return kSuccess;
}


int LegacyTimestampCompare(
  const cockroach::util::hlc::LegacyTimestamp& t1,
  const cockroach::util::hlc::LegacyTimestamp& t2
) {
  if (t1.wall_time() != t2.wall_time()) {
    if (t1.wall_time() < t2.wall_time()) {
      return -1;
    }
    return 1;
  }
  if (t1.logical() == t2.logical()) {
    return 0;
  } else if (t1.logical() < t2.logical()) {
    return -1;
  }
  return 1;
}


DBStatus DBIncIterToSst(
  DBIterator* iter,
  DBKey start,
  DBKey end,
  bool revisions,
  DBString* data,
  int64_t* entries,
  int64_t* data_size,
  DBString* intentErr
) {
  DBSstFileWriter* sst = DBSstFileWriterNew();
  DBStatus status = DBSstFileWriterOpen(sst);
  if (status.data != NULL) {
    return status;
  }

  cockroach::util::hlc::LegacyTimestamp start_time;
  cockroach::util::hlc::LegacyTimestamp end_time;

  start_time.set_wall_time(start.wall_time);
  start_time.set_logical(start.logical);
  end_time.set_wall_time(end.wall_time);
  end_time.set_logical(end.logical);

  // Skip deletes iff non-incremental (i.e. start == 0) and not doing mvcc revs.
  bool skipDeletes = start.wall_time == 0 && start.logical == 0 && !revisions;

  std::string end_key = std::string(end.key.data, end.key.len);

  *entries = 0;
  *data_size = 0;

  DBIterState state = DBIterSeek(iter, start);
  while (state.valid) {
    if (std::string(state.key.key.data, state.key.key.len) >= end_key) {
      // std::cout << prettyPrintKey(state.key) << " after " << prettyPrintKey(end) <<  " DONE!" <<  std::endl;
      break;
    }

    cockroach::storage::engine::enginepb::MVCCMetadata meta;
    if (state.key.wall_time != 0 || state.key.logical != 0) {
      meta.mutable_timestamp()->set_wall_time(state.key.wall_time);
      meta.mutable_timestamp()->set_logical(state.key.logical);
    } else {
      if (!meta.ParseFromArray(state.value.data, state.value.len)) {
        std::cout << "BAD BAD BAD" <<  std::endl;
        break;
      }
    }

    if (meta.has_txn()) {
      if (LegacyTimestampCompare(meta.timestamp(), end_time) < 0) {
        // std::cout << "DBIncIterReset end key" << rocksdb::Slice(iter->end_key).ToString(true) << std::endl;
        std::cout << "unresolved intent before our cutoff; give up." <<  std::endl;
        cockroach::roachpb::WriteIntentError err;
        cockroach::roachpb::Intent* intent = err.add_intents();
        intent->mutable_span()->set_key(ToString(state.key.key));
        intent->mutable_txn()->CopyFrom(meta.txn());
        *intentErr = ToDBString(err.SerializeAsString());
        DBSstFileWriterClose(sst);
        return ToDBString("write intent error");
      }
      std::cout << "txn is after when we're interested, so look for earlier version." <<  std::endl;
      state = DBIterNext(iter, false);
      continue;
    }
    if (LegacyTimestampCompare(meta.timestamp(), end_time) > 0) {
      // std::cout << prettyPrintKey(state.key) << " at time " << meta.timestamp().wall_time() << " too new for us " << end_time.wall_time() << " keep looking for something older." <<  std::endl;
      state = DBIterNext(iter, false);
      continue;
    }
    if (LegacyTimestampCompare(meta.timestamp(), start_time) <= 0) {
      // std::cout << prettyPrintKey(state.key) << " at time " << meta.timestamp().wall_time() << " too old for us " << end_time.wall_time() << " keep looking for something older." <<  std::endl;
      std::cout << "too old for us, go to next key." <<  std::endl;
      state = DBIterNext(iter, true);
      continue;
    }

    // Ignore delete tombstone (len=0)
    if (state.value.len == 0 && skipDeletes) {
      state = DBIterNext(iter, true);
      continue;
    }

    status = DBSstFileWriterAdd(sst, state.key, state.value);
    if (status.data != NULL) {
      // std::cout << prettyPrintKey(state.key) << " BAD BAD BAD " <<  std::endl;
      DBSstFileWriterClose(sst);
      return status;
    }
    (*entries)++;
    // TODO(dan): Account for timestamp in data_size;
    (*data_size) += state.key.key.len + state.value.len + 12;

    // std::cout << prettyPrintKey(state.key) << " at time " << meta.timestamp().wall_time() << " ADDED " << *entries <<  std::endl;
    if (revisions) {
      state = DBIterNext(iter, false);
    } else {
      state = DBIterNext(iter, true);
    }
  }

  if (*entries == 0) {
    return kSuccess;
  }
  auto res = DBSstFileWriterFinish(sst, data);
  DBSstFileWriterClose(sst);
  return res;
}
