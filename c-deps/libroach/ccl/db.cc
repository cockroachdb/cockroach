// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

#include <iostream>
#include <memory>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include "../protosccl/ccl/baseccl/encryption_options.pb.h"

#include <libroachccl.h>

#include "../protos/roachpb/errors.pb.h"
#include "../protos/storage/engine/enginepb/mvcc.pb.h"

#include "../db.h"

DBStatus parse_extra_options(const DBSlice s) {
  if (s.len == 0) {
    return kSuccess;
  }

  cockroach::ccl::baseccl::EncryptionOptions opts;
  if (!opts.ParseFromArray(s.data, s.len)) {
    return FmtStatus("failed to parse extra options");
  }

  if (opts.key_source() != cockroach::ccl::baseccl::KeyFiles) {
    return FmtStatus("unknown encryption key source: %d", opts.key_source());
  }

  std::cout << "found encryption options:\n"
            << "  active key: " << opts.key_files().current_key() << "\n"
            << "  old key: " << opts.key_files().old_key() << "\n"
            << "  rotation duration: " << opts.data_key_rotation_period() << "\n";

  return FmtStatus("encryption is not supported");
}

// DBOpenHook parses the extra_options field of DBOptions.
DBStatus DBOpenHook(const DBOptions opts) { return parse_extra_options(opts.extra_options); }

DBStatus DBBatchReprVerify(DBSlice repr, DBKey start, DBKey end, int64_t now_nanos, MVCCStatsResult* stats) {
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

int LegacyTimestampCompare(const cockroach::util::hlc::LegacyTimestamp& t1,
                           const cockroach::util::hlc::LegacyTimestamp& t2) {
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

DBStatus DBIncIterToSst(DBIterator* iter, DBKey start, DBKey end, bool revisions, DBString* data, int64_t* entries,
                        int64_t* data_size, DBString* intentErr) {
  const rocksdb::Comparator* kComparator = CockroachComparator();

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

  auto iter_rep = iter->rep.get();
  const char* err = NULL;

  iter->rep->Seek(EncodeKey(start));

  while (err == NULL && iter_rep->Valid()) {
    rocksdb::Slice key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(iter->rep->key(), &key, &wall_time, &logical)) {
      return ToDBString("unable to decode key");
    }

    if (kComparator->Compare(key, end_key) >= 0) {
      break;
    }

    auto value = iter_rep->value();

    cockroach::storage::engine::enginepb::MVCCMetadata meta;
    if (wall_time != 0 || logical != 0) {
      meta.mutable_timestamp()->set_wall_time(wall_time);
      meta.mutable_timestamp()->set_logical(logical);
    } else {
      if (!meta.ParseFromArray(value.data(), value.size())) {
        return ToDBString("failed to parse meta");
      }
    }

    if (meta.has_txn()) {
      if (LegacyTimestampCompare(meta.timestamp(), end_time) < 0) {
        cockroach::roachpb::WriteIntentError err;
        cockroach::roachpb::Intent* intent = err.add_intents();
        intent->mutable_span()->set_key(key.data(), key.size());
        intent->mutable_txn()->CopyFrom(meta.txn());
        *intentErr = ToDBString(err.SerializeAsString());
        DBSstFileWriterClose(sst);
        return ToDBString("write intent error");
      }
      err = DBIterAdvance(iter, false);
      continue;
    }
    if (LegacyTimestampCompare(meta.timestamp(), end_time) > 0) {
      err = DBIterAdvance(iter, false);
      continue;
    }
    if (LegacyTimestampCompare(meta.timestamp(), start_time) <= 0) {
      err = DBIterAdvance(iter, true);
      continue;
    }

    // Ignore delete tombstone (len=0)
    if (value.size() == 0 && skipDeletes) {
      err = DBIterAdvance(iter, true);
      continue;
    }

    status = DBSstFileWriterAddRaw(sst, iter->rep->key(), value);
    if (status.data != NULL) {
      DBSstFileWriterClose(sst);
      return status;
    }
    (*entries)++;
    (*data_size) += iter->rep->key().size() + value.size();

    if (revisions) {
      err = DBIterAdvance(iter, false);
    } else {
      err = DBIterAdvance(iter, true);
    }
  }

  if (err != NULL) {
    return ToDBString(err);
  }

  if (*entries == 0) {
    return kSuccess;
  }
  auto res = DBSstFileWriterFinish(sst, data);
  DBSstFileWriterClose(sst);
  return res;
}
