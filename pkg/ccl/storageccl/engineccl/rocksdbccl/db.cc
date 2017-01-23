// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

#include <algorithm>
#include <atomic>
#include <stdarg.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/stringprintf.h>
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "cockroach/pkg/roachpb/data.pb.h"
#include "cockroach/pkg/roachpb/internal.pb.h"
#include "cockroach/pkg/storage/engine/enginepb/rocksdb.pb.h"
#include "cockroach/pkg/storage/engine/enginepb/mvcc.pb.h"
#include "db.h"
#include "db_cpp.h"
#include "encoding.h"
#include "eventlistener.h"

#include <iostream>

extern "C" {
#include "_cgo_export.h"
}

const DBStatus kSuccess = { NULL, 0 };

DBStatus DBEngineAddFile(DBEngine* db, DBSlice path) {
  const std::vector<std::string> paths = { ToString(path) };
  rocksdb::Status status = RawDB(db)->AddFile(paths);
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

struct DBSstFileWriter {
  std::unique_ptr<rocksdb::Options> options;
  rocksdb::SstFileWriter rep;

  DBSstFileWriter(rocksdb::Options* o)
      : options(o),
        rep(rocksdb::EnvOptions(), *o, o->comparator) {
  }
  virtual ~DBSstFileWriter() { }
};

DBSstFileWriter* DBSstFileWriterNew() {
  rocksdb::Options* options = new rocksdb::Options();
  options->comparator = CockroachComparator();
  return new DBSstFileWriter(options);
}

DBStatus DBSstFileWriterOpen(DBSstFileWriter* fw, DBSlice path) {
  rocksdb::Status status = fw->rep.Open(ToString(path));
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

DBStatus DBSstFileWriterAdd(DBSstFileWriter* fw, DBKey key, DBSlice val) {
  rocksdb::Status status = fw->rep.Add(EncodeKey(key), ToSlice(val));
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}

DBStatus DBSstFileWriterClose(DBSstFileWriter* fw) {
  rocksdb::Status status = fw->rep.Finish();
  delete fw;
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  return kSuccess;
}
