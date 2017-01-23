// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

#ifndef ROACHLIBCCL_DB_H
#define ROACHLIBCCL_DB_H

#include <stdbool.h>
#include <stdint.h>

#include "../../../../storage/engine/rocksdb/db.h"

#ifdef __cplusplus
extern "C" {
#endif

// Bulk adds the file at the given path to a database. See the RocksDB
// documentation on `AddFile` for the various restrictions on what can be added.
DBStatus DBEngineAddFile(DBEngine* db, DBSlice path);

typedef struct DBSstFileWriter DBSstFileWriter;

// Creates a new SstFileWriter with the default configuration.
DBSstFileWriter* DBSstFileWriterNew();

// Opens a file at the given path for output of an sstable.
DBStatus DBSstFileWriterOpen(DBSstFileWriter* fw, DBSlice path);

// Adds a kv entry to the sstable being built. An error is returned if it is
// not greater than any previously added entry (according to the comparator
// configured during writer creation). `Open` must have been called. `Close`
// cannot have been called.
DBStatus DBSstFileWriterAdd(DBSstFileWriter* fw, DBKey key, DBSlice val);

// Closes the writer, flushing any remaining writes to disk and freeing
// memory and other resources. At least one kv entry must have been added.
DBStatus DBSstFileWriterClose(DBSstFileWriter* fw);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif // ROACHLIBCCL_DB_H

// local variables:
// mode: c++
// end:
