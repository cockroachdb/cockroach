// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <string>

// Write 'contents' to a temporary file, sync, rename to 'filename'.
// On non-OK status, either the original or new file will be readable,
// and not some intermediate corrupted state.
rocksdb::Status SafeWriteStringToFile(rocksdb::Env* env, rocksdb::Directory* dir,
                                      const std::string& filename, const std::string& contents);

// PathAppend takes two path components and returns the new path
// with a separator if not already present.
std::string PathAppend(const std::string& path1, const std::string& path2);
