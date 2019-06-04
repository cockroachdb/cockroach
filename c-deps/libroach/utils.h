// Copyright 2017 The Cockroach Authors.
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

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <string>

// Write 'contents' to a temporary file, sync, rename to 'filename'.
// On non-OK status, the original file has not been touched.
rocksdb::Status SafeWriteStringToFile(rocksdb::Env* env, const std::string& filename,
                                      const std::string& contents);

// PathAppend takes two path components and returns the new path
// with a separator if not already present.
std::string PathAppend(const std::string& path1, const std::string& path2);
