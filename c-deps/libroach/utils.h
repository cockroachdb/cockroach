// Copyright 2017 The Cockroach Authors.
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
