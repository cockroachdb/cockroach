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

#include "utils.h"

static const std::string kTempFileNameSuffix = ".crdbtmp";

rocksdb::Status SafeWriteStringToFile(rocksdb::Env* env, const std::string& filename,
                                      const std::string& contents) {
  std::string tmpname = filename + kTempFileNameSuffix;
  auto status = rocksdb::WriteStringToFile(env, contents, tmpname, true /* should_sync */);
  if (status.ok()) {
    status = env->RenameFile(tmpname, filename);
  }
  if (!status.ok()) {
    env->DeleteFile(tmpname);
  }
  return status;
}

std::string PathAppend(const std::string& path1, const std::string& path2) {
  if (path2.size() == 0) {
    return path1;
  }
  if ((path1.size() > 0 && path1[path1.size() - 1] == '/') || path2[0] == '/') {
    // Separator is present at end of path1, or beginning of path2).
    return path1 + path2;
  }
  return path1 + '/' + path2;
}
