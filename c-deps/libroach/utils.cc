// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils.h"

static const std::string kTempFileNameSuffix = ".crdbtmp";

rocksdb::Status SafeWriteStringToFile(rocksdb::Env* env, rocksdb::Directory* dir,
                                      const std::string& filename, const std::string& contents) {
  std::string tmpname = filename + kTempFileNameSuffix;
  auto status = rocksdb::WriteStringToFile(env, contents, tmpname, true /* should_sync */);
  if (status.ok()) {
    status = env->RenameFile(tmpname, filename);
  }
  if (!status.ok()) {
    env->DeleteFile(tmpname);
    return status;
  }
  return dir->Fsync();
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
