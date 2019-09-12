// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "testutils.h"
#include <err.h>
#include <ftw.h>
#include <google/protobuf/stubs/stringprintf.h>
#include <gtest/gtest.h>
#include <regex>
#include <rocksdb/status.h>
#include <stdlib.h>
#include <string>
#include "fmt.h"

extern "C" {
// Tests are run in plain C++, we need a symbol for rocksDBLog, normally
// implemented on the Go side.
void __attribute__((weak)) rocksDBLog(bool, int, char*, int) {}
}  // extern "C"

namespace testutils {

static int nftw_unlink_cb(const char* name, const struct stat*, int type, struct FTW*) {
  if (type == FTW_DP) {
    return rmdir(name);
  } else if (type == FTW_F || type == FTW_SL) {
    return unlink(name);
  }
  return 0;
}

TempDirHandler::TempDirHandler() {
  const char* ostmpdir = getenv("TEMPDIR");
  if (ostmpdir == NULL) {
    ostmpdir = "/tmp";
  }
  size_t len = strlen(ostmpdir);
  if (ostmpdir[len - 1] == '/') {
    len--;
  }

  // mkdtemp needs a []char to modify.
  const char* dirname = "/roachccl.XXXXXX";
  char* tmpl = new char[len + strlen(dirname) + 1];
  strncpy(tmpl, ostmpdir, len);
  strcpy(&tmpl[len], dirname);

  if (mkdtemp(tmpl) == NULL) {
    err(1, "creating temporary directory %s", tmpl);
  }
  tmp_dir_ = tmpl;
  delete[] tmpl;
}

TempDirHandler::~TempDirHandler() {
  if (tmp_dir_ == "") {
    return;
  }
  const int fd_limit = 16;
  if (nftw(tmp_dir_.c_str(), nftw_unlink_cb, fd_limit, FTW_DEPTH | FTW_PHYS) != 0) {
    err(1, "removing temporary directory %s", tmp_dir_.c_str());
  }
}

std::string TempDirHandler::Path(const std::string& subpath) { return tmp_dir_ + "/" + subpath; }

rocksdb::Status compareErrorMessage(rocksdb::Status status, const char* err_msg, bool partial) {
  if (strcmp("", err_msg) == 0) {
    // Expected success.
    if (status.ok()) {
      return rocksdb::Status::OK();
    }
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("expected success, got error \"%s\"", status.getState()));
  }

  // Expected failure.
  if (status.ok()) {
    return rocksdb::Status::InvalidArgument(
        fmt::StringPrintf("expected error \"%s\", got success", err_msg));
  }
  std::regex re(err_msg);
  if (partial) {
    // Partial regexp match.
    std::cmatch cm;
    if (std::regex_search(status.getState(), cm, re)) {
      return rocksdb::Status::OK();
    }
  } else {
    // Full regexp match.
    if (std::regex_match(status.getState(), re)) {
      return rocksdb::Status::OK();
    }
  }

  return rocksdb::Status::InvalidArgument(
      fmt::StringPrintf("expected error \"%s\", got \"%s\"", err_msg, status.getState()));
}

rocksdb::Status compareErrorMessage(rocksdb::Status status, std::string err_msg, bool partial) {
  return compareErrorMessage(status, err_msg.c_str(), partial);
}

}  // namespace testutils
