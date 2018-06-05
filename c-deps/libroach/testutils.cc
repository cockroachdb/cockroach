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

#include "testutils.h"
#include <experimental/filesystem>
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
void __attribute__((weak)) rocksDBLog(char*, int) {}
}  // extern "C"

namespace testutils {

TempDirHandler::TempDirHandler() {}

TempDirHandler::~TempDirHandler() {
  if (tmp_dir_ == "") {
    return;
  }
  std::experimental::filesystem::remove_all(tmp_dir_);
}

bool TempDirHandler::Init() {
  auto fs_dir = std::experimental::filesystem::temp_directory_path() / "tmpccl.XXXXXX";

  // mkdtemp needs a []char to modify.
  char* tmpl = new char[strlen(fs_dir.c_str()) + 1];
  strncpy(tmpl, fs_dir.c_str(), strlen(fs_dir.c_str()));

  auto tmp_c_dir = mkdtemp(tmpl);
  if (tmp_c_dir != NULL) {
    tmp_dir_ = std::string(tmp_c_dir);
  } else {
    std::cerr << "Error creating temp directory" << std::endl;
  }

  delete[] tmpl;
  return (tmp_c_dir != NULL);
}

std::string TempDirHandler::Path(const std::string& subpath) {
  auto fullpath = std::experimental::filesystem::path(tmp_dir_) / subpath;
  return fullpath.string();
}

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
