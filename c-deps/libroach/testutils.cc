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

#include <google/protobuf/stubs/stringprintf.h>
#include <gtest/gtest.h>
#include <rocksdb/status.h>
#include <string>

using google::protobuf::StringPrintf;

namespace testutils {
rocksdb::Status compareErrorMessage(rocksdb::Status status, const char* err_msg) {
  if (strcmp("", err_msg) == 0) {
    // Expected success.
    if (status.ok()) {
      return rocksdb::Status::OK();
    } else {
      return rocksdb::Status::InvalidArgument(StringPrintf("expected success, got error \"%s\"", status.getState()));
    }
  }

  // Expected failure.
  if (status.ok()) {
    return rocksdb::Status::InvalidArgument(StringPrintf("expected error \"%s\", got success", err_msg));
  }
  if (strcmp(err_msg, status.getState()) == 0) {
    return rocksdb::Status::OK();
  }
  return rocksdb::Status::InvalidArgument(
      StringPrintf("expected error \"%s\", got \"%s\"", err_msg, status.getState()));
}

rocksdb::Status compareErrorMessage(rocksdb::Status status, std::string err_msg) {
  return compareErrorMessage(status, err_msg.c_str());
}

}  // namespace testutils
