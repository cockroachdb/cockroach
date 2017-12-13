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

#include <gtest/gtest.h>
#include <rocksdb/status.h>
#include <string>

namespace testutils {

rocksdb::Status compareErrorMessage(rocksdb::Status status, const char* err_msg);
rocksdb::Status compareErrorMessage(rocksdb::Status status, std::string err_msg);

}  // namespace testutils

// clang-format isn't so great for macros.
// clang-format off

#define EXPECT_OK(status) { auto s(status); EXPECT_TRUE(s.ok()) << "got: " << s.getState(); }
#define ASSERT_OK(status) { auto s(status); ASSERT_TRUE(s.ok()) << "got: " << s.getState(); }

// If err_msg is empty, status must be ok. Otherwise, the status message must match
// the regexp err_msg (full match).
#define EXPECT_ERR(status, err_msg)\
  {\
    auto s(testutils::compareErrorMessage(status, err_msg)); \
    EXPECT_TRUE(s.ok()) << s.getState();\
  }

// clang-format on
