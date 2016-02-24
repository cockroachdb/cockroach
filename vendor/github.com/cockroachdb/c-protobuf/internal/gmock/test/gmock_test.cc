// Copyright 2008, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Author: wan@google.com (Zhanyong Wan)

// Google Mock - a framework for writing C++ mock classes.
//
// This file tests code in gmock.cc.

#include "gmock/gmock.h"

#include <string>
#include "gtest/gtest.h"

using testing::GMOCK_FLAG(verbose);
using testing::InitGoogleMock;
using testing::internal::g_init_gtest_count;

// Verifies that calling InitGoogleMock() on argv results in new_argv,
// and the gmock_verbose flag's value is set to expected_gmock_verbose.
template <typename Char, int M, int N>
void TestInitGoogleMock(const Char* (&argv)[M], const Char* (&new_argv)[N],
                        const ::std::string& expected_gmock_verbose) {
  const ::std::string old_verbose = GMOCK_FLAG(verbose);

  int argc = M;
  InitGoogleMock(&argc, const_cast<Char**>(argv));
  ASSERT_EQ(N, argc) << "The new argv has wrong number of elements.";

  for (int i = 0; i < N; i++) {
    EXPECT_STREQ(new_argv[i], argv[i]);
  }

  EXPECT_EQ(expected_gmock_verbose, GMOCK_FLAG(verbose).c_str());
  GMOCK_FLAG(verbose) = old_verbose;  // Restores the gmock_verbose flag.
}

TEST(InitGoogleMockTest, ParsesInvalidCommandLine) {
  const char* argv[] = {
    NULL
  };

  const char* new_argv[] = {
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(InitGoogleMockTest, ParsesEmptyCommandLine) {
  const char* argv[] = {
    "foo.exe",
    NULL
  };

  const char* new_argv[] = {
    "foo.exe",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(InitGoogleMockTest, ParsesSingleFlag) {
  const char* argv[] = {
    "foo.exe",
    "--gmock_verbose=info",
    NULL
  };

  const char* new_argv[] = {
    "foo.exe",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "info");
}

TEST(InitGoogleMockTest, ParsesUnrecognizedFlag) {
  const char* argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    NULL
  };

  const char* new_argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(InitGoogleMockTest, ParsesGoogleMockFlagAndUnrecognizedFlag) {
  const char* argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    "--gmock_verbose=error",
    NULL
  };

  const char* new_argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "error");
}

TEST(InitGoogleMockTest, CallsInitGoogleTest) {
  const int old_init_gtest_count = g_init_gtest_count;
  const char* argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    "--gmock_verbose=error",
    NULL
  };

  const char* new_argv[] = {
    "foo.exe",
    "--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "error");
  EXPECT_EQ(old_init_gtest_count + 1, g_init_gtest_count);
}

TEST(WideInitGoogleMockTest, ParsesInvalidCommandLine) {
  const wchar_t* argv[] = {
    NULL
  };

  const wchar_t* new_argv[] = {
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(WideInitGoogleMockTest, ParsesEmptyCommandLine) {
  const wchar_t* argv[] = {
    L"foo.exe",
    NULL
  };

  const wchar_t* new_argv[] = {
    L"foo.exe",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(WideInitGoogleMockTest, ParsesSingleFlag) {
  const wchar_t* argv[] = {
    L"foo.exe",
    L"--gmock_verbose=info",
    NULL
  };

  const wchar_t* new_argv[] = {
    L"foo.exe",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "info");
}

TEST(WideInitGoogleMockTest, ParsesUnrecognizedFlag) {
  const wchar_t* argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    NULL
  };

  const wchar_t* new_argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, GMOCK_FLAG(verbose));
}

TEST(WideInitGoogleMockTest, ParsesGoogleMockFlagAndUnrecognizedFlag) {
  const wchar_t* argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    L"--gmock_verbose=error",
    NULL
  };

  const wchar_t* new_argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "error");
}

TEST(WideInitGoogleMockTest, CallsInitGoogleTest) {
  const int old_init_gtest_count = g_init_gtest_count;
  const wchar_t* argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    L"--gmock_verbose=error",
    NULL
  };

  const wchar_t* new_argv[] = {
    L"foo.exe",
    L"--non_gmock_flag=blah",
    NULL
  };

  TestInitGoogleMock(argv, new_argv, "error");
  EXPECT_EQ(old_init_gtest_count + 1, g_init_gtest_count);
}

// Makes sure Google Mock flags can be accessed in code.
TEST(FlagTest, IsAccessibleInCode) {
  bool dummy = testing::GMOCK_FLAG(catch_leaked_mocks) &&
      testing::GMOCK_FLAG(verbose) == "";
  (void)dummy;  // Avoids the "unused local variable" warning.
}
