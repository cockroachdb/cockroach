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

#include "gmock/gmock-generated-nice-strict.h"

#include <string>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"

// This must not be defined inside the ::testing namespace, or it will
// clash with ::testing::Mock.
class Mock {
 public:
  Mock() {}

  MOCK_METHOD0(DoThis, void());

 private:
  GTEST_DISALLOW_COPY_AND_ASSIGN_(Mock);
};

namespace testing {
namespace gmock_nice_strict_test {

using testing::internal::string;
using testing::GMOCK_FLAG(verbose);
using testing::HasSubstr;
using testing::NaggyMock;
using testing::NiceMock;
using testing::StrictMock;

#if GTEST_HAS_STREAM_REDIRECTION
using testing::internal::CaptureStdout;
using testing::internal::GetCapturedStdout;
#endif

// Defines some mock classes needed by the tests.

class Foo {
 public:
  virtual ~Foo() {}

  virtual void DoThis() = 0;
  virtual int DoThat(bool flag) = 0;
};

class MockFoo : public Foo {
 public:
  MockFoo() {}
  void Delete() { delete this; }

  MOCK_METHOD0(DoThis, void());
  MOCK_METHOD1(DoThat, int(bool flag));

 private:
  GTEST_DISALLOW_COPY_AND_ASSIGN_(MockFoo);
};

class MockBar {
 public:
  explicit MockBar(const string& s) : str_(s) {}

  MockBar(char a1, char a2, string a3, string a4, int a5, int a6,
          const string& a7, const string& a8, bool a9, bool a10) {
    str_ = string() + a1 + a2 + a3 + a4 + static_cast<char>(a5) +
        static_cast<char>(a6) + a7 + a8 + (a9 ? 'T' : 'F') + (a10 ? 'T' : 'F');
  }

  virtual ~MockBar() {}

  const string& str() const { return str_; }

  MOCK_METHOD0(This, int());
  MOCK_METHOD2(That, string(int, bool));

 private:
  string str_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(MockBar);
};

#if GTEST_HAS_STREAM_REDIRECTION

// Tests that a raw mock generates warnings for uninteresting calls.
TEST(RawMockTest, WarningForUninterestingCall) {
  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "warning";

  MockFoo raw_foo;

  CaptureStdout();
  raw_foo.DoThis();
  raw_foo.DoThat(true);
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

// Tests that a raw mock generates warnings for uninteresting calls
// that delete the mock object.
TEST(RawMockTest, WarningForUninterestingCallAfterDeath) {
  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "warning";

  MockFoo* const raw_foo = new MockFoo;

  ON_CALL(*raw_foo, DoThis())
      .WillByDefault(Invoke(raw_foo, &MockFoo::Delete));

  CaptureStdout();
  raw_foo->DoThis();
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

// Tests that a raw mock generates informational logs for
// uninteresting calls.
TEST(RawMockTest, InfoForUninterestingCall) {
  MockFoo raw_foo;

  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "info";
  CaptureStdout();
  raw_foo.DoThis();
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

// Tests that a nice mock generates no warning for uninteresting calls.
TEST(NiceMockTest, NoWarningForUninterestingCall) {
  NiceMock<MockFoo> nice_foo;

  CaptureStdout();
  nice_foo.DoThis();
  nice_foo.DoThat(true);
  EXPECT_EQ("", GetCapturedStdout());
}

// Tests that a nice mock generates no warning for uninteresting calls
// that delete the mock object.
TEST(NiceMockTest, NoWarningForUninterestingCallAfterDeath) {
  NiceMock<MockFoo>* const nice_foo = new NiceMock<MockFoo>;

  ON_CALL(*nice_foo, DoThis())
      .WillByDefault(Invoke(nice_foo, &MockFoo::Delete));

  CaptureStdout();
  nice_foo->DoThis();
  EXPECT_EQ("", GetCapturedStdout());
}

// Tests that a nice mock generates informational logs for
// uninteresting calls.
TEST(NiceMockTest, InfoForUninterestingCall) {
  NiceMock<MockFoo> nice_foo;

  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "info";
  CaptureStdout();
  nice_foo.DoThis();
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

#endif  // GTEST_HAS_STREAM_REDIRECTION

// Tests that a nice mock allows expected calls.
TEST(NiceMockTest, AllowsExpectedCall) {
  NiceMock<MockFoo> nice_foo;

  EXPECT_CALL(nice_foo, DoThis());
  nice_foo.DoThis();
}

// Tests that an unexpected call on a nice mock fails.
TEST(NiceMockTest, UnexpectedCallFails) {
  NiceMock<MockFoo> nice_foo;

  EXPECT_CALL(nice_foo, DoThis()).Times(0);
  EXPECT_NONFATAL_FAILURE(nice_foo.DoThis(), "called more times than expected");
}

// Tests that NiceMock works with a mock class that has a non-default
// constructor.
TEST(NiceMockTest, NonDefaultConstructor) {
  NiceMock<MockBar> nice_bar("hi");
  EXPECT_EQ("hi", nice_bar.str());

  nice_bar.This();
  nice_bar.That(5, true);
}

// Tests that NiceMock works with a mock class that has a 10-ary
// non-default constructor.
TEST(NiceMockTest, NonDefaultConstructor10) {
  NiceMock<MockBar> nice_bar('a', 'b', "c", "d", 'e', 'f',
                             "g", "h", true, false);
  EXPECT_EQ("abcdefghTF", nice_bar.str());

  nice_bar.This();
  nice_bar.That(5, true);
}

#if !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE
// Tests that NiceMock<Mock> compiles where Mock is a user-defined
// class (as opposed to ::testing::Mock).  We had to work around an
// MSVC 8.0 bug that caused the symbol Mock used in the definition of
// NiceMock to be looked up in the wrong context, and this test
// ensures that our fix works.
//
// We have to skip this test on Symbian and Windows Mobile, as it
// causes the program to crash there, for reasons unclear to us yet.
TEST(NiceMockTest, AcceptsClassNamedMock) {
  NiceMock< ::Mock> nice;
  EXPECT_CALL(nice, DoThis());
  nice.DoThis();
}
#endif  // !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE

#if GTEST_HAS_STREAM_REDIRECTION

// Tests that a naggy mock generates warnings for uninteresting calls.
TEST(NaggyMockTest, WarningForUninterestingCall) {
  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "warning";

  NaggyMock<MockFoo> naggy_foo;

  CaptureStdout();
  naggy_foo.DoThis();
  naggy_foo.DoThat(true);
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

// Tests that a naggy mock generates a warning for an uninteresting call
// that deletes the mock object.
TEST(NaggyMockTest, WarningForUninterestingCallAfterDeath) {
  const string saved_flag = GMOCK_FLAG(verbose);
  GMOCK_FLAG(verbose) = "warning";

  NaggyMock<MockFoo>* const naggy_foo = new NaggyMock<MockFoo>;

  ON_CALL(*naggy_foo, DoThis())
      .WillByDefault(Invoke(naggy_foo, &MockFoo::Delete));

  CaptureStdout();
  naggy_foo->DoThis();
  EXPECT_THAT(GetCapturedStdout(),
              HasSubstr("Uninteresting mock function call"));

  GMOCK_FLAG(verbose) = saved_flag;
}

#endif  // GTEST_HAS_STREAM_REDIRECTION

// Tests that a naggy mock allows expected calls.
TEST(NaggyMockTest, AllowsExpectedCall) {
  NaggyMock<MockFoo> naggy_foo;

  EXPECT_CALL(naggy_foo, DoThis());
  naggy_foo.DoThis();
}

// Tests that an unexpected call on a naggy mock fails.
TEST(NaggyMockTest, UnexpectedCallFails) {
  NaggyMock<MockFoo> naggy_foo;

  EXPECT_CALL(naggy_foo, DoThis()).Times(0);
  EXPECT_NONFATAL_FAILURE(naggy_foo.DoThis(),
                          "called more times than expected");
}

// Tests that NaggyMock works with a mock class that has a non-default
// constructor.
TEST(NaggyMockTest, NonDefaultConstructor) {
  NaggyMock<MockBar> naggy_bar("hi");
  EXPECT_EQ("hi", naggy_bar.str());

  naggy_bar.This();
  naggy_bar.That(5, true);
}

// Tests that NaggyMock works with a mock class that has a 10-ary
// non-default constructor.
TEST(NaggyMockTest, NonDefaultConstructor10) {
  NaggyMock<MockBar> naggy_bar('0', '1', "2", "3", '4', '5',
                               "6", "7", true, false);
  EXPECT_EQ("01234567TF", naggy_bar.str());

  naggy_bar.This();
  naggy_bar.That(5, true);
}

#if !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE
// Tests that NaggyMock<Mock> compiles where Mock is a user-defined
// class (as opposed to ::testing::Mock).  We had to work around an
// MSVC 8.0 bug that caused the symbol Mock used in the definition of
// NaggyMock to be looked up in the wrong context, and this test
// ensures that our fix works.
//
// We have to skip this test on Symbian and Windows Mobile, as it
// causes the program to crash there, for reasons unclear to us yet.
TEST(NaggyMockTest, AcceptsClassNamedMock) {
  NaggyMock< ::Mock> naggy;
  EXPECT_CALL(naggy, DoThis());
  naggy.DoThis();
}
#endif  // !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE

// Tests that a strict mock allows expected calls.
TEST(StrictMockTest, AllowsExpectedCall) {
  StrictMock<MockFoo> strict_foo;

  EXPECT_CALL(strict_foo, DoThis());
  strict_foo.DoThis();
}

// Tests that an unexpected call on a strict mock fails.
TEST(StrictMockTest, UnexpectedCallFails) {
  StrictMock<MockFoo> strict_foo;

  EXPECT_CALL(strict_foo, DoThis()).Times(0);
  EXPECT_NONFATAL_FAILURE(strict_foo.DoThis(),
                          "called more times than expected");
}

// Tests that an uninteresting call on a strict mock fails.
TEST(StrictMockTest, UninterestingCallFails) {
  StrictMock<MockFoo> strict_foo;

  EXPECT_NONFATAL_FAILURE(strict_foo.DoThis(),
                          "Uninteresting mock function call");
}

// Tests that an uninteresting call on a strict mock fails, even if
// the call deletes the mock object.
TEST(StrictMockTest, UninterestingCallFailsAfterDeath) {
  StrictMock<MockFoo>* const strict_foo = new StrictMock<MockFoo>;

  ON_CALL(*strict_foo, DoThis())
      .WillByDefault(Invoke(strict_foo, &MockFoo::Delete));

  EXPECT_NONFATAL_FAILURE(strict_foo->DoThis(),
                          "Uninteresting mock function call");
}

// Tests that StrictMock works with a mock class that has a
// non-default constructor.
TEST(StrictMockTest, NonDefaultConstructor) {
  StrictMock<MockBar> strict_bar("hi");
  EXPECT_EQ("hi", strict_bar.str());

  EXPECT_NONFATAL_FAILURE(strict_bar.That(5, true),
                          "Uninteresting mock function call");
}

// Tests that StrictMock works with a mock class that has a 10-ary
// non-default constructor.
TEST(StrictMockTest, NonDefaultConstructor10) {
  StrictMock<MockBar> strict_bar('a', 'b', "c", "d", 'e', 'f',
                                 "g", "h", true, false);
  EXPECT_EQ("abcdefghTF", strict_bar.str());

  EXPECT_NONFATAL_FAILURE(strict_bar.That(5, true),
                          "Uninteresting mock function call");
}

#if !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE
// Tests that StrictMock<Mock> compiles where Mock is a user-defined
// class (as opposed to ::testing::Mock).  We had to work around an
// MSVC 8.0 bug that caused the symbol Mock used in the definition of
// StrictMock to be looked up in the wrong context, and this test
// ensures that our fix works.
//
// We have to skip this test on Symbian and Windows Mobile, as it
// causes the program to crash there, for reasons unclear to us yet.
TEST(StrictMockTest, AcceptsClassNamedMock) {
  StrictMock< ::Mock> strict;
  EXPECT_CALL(strict, DoThis());
  strict.DoThis();
}
#endif  // !GTEST_OS_SYMBIAN && !GTEST_OS_WINDOWS_MOBILE

}  // namespace gmock_nice_strict_test
}  // namespace testing
