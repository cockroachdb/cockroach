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
// Author: mheule@google.com (Markus Heule)
//
// Google C++ Testing Framework (Google Test)
//
// Sometimes it's desirable to build Google Test by compiling a single file.
// This file serves this purpose.

// This line ensures that gtest.h can be compiled on its own, even
// when it's fused.
#include "gtest/gtest.h"

// The following lines pull in the real gtest *.cc files.
// Copyright 2005, Google Inc.
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
//
// The Google C++ Testing Framework (Google Test)

// Copyright 2007, Google Inc.
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
//
// Utilities for testing Google Test itself and code that uses Google Test
// (e.g. frameworks built on top of Google Test).

#ifndef GTEST_INCLUDE_GTEST_GTEST_SPI_H_
#define GTEST_INCLUDE_GTEST_GTEST_SPI_H_


namespace testing {

// This helper class can be used to mock out Google Test failure reporting
// so that we can test Google Test or code that builds on Google Test.
//
// An object of this class appends a TestPartResult object to the
// TestPartResultArray object given in the constructor whenever a Google Test
// failure is reported. It can either intercept only failures that are
// generated in the same thread that created this object or it can intercept
// all generated failures. The scope of this mock object can be controlled with
// the second argument to the two arguments constructor.
class GTEST_API_ ScopedFakeTestPartResultReporter
    : public TestPartResultReporterInterface {
 public:
  // The two possible mocking modes of this object.
  enum InterceptMode {
    INTERCEPT_ONLY_CURRENT_THREAD,  // Intercepts only thread local failures.
    INTERCEPT_ALL_THREADS           // Intercepts all failures.
  };

  // The c'tor sets this object as the test part result reporter used
  // by Google Test.  The 'result' parameter specifies where to report the
  // results. This reporter will only catch failures generated in the current
  // thread. DEPRECATED
  explicit ScopedFakeTestPartResultReporter(TestPartResultArray* result);

  // Same as above, but you can choose the interception scope of this object.
  ScopedFakeTestPartResultReporter(InterceptMode intercept_mode,
                                   TestPartResultArray* result);

  // The d'tor restores the previous test part result reporter.
  virtual ~ScopedFakeTestPartResultReporter();

  // Appends the TestPartResult object to the TestPartResultArray
  // received in the constructor.
  //
  // This method is from the TestPartResultReporterInterface
  // interface.
  virtual void ReportTestPartResult(const TestPartResult& result);
 private:
  void Init();

  const InterceptMode intercept_mode_;
  TestPartResultReporterInterface* old_reporter_;
  TestPartResultArray* const result_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(ScopedFakeTestPartResultReporter);
};

namespace internal {

// A helper class for implementing EXPECT_FATAL_FAILURE() and
// EXPECT_NONFATAL_FAILURE().  Its destructor verifies that the given
// TestPartResultArray contains exactly one failure that has the given
// type and contains the given substring.  If that's not the case, a
// non-fatal failure will be generated.
class GTEST_API_ SingleFailureChecker {
 public:
  // The constructor remembers the arguments.
  SingleFailureChecker(const TestPartResultArray* results,
                       TestPartResult::Type type,
                       const string& substr);
  ~SingleFailureChecker();
 private:
  const TestPartResultArray* const results_;
  const TestPartResult::Type type_;
  const string substr_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(SingleFailureChecker);
};

}  // namespace internal

}  // namespace testing

// A set of macros for testing Google Test assertions or code that's expected
// to generate Google Test fatal failures.  It verifies that the given
// statement will cause exactly one fatal Google Test failure with 'substr'
// being part of the failure message.
//
// There are two different versions of this macro. EXPECT_FATAL_FAILURE only
// affects and considers failures generated in the current thread and
// EXPECT_FATAL_FAILURE_ON_ALL_THREADS does the same but for all threads.
//
// The verification of the assertion is done correctly even when the statement
// throws an exception or aborts the current function.
//
// Known restrictions:
//   - 'statement' cannot reference local non-static variables or
//     non-static members of the current object.
//   - 'statement' cannot return a value.
//   - You cannot stream a failure message to this macro.
//
// Note that even though the implementations of the following two
// macros are much alike, we cannot refactor them to use a common
// helper macro, due to some peculiarity in how the preprocessor
// works.  The AcceptsMacroThatExpandsToUnprotectedComma test in
// gtest_unittest.cc will fail to compile if we do that.
#define EXPECT_FATAL_FAILURE(statement, substr) \
  do { \
    class GTestExpectFatalFailureHelper {\
     public:\
      static void Execute() { statement; }\
    };\
    ::testing::TestPartResultArray gtest_failures;\
    ::testing::internal::SingleFailureChecker gtest_checker(\
        &gtest_failures, ::testing::TestPartResult::kFatalFailure, (substr));\
    {\
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(\
          ::testing::ScopedFakeTestPartResultReporter:: \
          INTERCEPT_ONLY_CURRENT_THREAD, &gtest_failures);\
      GTestExpectFatalFailureHelper::Execute();\
    }\
  } while (::testing::internal::AlwaysFalse())

#define EXPECT_FATAL_FAILURE_ON_ALL_THREADS(statement, substr) \
  do { \
    class GTestExpectFatalFailureHelper {\
     public:\
      static void Execute() { statement; }\
    };\
    ::testing::TestPartResultArray gtest_failures;\
    ::testing::internal::SingleFailureChecker gtest_checker(\
        &gtest_failures, ::testing::TestPartResult::kFatalFailure, (substr));\
    {\
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(\
          ::testing::ScopedFakeTestPartResultReporter:: \
          INTERCEPT_ALL_THREADS, &gtest_failures);\
      GTestExpectFatalFailureHelper::Execute();\
    }\
  } while (::testing::internal::AlwaysFalse())

// A macro for testing Google Test assertions or code that's expected to
// generate Google Test non-fatal failures.  It asserts that the given
// statement will cause exactly one non-fatal Google Test failure with 'substr'
// being part of the failure message.
//
// There are two different versions of this macro. EXPECT_NONFATAL_FAILURE only
// affects and considers failures generated in the current thread and
// EXPECT_NONFATAL_FAILURE_ON_ALL_THREADS does the same but for all threads.
//
// 'statement' is allowed to reference local variables and members of
// the current object.
//
// The verification of the assertion is done correctly even when the statement
// throws an exception or aborts the current function.
//
// Known restrictions:
//   - You cannot stream a failure message to this macro.
//
// Note that even though the implementations of the following two
// macros are much alike, we cannot refactor them to use a common
// helper macro, due to some peculiarity in how the preprocessor
// works.  If we do that, the code won't compile when the user gives
// EXPECT_NONFATAL_FAILURE() a statement that contains a macro that
// expands to code containing an unprotected comma.  The
// AcceptsMacroThatExpandsToUnprotectedComma test in gtest_unittest.cc
// catches that.
//
// For the same reason, we have to write
//   if (::testing::internal::AlwaysTrue()) { statement; }
// instead of
//   GTEST_SUPPRESS_UNREACHABLE_CODE_WARNING_BELOW_(statement)
// to avoid an MSVC warning on unreachable code.
#define EXPECT_NONFATAL_FAILURE(statement, substr) \
  do {\
    ::testing::TestPartResultArray gtest_failures;\
    ::testing::internal::SingleFailureChecker gtest_checker(\
        &gtest_failures, ::testing::TestPartResult::kNonFatalFailure, \
        (substr));\
    {\
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(\
          ::testing::ScopedFakeTestPartResultReporter:: \
          INTERCEPT_ONLY_CURRENT_THREAD, &gtest_failures);\
      if (::testing::internal::AlwaysTrue()) { statement; }\
    }\
  } while (::testing::internal::AlwaysFalse())

#define EXPECT_NONFATAL_FAILURE_ON_ALL_THREADS(statement, substr) \
  do {\
    ::testing::TestPartResultArray gtest_failures;\
    ::testing::internal::SingleFailureChecker gtest_checker(\
        &gtest_failures, ::testing::TestPartResult::kNonFatalFailure, \
        (substr));\
    {\
      ::testing::ScopedFakeTestPartResultReporter gtest_reporter(\
          ::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ALL_THREADS, \
          &gtest_failures);\
      if (::testing::internal::AlwaysTrue()) { statement; }\
    }\
  } while (::testing::internal::AlwaysFalse())

#endif  // GTEST_INCLUDE_GTEST_GTEST_SPI_H_

#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <wchar.h>
#include <wctype.h>

#include <algorithm>
#include <iomanip>
#include <limits>
#include <ostream>  // NOLINT
#include <sstream>
#include <vector>

#if GTEST_OS_LINUX

// TODO(kenton@google.com): Use autoconf to detect availability of
// gettimeofday().
# define GTEST_HAS_GETTIMEOFDAY_ 1

# include <fcntl.h>  // NOLINT
# include <limits.h>  // NOLINT
# include <sched.h>  // NOLINT
// Declares vsnprintf().  This header is not available on Windows.
# include <strings.h>  // NOLINT
# include <sys/mman.h>  // NOLINT
# include <sys/time.h>  // NOLINT
# include <unistd.h>  // NOLINT
# include <string>

#elif GTEST_OS_SYMBIAN
# define GTEST_HAS_GETTIMEOFDAY_ 1
# include <sys/time.h>  // NOLINT

#elif GTEST_OS_ZOS
# define GTEST_HAS_GETTIMEOFDAY_ 1
# include <sys/time.h>  // NOLINT

// On z/OS we additionally need strings.h for strcasecmp.
# include <strings.h>  // NOLINT

#elif GTEST_OS_WINDOWS_MOBILE  // We are on Windows CE.

# include <windows.h>  // NOLINT

#elif GTEST_OS_WINDOWS  // We are on Windows proper.

# include <io.h>  // NOLINT
# include <sys/timeb.h>  // NOLINT
# include <sys/types.h>  // NOLINT
# include <sys/stat.h>  // NOLINT

# if GTEST_OS_WINDOWS_MINGW
// MinGW has gettimeofday() but not _ftime64().
// TODO(kenton@google.com): Use autoconf to detect availability of
//   gettimeofday().
// TODO(kenton@google.com): There are other ways to get the time on
//   Windows, like GetTickCount() or GetSystemTimeAsFileTime().  MinGW
//   supports these.  consider using them instead.
#  define GTEST_HAS_GETTIMEOFDAY_ 1
#  include <sys/time.h>  // NOLINT
# endif  // GTEST_OS_WINDOWS_MINGW

// cpplint thinks that the header is already included, so we want to
// silence it.
# include <windows.h>  // NOLINT

#else

// Assume other platforms have gettimeofday().
// TODO(kenton@google.com): Use autoconf to detect availability of
//   gettimeofday().
# define GTEST_HAS_GETTIMEOFDAY_ 1

// cpplint thinks that the header is already included, so we want to
// silence it.
# include <sys/time.h>  // NOLINT
# include <unistd.h>  // NOLINT

#endif  // GTEST_OS_LINUX

#if GTEST_HAS_EXCEPTIONS
# include <stdexcept>
#endif

#if GTEST_CAN_STREAM_RESULTS_
# include <arpa/inet.h>  // NOLINT
# include <netdb.h>  // NOLINT
#endif

// Indicates that this translation unit is part of Google Test's
// implementation.  It must come before gtest-internal-inl.h is
// included, or there will be a compiler error.  This trick is to
// prevent a user from accidentally including gtest-internal-inl.h in
// his code.
#define GTEST_IMPLEMENTATION_ 1
// Copyright 2005, Google Inc.
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

// Utility functions and classes used by the Google C++ testing framework.
//
// Author: wan@google.com (Zhanyong Wan)
//
// This file contains purely Google Test's internal implementation.  Please
// DO NOT #INCLUDE IT IN A USER PROGRAM.

#ifndef GTEST_SRC_GTEST_INTERNAL_INL_H_
#define GTEST_SRC_GTEST_INTERNAL_INL_H_

// GTEST_IMPLEMENTATION_ is defined to 1 iff the current translation unit is
// part of Google Test's implementation; otherwise it's undefined.
#if !GTEST_IMPLEMENTATION_
// A user is trying to include this from his code - just say no.
# error "gtest-internal-inl.h is part of Google Test's internal implementation."
# error "It must not be included except by Google Test itself."
#endif  // GTEST_IMPLEMENTATION_

#ifndef _WIN32_WCE
# include <errno.h>
#endif  // !_WIN32_WCE
#include <stddef.h>
#include <stdlib.h>  // For strtoll/_strtoul64/malloc/free.
#include <string.h>  // For memmove.

#include <algorithm>
#include <string>
#include <vector>


#if GTEST_CAN_STREAM_RESULTS_
# include <arpa/inet.h>  // NOLINT
# include <netdb.h>  // NOLINT
#endif

#if GTEST_OS_WINDOWS
# include <windows.h>  // NOLINT
#endif  // GTEST_OS_WINDOWS


namespace testing {

// Declares the flags.
//
// We don't want the users to modify this flag in the code, but want
// Google Test's own unit tests to be able to access it. Therefore we
// declare it here as opposed to in gtest.h.
GTEST_DECLARE_bool_(death_test_use_fork);

namespace internal {

// The value of GetTestTypeId() as seen from within the Google Test
// library.  This is solely for testing GetTestTypeId().
GTEST_API_ extern const TypeId kTestTypeIdInGoogleTest;

// Names of the flags (needed for parsing Google Test flags).
const char kAlsoRunDisabledTestsFlag[] = "also_run_disabled_tests";
const char kBreakOnFailureFlag[] = "break_on_failure";
const char kCatchExceptionsFlag[] = "catch_exceptions";
const char kColorFlag[] = "color";
const char kFilterFlag[] = "filter";
const char kListTestsFlag[] = "list_tests";
const char kOutputFlag[] = "output";
const char kPrintTimeFlag[] = "print_time";
const char kRandomSeedFlag[] = "random_seed";
const char kRepeatFlag[] = "repeat";
const char kShuffleFlag[] = "shuffle";
const char kStackTraceDepthFlag[] = "stack_trace_depth";
const char kStreamResultToFlag[] = "stream_result_to";
const char kThrowOnFailureFlag[] = "throw_on_failure";

// A valid random seed must be in [1, kMaxRandomSeed].
const int kMaxRandomSeed = 99999;

// g_help_flag is true iff the --help flag or an equivalent form is
// specified on the command line.
GTEST_API_ extern bool g_help_flag;

// Returns the current time in milliseconds.
GTEST_API_ TimeInMillis GetTimeInMillis();

// Returns true iff Google Test should use colors in the output.
GTEST_API_ bool ShouldUseColor(bool stdout_is_tty);

// Formats the given time in milliseconds as seconds.
GTEST_API_ std::string FormatTimeInMillisAsSeconds(TimeInMillis ms);

// Converts the given time in milliseconds to a date string in the ISO 8601
// format, without the timezone information.  N.B.: due to the use the
// non-reentrant localtime() function, this function is not thread safe.  Do
// not use it in any code that can be called from multiple threads.
GTEST_API_ std::string FormatEpochTimeInMillisAsIso8601(TimeInMillis ms);

// Parses a string for an Int32 flag, in the form of "--flag=value".
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
GTEST_API_ bool ParseInt32Flag(
    const char* str, const char* flag, Int32* value);

// Returns a random seed in range [1, kMaxRandomSeed] based on the
// given --gtest_random_seed flag value.
inline int GetRandomSeedFromFlag(Int32 random_seed_flag) {
  const unsigned int raw_seed = (random_seed_flag == 0) ?
      static_cast<unsigned int>(GetTimeInMillis()) :
      static_cast<unsigned int>(random_seed_flag);

  // Normalizes the actual seed to range [1, kMaxRandomSeed] such that
  // it's easy to type.
  const int normalized_seed =
      static_cast<int>((raw_seed - 1U) %
                       static_cast<unsigned int>(kMaxRandomSeed)) + 1;
  return normalized_seed;
}

// Returns the first valid random seed after 'seed'.  The behavior is
// undefined if 'seed' is invalid.  The seed after kMaxRandomSeed is
// considered to be 1.
inline int GetNextRandomSeed(int seed) {
  GTEST_CHECK_(1 <= seed && seed <= kMaxRandomSeed)
      << "Invalid random seed " << seed << " - must be in [1, "
      << kMaxRandomSeed << "].";
  const int next_seed = seed + 1;
  return (next_seed > kMaxRandomSeed) ? 1 : next_seed;
}

// This class saves the values of all Google Test flags in its c'tor, and
// restores them in its d'tor.
class GTestFlagSaver {
 public:
  // The c'tor.
  GTestFlagSaver() {
    also_run_disabled_tests_ = GTEST_FLAG(also_run_disabled_tests);
    break_on_failure_ = GTEST_FLAG(break_on_failure);
    catch_exceptions_ = GTEST_FLAG(catch_exceptions);
    color_ = GTEST_FLAG(color);
    death_test_style_ = GTEST_FLAG(death_test_style);
    death_test_use_fork_ = GTEST_FLAG(death_test_use_fork);
    filter_ = GTEST_FLAG(filter);
    internal_run_death_test_ = GTEST_FLAG(internal_run_death_test);
    list_tests_ = GTEST_FLAG(list_tests);
    output_ = GTEST_FLAG(output);
    print_time_ = GTEST_FLAG(print_time);
    random_seed_ = GTEST_FLAG(random_seed);
    repeat_ = GTEST_FLAG(repeat);
    shuffle_ = GTEST_FLAG(shuffle);
    stack_trace_depth_ = GTEST_FLAG(stack_trace_depth);
    stream_result_to_ = GTEST_FLAG(stream_result_to);
    throw_on_failure_ = GTEST_FLAG(throw_on_failure);
  }

  // The d'tor is not virtual.  DO NOT INHERIT FROM THIS CLASS.
  ~GTestFlagSaver() {
    GTEST_FLAG(also_run_disabled_tests) = also_run_disabled_tests_;
    GTEST_FLAG(break_on_failure) = break_on_failure_;
    GTEST_FLAG(catch_exceptions) = catch_exceptions_;
    GTEST_FLAG(color) = color_;
    GTEST_FLAG(death_test_style) = death_test_style_;
    GTEST_FLAG(death_test_use_fork) = death_test_use_fork_;
    GTEST_FLAG(filter) = filter_;
    GTEST_FLAG(internal_run_death_test) = internal_run_death_test_;
    GTEST_FLAG(list_tests) = list_tests_;
    GTEST_FLAG(output) = output_;
    GTEST_FLAG(print_time) = print_time_;
    GTEST_FLAG(random_seed) = random_seed_;
    GTEST_FLAG(repeat) = repeat_;
    GTEST_FLAG(shuffle) = shuffle_;
    GTEST_FLAG(stack_trace_depth) = stack_trace_depth_;
    GTEST_FLAG(stream_result_to) = stream_result_to_;
    GTEST_FLAG(throw_on_failure) = throw_on_failure_;
  }

 private:
  // Fields for saving the original values of flags.
  bool also_run_disabled_tests_;
  bool break_on_failure_;
  bool catch_exceptions_;
  std::string color_;
  std::string death_test_style_;
  bool death_test_use_fork_;
  std::string filter_;
  std::string internal_run_death_test_;
  bool list_tests_;
  std::string output_;
  bool print_time_;
  internal::Int32 random_seed_;
  internal::Int32 repeat_;
  bool shuffle_;
  internal::Int32 stack_trace_depth_;
  std::string stream_result_to_;
  bool throw_on_failure_;
} GTEST_ATTRIBUTE_UNUSED_;

// Converts a Unicode code point to a narrow string in UTF-8 encoding.
// code_point parameter is of type UInt32 because wchar_t may not be
// wide enough to contain a code point.
// If the code_point is not a valid Unicode code point
// (i.e. outside of Unicode range U+0 to U+10FFFF) it will be converted
// to "(Invalid Unicode 0xXXXXXXXX)".
GTEST_API_ std::string CodePointToUtf8(UInt32 code_point);

// Converts a wide string to a narrow string in UTF-8 encoding.
// The wide string is assumed to have the following encoding:
//   UTF-16 if sizeof(wchar_t) == 2 (on Windows, Cygwin, Symbian OS)
//   UTF-32 if sizeof(wchar_t) == 4 (on Linux)
// Parameter str points to a null-terminated wide string.
// Parameter num_chars may additionally limit the number
// of wchar_t characters processed. -1 is used when the entire string
// should be processed.
// If the string contains code points that are not valid Unicode code points
// (i.e. outside of Unicode range U+0 to U+10FFFF) they will be output
// as '(Invalid Unicode 0xXXXXXXXX)'. If the string is in UTF16 encoding
// and contains invalid UTF-16 surrogate pairs, values in those pairs
// will be encoded as individual Unicode characters from Basic Normal Plane.
GTEST_API_ std::string WideStringToUtf8(const wchar_t* str, int num_chars);

// Reads the GTEST_SHARD_STATUS_FILE environment variable, and creates the file
// if the variable is present. If a file already exists at this location, this
// function will write over it. If the variable is present, but the file cannot
// be created, prints an error and exits.
void WriteToShardStatusFileIfNeeded();

// Checks whether sharding is enabled by examining the relevant
// environment variable values. If the variables are present,
// but inconsistent (e.g., shard_index >= total_shards), prints
// an error and exits. If in_subprocess_for_death_test, sharding is
// disabled because it must only be applied to the original test
// process. Otherwise, we could filter out death tests we intended to execute.
GTEST_API_ bool ShouldShard(const char* total_shards_str,
                            const char* shard_index_str,
                            bool in_subprocess_for_death_test);

// Parses the environment variable var as an Int32. If it is unset,
// returns default_val. If it is not an Int32, prints an error and
// and aborts.
GTEST_API_ Int32 Int32FromEnvOrDie(const char* env_var, Int32 default_val);

// Given the total number of shards, the shard index, and the test id,
// returns true iff the test should be run on this shard. The test id is
// some arbitrary but unique non-negative integer assigned to each test
// method. Assumes that 0 <= shard_index < total_shards.
GTEST_API_ bool ShouldRunTestOnShard(
    int total_shards, int shard_index, int test_id);

// STL container utilities.

// Returns the number of elements in the given container that satisfy
// the given predicate.
template <class Container, typename Predicate>
inline int CountIf(const Container& c, Predicate predicate) {
  // Implemented as an explicit loop since std::count_if() in libCstd on
  // Solaris has a non-standard signature.
  int count = 0;
  for (typename Container::const_iterator it = c.begin(); it != c.end(); ++it) {
    if (predicate(*it))
      ++count;
  }
  return count;
}

// Applies a function/functor to each element in the container.
template <class Container, typename Functor>
void ForEach(const Container& c, Functor functor) {
  std::for_each(c.begin(), c.end(), functor);
}

// Returns the i-th element of the vector, or default_value if i is not
// in range [0, v.size()).
template <typename E>
inline E GetElementOr(const std::vector<E>& v, int i, E default_value) {
  return (i < 0 || i >= static_cast<int>(v.size())) ? default_value : v[i];
}

// Performs an in-place shuffle of a range of the vector's elements.
// 'begin' and 'end' are element indices as an STL-style range;
// i.e. [begin, end) are shuffled, where 'end' == size() means to
// shuffle to the end of the vector.
template <typename E>
void ShuffleRange(internal::Random* random, int begin, int end,
                  std::vector<E>* v) {
  const int size = static_cast<int>(v->size());
  GTEST_CHECK_(0 <= begin && begin <= size)
      << "Invalid shuffle range start " << begin << ": must be in range [0, "
      << size << "].";
  GTEST_CHECK_(begin <= end && end <= size)
      << "Invalid shuffle range finish " << end << ": must be in range ["
      << begin << ", " << size << "].";

  // Fisher-Yates shuffle, from
  // http://en.wikipedia.org/wiki/Fisher-Yates_shuffle
  for (int range_width = end - begin; range_width >= 2; range_width--) {
    const int last_in_range = begin + range_width - 1;
    const int selected = begin + random->Generate(range_width);
    std::swap((*v)[selected], (*v)[last_in_range]);
  }
}

// Performs an in-place shuffle of the vector's elements.
template <typename E>
inline void Shuffle(internal::Random* random, std::vector<E>* v) {
  ShuffleRange(random, 0, static_cast<int>(v->size()), v);
}

// A function for deleting an object.  Handy for being used as a
// functor.
template <typename T>
static void Delete(T* x) {
  delete x;
}

// A predicate that checks the key of a TestProperty against a known key.
//
// TestPropertyKeyIs is copyable.
class TestPropertyKeyIs {
 public:
  // Constructor.
  //
  // TestPropertyKeyIs has NO default constructor.
  explicit TestPropertyKeyIs(const std::string& key) : key_(key) {}

  // Returns true iff the test name of test property matches on key_.
  bool operator()(const TestProperty& test_property) const {
    return test_property.key() == key_;
  }

 private:
  std::string key_;
};

// Class UnitTestOptions.
//
// This class contains functions for processing options the user
// specifies when running the tests.  It has only static members.
//
// In most cases, the user can specify an option using either an
// environment variable or a command line flag.  E.g. you can set the
// test filter using either GTEST_FILTER or --gtest_filter.  If both
// the variable and the flag are present, the latter overrides the
// former.
class GTEST_API_ UnitTestOptions {
 public:
  // Functions for processing the gtest_output flag.

  // Returns the output format, or "" for normal printed output.
  static std::string GetOutputFormat();

  // Returns the absolute path of the requested output file, or the
  // default (test_detail.xml in the original working directory) if
  // none was explicitly specified.
  static std::string GetAbsolutePathToOutputFile();

  // Functions for processing the gtest_filter flag.

  // Returns true iff the wildcard pattern matches the string.  The
  // first ':' or '\0' character in pattern marks the end of it.
  //
  // This recursive algorithm isn't very efficient, but is clear and
  // works well enough for matching test names, which are short.
  static bool PatternMatchesString(const char *pattern, const char *str);

  // Returns true iff the user-specified filter matches the test case
  // name and the test name.
  static bool FilterMatchesTest(const std::string &test_case_name,
                                const std::string &test_name);

#if GTEST_OS_WINDOWS
  // Function for supporting the gtest_catch_exception flag.

  // Returns EXCEPTION_EXECUTE_HANDLER if Google Test should handle the
  // given SEH exception, or EXCEPTION_CONTINUE_SEARCH otherwise.
  // This function is useful as an __except condition.
  static int GTestShouldProcessSEH(DWORD exception_code);
#endif  // GTEST_OS_WINDOWS

  // Returns true if "name" matches the ':' separated list of glob-style
  // filters in "filter".
  static bool MatchesFilter(const std::string& name, const char* filter);
};

// Returns the current application's name, removing directory path if that
// is present.  Used by UnitTestOptions::GetOutputFile.
GTEST_API_ FilePath GetCurrentExecutableName();

// The role interface for getting the OS stack trace as a string.
class OsStackTraceGetterInterface {
 public:
  OsStackTraceGetterInterface() {}
  virtual ~OsStackTraceGetterInterface() {}

  // Returns the current OS stack trace as an std::string.  Parameters:
  //
  //   max_depth  - the maximum number of stack frames to be included
  //                in the trace.
  //   skip_count - the number of top frames to be skipped; doesn't count
  //                against max_depth.
  virtual string CurrentStackTrace(int max_depth, int skip_count) = 0;

  // UponLeavingGTest() should be called immediately before Google Test calls
  // user code. It saves some information about the current stack that
  // CurrentStackTrace() will use to find and hide Google Test stack frames.
  virtual void UponLeavingGTest() = 0;

 private:
  GTEST_DISALLOW_COPY_AND_ASSIGN_(OsStackTraceGetterInterface);
};

// A working implementation of the OsStackTraceGetterInterface interface.
class OsStackTraceGetter : public OsStackTraceGetterInterface {
 public:
  OsStackTraceGetter() : caller_frame_(NULL) {}

  virtual string CurrentStackTrace(int max_depth, int skip_count)
      GTEST_LOCK_EXCLUDED_(mutex_);

  virtual void UponLeavingGTest() GTEST_LOCK_EXCLUDED_(mutex_);

  // This string is inserted in place of stack frames that are part of
  // Google Test's implementation.
  static const char* const kElidedFramesMarker;

 private:
  Mutex mutex_;  // protects all internal state

  // We save the stack frame below the frame that calls user code.
  // We do this because the address of the frame immediately below
  // the user code changes between the call to UponLeavingGTest()
  // and any calls to CurrentStackTrace() from within the user code.
  void* caller_frame_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(OsStackTraceGetter);
};

// Information about a Google Test trace point.
struct TraceInfo {
  const char* file;
  int line;
  std::string message;
};

// This is the default global test part result reporter used in UnitTestImpl.
// This class should only be used by UnitTestImpl.
class DefaultGlobalTestPartResultReporter
  : public TestPartResultReporterInterface {
 public:
  explicit DefaultGlobalTestPartResultReporter(UnitTestImpl* unit_test);
  // Implements the TestPartResultReporterInterface. Reports the test part
  // result in the current test.
  virtual void ReportTestPartResult(const TestPartResult& result);

 private:
  UnitTestImpl* const unit_test_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(DefaultGlobalTestPartResultReporter);
};

// This is the default per thread test part result reporter used in
// UnitTestImpl. This class should only be used by UnitTestImpl.
class DefaultPerThreadTestPartResultReporter
    : public TestPartResultReporterInterface {
 public:
  explicit DefaultPerThreadTestPartResultReporter(UnitTestImpl* unit_test);
  // Implements the TestPartResultReporterInterface. The implementation just
  // delegates to the current global test part result reporter of *unit_test_.
  virtual void ReportTestPartResult(const TestPartResult& result);

 private:
  UnitTestImpl* const unit_test_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(DefaultPerThreadTestPartResultReporter);
};

// The private implementation of the UnitTest class.  We don't protect
// the methods under a mutex, as this class is not accessible by a
// user and the UnitTest class that delegates work to this class does
// proper locking.
class GTEST_API_ UnitTestImpl {
 public:
  explicit UnitTestImpl(UnitTest* parent);
  virtual ~UnitTestImpl();

  // There are two different ways to register your own TestPartResultReporter.
  // You can register your own repoter to listen either only for test results
  // from the current thread or for results from all threads.
  // By default, each per-thread test result repoter just passes a new
  // TestPartResult to the global test result reporter, which registers the
  // test part result for the currently running test.

  // Returns the global test part result reporter.
  TestPartResultReporterInterface* GetGlobalTestPartResultReporter();

  // Sets the global test part result reporter.
  void SetGlobalTestPartResultReporter(
      TestPartResultReporterInterface* reporter);

  // Returns the test part result reporter for the current thread.
  TestPartResultReporterInterface* GetTestPartResultReporterForCurrentThread();

  // Sets the test part result reporter for the current thread.
  void SetTestPartResultReporterForCurrentThread(
      TestPartResultReporterInterface* reporter);

  // Gets the number of successful test cases.
  int successful_test_case_count() const;

  // Gets the number of failed test cases.
  int failed_test_case_count() const;

  // Gets the number of all test cases.
  int total_test_case_count() const;

  // Gets the number of all test cases that contain at least one test
  // that should run.
  int test_case_to_run_count() const;

  // Gets the number of successful tests.
  int successful_test_count() const;

  // Gets the number of failed tests.
  int failed_test_count() const;

  // Gets the number of disabled tests that will be reported in the XML report.
  int reportable_disabled_test_count() const;

  // Gets the number of disabled tests.
  int disabled_test_count() const;

  // Gets the number of tests to be printed in the XML report.
  int reportable_test_count() const;

  // Gets the number of all tests.
  int total_test_count() const;

  // Gets the number of tests that should run.
  int test_to_run_count() const;

  // Gets the time of the test program start, in ms from the start of the
  // UNIX epoch.
  TimeInMillis start_timestamp() const { return start_timestamp_; }

  // Gets the elapsed time, in milliseconds.
  TimeInMillis elapsed_time() const { return elapsed_time_; }

  // Returns true iff the unit test passed (i.e. all test cases passed).
  bool Passed() const { return !Failed(); }

  // Returns true iff the unit test failed (i.e. some test case failed
  // or something outside of all tests failed).
  bool Failed() const {
    return failed_test_case_count() > 0 || ad_hoc_test_result()->Failed();
  }

  // Gets the i-th test case among all the test cases. i can range from 0 to
  // total_test_case_count() - 1. If i is not in that range, returns NULL.
  const TestCase* GetTestCase(int i) const {
    const int index = GetElementOr(test_case_indices_, i, -1);
    return index < 0 ? NULL : test_cases_[i];
  }

  // Gets the i-th test case among all the test cases. i can range from 0 to
  // total_test_case_count() - 1. If i is not in that range, returns NULL.
  TestCase* GetMutableTestCase(int i) {
    const int index = GetElementOr(test_case_indices_, i, -1);
    return index < 0 ? NULL : test_cases_[index];
  }

  // Provides access to the event listener list.
  TestEventListeners* listeners() { return &listeners_; }

  // Returns the TestResult for the test that's currently running, or
  // the TestResult for the ad hoc test if no test is running.
  TestResult* current_test_result();

  // Returns the TestResult for the ad hoc test.
  const TestResult* ad_hoc_test_result() const { return &ad_hoc_test_result_; }

  // Sets the OS stack trace getter.
  //
  // Does nothing if the input and the current OS stack trace getter
  // are the same; otherwise, deletes the old getter and makes the
  // input the current getter.
  void set_os_stack_trace_getter(OsStackTraceGetterInterface* getter);

  // Returns the current OS stack trace getter if it is not NULL;
  // otherwise, creates an OsStackTraceGetter, makes it the current
  // getter, and returns it.
  OsStackTraceGetterInterface* os_stack_trace_getter();

  // Returns the current OS stack trace as an std::string.
  //
  // The maximum number of stack frames to be included is specified by
  // the gtest_stack_trace_depth flag.  The skip_count parameter
  // specifies the number of top frames to be skipped, which doesn't
  // count against the number of frames to be included.
  //
  // For example, if Foo() calls Bar(), which in turn calls
  // CurrentOsStackTraceExceptTop(1), Foo() will be included in the
  // trace but Bar() and CurrentOsStackTraceExceptTop() won't.
  std::string CurrentOsStackTraceExceptTop(int skip_count) GTEST_NO_INLINE_;

  // Finds and returns a TestCase with the given name.  If one doesn't
  // exist, creates one and returns it.
  //
  // Arguments:
  //
  //   test_case_name: name of the test case
  //   type_param:     the name of the test's type parameter, or NULL if
  //                   this is not a typed or a type-parameterized test.
  //   set_up_tc:      pointer to the function that sets up the test case
  //   tear_down_tc:   pointer to the function that tears down the test case
  TestCase* GetTestCase(const char* test_case_name,
                        const char* type_param,
                        Test::SetUpTestCaseFunc set_up_tc,
                        Test::TearDownTestCaseFunc tear_down_tc);

  // Adds a TestInfo to the unit test.
  //
  // Arguments:
  //
  //   set_up_tc:    pointer to the function that sets up the test case
  //   tear_down_tc: pointer to the function that tears down the test case
  //   test_info:    the TestInfo object
  void AddTestInfo(Test::SetUpTestCaseFunc set_up_tc,
                   Test::TearDownTestCaseFunc tear_down_tc,
                   TestInfo* test_info) {
    // In order to support thread-safe death tests, we need to
    // remember the original working directory when the test program
    // was first invoked.  We cannot do this in RUN_ALL_TESTS(), as
    // the user may have changed the current directory before calling
    // RUN_ALL_TESTS().  Therefore we capture the current directory in
    // AddTestInfo(), which is called to register a TEST or TEST_F
    // before main() is reached.
    if (original_working_dir_.IsEmpty()) {
      original_working_dir_.Set(FilePath::GetCurrentDir());
      GTEST_CHECK_(!original_working_dir_.IsEmpty())
          << "Failed to get the current working directory.";
    }

    GetTestCase(test_info->test_case_name(),
                test_info->type_param(),
                set_up_tc,
                tear_down_tc)->AddTestInfo(test_info);
  }

#if GTEST_HAS_PARAM_TEST
  // Returns ParameterizedTestCaseRegistry object used to keep track of
  // value-parameterized tests and instantiate and register them.
  internal::ParameterizedTestCaseRegistry& parameterized_test_registry() {
    return parameterized_test_registry_;
  }
#endif  // GTEST_HAS_PARAM_TEST

  // Sets the TestCase object for the test that's currently running.
  void set_current_test_case(TestCase* a_current_test_case) {
    current_test_case_ = a_current_test_case;
  }

  // Sets the TestInfo object for the test that's currently running.  If
  // current_test_info is NULL, the assertion results will be stored in
  // ad_hoc_test_result_.
  void set_current_test_info(TestInfo* a_current_test_info) {
    current_test_info_ = a_current_test_info;
  }

  // Registers all parameterized tests defined using TEST_P and
  // INSTANTIATE_TEST_CASE_P, creating regular tests for each test/parameter
  // combination. This method can be called more then once; it has guards
  // protecting from registering the tests more then once.  If
  // value-parameterized tests are disabled, RegisterParameterizedTests is
  // present but does nothing.
  void RegisterParameterizedTests();

  // Runs all tests in this UnitTest object, prints the result, and
  // returns true if all tests are successful.  If any exception is
  // thrown during a test, this test is considered to be failed, but
  // the rest of the tests will still be run.
  bool RunAllTests();

  // Clears the results of all tests, except the ad hoc tests.
  void ClearNonAdHocTestResult() {
    ForEach(test_cases_, TestCase::ClearTestCaseResult);
  }

  // Clears the results of ad-hoc test assertions.
  void ClearAdHocTestResult() {
    ad_hoc_test_result_.Clear();
  }

  // Adds a TestProperty to the current TestResult object when invoked in a
  // context of a test or a test case, or to the global property set. If the
  // result already contains a property with the same key, the value will be
  // updated.
  void RecordProperty(const TestProperty& test_property);

  enum ReactionToSharding {
    HONOR_SHARDING_PROTOCOL,
    IGNORE_SHARDING_PROTOCOL
  };

  // Matches the full name of each test against the user-specified
  // filter to decide whether the test should run, then records the
  // result in each TestCase and TestInfo object.
  // If shard_tests == HONOR_SHARDING_PROTOCOL, further filters tests
  // based on sharding variables in the environment.
  // Returns the number of tests that should run.
  int FilterTests(ReactionToSharding shard_tests);

  // Prints the names of the tests matching the user-specified filter flag.
  void ListTestsMatchingFilter();

  const TestCase* current_test_case() const { return current_test_case_; }
  TestInfo* current_test_info() { return current_test_info_; }
  const TestInfo* current_test_info() const { return current_test_info_; }

  // Returns the vector of environments that need to be set-up/torn-down
  // before/after the tests are run.
  std::vector<Environment*>& environments() { return environments_; }

  // Getters for the per-thread Google Test trace stack.
  std::vector<TraceInfo>& gtest_trace_stack() {
    return *(gtest_trace_stack_.pointer());
  }
  const std::vector<TraceInfo>& gtest_trace_stack() const {
    return gtest_trace_stack_.get();
  }

#if GTEST_HAS_DEATH_TEST
  void InitDeathTestSubprocessControlInfo() {
    internal_run_death_test_flag_.reset(ParseInternalRunDeathTestFlag());
  }
  // Returns a pointer to the parsed --gtest_internal_run_death_test
  // flag, or NULL if that flag was not specified.
  // This information is useful only in a death test child process.
  // Must not be called before a call to InitGoogleTest.
  const InternalRunDeathTestFlag* internal_run_death_test_flag() const {
    return internal_run_death_test_flag_.get();
  }

  // Returns a pointer to the current death test factory.
  internal::DeathTestFactory* death_test_factory() {
    return death_test_factory_.get();
  }

  void SuppressTestEventsIfInSubprocess();

  friend class ReplaceDeathTestFactory;
#endif  // GTEST_HAS_DEATH_TEST

  // Initializes the event listener performing XML output as specified by
  // UnitTestOptions. Must not be called before InitGoogleTest.
  void ConfigureXmlOutput();

#if GTEST_CAN_STREAM_RESULTS_
  // Initializes the event listener for streaming test results to a socket.
  // Must not be called before InitGoogleTest.
  void ConfigureStreamingOutput();
#endif

  // Performs initialization dependent upon flag values obtained in
  // ParseGoogleTestFlagsOnly.  Is called from InitGoogleTest after the call to
  // ParseGoogleTestFlagsOnly.  In case a user neglects to call InitGoogleTest
  // this function is also called from RunAllTests.  Since this function can be
  // called more than once, it has to be idempotent.
  void PostFlagParsingInit();

  // Gets the random seed used at the start of the current test iteration.
  int random_seed() const { return random_seed_; }

  // Gets the random number generator.
  internal::Random* random() { return &random_; }

  // Shuffles all test cases, and the tests within each test case,
  // making sure that death tests are still run first.
  void ShuffleTests();

  // Restores the test cases and tests to their order before the first shuffle.
  void UnshuffleTests();

  // Returns the value of GTEST_FLAG(catch_exceptions) at the moment
  // UnitTest::Run() starts.
  bool catch_exceptions() const { return catch_exceptions_; }

 private:
  friend class ::testing::UnitTest;

  // Used by UnitTest::Run() to capture the state of
  // GTEST_FLAG(catch_exceptions) at the moment it starts.
  void set_catch_exceptions(bool value) { catch_exceptions_ = value; }

  // The UnitTest object that owns this implementation object.
  UnitTest* const parent_;

  // The working directory when the first TEST() or TEST_F() was
  // executed.
  internal::FilePath original_working_dir_;

  // The default test part result reporters.
  DefaultGlobalTestPartResultReporter default_global_test_part_result_reporter_;
  DefaultPerThreadTestPartResultReporter
      default_per_thread_test_part_result_reporter_;

  // Points to (but doesn't own) the global test part result reporter.
  TestPartResultReporterInterface* global_test_part_result_repoter_;

  // Protects read and write access to global_test_part_result_reporter_.
  internal::Mutex global_test_part_result_reporter_mutex_;

  // Points to (but doesn't own) the per-thread test part result reporter.
  internal::ThreadLocal<TestPartResultReporterInterface*>
      per_thread_test_part_result_reporter_;

  // The vector of environments that need to be set-up/torn-down
  // before/after the tests are run.
  std::vector<Environment*> environments_;

  // The vector of TestCases in their original order.  It owns the
  // elements in the vector.
  std::vector<TestCase*> test_cases_;

  // Provides a level of indirection for the test case list to allow
  // easy shuffling and restoring the test case order.  The i-th
  // element of this vector is the index of the i-th test case in the
  // shuffled order.
  std::vector<int> test_case_indices_;

#if GTEST_HAS_PARAM_TEST
  // ParameterizedTestRegistry object used to register value-parameterized
  // tests.
  internal::ParameterizedTestCaseRegistry parameterized_test_registry_;

  // Indicates whether RegisterParameterizedTests() has been called already.
  bool parameterized_tests_registered_;
#endif  // GTEST_HAS_PARAM_TEST

  // Index of the last death test case registered.  Initially -1.
  int last_death_test_case_;

  // This points to the TestCase for the currently running test.  It
  // changes as Google Test goes through one test case after another.
  // When no test is running, this is set to NULL and Google Test
  // stores assertion results in ad_hoc_test_result_.  Initially NULL.
  TestCase* current_test_case_;

  // This points to the TestInfo for the currently running test.  It
  // changes as Google Test goes through one test after another.  When
  // no test is running, this is set to NULL and Google Test stores
  // assertion results in ad_hoc_test_result_.  Initially NULL.
  TestInfo* current_test_info_;

  // Normally, a user only writes assertions inside a TEST or TEST_F,
  // or inside a function called by a TEST or TEST_F.  Since Google
  // Test keeps track of which test is current running, it can
  // associate such an assertion with the test it belongs to.
  //
  // If an assertion is encountered when no TEST or TEST_F is running,
  // Google Test attributes the assertion result to an imaginary "ad hoc"
  // test, and records the result in ad_hoc_test_result_.
  TestResult ad_hoc_test_result_;

  // The list of event listeners that can be used to track events inside
  // Google Test.
  TestEventListeners listeners_;

  // The OS stack trace getter.  Will be deleted when the UnitTest
  // object is destructed.  By default, an OsStackTraceGetter is used,
  // but the user can set this field to use a custom getter if that is
  // desired.
  OsStackTraceGetterInterface* os_stack_trace_getter_;

  // True iff PostFlagParsingInit() has been called.
  bool post_flag_parse_init_performed_;

  // The random number seed used at the beginning of the test run.
  int random_seed_;

  // Our random number generator.
  internal::Random random_;

  // The time of the test program start, in ms from the start of the
  // UNIX epoch.
  TimeInMillis start_timestamp_;

  // How long the test took to run, in milliseconds.
  TimeInMillis elapsed_time_;

#if GTEST_HAS_DEATH_TEST
  // The decomposed components of the gtest_internal_run_death_test flag,
  // parsed when RUN_ALL_TESTS is called.
  internal::scoped_ptr<InternalRunDeathTestFlag> internal_run_death_test_flag_;
  internal::scoped_ptr<internal::DeathTestFactory> death_test_factory_;
#endif  // GTEST_HAS_DEATH_TEST

  // A per-thread stack of traces created by the SCOPED_TRACE() macro.
  internal::ThreadLocal<std::vector<TraceInfo> > gtest_trace_stack_;

  // The value of GTEST_FLAG(catch_exceptions) at the moment RunAllTests()
  // starts.
  bool catch_exceptions_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(UnitTestImpl);
};  // class UnitTestImpl

// Convenience function for accessing the global UnitTest
// implementation object.
inline UnitTestImpl* GetUnitTestImpl() {
  return UnitTest::GetInstance()->impl();
}

#if GTEST_USES_SIMPLE_RE

// Internal helper functions for implementing the simple regular
// expression matcher.
GTEST_API_ bool IsInSet(char ch, const char* str);
GTEST_API_ bool IsAsciiDigit(char ch);
GTEST_API_ bool IsAsciiPunct(char ch);
GTEST_API_ bool IsRepeat(char ch);
GTEST_API_ bool IsAsciiWhiteSpace(char ch);
GTEST_API_ bool IsAsciiWordChar(char ch);
GTEST_API_ bool IsValidEscape(char ch);
GTEST_API_ bool AtomMatchesChar(bool escaped, char pattern, char ch);
GTEST_API_ bool ValidateRegex(const char* regex);
GTEST_API_ bool MatchRegexAtHead(const char* regex, const char* str);
GTEST_API_ bool MatchRepetitionAndRegexAtHead(
    bool escaped, char ch, char repeat, const char* regex, const char* str);
GTEST_API_ bool MatchRegexAnywhere(const char* regex, const char* str);

#endif  // GTEST_USES_SIMPLE_RE

// Parses the command line for Google Test flags, without initializing
// other parts of Google Test.
GTEST_API_ void ParseGoogleTestFlagsOnly(int* argc, char** argv);
GTEST_API_ void ParseGoogleTestFlagsOnly(int* argc, wchar_t** argv);

#if GTEST_HAS_DEATH_TEST

// Returns the message describing the last system error, regardless of the
// platform.
GTEST_API_ std::string GetLastErrnoDescription();

# if GTEST_OS_WINDOWS
// Provides leak-safe Windows kernel handle ownership.
class AutoHandle {
 public:
  AutoHandle() : handle_(INVALID_HANDLE_VALUE) {}
  explicit AutoHandle(HANDLE handle) : handle_(handle) {}

  ~AutoHandle() { Reset(); }

  HANDLE Get() const { return handle_; }
  void Reset() { Reset(INVALID_HANDLE_VALUE); }
  void Reset(HANDLE handle) {
    if (handle != handle_) {
      if (handle_ != INVALID_HANDLE_VALUE)
        ::CloseHandle(handle_);
      handle_ = handle;
    }
  }

 private:
  HANDLE handle_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(AutoHandle);
};
# endif  // GTEST_OS_WINDOWS

// Attempts to parse a string into a positive integer pointed to by the
// number parameter.  Returns true if that is possible.
// GTEST_HAS_DEATH_TEST implies that we have ::std::string, so we can use
// it here.
template <typename Integer>
bool ParseNaturalNumber(const ::std::string& str, Integer* number) {
  // Fail fast if the given string does not begin with a digit;
  // this bypasses strtoXXX's "optional leading whitespace and plus
  // or minus sign" semantics, which are undesirable here.
  if (str.empty() || !IsDigit(str[0])) {
    return false;
  }
  errno = 0;

  char* end;
  // BiggestConvertible is the largest integer type that system-provided
  // string-to-number conversion routines can return.

# if GTEST_OS_WINDOWS && !defined(__GNUC__)

  // MSVC and C++ Builder define __int64 instead of the standard long long.
  typedef unsigned __int64 BiggestConvertible;
  const BiggestConvertible parsed = _strtoui64(str.c_str(), &end, 10);

# else

  typedef unsigned long long BiggestConvertible;  // NOLINT
  const BiggestConvertible parsed = strtoull(str.c_str(), &end, 10);

# endif  // GTEST_OS_WINDOWS && !defined(__GNUC__)

  const bool parse_success = *end == '\0' && errno == 0;

  // TODO(vladl@google.com): Convert this to compile time assertion when it is
  // available.
  GTEST_CHECK_(sizeof(Integer) <= sizeof(parsed));

  const Integer result = static_cast<Integer>(parsed);
  if (parse_success && static_cast<BiggestConvertible>(result) == parsed) {
    *number = result;
    return true;
  }
  return false;
}
#endif  // GTEST_HAS_DEATH_TEST

// TestResult contains some private methods that should be hidden from
// Google Test user but are required for testing. This class allow our tests
// to access them.
//
// This class is supplied only for the purpose of testing Google Test's own
// constructs. Do not use it in user tests, either directly or indirectly.
class TestResultAccessor {
 public:
  static void RecordProperty(TestResult* test_result,
                             const std::string& xml_element,
                             const TestProperty& property) {
    test_result->RecordProperty(xml_element, property);
  }

  static void ClearTestPartResults(TestResult* test_result) {
    test_result->ClearTestPartResults();
  }

  static const std::vector<testing::TestPartResult>& test_part_results(
      const TestResult& test_result) {
    return test_result.test_part_results();
  }
};

#if GTEST_CAN_STREAM_RESULTS_

// Streams test results to the given port on the given host machine.
class StreamingListener : public EmptyTestEventListener {
 public:
  // Abstract base class for writing strings to a socket.
  class AbstractSocketWriter {
   public:
    virtual ~AbstractSocketWriter() {}

    // Sends a string to the socket.
    virtual void Send(const string& message) = 0;

    // Closes the socket.
    virtual void CloseConnection() {}

    // Sends a string and a newline to the socket.
    void SendLn(const string& message) {
      Send(message + "\n");
    }
  };

  // Concrete class for actually writing strings to a socket.
  class SocketWriter : public AbstractSocketWriter {
   public:
    SocketWriter(const string& host, const string& port)
        : sockfd_(-1), host_name_(host), port_num_(port) {
      MakeConnection();
    }

    virtual ~SocketWriter() {
      if (sockfd_ != -1)
        CloseConnection();
    }

    // Sends a string to the socket.
    virtual void Send(const string& message) {
      GTEST_CHECK_(sockfd_ != -1)
          << "Send() can be called only when there is a connection.";

      const int len = static_cast<int>(message.length());
      if (write(sockfd_, message.c_str(), len) != len) {
        GTEST_LOG_(WARNING)
            << "stream_result_to: failed to stream to "
            << host_name_ << ":" << port_num_;
      }
    }

   private:
    // Creates a client socket and connects to the server.
    void MakeConnection();

    // Closes the socket.
    void CloseConnection() {
      GTEST_CHECK_(sockfd_ != -1)
          << "CloseConnection() can be called only when there is a connection.";

      close(sockfd_);
      sockfd_ = -1;
    }

    int sockfd_;  // socket file descriptor
    const string host_name_;
    const string port_num_;

    GTEST_DISALLOW_COPY_AND_ASSIGN_(SocketWriter);
  };  // class SocketWriter

  // Escapes '=', '&', '%', and '\n' characters in str as "%xx".
  static string UrlEncode(const char* str);

  StreamingListener(const string& host, const string& port)
      : socket_writer_(new SocketWriter(host, port)) { Start(); }

  explicit StreamingListener(AbstractSocketWriter* socket_writer)
      : socket_writer_(socket_writer) { Start(); }

  void OnTestProgramStart(const UnitTest& /* unit_test */) {
    SendLn("event=TestProgramStart");
  }

  void OnTestProgramEnd(const UnitTest& unit_test) {
    // Note that Google Test current only report elapsed time for each
    // test iteration, not for the entire test program.
    SendLn("event=TestProgramEnd&passed=" + FormatBool(unit_test.Passed()));

    // Notify the streaming server to stop.
    socket_writer_->CloseConnection();
  }

  void OnTestIterationStart(const UnitTest& /* unit_test */, int iteration) {
    SendLn("event=TestIterationStart&iteration=" +
           StreamableToString(iteration));
  }

  void OnTestIterationEnd(const UnitTest& unit_test, int /* iteration */) {
    SendLn("event=TestIterationEnd&passed=" +
           FormatBool(unit_test.Passed()) + "&elapsed_time=" +
           StreamableToString(unit_test.elapsed_time()) + "ms");
  }

  void OnTestCaseStart(const TestCase& test_case) {
    SendLn(std::string("event=TestCaseStart&name=") + test_case.name());
  }

  void OnTestCaseEnd(const TestCase& test_case) {
    SendLn("event=TestCaseEnd&passed=" + FormatBool(test_case.Passed())
           + "&elapsed_time=" + StreamableToString(test_case.elapsed_time())
           + "ms");
  }

  void OnTestStart(const TestInfo& test_info) {
    SendLn(std::string("event=TestStart&name=") + test_info.name());
  }

  void OnTestEnd(const TestInfo& test_info) {
    SendLn("event=TestEnd&passed=" +
           FormatBool((test_info.result())->Passed()) +
           "&elapsed_time=" +
           StreamableToString((test_info.result())->elapsed_time()) + "ms");
  }

  void OnTestPartResult(const TestPartResult& test_part_result) {
    const char* file_name = test_part_result.file_name();
    if (file_name == NULL)
      file_name = "";
    SendLn("event=TestPartResult&file=" + UrlEncode(file_name) +
           "&line=" + StreamableToString(test_part_result.line_number()) +
           "&message=" + UrlEncode(test_part_result.message()));
  }

 private:
  // Sends the given message and a newline to the socket.
  void SendLn(const string& message) { socket_writer_->SendLn(message); }

  // Called at the start of streaming to notify the receiver what
  // protocol we are using.
  void Start() { SendLn("gtest_streaming_protocol_version=1.0"); }

  string FormatBool(bool value) { return value ? "1" : "0"; }

  const scoped_ptr<AbstractSocketWriter> socket_writer_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(StreamingListener);
};  // class StreamingListener

#endif  // GTEST_CAN_STREAM_RESULTS_

}  // namespace internal
}  // namespace testing

#endif  // GTEST_SRC_GTEST_INTERNAL_INL_H_
#undef GTEST_IMPLEMENTATION_

#if GTEST_OS_WINDOWS
# define vsnprintf _vsnprintf
#endif  // GTEST_OS_WINDOWS

namespace testing {

using internal::CountIf;
using internal::ForEach;
using internal::GetElementOr;
using internal::Shuffle;

// Constants.

// A test whose test case name or test name matches this filter is
// disabled and not run.
static const char kDisableTestFilter[] = "DISABLED_*:*/DISABLED_*";

// A test case whose name matches this filter is considered a death
// test case and will be run before test cases whose name doesn't
// match this filter.
static const char kDeathTestCaseFilter[] = "*DeathTest:*DeathTest/*";

// A test filter that matches everything.
static const char kUniversalFilter[] = "*";

// The default output file for XML output.
static const char kDefaultOutputFile[] = "test_detail.xml";

// The environment variable name for the test shard index.
static const char kTestShardIndex[] = "GTEST_SHARD_INDEX";
// The environment variable name for the total number of test shards.
static const char kTestTotalShards[] = "GTEST_TOTAL_SHARDS";
// The environment variable name for the test shard status file.
static const char kTestShardStatusFile[] = "GTEST_SHARD_STATUS_FILE";

namespace internal {

// The text used in failure messages to indicate the start of the
// stack trace.
const char kStackTraceMarker[] = "\nStack trace:\n";

// g_help_flag is true iff the --help flag or an equivalent form is
// specified on the command line.
bool g_help_flag = false;

}  // namespace internal

static const char* GetDefaultFilter() {
  return kUniversalFilter;
}

GTEST_DEFINE_bool_(
    also_run_disabled_tests,
    internal::BoolFromGTestEnv("also_run_disabled_tests", false),
    "Run disabled tests too, in addition to the tests normally being run.");

GTEST_DEFINE_bool_(
    break_on_failure,
    internal::BoolFromGTestEnv("break_on_failure", false),
    "True iff a failed assertion should be a debugger break-point.");

GTEST_DEFINE_bool_(
    catch_exceptions,
    internal::BoolFromGTestEnv("catch_exceptions", true),
    "True iff " GTEST_NAME_
    " should catch exceptions and treat them as test failures.");

GTEST_DEFINE_string_(
    color,
    internal::StringFromGTestEnv("color", "auto"),
    "Whether to use colors in the output.  Valid values: yes, no, "
    "and auto.  'auto' means to use colors if the output is "
    "being sent to a terminal and the TERM environment variable "
    "is set to a terminal type that supports colors.");

GTEST_DEFINE_string_(
    filter,
    internal::StringFromGTestEnv("filter", GetDefaultFilter()),
    "A colon-separated list of glob (not regex) patterns "
    "for filtering the tests to run, optionally followed by a "
    "'-' and a : separated list of negative patterns (tests to "
    "exclude).  A test is run if it matches one of the positive "
    "patterns and does not match any of the negative patterns.");

GTEST_DEFINE_bool_(list_tests, false,
                   "List all tests without running them.");

GTEST_DEFINE_string_(
    output,
    internal::StringFromGTestEnv("output", ""),
    "A format (currently must be \"xml\"), optionally followed "
    "by a colon and an output file name or directory. A directory "
    "is indicated by a trailing pathname separator. "
    "Examples: \"xml:filename.xml\", \"xml::directoryname/\". "
    "If a directory is specified, output files will be created "
    "within that directory, with file-names based on the test "
    "executable's name and, if necessary, made unique by adding "
    "digits.");

GTEST_DEFINE_bool_(
    print_time,
    internal::BoolFromGTestEnv("print_time", true),
    "True iff " GTEST_NAME_
    " should display elapsed time in text output.");

GTEST_DEFINE_int32_(
    random_seed,
    internal::Int32FromGTestEnv("random_seed", 0),
    "Random number seed to use when shuffling test orders.  Must be in range "
    "[1, 99999], or 0 to use a seed based on the current time.");

GTEST_DEFINE_int32_(
    repeat,
    internal::Int32FromGTestEnv("repeat", 1),
    "How many times to repeat each test.  Specify a negative number "
    "for repeating forever.  Useful for shaking out flaky tests.");

GTEST_DEFINE_bool_(
    show_internal_stack_frames, false,
    "True iff " GTEST_NAME_ " should include internal stack frames when "
    "printing test failure stack traces.");

GTEST_DEFINE_bool_(
    shuffle,
    internal::BoolFromGTestEnv("shuffle", false),
    "True iff " GTEST_NAME_
    " should randomize tests' order on every run.");

GTEST_DEFINE_int32_(
    stack_trace_depth,
    internal::Int32FromGTestEnv("stack_trace_depth", kMaxStackTraceDepth),
    "The maximum number of stack frames to print when an "
    "assertion fails.  The valid range is 0 through 100, inclusive.");

GTEST_DEFINE_string_(
    stream_result_to,
    internal::StringFromGTestEnv("stream_result_to", ""),
    "This flag specifies the host name and the port number on which to stream "
    "test results. Example: \"localhost:555\". The flag is effective only on "
    "Linux.");

GTEST_DEFINE_bool_(
    throw_on_failure,
    internal::BoolFromGTestEnv("throw_on_failure", false),
    "When this flag is specified, a failed assertion will throw an exception "
    "if exceptions are enabled or exit the program with a non-zero code "
    "otherwise.");

namespace internal {

// Generates a random number from [0, range), using a Linear
// Congruential Generator (LCG).  Crashes if 'range' is 0 or greater
// than kMaxRange.
UInt32 Random::Generate(UInt32 range) {
  // These constants are the same as are used in glibc's rand(3).
  state_ = (1103515245U*state_ + 12345U) % kMaxRange;

  GTEST_CHECK_(range > 0)
      << "Cannot generate a number in the range [0, 0).";
  GTEST_CHECK_(range <= kMaxRange)
      << "Generation of a number in [0, " << range << ") was requested, "
      << "but this can only generate numbers in [0, " << kMaxRange << ").";

  // Converting via modulus introduces a bit of downward bias, but
  // it's simple, and a linear congruential generator isn't too good
  // to begin with.
  return state_ % range;
}

// GTestIsInitialized() returns true iff the user has initialized
// Google Test.  Useful for catching the user mistake of not initializing
// Google Test before calling RUN_ALL_TESTS().
//
// A user must call testing::InitGoogleTest() to initialize Google
// Test.  g_init_gtest_count is set to the number of times
// InitGoogleTest() has been called.  We don't protect this variable
// under a mutex as it is only accessed in the main thread.
GTEST_API_ int g_init_gtest_count = 0;
static bool GTestIsInitialized() { return g_init_gtest_count != 0; }

// Iterates over a vector of TestCases, keeping a running sum of the
// results of calling a given int-returning method on each.
// Returns the sum.
static int SumOverTestCaseList(const std::vector<TestCase*>& case_list,
                               int (TestCase::*method)() const) {
  int sum = 0;
  for (size_t i = 0; i < case_list.size(); i++) {
    sum += (case_list[i]->*method)();
  }
  return sum;
}

// Returns true iff the test case passed.
static bool TestCasePassed(const TestCase* test_case) {
  return test_case->should_run() && test_case->Passed();
}

// Returns true iff the test case failed.
static bool TestCaseFailed(const TestCase* test_case) {
  return test_case->should_run() && test_case->Failed();
}

// Returns true iff test_case contains at least one test that should
// run.
static bool ShouldRunTestCase(const TestCase* test_case) {
  return test_case->should_run();
}

// AssertHelper constructor.
AssertHelper::AssertHelper(TestPartResult::Type type,
                           const char* file,
                           int line,
                           const char* message)
    : data_(new AssertHelperData(type, file, line, message)) {
}

AssertHelper::~AssertHelper() {
  delete data_;
}

// Message assignment, for assertion streaming support.
void AssertHelper::operator=(const Message& message) const {
  UnitTest::GetInstance()->
    AddTestPartResult(data_->type, data_->file, data_->line,
                      AppendUserMessage(data_->message, message),
                      UnitTest::GetInstance()->impl()
                      ->CurrentOsStackTraceExceptTop(1)
                      // Skips the stack frame for this function itself.
                      );  // NOLINT
}

// Mutex for linked pointers.
GTEST_API_ GTEST_DEFINE_STATIC_MUTEX_(g_linked_ptr_mutex);

// Application pathname gotten in InitGoogleTest.
std::string g_executable_path;

// Returns the current application's name, removing directory path if that
// is present.
FilePath GetCurrentExecutableName() {
  FilePath result;

#if GTEST_OS_WINDOWS
  result.Set(FilePath(g_executable_path).RemoveExtension("exe"));
#else
  result.Set(FilePath(g_executable_path));
#endif  // GTEST_OS_WINDOWS

  return result.RemoveDirectoryName();
}

// Functions for processing the gtest_output flag.

// Returns the output format, or "" for normal printed output.
std::string UnitTestOptions::GetOutputFormat() {
  const char* const gtest_output_flag = GTEST_FLAG(output).c_str();
  if (gtest_output_flag == NULL) return std::string("");

  const char* const colon = strchr(gtest_output_flag, ':');
  return (colon == NULL) ?
      std::string(gtest_output_flag) :
      std::string(gtest_output_flag, colon - gtest_output_flag);
}

// Returns the name of the requested output file, or the default if none
// was explicitly specified.
std::string UnitTestOptions::GetAbsolutePathToOutputFile() {
  const char* const gtest_output_flag = GTEST_FLAG(output).c_str();
  if (gtest_output_flag == NULL)
    return "";

  const char* const colon = strchr(gtest_output_flag, ':');
  if (colon == NULL)
    return internal::FilePath::ConcatPaths(
        internal::FilePath(
            UnitTest::GetInstance()->original_working_dir()),
        internal::FilePath(kDefaultOutputFile)).string();

  internal::FilePath output_name(colon + 1);
  if (!output_name.IsAbsolutePath())
    // TODO(wan@google.com): on Windows \some\path is not an absolute
    // path (as its meaning depends on the current drive), yet the
    // following logic for turning it into an absolute path is wrong.
    // Fix it.
    output_name = internal::FilePath::ConcatPaths(
        internal::FilePath(UnitTest::GetInstance()->original_working_dir()),
        internal::FilePath(colon + 1));

  if (!output_name.IsDirectory())
    return output_name.string();

  internal::FilePath result(internal::FilePath::GenerateUniqueFileName(
      output_name, internal::GetCurrentExecutableName(),
      GetOutputFormat().c_str()));
  return result.string();
}

// Returns true iff the wildcard pattern matches the string.  The
// first ':' or '\0' character in pattern marks the end of it.
//
// This recursive algorithm isn't very efficient, but is clear and
// works well enough for matching test names, which are short.
bool UnitTestOptions::PatternMatchesString(const char *pattern,
                                           const char *str) {
  switch (*pattern) {
    case '\0':
    case ':':  // Either ':' or '\0' marks the end of the pattern.
      return *str == '\0';
    case '?':  // Matches any single character.
      return *str != '\0' && PatternMatchesString(pattern + 1, str + 1);
    case '*':  // Matches any string (possibly empty) of characters.
      return (*str != '\0' && PatternMatchesString(pattern, str + 1)) ||
          PatternMatchesString(pattern + 1, str);
    default:  // Non-special character.  Matches itself.
      return *pattern == *str &&
          PatternMatchesString(pattern + 1, str + 1);
  }
}

bool UnitTestOptions::MatchesFilter(
    const std::string& name, const char* filter) {
  const char *cur_pattern = filter;
  for (;;) {
    if (PatternMatchesString(cur_pattern, name.c_str())) {
      return true;
    }

    // Finds the next pattern in the filter.
    cur_pattern = strchr(cur_pattern, ':');

    // Returns if no more pattern can be found.
    if (cur_pattern == NULL) {
      return false;
    }

    // Skips the pattern separater (the ':' character).
    cur_pattern++;
  }
}

// Returns true iff the user-specified filter matches the test case
// name and the test name.
bool UnitTestOptions::FilterMatchesTest(const std::string &test_case_name,
                                        const std::string &test_name) {
  const std::string& full_name = test_case_name + "." + test_name.c_str();

  // Split --gtest_filter at '-', if there is one, to separate into
  // positive filter and negative filter portions
  const char* const p = GTEST_FLAG(filter).c_str();
  const char* const dash = strchr(p, '-');
  std::string positive;
  std::string negative;
  if (dash == NULL) {
    positive = GTEST_FLAG(filter).c_str();  // Whole string is a positive filter
    negative = "";
  } else {
    positive = std::string(p, dash);   // Everything up to the dash
    negative = std::string(dash + 1);  // Everything after the dash
    if (positive.empty()) {
      // Treat '-test1' as the same as '*-test1'
      positive = kUniversalFilter;
    }
  }

  // A filter is a colon-separated list of patterns.  It matches a
  // test if any pattern in it matches the test.
  return (MatchesFilter(full_name, positive.c_str()) &&
          !MatchesFilter(full_name, negative.c_str()));
}

#if GTEST_HAS_SEH
// Returns EXCEPTION_EXECUTE_HANDLER if Google Test should handle the
// given SEH exception, or EXCEPTION_CONTINUE_SEARCH otherwise.
// This function is useful as an __except condition.
int UnitTestOptions::GTestShouldProcessSEH(DWORD exception_code) {
  // Google Test should handle a SEH exception if:
  //   1. the user wants it to, AND
  //   2. this is not a breakpoint exception, AND
  //   3. this is not a C++ exception (VC++ implements them via SEH,
  //      apparently).
  //
  // SEH exception code for C++ exceptions.
  // (see http://support.microsoft.com/kb/185294 for more information).
  const DWORD kCxxExceptionCode = 0xe06d7363;

  bool should_handle = true;

  if (!GTEST_FLAG(catch_exceptions))
    should_handle = false;
  else if (exception_code == EXCEPTION_BREAKPOINT)
    should_handle = false;
  else if (exception_code == kCxxExceptionCode)
    should_handle = false;

  return should_handle ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH;
}
#endif  // GTEST_HAS_SEH

}  // namespace internal

// The c'tor sets this object as the test part result reporter used by
// Google Test.  The 'result' parameter specifies where to report the
// results. Intercepts only failures from the current thread.
ScopedFakeTestPartResultReporter::ScopedFakeTestPartResultReporter(
    TestPartResultArray* result)
    : intercept_mode_(INTERCEPT_ONLY_CURRENT_THREAD),
      result_(result) {
  Init();
}

// The c'tor sets this object as the test part result reporter used by
// Google Test.  The 'result' parameter specifies where to report the
// results.
ScopedFakeTestPartResultReporter::ScopedFakeTestPartResultReporter(
    InterceptMode intercept_mode, TestPartResultArray* result)
    : intercept_mode_(intercept_mode),
      result_(result) {
  Init();
}

void ScopedFakeTestPartResultReporter::Init() {
  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  if (intercept_mode_ == INTERCEPT_ALL_THREADS) {
    old_reporter_ = impl->GetGlobalTestPartResultReporter();
    impl->SetGlobalTestPartResultReporter(this);
  } else {
    old_reporter_ = impl->GetTestPartResultReporterForCurrentThread();
    impl->SetTestPartResultReporterForCurrentThread(this);
  }
}

// The d'tor restores the test part result reporter used by Google Test
// before.
ScopedFakeTestPartResultReporter::~ScopedFakeTestPartResultReporter() {
  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  if (intercept_mode_ == INTERCEPT_ALL_THREADS) {
    impl->SetGlobalTestPartResultReporter(old_reporter_);
  } else {
    impl->SetTestPartResultReporterForCurrentThread(old_reporter_);
  }
}

// Increments the test part result count and remembers the result.
// This method is from the TestPartResultReporterInterface interface.
void ScopedFakeTestPartResultReporter::ReportTestPartResult(
    const TestPartResult& result) {
  result_->Append(result);
}

namespace internal {

// Returns the type ID of ::testing::Test.  We should always call this
// instead of GetTypeId< ::testing::Test>() to get the type ID of
// testing::Test.  This is to work around a suspected linker bug when
// using Google Test as a framework on Mac OS X.  The bug causes
// GetTypeId< ::testing::Test>() to return different values depending
// on whether the call is from the Google Test framework itself or
// from user test code.  GetTestTypeId() is guaranteed to always
// return the same value, as it always calls GetTypeId<>() from the
// gtest.cc, which is within the Google Test framework.
TypeId GetTestTypeId() {
  return GetTypeId<Test>();
}

// The value of GetTestTypeId() as seen from within the Google Test
// library.  This is solely for testing GetTestTypeId().
extern const TypeId kTestTypeIdInGoogleTest = GetTestTypeId();

// This predicate-formatter checks that 'results' contains a test part
// failure of the given type and that the failure message contains the
// given substring.
AssertionResult HasOneFailure(const char* /* results_expr */,
                              const char* /* type_expr */,
                              const char* /* substr_expr */,
                              const TestPartResultArray& results,
                              TestPartResult::Type type,
                              const string& substr) {
  const std::string expected(type == TestPartResult::kFatalFailure ?
                        "1 fatal failure" :
                        "1 non-fatal failure");
  Message msg;
  if (results.size() != 1) {
    msg << "Expected: " << expected << "\n"
        << "  Actual: " << results.size() << " failures";
    for (int i = 0; i < results.size(); i++) {
      msg << "\n" << results.GetTestPartResult(i);
    }
    return AssertionFailure() << msg;
  }

  const TestPartResult& r = results.GetTestPartResult(0);
  if (r.type() != type) {
    return AssertionFailure() << "Expected: " << expected << "\n"
                              << "  Actual:\n"
                              << r;
  }

  if (strstr(r.message(), substr.c_str()) == NULL) {
    return AssertionFailure() << "Expected: " << expected << " containing \""
                              << substr << "\"\n"
                              << "  Actual:\n"
                              << r;
  }

  return AssertionSuccess();
}

// The constructor of SingleFailureChecker remembers where to look up
// test part results, what type of failure we expect, and what
// substring the failure message should contain.
SingleFailureChecker:: SingleFailureChecker(
    const TestPartResultArray* results,
    TestPartResult::Type type,
    const string& substr)
    : results_(results),
      type_(type),
      substr_(substr) {}

// The destructor of SingleFailureChecker verifies that the given
// TestPartResultArray contains exactly one failure that has the given
// type and contains the given substring.  If that's not the case, a
// non-fatal failure will be generated.
SingleFailureChecker::~SingleFailureChecker() {
  EXPECT_PRED_FORMAT3(HasOneFailure, *results_, type_, substr_);
}

DefaultGlobalTestPartResultReporter::DefaultGlobalTestPartResultReporter(
    UnitTestImpl* unit_test) : unit_test_(unit_test) {}

void DefaultGlobalTestPartResultReporter::ReportTestPartResult(
    const TestPartResult& result) {
  unit_test_->current_test_result()->AddTestPartResult(result);
  unit_test_->listeners()->repeater()->OnTestPartResult(result);
}

DefaultPerThreadTestPartResultReporter::DefaultPerThreadTestPartResultReporter(
    UnitTestImpl* unit_test) : unit_test_(unit_test) {}

void DefaultPerThreadTestPartResultReporter::ReportTestPartResult(
    const TestPartResult& result) {
  unit_test_->GetGlobalTestPartResultReporter()->ReportTestPartResult(result);
}

// Returns the global test part result reporter.
TestPartResultReporterInterface*
UnitTestImpl::GetGlobalTestPartResultReporter() {
  internal::MutexLock lock(&global_test_part_result_reporter_mutex_);
  return global_test_part_result_repoter_;
}

// Sets the global test part result reporter.
void UnitTestImpl::SetGlobalTestPartResultReporter(
    TestPartResultReporterInterface* reporter) {
  internal::MutexLock lock(&global_test_part_result_reporter_mutex_);
  global_test_part_result_repoter_ = reporter;
}

// Returns the test part result reporter for the current thread.
TestPartResultReporterInterface*
UnitTestImpl::GetTestPartResultReporterForCurrentThread() {
  return per_thread_test_part_result_reporter_.get();
}

// Sets the test part result reporter for the current thread.
void UnitTestImpl::SetTestPartResultReporterForCurrentThread(
    TestPartResultReporterInterface* reporter) {
  per_thread_test_part_result_reporter_.set(reporter);
}

// Gets the number of successful test cases.
int UnitTestImpl::successful_test_case_count() const {
  return CountIf(test_cases_, TestCasePassed);
}

// Gets the number of failed test cases.
int UnitTestImpl::failed_test_case_count() const {
  return CountIf(test_cases_, TestCaseFailed);
}

// Gets the number of all test cases.
int UnitTestImpl::total_test_case_count() const {
  return static_cast<int>(test_cases_.size());
}

// Gets the number of all test cases that contain at least one test
// that should run.
int UnitTestImpl::test_case_to_run_count() const {
  return CountIf(test_cases_, ShouldRunTestCase);
}

// Gets the number of successful tests.
int UnitTestImpl::successful_test_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::successful_test_count);
}

// Gets the number of failed tests.
int UnitTestImpl::failed_test_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::failed_test_count);
}

// Gets the number of disabled tests that will be reported in the XML report.
int UnitTestImpl::reportable_disabled_test_count() const {
  return SumOverTestCaseList(test_cases_,
                             &TestCase::reportable_disabled_test_count);
}

// Gets the number of disabled tests.
int UnitTestImpl::disabled_test_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::disabled_test_count);
}

// Gets the number of tests to be printed in the XML report.
int UnitTestImpl::reportable_test_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::reportable_test_count);
}

// Gets the number of all tests.
int UnitTestImpl::total_test_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::total_test_count);
}

// Gets the number of tests that should run.
int UnitTestImpl::test_to_run_count() const {
  return SumOverTestCaseList(test_cases_, &TestCase::test_to_run_count);
}

// Returns the current OS stack trace as an std::string.
//
// The maximum number of stack frames to be included is specified by
// the gtest_stack_trace_depth flag.  The skip_count parameter
// specifies the number of top frames to be skipped, which doesn't
// count against the number of frames to be included.
//
// For example, if Foo() calls Bar(), which in turn calls
// CurrentOsStackTraceExceptTop(1), Foo() will be included in the
// trace but Bar() and CurrentOsStackTraceExceptTop() won't.
std::string UnitTestImpl::CurrentOsStackTraceExceptTop(int skip_count) {
  (void)skip_count;
  return "";
}

// Returns the current time in milliseconds.
TimeInMillis GetTimeInMillis() {
#if GTEST_OS_WINDOWS_MOBILE || defined(__BORLANDC__)
  // Difference between 1970-01-01 and 1601-01-01 in milliseconds.
  // http://analogous.blogspot.com/2005/04/epoch.html
  const TimeInMillis kJavaEpochToWinFileTimeDelta =
    static_cast<TimeInMillis>(116444736UL) * 100000UL;
  const DWORD kTenthMicrosInMilliSecond = 10000;

  SYSTEMTIME now_systime;
  FILETIME now_filetime;
  ULARGE_INTEGER now_int64;
  // TODO(kenton@google.com): Shouldn't this just use
  //   GetSystemTimeAsFileTime()?
  GetSystemTime(&now_systime);
  if (SystemTimeToFileTime(&now_systime, &now_filetime)) {
    now_int64.LowPart = now_filetime.dwLowDateTime;
    now_int64.HighPart = now_filetime.dwHighDateTime;
    now_int64.QuadPart = (now_int64.QuadPart / kTenthMicrosInMilliSecond) -
      kJavaEpochToWinFileTimeDelta;
    return now_int64.QuadPart;
  }
  return 0;
#elif GTEST_OS_WINDOWS && !GTEST_HAS_GETTIMEOFDAY_
  __timeb64 now;

# ifdef _MSC_VER

  // MSVC 8 deprecates _ftime64(), so we want to suppress warning 4996
  // (deprecated function) there.
  // TODO(kenton@google.com): Use GetTickCount()?  Or use
  //   SystemTimeToFileTime()
#  pragma warning(push)          // Saves the current warning state.
#  pragma warning(disable:4996)  // Temporarily disables warning 4996.
  _ftime64(&now);
#  pragma warning(pop)           // Restores the warning state.
# else

  _ftime64(&now);

# endif  // _MSC_VER

  return static_cast<TimeInMillis>(now.time) * 1000 + now.millitm;
#elif GTEST_HAS_GETTIMEOFDAY_
  struct timeval now;
  gettimeofday(&now, NULL);
  return static_cast<TimeInMillis>(now.tv_sec) * 1000 + now.tv_usec / 1000;
#else
# error "Don't know how to get the current time on your system."
#endif
}

// Utilities

// class String.

#if GTEST_OS_WINDOWS_MOBILE
// Creates a UTF-16 wide string from the given ANSI string, allocating
// memory using new. The caller is responsible for deleting the return
// value using delete[]. Returns the wide string, or NULL if the
// input is NULL.
LPCWSTR String::AnsiToUtf16(const char* ansi) {
  if (!ansi) return NULL;
  const int length = strlen(ansi);
  const int unicode_length =
      MultiByteToWideChar(CP_ACP, 0, ansi, length,
                          NULL, 0);
  WCHAR* unicode = new WCHAR[unicode_length + 1];
  MultiByteToWideChar(CP_ACP, 0, ansi, length,
                      unicode, unicode_length);
  unicode[unicode_length] = 0;
  return unicode;
}

// Creates an ANSI string from the given wide string, allocating
// memory using new. The caller is responsible for deleting the return
// value using delete[]. Returns the ANSI string, or NULL if the
// input is NULL.
const char* String::Utf16ToAnsi(LPCWSTR utf16_str)  {
  if (!utf16_str) return NULL;
  const int ansi_length =
      WideCharToMultiByte(CP_ACP, 0, utf16_str, -1,
                          NULL, 0, NULL, NULL);
  char* ansi = new char[ansi_length + 1];
  WideCharToMultiByte(CP_ACP, 0, utf16_str, -1,
                      ansi, ansi_length, NULL, NULL);
  ansi[ansi_length] = 0;
  return ansi;
}

#endif  // GTEST_OS_WINDOWS_MOBILE

// Compares two C strings.  Returns true iff they have the same content.
//
// Unlike strcmp(), this function can handle NULL argument(s).  A NULL
// C string is considered different to any non-NULL C string,
// including the empty string.
bool String::CStringEquals(const char * lhs, const char * rhs) {
  if ( lhs == NULL ) return rhs == NULL;

  if ( rhs == NULL ) return false;

  return strcmp(lhs, rhs) == 0;
}

#if GTEST_HAS_STD_WSTRING || GTEST_HAS_GLOBAL_WSTRING

// Converts an array of wide chars to a narrow string using the UTF-8
// encoding, and streams the result to the given Message object.
static void StreamWideCharsToMessage(const wchar_t* wstr, size_t length,
                                     Message* msg) {
  for (size_t i = 0; i != length; ) {  // NOLINT
    if (wstr[i] != L'\0') {
      *msg << WideStringToUtf8(wstr + i, static_cast<int>(length - i));
      while (i != length && wstr[i] != L'\0')
        i++;
    } else {
      *msg << '\0';
      i++;
    }
  }
}

#endif  // GTEST_HAS_STD_WSTRING || GTEST_HAS_GLOBAL_WSTRING

}  // namespace internal

// Constructs an empty Message.
// We allocate the stringstream separately because otherwise each use of
// ASSERT/EXPECT in a procedure adds over 200 bytes to the procedure's
// stack frame leading to huge stack frames in some cases; gcc does not reuse
// the stack space.
Message::Message() : ss_(new ::std::stringstream) {
  // By default, we want there to be enough precision when printing
  // a double to a Message.
  *ss_ << std::setprecision(std::numeric_limits<double>::digits10 + 2);
}

// These two overloads allow streaming a wide C string to a Message
// using the UTF-8 encoding.
Message& Message::operator <<(const wchar_t* wide_c_str) {
  return *this << internal::String::ShowWideCString(wide_c_str);
}
Message& Message::operator <<(wchar_t* wide_c_str) {
  return *this << internal::String::ShowWideCString(wide_c_str);
}

#if GTEST_HAS_STD_WSTRING
// Converts the given wide string to a narrow string using the UTF-8
// encoding, and streams the result to this Message object.
Message& Message::operator <<(const ::std::wstring& wstr) {
  internal::StreamWideCharsToMessage(wstr.c_str(), wstr.length(), this);
  return *this;
}
#endif  // GTEST_HAS_STD_WSTRING

#if GTEST_HAS_GLOBAL_WSTRING
// Converts the given wide string to a narrow string using the UTF-8
// encoding, and streams the result to this Message object.
Message& Message::operator <<(const ::wstring& wstr) {
  internal::StreamWideCharsToMessage(wstr.c_str(), wstr.length(), this);
  return *this;
}
#endif  // GTEST_HAS_GLOBAL_WSTRING

// Gets the text streamed to this object so far as an std::string.
// Each '\0' character in the buffer is replaced with "\\0".
std::string Message::GetString() const {
  return internal::StringStreamToString(ss_.get());
}

// AssertionResult constructors.
// Used in EXPECT_TRUE/FALSE(assertion_result).
AssertionResult::AssertionResult(const AssertionResult& other)
    : success_(other.success_),
      message_(other.message_.get() != NULL ?
               new ::std::string(*other.message_) :
               static_cast< ::std::string*>(NULL)) {
}

// Returns the assertion's negation. Used with EXPECT/ASSERT_FALSE.
AssertionResult AssertionResult::operator!() const {
  AssertionResult negation(!success_);
  if (message_.get() != NULL)
    negation << *message_;
  return negation;
}

// Makes a successful assertion result.
AssertionResult AssertionSuccess() {
  return AssertionResult(true);
}

// Makes a failed assertion result.
AssertionResult AssertionFailure() {
  return AssertionResult(false);
}

// Makes a failed assertion result with the given failure message.
// Deprecated; use AssertionFailure() << message.
AssertionResult AssertionFailure(const Message& message) {
  return AssertionFailure() << message;
}

namespace internal {

// Constructs and returns the message for an equality assertion
// (e.g. ASSERT_EQ, EXPECT_STREQ, etc) failure.
//
// The first four parameters are the expressions used in the assertion
// and their values, as strings.  For example, for ASSERT_EQ(foo, bar)
// where foo is 5 and bar is 6, we have:
//
//   expected_expression: "foo"
//   actual_expression:   "bar"
//   expected_value:      "5"
//   actual_value:        "6"
//
// The ignoring_case parameter is true iff the assertion is a
// *_STRCASEEQ*.  When it's true, the string " (ignoring case)" will
// be inserted into the message.
AssertionResult EqFailure(const char* expected_expression,
                          const char* actual_expression,
                          const std::string& expected_value,
                          const std::string& actual_value,
                          bool ignoring_case) {
  Message msg;
  msg << "Value of: " << actual_expression;
  if (actual_value != actual_expression) {
    msg << "\n  Actual: " << actual_value;
  }

  msg << "\nExpected: " << expected_expression;
  if (ignoring_case) {
    msg << " (ignoring case)";
  }
  if (expected_value != expected_expression) {
    msg << "\nWhich is: " << expected_value;
  }

  return AssertionFailure() << msg;
}

// Constructs a failure message for Boolean assertions such as EXPECT_TRUE.
std::string GetBoolAssertionFailureMessage(
    const AssertionResult& assertion_result,
    const char* expression_text,
    const char* actual_predicate_value,
    const char* expected_predicate_value) {
  const char* actual_message = assertion_result.message();
  Message msg;
  msg << "Value of: " << expression_text
      << "\n  Actual: " << actual_predicate_value;
  if (actual_message[0] != '\0')
    msg << " (" << actual_message << ")";
  msg << "\nExpected: " << expected_predicate_value;
  return msg.GetString();
}

// Helper function for implementing ASSERT_NEAR.
AssertionResult DoubleNearPredFormat(const char* expr1,
                                     const char* expr2,
                                     const char* abs_error_expr,
                                     double val1,
                                     double val2,
                                     double abs_error) {
  const double diff = fabs(val1 - val2);
  if (diff <= abs_error) return AssertionSuccess();

  // TODO(wan): do not print the value of an expression if it's
  // already a literal.
  return AssertionFailure()
      << "The difference between " << expr1 << " and " << expr2
      << " is " << diff << ", which exceeds " << abs_error_expr << ", where\n"
      << expr1 << " evaluates to " << val1 << ",\n"
      << expr2 << " evaluates to " << val2 << ", and\n"
      << abs_error_expr << " evaluates to " << abs_error << ".";
}


// Helper template for implementing FloatLE() and DoubleLE().
template <typename RawType>
AssertionResult FloatingPointLE(const char* expr1,
                                const char* expr2,
                                RawType val1,
                                RawType val2) {
  // Returns success if val1 is less than val2,
  if (val1 < val2) {
    return AssertionSuccess();
  }

  // or if val1 is almost equal to val2.
  const FloatingPoint<RawType> lhs(val1), rhs(val2);
  if (lhs.AlmostEquals(rhs)) {
    return AssertionSuccess();
  }

  // Note that the above two checks will both fail if either val1 or
  // val2 is NaN, as the IEEE floating-point standard requires that
  // any predicate involving a NaN must return false.

  ::std::stringstream val1_ss;
  val1_ss << std::setprecision(std::numeric_limits<RawType>::digits10 + 2)
          << val1;

  ::std::stringstream val2_ss;
  val2_ss << std::setprecision(std::numeric_limits<RawType>::digits10 + 2)
          << val2;

  return AssertionFailure()
      << "Expected: (" << expr1 << ") <= (" << expr2 << ")\n"
      << "  Actual: " << StringStreamToString(&val1_ss) << " vs "
      << StringStreamToString(&val2_ss);
}

}  // namespace internal

// Asserts that val1 is less than, or almost equal to, val2.  Fails
// otherwise.  In particular, it fails if either val1 or val2 is NaN.
AssertionResult FloatLE(const char* expr1, const char* expr2,
                        float val1, float val2) {
  return internal::FloatingPointLE<float>(expr1, expr2, val1, val2);
}

// Asserts that val1 is less than, or almost equal to, val2.  Fails
// otherwise.  In particular, it fails if either val1 or val2 is NaN.
AssertionResult DoubleLE(const char* expr1, const char* expr2,
                         double val1, double val2) {
  return internal::FloatingPointLE<double>(expr1, expr2, val1, val2);
}

namespace internal {

// The helper function for {ASSERT|EXPECT}_EQ with int or enum
// arguments.
AssertionResult CmpHelperEQ(const char* expected_expression,
                            const char* actual_expression,
                            BiggestInt expected,
                            BiggestInt actual) {
  if (expected == actual) {
    return AssertionSuccess();
  }

  return EqFailure(expected_expression,
                   actual_expression,
                   FormatForComparisonFailureMessage(expected, actual),
                   FormatForComparisonFailureMessage(actual, expected),
                   false);
}

// A macro for implementing the helper functions needed to implement
// ASSERT_?? and EXPECT_?? with integer or enum arguments.  It is here
// just to avoid copy-and-paste of similar code.
#define GTEST_IMPL_CMP_HELPER_(op_name, op)\
AssertionResult CmpHelper##op_name(const char* expr1, const char* expr2, \
                                   BiggestInt val1, BiggestInt val2) {\
  if (val1 op val2) {\
    return AssertionSuccess();\
  } else {\
    return AssertionFailure() \
        << "Expected: (" << expr1 << ") " #op " (" << expr2\
        << "), actual: " << FormatForComparisonFailureMessage(val1, val2)\
        << " vs " << FormatForComparisonFailureMessage(val2, val1);\
  }\
}

// Implements the helper function for {ASSERT|EXPECT}_NE with int or
// enum arguments.
GTEST_IMPL_CMP_HELPER_(NE, !=)
// Implements the helper function for {ASSERT|EXPECT}_LE with int or
// enum arguments.
GTEST_IMPL_CMP_HELPER_(LE, <=)
// Implements the helper function for {ASSERT|EXPECT}_LT with int or
// enum arguments.
GTEST_IMPL_CMP_HELPER_(LT, < )
// Implements the helper function for {ASSERT|EXPECT}_GE with int or
// enum arguments.
GTEST_IMPL_CMP_HELPER_(GE, >=)
// Implements the helper function for {ASSERT|EXPECT}_GT with int or
// enum arguments.
GTEST_IMPL_CMP_HELPER_(GT, > )

#undef GTEST_IMPL_CMP_HELPER_

// The helper function for {ASSERT|EXPECT}_STREQ.
AssertionResult CmpHelperSTREQ(const char* expected_expression,
                               const char* actual_expression,
                               const char* expected,
                               const char* actual) {
  if (String::CStringEquals(expected, actual)) {
    return AssertionSuccess();
  }

  return EqFailure(expected_expression,
                   actual_expression,
                   PrintToString(expected),
                   PrintToString(actual),
                   false);
}

// The helper function for {ASSERT|EXPECT}_STRCASEEQ.
AssertionResult CmpHelperSTRCASEEQ(const char* expected_expression,
                                   const char* actual_expression,
                                   const char* expected,
                                   const char* actual) {
  if (String::CaseInsensitiveCStringEquals(expected, actual)) {
    return AssertionSuccess();
  }

  return EqFailure(expected_expression,
                   actual_expression,
                   PrintToString(expected),
                   PrintToString(actual),
                   true);
}

// The helper function for {ASSERT|EXPECT}_STRNE.
AssertionResult CmpHelperSTRNE(const char* s1_expression,
                               const char* s2_expression,
                               const char* s1,
                               const char* s2) {
  if (!String::CStringEquals(s1, s2)) {
    return AssertionSuccess();
  } else {
    return AssertionFailure() << "Expected: (" << s1_expression << ") != ("
                              << s2_expression << "), actual: \""
                              << s1 << "\" vs \"" << s2 << "\"";
  }
}

// The helper function for {ASSERT|EXPECT}_STRCASENE.
AssertionResult CmpHelperSTRCASENE(const char* s1_expression,
                                   const char* s2_expression,
                                   const char* s1,
                                   const char* s2) {
  if (!String::CaseInsensitiveCStringEquals(s1, s2)) {
    return AssertionSuccess();
  } else {
    return AssertionFailure()
        << "Expected: (" << s1_expression << ") != ("
        << s2_expression << ") (ignoring case), actual: \""
        << s1 << "\" vs \"" << s2 << "\"";
  }
}

}  // namespace internal

namespace {

// Helper functions for implementing IsSubString() and IsNotSubstring().

// This group of overloaded functions return true iff needle is a
// substring of haystack.  NULL is considered a substring of itself
// only.

bool IsSubstringPred(const char* needle, const char* haystack) {
  if (needle == NULL || haystack == NULL)
    return needle == haystack;

  return strstr(haystack, needle) != NULL;
}

bool IsSubstringPred(const wchar_t* needle, const wchar_t* haystack) {
  if (needle == NULL || haystack == NULL)
    return needle == haystack;

  return wcsstr(haystack, needle) != NULL;
}

// StringType here can be either ::std::string or ::std::wstring.
template <typename StringType>
bool IsSubstringPred(const StringType& needle,
                     const StringType& haystack) {
  return haystack.find(needle) != StringType::npos;
}

// This function implements either IsSubstring() or IsNotSubstring(),
// depending on the value of the expected_to_be_substring parameter.
// StringType here can be const char*, const wchar_t*, ::std::string,
// or ::std::wstring.
template <typename StringType>
AssertionResult IsSubstringImpl(
    bool expected_to_be_substring,
    const char* needle_expr, const char* haystack_expr,
    const StringType& needle, const StringType& haystack) {
  if (IsSubstringPred(needle, haystack) == expected_to_be_substring)
    return AssertionSuccess();

  const bool is_wide_string = sizeof(needle[0]) > 1;
  const char* const begin_string_quote = is_wide_string ? "L\"" : "\"";
  return AssertionFailure()
      << "Value of: " << needle_expr << "\n"
      << "  Actual: " << begin_string_quote << needle << "\"\n"
      << "Expected: " << (expected_to_be_substring ? "" : "not ")
      << "a substring of " << haystack_expr << "\n"
      << "Which is: " << begin_string_quote << haystack << "\"";
}

}  // namespace

// IsSubstring() and IsNotSubstring() check whether needle is a
// substring of haystack (NULL is considered a substring of itself
// only), and return an appropriate error message when they fail.

AssertionResult IsSubstring(
    const char* needle_expr, const char* haystack_expr,
    const char* needle, const char* haystack) {
  return IsSubstringImpl(true, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsSubstring(
    const char* needle_expr, const char* haystack_expr,
    const wchar_t* needle, const wchar_t* haystack) {
  return IsSubstringImpl(true, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsNotSubstring(
    const char* needle_expr, const char* haystack_expr,
    const char* needle, const char* haystack) {
  return IsSubstringImpl(false, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsNotSubstring(
    const char* needle_expr, const char* haystack_expr,
    const wchar_t* needle, const wchar_t* haystack) {
  return IsSubstringImpl(false, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsSubstring(
    const char* needle_expr, const char* haystack_expr,
    const ::std::string& needle, const ::std::string& haystack) {
  return IsSubstringImpl(true, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsNotSubstring(
    const char* needle_expr, const char* haystack_expr,
    const ::std::string& needle, const ::std::string& haystack) {
  return IsSubstringImpl(false, needle_expr, haystack_expr, needle, haystack);
}

#if GTEST_HAS_STD_WSTRING
AssertionResult IsSubstring(
    const char* needle_expr, const char* haystack_expr,
    const ::std::wstring& needle, const ::std::wstring& haystack) {
  return IsSubstringImpl(true, needle_expr, haystack_expr, needle, haystack);
}

AssertionResult IsNotSubstring(
    const char* needle_expr, const char* haystack_expr,
    const ::std::wstring& needle, const ::std::wstring& haystack) {
  return IsSubstringImpl(false, needle_expr, haystack_expr, needle, haystack);
}
#endif  // GTEST_HAS_STD_WSTRING

namespace internal {

#if GTEST_OS_WINDOWS

namespace {

// Helper function for IsHRESULT{SuccessFailure} predicates
AssertionResult HRESULTFailureHelper(const char* expr,
                                     const char* expected,
                                     long hr) {  // NOLINT
# if GTEST_OS_WINDOWS_MOBILE

  // Windows CE doesn't support FormatMessage.
  const char error_text[] = "";

# else

  // Looks up the human-readable system message for the HRESULT code
  // and since we're not passing any params to FormatMessage, we don't
  // want inserts expanded.
  const DWORD kFlags = FORMAT_MESSAGE_FROM_SYSTEM |
                       FORMAT_MESSAGE_IGNORE_INSERTS;
  const DWORD kBufSize = 4096;
  // Gets the system's human readable message string for this HRESULT.
  char error_text[kBufSize] = { '\0' };
  DWORD message_length = ::FormatMessageA(kFlags,
                                          0,  // no source, we're asking system
                                          hr,  // the error
                                          0,  // no line width restrictions
                                          error_text,  // output buffer
                                          kBufSize,  // buf size
                                          NULL);  // no arguments for inserts
  // Trims tailing white space (FormatMessage leaves a trailing CR-LF)
  for (; message_length && IsSpace(error_text[message_length - 1]);
          --message_length) {
    error_text[message_length - 1] = '\0';
  }

# endif  // GTEST_OS_WINDOWS_MOBILE

  const std::string error_hex("0x" + String::FormatHexInt(hr));
  return ::testing::AssertionFailure()
      << "Expected: " << expr << " " << expected << ".\n"
      << "  Actual: " << error_hex << " " << error_text << "\n";
}

}  // namespace

AssertionResult IsHRESULTSuccess(const char* expr, long hr) {  // NOLINT
  if (SUCCEEDED(hr)) {
    return AssertionSuccess();
  }
  return HRESULTFailureHelper(expr, "succeeds", hr);
}

AssertionResult IsHRESULTFailure(const char* expr, long hr) {  // NOLINT
  if (FAILED(hr)) {
    return AssertionSuccess();
  }
  return HRESULTFailureHelper(expr, "fails", hr);
}

#endif  // GTEST_OS_WINDOWS

// Utility functions for encoding Unicode text (wide strings) in
// UTF-8.

// A Unicode code-point can have upto 21 bits, and is encoded in UTF-8
// like this:
//
// Code-point length   Encoding
//   0 -  7 bits       0xxxxxxx
//   8 - 11 bits       110xxxxx 10xxxxxx
//  12 - 16 bits       1110xxxx 10xxxxxx 10xxxxxx
//  17 - 21 bits       11110xxx 10xxxxxx 10xxxxxx 10xxxxxx

// The maximum code-point a one-byte UTF-8 sequence can represent.
const UInt32 kMaxCodePoint1 = (static_cast<UInt32>(1) <<  7) - 1;

// The maximum code-point a two-byte UTF-8 sequence can represent.
const UInt32 kMaxCodePoint2 = (static_cast<UInt32>(1) << (5 + 6)) - 1;

// The maximum code-point a three-byte UTF-8 sequence can represent.
const UInt32 kMaxCodePoint3 = (static_cast<UInt32>(1) << (4 + 2*6)) - 1;

// The maximum code-point a four-byte UTF-8 sequence can represent.
const UInt32 kMaxCodePoint4 = (static_cast<UInt32>(1) << (3 + 3*6)) - 1;

// Chops off the n lowest bits from a bit pattern.  Returns the n
// lowest bits.  As a side effect, the original bit pattern will be
// shifted to the right by n bits.
inline UInt32 ChopLowBits(UInt32* bits, int n) {
  const UInt32 low_bits = *bits & ((static_cast<UInt32>(1) << n) - 1);
  *bits >>= n;
  return low_bits;
}

// Converts a Unicode code point to a narrow string in UTF-8 encoding.
// code_point parameter is of type UInt32 because wchar_t may not be
// wide enough to contain a code point.
// If the code_point is not a valid Unicode code point
// (i.e. outside of Unicode range U+0 to U+10FFFF) it will be converted
// to "(Invalid Unicode 0xXXXXXXXX)".
std::string CodePointToUtf8(UInt32 code_point) {
  if (code_point > kMaxCodePoint4) {
    return "(Invalid Unicode 0x" + String::FormatHexInt(code_point) + ")";
  }

  char str[5];  // Big enough for the largest valid code point.
  if (code_point <= kMaxCodePoint1) {
    str[1] = '\0';
    str[0] = static_cast<char>(code_point);                          // 0xxxxxxx
  } else if (code_point <= kMaxCodePoint2) {
    str[2] = '\0';
    str[1] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[0] = static_cast<char>(0xC0 | code_point);                   // 110xxxxx
  } else if (code_point <= kMaxCodePoint3) {
    str[3] = '\0';
    str[2] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[1] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[0] = static_cast<char>(0xE0 | code_point);                   // 1110xxxx
  } else {  // code_point <= kMaxCodePoint4
    str[4] = '\0';
    str[3] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[2] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[1] = static_cast<char>(0x80 | ChopLowBits(&code_point, 6));  // 10xxxxxx
    str[0] = static_cast<char>(0xF0 | code_point);                   // 11110xxx
  }
  return str;
}

// The following two functions only make sense if the the system
// uses UTF-16 for wide string encoding. All supported systems
// with 16 bit wchar_t (Windows, Cygwin, Symbian OS) do use UTF-16.

// Determines if the arguments constitute UTF-16 surrogate pair
// and thus should be combined into a single Unicode code point
// using CreateCodePointFromUtf16SurrogatePair.
inline bool IsUtf16SurrogatePair(wchar_t first, wchar_t second) {
  return sizeof(wchar_t) == 2 &&
      (first & 0xFC00) == 0xD800 && (second & 0xFC00) == 0xDC00;
}

// Creates a Unicode code point from UTF16 surrogate pair.
inline UInt32 CreateCodePointFromUtf16SurrogatePair(wchar_t first,
                                                    wchar_t second) {
  const UInt32 mask = (1 << 10) - 1;
  return (sizeof(wchar_t) == 2) ?
      (((first & mask) << 10) | (second & mask)) + 0x10000 :
      // This function should not be called when the condition is
      // false, but we provide a sensible default in case it is.
      static_cast<UInt32>(first);
}

// Converts a wide string to a narrow string in UTF-8 encoding.
// The wide string is assumed to have the following encoding:
//   UTF-16 if sizeof(wchar_t) == 2 (on Windows, Cygwin, Symbian OS)
//   UTF-32 if sizeof(wchar_t) == 4 (on Linux)
// Parameter str points to a null-terminated wide string.
// Parameter num_chars may additionally limit the number
// of wchar_t characters processed. -1 is used when the entire string
// should be processed.
// If the string contains code points that are not valid Unicode code points
// (i.e. outside of Unicode range U+0 to U+10FFFF) they will be output
// as '(Invalid Unicode 0xXXXXXXXX)'. If the string is in UTF16 encoding
// and contains invalid UTF-16 surrogate pairs, values in those pairs
// will be encoded as individual Unicode characters from Basic Normal Plane.
std::string WideStringToUtf8(const wchar_t* str, int num_chars) {
  if (num_chars == -1)
    num_chars = static_cast<int>(wcslen(str));

  ::std::stringstream stream;
  for (int i = 0; i < num_chars; ++i) {
    UInt32 unicode_code_point;

    if (str[i] == L'\0') {
      break;
    } else if (i + 1 < num_chars && IsUtf16SurrogatePair(str[i], str[i + 1])) {
      unicode_code_point = CreateCodePointFromUtf16SurrogatePair(str[i],
                                                                 str[i + 1]);
      i++;
    } else {
      unicode_code_point = static_cast<UInt32>(str[i]);
    }

    stream << CodePointToUtf8(unicode_code_point);
  }
  return StringStreamToString(&stream);
}

// Converts a wide C string to an std::string using the UTF-8 encoding.
// NULL will be converted to "(null)".
std::string String::ShowWideCString(const wchar_t * wide_c_str) {
  if (wide_c_str == NULL)  return "(null)";

  return internal::WideStringToUtf8(wide_c_str, -1);
}

// Compares two wide C strings.  Returns true iff they have the same
// content.
//
// Unlike wcscmp(), this function can handle NULL argument(s).  A NULL
// C string is considered different to any non-NULL C string,
// including the empty string.
bool String::WideCStringEquals(const wchar_t * lhs, const wchar_t * rhs) {
  if (lhs == NULL) return rhs == NULL;

  if (rhs == NULL) return false;

  return wcscmp(lhs, rhs) == 0;
}

// Helper function for *_STREQ on wide strings.
AssertionResult CmpHelperSTREQ(const char* expected_expression,
                               const char* actual_expression,
                               const wchar_t* expected,
                               const wchar_t* actual) {
  if (String::WideCStringEquals(expected, actual)) {
    return AssertionSuccess();
  }

  return EqFailure(expected_expression,
                   actual_expression,
                   PrintToString(expected),
                   PrintToString(actual),
                   false);
}

// Helper function for *_STRNE on wide strings.
AssertionResult CmpHelperSTRNE(const char* s1_expression,
                               const char* s2_expression,
                               const wchar_t* s1,
                               const wchar_t* s2) {
  if (!String::WideCStringEquals(s1, s2)) {
    return AssertionSuccess();
  }

  return AssertionFailure() << "Expected: (" << s1_expression << ") != ("
                            << s2_expression << "), actual: "
                            << PrintToString(s1)
                            << " vs " << PrintToString(s2);
}

// Compares two C strings, ignoring case.  Returns true iff they have
// the same content.
//
// Unlike strcasecmp(), this function can handle NULL argument(s).  A
// NULL C string is considered different to any non-NULL C string,
// including the empty string.
bool String::CaseInsensitiveCStringEquals(const char * lhs, const char * rhs) {
  if (lhs == NULL)
    return rhs == NULL;
  if (rhs == NULL)
    return false;
  return posix::StrCaseCmp(lhs, rhs) == 0;
}

  // Compares two wide C strings, ignoring case.  Returns true iff they
  // have the same content.
  //
  // Unlike wcscasecmp(), this function can handle NULL argument(s).
  // A NULL C string is considered different to any non-NULL wide C string,
  // including the empty string.
  // NB: The implementations on different platforms slightly differ.
  // On windows, this method uses _wcsicmp which compares according to LC_CTYPE
  // environment variable. On GNU platform this method uses wcscasecmp
  // which compares according to LC_CTYPE category of the current locale.
  // On MacOS X, it uses towlower, which also uses LC_CTYPE category of the
  // current locale.
bool String::CaseInsensitiveWideCStringEquals(const wchar_t* lhs,
                                              const wchar_t* rhs) {
  if (lhs == NULL) return rhs == NULL;

  if (rhs == NULL) return false;

#if GTEST_OS_WINDOWS
  return _wcsicmp(lhs, rhs) == 0;
#elif GTEST_OS_LINUX && !GTEST_OS_LINUX_ANDROID
  return wcscasecmp(lhs, rhs) == 0;
#else
  // Android, Mac OS X and Cygwin don't define wcscasecmp.
  // Other unknown OSes may not define it either.
  wint_t left, right;
  do {
    left = towlower(*lhs++);
    right = towlower(*rhs++);
  } while (left && left == right);
  return left == right;
#endif  // OS selector
}

// Returns true iff str ends with the given suffix, ignoring case.
// Any string is considered to end with an empty suffix.
bool String::EndsWithCaseInsensitive(
    const std::string& str, const std::string& suffix) {
  const size_t str_len = str.length();
  const size_t suffix_len = suffix.length();
  return (str_len >= suffix_len) &&
         CaseInsensitiveCStringEquals(str.c_str() + str_len - suffix_len,
                                      suffix.c_str());
}

// Formats an int value as "%02d".
std::string String::FormatIntWidth2(int value) {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << value;
  return ss.str();
}

// Formats an int value as "%X".
std::string String::FormatHexInt(int value) {
  std::stringstream ss;
  ss << std::hex << std::uppercase << value;
  return ss.str();
}

// Formats a byte as "%02X".
std::string String::FormatByte(unsigned char value) {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << std::hex << std::uppercase
     << static_cast<unsigned int>(value);
  return ss.str();
}

// Converts the buffer in a stringstream to an std::string, converting NUL
// bytes to "\\0" along the way.
std::string StringStreamToString(::std::stringstream* ss) {
  const ::std::string& str = ss->str();
  const char* const start = str.c_str();
  const char* const end = start + str.length();

  std::string result;
  result.reserve(2 * (end - start));
  for (const char* ch = start; ch != end; ++ch) {
    if (*ch == '\0') {
      result += "\\0";  // Replaces NUL with "\\0";
    } else {
      result += *ch;
    }
  }

  return result;
}

// Appends the user-supplied message to the Google-Test-generated message.
std::string AppendUserMessage(const std::string& gtest_msg,
                              const Message& user_msg) {
  // Appends the user message if it's non-empty.
  const std::string user_msg_string = user_msg.GetString();
  if (user_msg_string.empty()) {
    return gtest_msg;
  }

  return gtest_msg + "\n" + user_msg_string;
}

}  // namespace internal

// class TestResult

// Creates an empty TestResult.
TestResult::TestResult()
    : death_test_count_(0),
      elapsed_time_(0) {
}

// D'tor.
TestResult::~TestResult() {
}

// Returns the i-th test part result among all the results. i can
// range from 0 to total_part_count() - 1. If i is not in that range,
// aborts the program.
const TestPartResult& TestResult::GetTestPartResult(int i) const {
  if (i < 0 || i >= total_part_count())
    internal::posix::Abort();
  return test_part_results_.at(i);
}

// Returns the i-th test property. i can range from 0 to
// test_property_count() - 1. If i is not in that range, aborts the
// program.
const TestProperty& TestResult::GetTestProperty(int i) const {
  if (i < 0 || i >= test_property_count())
    internal::posix::Abort();
  return test_properties_.at(i);
}

// Clears the test part results.
void TestResult::ClearTestPartResults() {
  test_part_results_.clear();
}

// Adds a test part result to the list.
void TestResult::AddTestPartResult(const TestPartResult& test_part_result) {
  test_part_results_.push_back(test_part_result);
}

// Adds a test property to the list. If a property with the same key as the
// supplied property is already represented, the value of this test_property
// replaces the old value for that key.
void TestResult::RecordProperty(const std::string& xml_element,
                                const TestProperty& test_property) {
  if (!ValidateTestProperty(xml_element, test_property)) {
    return;
  }
  internal::MutexLock lock(&test_properites_mutex_);
  const std::vector<TestProperty>::iterator property_with_matching_key =
      std::find_if(test_properties_.begin(), test_properties_.end(),
                   internal::TestPropertyKeyIs(test_property.key()));
  if (property_with_matching_key == test_properties_.end()) {
    test_properties_.push_back(test_property);
    return;
  }
  property_with_matching_key->SetValue(test_property.value());
}

// The list of reserved attributes used in the <testsuites> element of XML
// output.
static const char* const kReservedTestSuitesAttributes[] = {
  "disabled",
  "errors",
  "failures",
  "name",
  "random_seed",
  "tests",
  "time",
  "timestamp"
};

// The list of reserved attributes used in the <testsuite> element of XML
// output.
static const char* const kReservedTestSuiteAttributes[] = {
  "disabled",
  "errors",
  "failures",
  "name",
  "tests",
  "time"
};

// The list of reserved attributes used in the <testcase> element of XML output.
static const char* const kReservedTestCaseAttributes[] = {
  "classname",
  "name",
  "status",
  "time",
  "type_param",
  "value_param"
};

template <int kSize>
std::vector<std::string> ArrayAsVector(const char* const (&array)[kSize]) {
  return std::vector<std::string>(array, array + kSize);
}

static std::vector<std::string> GetReservedAttributesForElement(
    const std::string& xml_element) {
  if (xml_element == "testsuites") {
    return ArrayAsVector(kReservedTestSuitesAttributes);
  } else if (xml_element == "testsuite") {
    return ArrayAsVector(kReservedTestSuiteAttributes);
  } else if (xml_element == "testcase") {
    return ArrayAsVector(kReservedTestCaseAttributes);
  } else {
    GTEST_CHECK_(false) << "Unrecognized xml_element provided: " << xml_element;
  }
  // This code is unreachable but some compilers may not realizes that.
  return std::vector<std::string>();
}

static std::string FormatWordList(const std::vector<std::string>& words) {
  Message word_list;
  for (size_t i = 0; i < words.size(); ++i) {
    if (i > 0 && words.size() > 2) {
      word_list << ", ";
    }
    if (i == words.size() - 1) {
      word_list << "and ";
    }
    word_list << "'" << words[i] << "'";
  }
  return word_list.GetString();
}

bool ValidateTestPropertyName(const std::string& property_name,
                              const std::vector<std::string>& reserved_names) {
  if (std::find(reserved_names.begin(), reserved_names.end(), property_name) !=
          reserved_names.end()) {
    ADD_FAILURE() << "Reserved key used in RecordProperty(): " << property_name
                  << " (" << FormatWordList(reserved_names)
                  << " are reserved by " << GTEST_NAME_ << ")";
    return false;
  }
  return true;
}

// Adds a failure if the key is a reserved attribute of the element named
// xml_element.  Returns true if the property is valid.
bool TestResult::ValidateTestProperty(const std::string& xml_element,
                                      const TestProperty& test_property) {
  return ValidateTestPropertyName(test_property.key(),
                                  GetReservedAttributesForElement(xml_element));
}

// Clears the object.
void TestResult::Clear() {
  test_part_results_.clear();
  test_properties_.clear();
  death_test_count_ = 0;
  elapsed_time_ = 0;
}

// Returns true iff the test failed.
bool TestResult::Failed() const {
  for (int i = 0; i < total_part_count(); ++i) {
    if (GetTestPartResult(i).failed())
      return true;
  }
  return false;
}

// Returns true iff the test part fatally failed.
static bool TestPartFatallyFailed(const TestPartResult& result) {
  return result.fatally_failed();
}

// Returns true iff the test fatally failed.
bool TestResult::HasFatalFailure() const {
  return CountIf(test_part_results_, TestPartFatallyFailed) > 0;
}

// Returns true iff the test part non-fatally failed.
static bool TestPartNonfatallyFailed(const TestPartResult& result) {
  return result.nonfatally_failed();
}

// Returns true iff the test has a non-fatal failure.
bool TestResult::HasNonfatalFailure() const {
  return CountIf(test_part_results_, TestPartNonfatallyFailed) > 0;
}

// Gets the number of all test parts.  This is the sum of the number
// of successful test parts and the number of failed test parts.
int TestResult::total_part_count() const {
  return static_cast<int>(test_part_results_.size());
}

// Returns the number of the test properties.
int TestResult::test_property_count() const {
  return static_cast<int>(test_properties_.size());
}

// class Test

// Creates a Test object.

// The c'tor saves the values of all Google Test flags.
Test::Test()
    : gtest_flag_saver_(new internal::GTestFlagSaver) {
}

// The d'tor restores the values of all Google Test flags.
Test::~Test() {
  delete gtest_flag_saver_;
}

// Sets up the test fixture.
//
// A sub-class may override this.
void Test::SetUp() {
}

// Tears down the test fixture.
//
// A sub-class may override this.
void Test::TearDown() {
}

// Allows user supplied key value pairs to be recorded for later output.
void Test::RecordProperty(const std::string& key, const std::string& value) {
  UnitTest::GetInstance()->RecordProperty(key, value);
}

// Allows user supplied key value pairs to be recorded for later output.
void Test::RecordProperty(const std::string& key, int value) {
  Message value_message;
  value_message << value;
  RecordProperty(key, value_message.GetString().c_str());
}

namespace internal {

void ReportFailureInUnknownLocation(TestPartResult::Type result_type,
                                    const std::string& message) {
  // This function is a friend of UnitTest and as such has access to
  // AddTestPartResult.
  UnitTest::GetInstance()->AddTestPartResult(
      result_type,
      NULL,  // No info about the source file where the exception occurred.
      -1,    // We have no info on which line caused the exception.
      message,
      "");   // No stack trace, either.
}

}  // namespace internal

// Google Test requires all tests in the same test case to use the same test
// fixture class.  This function checks if the current test has the
// same fixture class as the first test in the current test case.  If
// yes, it returns true; otherwise it generates a Google Test failure and
// returns false.
bool Test::HasSameFixtureClass() {
  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  const TestCase* const test_case = impl->current_test_case();

  // Info about the first test in the current test case.
  const TestInfo* const first_test_info = test_case->test_info_list()[0];
  const internal::TypeId first_fixture_id = first_test_info->fixture_class_id_;
  const char* const first_test_name = first_test_info->name();

  // Info about the current test.
  const TestInfo* const this_test_info = impl->current_test_info();
  const internal::TypeId this_fixture_id = this_test_info->fixture_class_id_;
  const char* const this_test_name = this_test_info->name();

  if (this_fixture_id != first_fixture_id) {
    // Is the first test defined using TEST?
    const bool first_is_TEST = first_fixture_id == internal::GetTestTypeId();
    // Is this test defined using TEST?
    const bool this_is_TEST = this_fixture_id == internal::GetTestTypeId();

    if (first_is_TEST || this_is_TEST) {
      // The user mixed TEST and TEST_F in this test case - we'll tell
      // him/her how to fix it.

      // Gets the name of the TEST and the name of the TEST_F.  Note
      // that first_is_TEST and this_is_TEST cannot both be true, as
      // the fixture IDs are different for the two tests.
      const char* const TEST_name =
          first_is_TEST ? first_test_name : this_test_name;
      const char* const TEST_F_name =
          first_is_TEST ? this_test_name : first_test_name;

      ADD_FAILURE()
          << "All tests in the same test case must use the same test fixture\n"
          << "class, so mixing TEST_F and TEST in the same test case is\n"
          << "illegal.  In test case " << this_test_info->test_case_name()
          << ",\n"
          << "test " << TEST_F_name << " is defined using TEST_F but\n"
          << "test " << TEST_name << " is defined using TEST.  You probably\n"
          << "want to change the TEST to TEST_F or move it to another test\n"
          << "case.";
    } else {
      // The user defined two fixture classes with the same name in
      // two namespaces - we'll tell him/her how to fix it.
      ADD_FAILURE()
          << "All tests in the same test case must use the same test fixture\n"
          << "class.  However, in test case "
          << this_test_info->test_case_name() << ",\n"
          << "you defined test " << first_test_name
          << " and test " << this_test_name << "\n"
          << "using two different test fixture classes.  This can happen if\n"
          << "the two classes are from different namespaces or translation\n"
          << "units and have the same name.  You should probably rename one\n"
          << "of the classes to put the tests into different test cases.";
    }
    return false;
  }

  return true;
}

#if GTEST_HAS_SEH

// Adds an "exception thrown" fatal failure to the current test.  This
// function returns its result via an output parameter pointer because VC++
// prohibits creation of objects with destructors on stack in functions
// using __try (see error C2712).
static std::string* FormatSehExceptionMessage(DWORD exception_code,
                                              const char* location) {
  Message message;
  message << "SEH exception with code 0x" << std::setbase(16) <<
    exception_code << std::setbase(10) << " thrown in " << location << ".";

  return new std::string(message.GetString());
}

#endif  // GTEST_HAS_SEH

namespace internal {

#if GTEST_HAS_EXCEPTIONS

// Adds an "exception thrown" fatal failure to the current test.
static std::string FormatCxxExceptionMessage(const char* description,
                                             const char* location) {
  Message message;
  if (description != NULL) {
    message << "C++ exception with description \"" << description << "\"";
  } else {
    message << "Unknown C++ exception";
  }
  message << " thrown in " << location << ".";

  return message.GetString();
}

static std::string PrintTestPartResultToString(
    const TestPartResult& test_part_result);

GoogleTestFailureException::GoogleTestFailureException(
    const TestPartResult& failure)
    : ::std::runtime_error(PrintTestPartResultToString(failure).c_str()) {}

#endif  // GTEST_HAS_EXCEPTIONS

// We put these helper functions in the internal namespace as IBM's xlC
// compiler rejects the code if they were declared static.

// Runs the given method and handles SEH exceptions it throws, when
// SEH is supported; returns the 0-value for type Result in case of an
// SEH exception.  (Microsoft compilers cannot handle SEH and C++
// exceptions in the same function.  Therefore, we provide a separate
// wrapper function for handling SEH exceptions.)
template <class T, typename Result>
Result HandleSehExceptionsInMethodIfSupported(
    T* object, Result (T::*method)(), const char* location) {
#if GTEST_HAS_SEH
  __try {
    return (object->*method)();
  } __except (internal::UnitTestOptions::GTestShouldProcessSEH(  // NOLINT
      GetExceptionCode())) {
    // We create the exception message on the heap because VC++ prohibits
    // creation of objects with destructors on stack in functions using __try
    // (see error C2712).
    std::string* exception_message = FormatSehExceptionMessage(
        GetExceptionCode(), location);
    internal::ReportFailureInUnknownLocation(TestPartResult::kFatalFailure,
                                             *exception_message);
    delete exception_message;
    return static_cast<Result>(0);
  }
#else
  (void)location;
  return (object->*method)();
#endif  // GTEST_HAS_SEH
}

// Runs the given method and catches and reports C++ and/or SEH-style
// exceptions, if they are supported; returns the 0-value for type
// Result in case of an SEH exception.
template <class T, typename Result>
Result HandleExceptionsInMethodIfSupported(
    T* object, Result (T::*method)(), const char* location) {
  // NOTE: The user code can affect the way in which Google Test handles
  // exceptions by setting GTEST_FLAG(catch_exceptions), but only before
  // RUN_ALL_TESTS() starts. It is technically possible to check the flag
  // after the exception is caught and either report or re-throw the
  // exception based on the flag's value:
  //
  // try {
  //   // Perform the test method.
  // } catch (...) {
  //   if (GTEST_FLAG(catch_exceptions))
  //     // Report the exception as failure.
  //   else
  //     throw;  // Re-throws the original exception.
  // }
  //
  // However, the purpose of this flag is to allow the program to drop into
  // the debugger when the exception is thrown. On most platforms, once the
  // control enters the catch block, the exception origin information is
  // lost and the debugger will stop the program at the point of the
  // re-throw in this function -- instead of at the point of the original
  // throw statement in the code under test.  For this reason, we perform
  // the check early, sacrificing the ability to affect Google Test's
  // exception handling in the method where the exception is thrown.
  if (internal::GetUnitTestImpl()->catch_exceptions()) {
#if GTEST_HAS_EXCEPTIONS
    try {
      return HandleSehExceptionsInMethodIfSupported(object, method, location);
    } catch (const internal::GoogleTestFailureException&) {  // NOLINT
      // This exception type can only be thrown by a failed Google
      // Test assertion with the intention of letting another testing
      // framework catch it.  Therefore we just re-throw it.
      throw;
    } catch (const std::exception& e) {  // NOLINT
      internal::ReportFailureInUnknownLocation(
          TestPartResult::kFatalFailure,
          FormatCxxExceptionMessage(e.what(), location));
    } catch (...) {  // NOLINT
      internal::ReportFailureInUnknownLocation(
          TestPartResult::kFatalFailure,
          FormatCxxExceptionMessage(NULL, location));
    }
    return static_cast<Result>(0);
#else
    return HandleSehExceptionsInMethodIfSupported(object, method, location);
#endif  // GTEST_HAS_EXCEPTIONS
  } else {
    return (object->*method)();
  }
}

}  // namespace internal

// Runs the test and updates the test result.
void Test::Run() {
  if (!HasSameFixtureClass()) return;

  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  impl->os_stack_trace_getter()->UponLeavingGTest();
  internal::HandleExceptionsInMethodIfSupported(this, &Test::SetUp, "SetUp()");
  // We will run the test only if SetUp() was successful.
  if (!HasFatalFailure()) {
    impl->os_stack_trace_getter()->UponLeavingGTest();
    internal::HandleExceptionsInMethodIfSupported(
        this, &Test::TestBody, "the test body");
  }

  // However, we want to clean up as much as possible.  Hence we will
  // always call TearDown(), even if SetUp() or the test body has
  // failed.
  impl->os_stack_trace_getter()->UponLeavingGTest();
  internal::HandleExceptionsInMethodIfSupported(
      this, &Test::TearDown, "TearDown()");
}

// Returns true iff the current test has a fatal failure.
bool Test::HasFatalFailure() {
  return internal::GetUnitTestImpl()->current_test_result()->HasFatalFailure();
}

// Returns true iff the current test has a non-fatal failure.
bool Test::HasNonfatalFailure() {
  return internal::GetUnitTestImpl()->current_test_result()->
      HasNonfatalFailure();
}

// class TestInfo

// Constructs a TestInfo object. It assumes ownership of the test factory
// object.
TestInfo::TestInfo(const std::string& a_test_case_name,
                   const std::string& a_name,
                   const char* a_type_param,
                   const char* a_value_param,
                   internal::TypeId fixture_class_id,
                   internal::TestFactoryBase* factory)
    : test_case_name_(a_test_case_name),
      name_(a_name),
      type_param_(a_type_param ? new std::string(a_type_param) : NULL),
      value_param_(a_value_param ? new std::string(a_value_param) : NULL),
      fixture_class_id_(fixture_class_id),
      should_run_(false),
      is_disabled_(false),
      matches_filter_(false),
      factory_(factory),
      result_() {}

// Destructs a TestInfo object.
TestInfo::~TestInfo() { delete factory_; }

namespace internal {

// Creates a new TestInfo object and registers it with Google Test;
// returns the created object.
//
// Arguments:
//
//   test_case_name:   name of the test case
//   name:             name of the test
//   type_param:       the name of the test's type parameter, or NULL if
//                     this is not a typed or a type-parameterized test.
//   value_param:      text representation of the test's value parameter,
//                     or NULL if this is not a value-parameterized test.
//   fixture_class_id: ID of the test fixture class
//   set_up_tc:        pointer to the function that sets up the test case
//   tear_down_tc:     pointer to the function that tears down the test case
//   factory:          pointer to the factory that creates a test object.
//                     The newly created TestInfo instance will assume
//                     ownership of the factory object.
TestInfo* MakeAndRegisterTestInfo(
    const char* test_case_name,
    const char* name,
    const char* type_param,
    const char* value_param,
    TypeId fixture_class_id,
    SetUpTestCaseFunc set_up_tc,
    TearDownTestCaseFunc tear_down_tc,
    TestFactoryBase* factory) {
  TestInfo* const test_info =
      new TestInfo(test_case_name, name, type_param, value_param,
                   fixture_class_id, factory);
  GetUnitTestImpl()->AddTestInfo(set_up_tc, tear_down_tc, test_info);
  return test_info;
}

#if GTEST_HAS_PARAM_TEST
void ReportInvalidTestCaseType(const char* test_case_name,
                               const char* file, int line) {
  Message errors;
  errors
      << "Attempted redefinition of test case " << test_case_name << ".\n"
      << "All tests in the same test case must use the same test fixture\n"
      << "class.  However, in test case " << test_case_name << ", you tried\n"
      << "to define a test using a fixture class different from the one\n"
      << "used earlier. This can happen if the two fixture classes are\n"
      << "from different namespaces and have the same name. You should\n"
      << "probably rename one of the classes to put the tests into different\n"
      << "test cases.";

  fprintf(stderr, "%s %s", FormatFileLocation(file, line).c_str(),
          errors.GetString().c_str());
}
#endif  // GTEST_HAS_PARAM_TEST

}  // namespace internal

namespace {

// A predicate that checks the test name of a TestInfo against a known
// value.
//
// This is used for implementation of the TestCase class only.  We put
// it in the anonymous namespace to prevent polluting the outer
// namespace.
//
// TestNameIs is copyable.
class TestNameIs {
 public:
  // Constructor.
  //
  // TestNameIs has NO default constructor.
  explicit TestNameIs(const char* name)
      : name_(name) {}

  // Returns true iff the test name of test_info matches name_.
  bool operator()(const TestInfo * test_info) const {
    return test_info && test_info->name() == name_;
  }

 private:
  std::string name_;
};

}  // namespace

namespace internal {

// This method expands all parameterized tests registered with macros TEST_P
// and INSTANTIATE_TEST_CASE_P into regular tests and registers those.
// This will be done just once during the program runtime.
void UnitTestImpl::RegisterParameterizedTests() {
#if GTEST_HAS_PARAM_TEST
  if (!parameterized_tests_registered_) {
    parameterized_test_registry_.RegisterTests();
    parameterized_tests_registered_ = true;
  }
#endif
}

}  // namespace internal

// Creates the test object, runs it, records its result, and then
// deletes it.
void TestInfo::Run() {
  if (!should_run_) return;

  // Tells UnitTest where to store test result.
  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  impl->set_current_test_info(this);

  TestEventListener* repeater = UnitTest::GetInstance()->listeners().repeater();

  // Notifies the unit test event listeners that a test is about to start.
  repeater->OnTestStart(*this);

  const TimeInMillis start = internal::GetTimeInMillis();

  impl->os_stack_trace_getter()->UponLeavingGTest();

  // Creates the test object.
  Test* const test = internal::HandleExceptionsInMethodIfSupported(
      factory_, &internal::TestFactoryBase::CreateTest,
      "the test fixture's constructor");

  // Runs the test only if the test object was created and its
  // constructor didn't generate a fatal failure.
  if ((test != NULL) && !Test::HasFatalFailure()) {
    // This doesn't throw as all user code that can throw are wrapped into
    // exception handling code.
    test->Run();
  }

  // Deletes the test object.
  impl->os_stack_trace_getter()->UponLeavingGTest();
  internal::HandleExceptionsInMethodIfSupported(
      test, &Test::DeleteSelf_, "the test fixture's destructor");

  result_.set_elapsed_time(internal::GetTimeInMillis() - start);

  // Notifies the unit test event listener that a test has just finished.
  repeater->OnTestEnd(*this);

  // Tells UnitTest to stop associating assertion results to this
  // test.
  impl->set_current_test_info(NULL);
}

// class TestCase

// Gets the number of successful tests in this test case.
int TestCase::successful_test_count() const {
  return CountIf(test_info_list_, TestPassed);
}

// Gets the number of failed tests in this test case.
int TestCase::failed_test_count() const {
  return CountIf(test_info_list_, TestFailed);
}

// Gets the number of disabled tests that will be reported in the XML report.
int TestCase::reportable_disabled_test_count() const {
  return CountIf(test_info_list_, TestReportableDisabled);
}

// Gets the number of disabled tests in this test case.
int TestCase::disabled_test_count() const {
  return CountIf(test_info_list_, TestDisabled);
}

// Gets the number of tests to be printed in the XML report.
int TestCase::reportable_test_count() const {
  return CountIf(test_info_list_, TestReportable);
}

// Get the number of tests in this test case that should run.
int TestCase::test_to_run_count() const {
  return CountIf(test_info_list_, ShouldRunTest);
}

// Gets the number of all tests.
int TestCase::total_test_count() const {
  return static_cast<int>(test_info_list_.size());
}

// Creates a TestCase with the given name.
//
// Arguments:
//
//   name:         name of the test case
//   a_type_param: the name of the test case's type parameter, or NULL if
//                 this is not a typed or a type-parameterized test case.
//   set_up_tc:    pointer to the function that sets up the test case
//   tear_down_tc: pointer to the function that tears down the test case
TestCase::TestCase(const char* a_name, const char* a_type_param,
                   Test::SetUpTestCaseFunc set_up_tc,
                   Test::TearDownTestCaseFunc tear_down_tc)
    : name_(a_name),
      type_param_(a_type_param ? new std::string(a_type_param) : NULL),
      set_up_tc_(set_up_tc),
      tear_down_tc_(tear_down_tc),
      should_run_(false),
      elapsed_time_(0) {
}

// Destructor of TestCase.
TestCase::~TestCase() {
  // Deletes every Test in the collection.
  ForEach(test_info_list_, internal::Delete<TestInfo>);
}

// Returns the i-th test among all the tests. i can range from 0 to
// total_test_count() - 1. If i is not in that range, returns NULL.
const TestInfo* TestCase::GetTestInfo(int i) const {
  const int index = GetElementOr(test_indices_, i, -1);
  return index < 0 ? NULL : test_info_list_[index];
}

// Returns the i-th test among all the tests. i can range from 0 to
// total_test_count() - 1. If i is not in that range, returns NULL.
TestInfo* TestCase::GetMutableTestInfo(int i) {
  const int index = GetElementOr(test_indices_, i, -1);
  return index < 0 ? NULL : test_info_list_[index];
}

// Adds a test to this test case.  Will delete the test upon
// destruction of the TestCase object.
void TestCase::AddTestInfo(TestInfo * test_info) {
  test_info_list_.push_back(test_info);
  test_indices_.push_back(static_cast<int>(test_indices_.size()));
}

// Runs every test in this TestCase.
void TestCase::Run() {
  if (!should_run_) return;

  internal::UnitTestImpl* const impl = internal::GetUnitTestImpl();
  impl->set_current_test_case(this);

  TestEventListener* repeater = UnitTest::GetInstance()->listeners().repeater();

  repeater->OnTestCaseStart(*this);
  impl->os_stack_trace_getter()->UponLeavingGTest();
  internal::HandleExceptionsInMethodIfSupported(
      this, &TestCase::RunSetUpTestCase, "SetUpTestCase()");

  const internal::TimeInMillis start = internal::GetTimeInMillis();
  for (int i = 0; i < total_test_count(); i++) {
    GetMutableTestInfo(i)->Run();
  }
  elapsed_time_ = internal::GetTimeInMillis() - start;

  impl->os_stack_trace_getter()->UponLeavingGTest();
  internal::HandleExceptionsInMethodIfSupported(
      this, &TestCase::RunTearDownTestCase, "TearDownTestCase()");

  repeater->OnTestCaseEnd(*this);
  impl->set_current_test_case(NULL);
}

// Clears the results of all tests in this test case.
void TestCase::ClearResult() {
  ad_hoc_test_result_.Clear();
  ForEach(test_info_list_, TestInfo::ClearTestResult);
}

// Shuffles the tests in this test case.
void TestCase::ShuffleTests(internal::Random* random) {
  Shuffle(random, &test_indices_);
}

// Restores the test order to before the first shuffle.
void TestCase::UnshuffleTests() {
  for (size_t i = 0; i < test_indices_.size(); i++) {
    test_indices_[i] = static_cast<int>(i);
  }
}

// Formats a countable noun.  Depending on its quantity, either the
// singular form or the plural form is used. e.g.
//
// FormatCountableNoun(1, "formula", "formuli") returns "1 formula".
// FormatCountableNoun(5, "book", "books") returns "5 books".
static std::string FormatCountableNoun(int count,
                                       const char * singular_form,
                                       const char * plural_form) {
  return internal::StreamableToString(count) + " " +
      (count == 1 ? singular_form : plural_form);
}

// Formats the count of tests.
static std::string FormatTestCount(int test_count) {
  return FormatCountableNoun(test_count, "test", "tests");
}

// Formats the count of test cases.
static std::string FormatTestCaseCount(int test_case_count) {
  return FormatCountableNoun(test_case_count, "test case", "test cases");
}

// Converts a TestPartResult::Type enum to human-friendly string
// representation.  Both kNonFatalFailure and kFatalFailure are translated
// to "Failure", as the user usually doesn't care about the difference
// between the two when viewing the test result.
static const char * TestPartResultTypeToString(TestPartResult::Type type) {
  switch (type) {
    case TestPartResult::kSuccess:
      return "Success";

    case TestPartResult::kNonFatalFailure:
    case TestPartResult::kFatalFailure:
#ifdef _MSC_VER
      return "error: ";
#else
      return "Failure\n";
#endif
    default:
      return "Unknown result type";
  }
}

namespace internal {

// Prints a TestPartResult to an std::string.
static std::string PrintTestPartResultToString(
    const TestPartResult& test_part_result) {
  return (Message()
          << internal::FormatFileLocation(test_part_result.file_name(),
                                          test_part_result.line_number())
          << " " << TestPartResultTypeToString(test_part_result.type())
          << test_part_result.message()).GetString();
}

// Prints a TestPartResult.
static void PrintTestPartResult(const TestPartResult& test_part_result) {
  const std::string& result =
      PrintTestPartResultToString(test_part_result);
  printf("%s\n", result.c_str());
  fflush(stdout);
  // If the test program runs in Visual Studio or a debugger, the
  // following statements add the test part result message to the Output
  // window such that the user can double-click on it to jump to the
  // corresponding source code location; otherwise they do nothing.
#if GTEST_OS_WINDOWS && !GTEST_OS_WINDOWS_MOBILE
  // We don't call OutputDebugString*() on Windows Mobile, as printing
  // to stdout is done by OutputDebugString() there already - we don't
  // want the same message printed twice.
  ::OutputDebugStringA(result.c_str());
  ::OutputDebugStringA("\n");
#endif
}

// class PrettyUnitTestResultPrinter

enum GTestColor {
  COLOR_DEFAULT,
  COLOR_RED,
  COLOR_GREEN,
  COLOR_YELLOW
};

#if GTEST_OS_WINDOWS && !GTEST_OS_WINDOWS_MOBILE

// Returns the character attribute for the given color.
WORD GetColorAttribute(GTestColor color) {
  switch (color) {
    case COLOR_RED:    return FOREGROUND_RED;
    case COLOR_GREEN:  return FOREGROUND_GREEN;
    case COLOR_YELLOW: return FOREGROUND_RED | FOREGROUND_GREEN;
    default:           return 0;
  }
}

#else

// Returns the ANSI color code for the given color.  COLOR_DEFAULT is
// an invalid input.
const char* GetAnsiColorCode(GTestColor color) {
  switch (color) {
    case COLOR_RED:     return "1";
    case COLOR_GREEN:   return "2";
    case COLOR_YELLOW:  return "3";
    default:            return NULL;
  };
}

#endif  // GTEST_OS_WINDOWS && !GTEST_OS_WINDOWS_MOBILE

// Returns true iff Google Test should use colors in the output.
bool ShouldUseColor(bool stdout_is_tty) {
  const char* const gtest_color = GTEST_FLAG(color).c_str();

  if (String::CaseInsensitiveCStringEquals(gtest_color, "auto")) {
#if GTEST_OS_WINDOWS
    // On Windows the TERM variable is usually not set, but the
    // console there does support colors.
    return stdout_is_tty;
#else
    // On non-Windows platforms, we rely on the TERM variable.
    const char* const term = posix::GetEnv("TERM");
    const bool term_supports_color =
        String::CStringEquals(term, "xterm") ||
        String::CStringEquals(term, "xterm-color") ||
        String::CStringEquals(term, "xterm-256color") ||
        String::CStringEquals(term, "screen") ||
        String::CStringEquals(term, "screen-256color") ||
        String::CStringEquals(term, "linux") ||
        String::CStringEquals(term, "cygwin");
    return stdout_is_tty && term_supports_color;
#endif  // GTEST_OS_WINDOWS
  }

  return String::CaseInsensitiveCStringEquals(gtest_color, "yes") ||
      String::CaseInsensitiveCStringEquals(gtest_color, "true") ||
      String::CaseInsensitiveCStringEquals(gtest_color, "t") ||
      String::CStringEquals(gtest_color, "1");
  // We take "yes", "true", "t", and "1" as meaning "yes".  If the
  // value is neither one of these nor "auto", we treat it as "no" to
  // be conservative.
}

// Helpers for printing colored strings to stdout. Note that on Windows, we
// cannot simply emit special characters and have the terminal change colors.
// This routine must actually emit the characters rather than return a string
// that would be colored when printed, as can be done on Linux.
void ColoredPrintf(GTestColor color, const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);

#if GTEST_OS_WINDOWS_MOBILE || GTEST_OS_SYMBIAN || GTEST_OS_ZOS || GTEST_OS_IOS
  const bool use_color = false;
#else
  static const bool in_color_mode =
      ShouldUseColor(posix::IsATTY(posix::FileNo(stdout)) != 0);
  const bool use_color = in_color_mode && (color != COLOR_DEFAULT);
#endif  // GTEST_OS_WINDOWS_MOBILE || GTEST_OS_SYMBIAN || GTEST_OS_ZOS
  // The '!= 0' comparison is necessary to satisfy MSVC 7.1.

  if (!use_color) {
    vprintf(fmt, args);
    va_end(args);
    return;
  }

#if GTEST_OS_WINDOWS && !GTEST_OS_WINDOWS_MOBILE
  const HANDLE stdout_handle = GetStdHandle(STD_OUTPUT_HANDLE);

  // Gets the current text color.
  CONSOLE_SCREEN_BUFFER_INFO buffer_info;
  GetConsoleScreenBufferInfo(stdout_handle, &buffer_info);
  const WORD old_color_attrs = buffer_info.wAttributes;

  // We need to flush the stream buffers into the console before each
  // SetConsoleTextAttribute call lest it affect the text that is already
  // printed but has not yet reached the console.
  fflush(stdout);
  SetConsoleTextAttribute(stdout_handle,
                          GetColorAttribute(color) | FOREGROUND_INTENSITY);
  vprintf(fmt, args);

  fflush(stdout);
  // Restores the text color.
  SetConsoleTextAttribute(stdout_handle, old_color_attrs);
#else
  printf("\033[0;3%sm", GetAnsiColorCode(color));
  vprintf(fmt, args);
  printf("\033[m");  // Resets the terminal to default.
#endif  // GTEST_OS_WINDOWS && !GTEST_OS_WINDOWS_MOBILE
  va_end(args);
}

// Text printed in Google Test's text output and --gunit_list_tests
// output to label the type parameter and value parameter for a test.
static const char kTypeParamLabel[] = "TypeParam";
static const char kValueParamLabel[] = "GetParam()";

void PrintFullTestCommentIfPresent(const TestInfo& test_info) {
  const char* const type_param = test_info.type_param();
  const char* const value_param = test_info.value_param();

  if (type_param != NULL || value_param != NULL) {
    printf(", where ");
    if (type_param != NULL) {
      printf("%s = %s", kTypeParamLabel, type_param);
      if (value_param != NULL)
        printf(" and ");
    }
    if (value_param != NULL) {
      printf("%s = %s", kValueParamLabel, value_param);
    }
  }
}

// This class implements the TestEventListener interface.
//
// Class PrettyUnitTestResultPrinter is copyable.
class PrettyUnitTestResultPrinter : public TestEventListener {
 public:
  PrettyUnitTestResultPrinter() {}
  static void PrintTestName(const char * test_case, const char * test) {
    printf("%s.%s", test_case, test);
  }

  // The following methods override what's in the TestEventListener class.
  virtual void OnTestProgramStart(const UnitTest& /*unit_test*/) {}
  virtual void OnTestIterationStart(const UnitTest& unit_test, int iteration);
  virtual void OnEnvironmentsSetUpStart(const UnitTest& unit_test);
  virtual void OnEnvironmentsSetUpEnd(const UnitTest& /*unit_test*/) {}
  virtual void OnTestCaseStart(const TestCase& test_case);
  virtual void OnTestStart(const TestInfo& test_info);
  virtual void OnTestPartResult(const TestPartResult& result);
  virtual void OnTestEnd(const TestInfo& test_info);
  virtual void OnTestCaseEnd(const TestCase& test_case);
  virtual void OnEnvironmentsTearDownStart(const UnitTest& unit_test);
  virtual void OnEnvironmentsTearDownEnd(const UnitTest& /*unit_test*/) {}
  virtual void OnTestIterationEnd(const UnitTest& unit_test, int iteration);
  virtual void OnTestProgramEnd(const UnitTest& /*unit_test*/) {}

 private:
  static void PrintFailedTests(const UnitTest& unit_test);
};

  // Fired before each iteration of tests starts.
void PrettyUnitTestResultPrinter::OnTestIterationStart(
    const UnitTest& unit_test, int iteration) {
  if (GTEST_FLAG(repeat) != 1)
    printf("\nRepeating all tests (iteration %d) . . .\n\n", iteration + 1);

  const char* const filter = GTEST_FLAG(filter).c_str();

  // Prints the filter if it's not *.  This reminds the user that some
  // tests may be skipped.
  if (!String::CStringEquals(filter, kUniversalFilter)) {
    ColoredPrintf(COLOR_YELLOW,
                  "Note: %s filter = %s\n", GTEST_NAME_, filter);
  }

  if (internal::ShouldShard(kTestTotalShards, kTestShardIndex, false)) {
    const Int32 shard_index = Int32FromEnvOrDie(kTestShardIndex, -1);
    ColoredPrintf(COLOR_YELLOW,
                  "Note: This is test shard %d of %s.\n",
                  static_cast<int>(shard_index) + 1,
                  internal::posix::GetEnv(kTestTotalShards));
  }

  if (GTEST_FLAG(shuffle)) {
    ColoredPrintf(COLOR_YELLOW,
                  "Note: Randomizing tests' orders with a seed of %d .\n",
                  unit_test.random_seed());
  }

  ColoredPrintf(COLOR_GREEN,  "[==========] ");
  printf("Running %s from %s.\n",
         FormatTestCount(unit_test.test_to_run_count()).c_str(),
         FormatTestCaseCount(unit_test.test_case_to_run_count()).c_str());
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnEnvironmentsSetUpStart(
    const UnitTest& /*unit_test*/) {
  ColoredPrintf(COLOR_GREEN,  "[----------] ");
  printf("Global test environment set-up.\n");
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnTestCaseStart(const TestCase& test_case) {
  const std::string counts =
      FormatCountableNoun(test_case.test_to_run_count(), "test", "tests");
  ColoredPrintf(COLOR_GREEN, "[----------] ");
  printf("%s from %s", counts.c_str(), test_case.name());
  if (test_case.type_param() == NULL) {
    printf("\n");
  } else {
    printf(", where %s = %s\n", kTypeParamLabel, test_case.type_param());
  }
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnTestStart(const TestInfo& test_info) {
  ColoredPrintf(COLOR_GREEN,  "[ RUN      ] ");
  PrintTestName(test_info.test_case_name(), test_info.name());
  printf("\n");
  fflush(stdout);
}

// Called after an assertion failure.
void PrettyUnitTestResultPrinter::OnTestPartResult(
    const TestPartResult& result) {
  // If the test part succeeded, we don't need to do anything.
  if (result.type() == TestPartResult::kSuccess)
    return;

  // Print failure message from the assertion (e.g. expected this and got that).
  PrintTestPartResult(result);
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnTestEnd(const TestInfo& test_info) {
  if (test_info.result()->Passed()) {
    ColoredPrintf(COLOR_GREEN, "[       OK ] ");
  } else {
    ColoredPrintf(COLOR_RED, "[  FAILED  ] ");
  }
  PrintTestName(test_info.test_case_name(), test_info.name());
  if (test_info.result()->Failed())
    PrintFullTestCommentIfPresent(test_info);

  if (GTEST_FLAG(print_time)) {
    printf(" (%s ms)\n", internal::StreamableToString(
           test_info.result()->elapsed_time()).c_str());
  } else {
    printf("\n");
  }
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnTestCaseEnd(const TestCase& test_case) {
  if (!GTEST_FLAG(print_time)) return;

  const std::string counts =
      FormatCountableNoun(test_case.test_to_run_count(), "test", "tests");
  ColoredPrintf(COLOR_GREEN, "[----------] ");
  printf("%s from %s (%s ms total)\n\n",
         counts.c_str(), test_case.name(),
         internal::StreamableToString(test_case.elapsed_time()).c_str());
  fflush(stdout);
}

void PrettyUnitTestResultPrinter::OnEnvironmentsTearDownStart(
    const UnitTest& /*unit_test*/) {
  ColoredPrintf(COLOR_GREEN,  "[----------] ");
  printf("Global test environment tear-down\n");
  fflush(stdout);
}

// Internal helper for printing the list of failed tests.
void PrettyUnitTestResultPrinter::PrintFailedTests(const UnitTest& unit_test) {
  const int failed_test_count = unit_test.failed_test_count();
  if (failed_test_count == 0) {
    return;
  }

  for (int i = 0; i < unit_test.total_test_case_count(); ++i) {
    const TestCase& test_case = *unit_test.GetTestCase(i);
    if (!test_case.should_run() || (test_case.failed_test_count() == 0)) {
      continue;
    }
    for (int j = 0; j < test_case.total_test_count(); ++j) {
      const TestInfo& test_info = *test_case.GetTestInfo(j);
      if (!test_info.should_run() || test_info.result()->Passed()) {
        continue;
      }
      ColoredPrintf(COLOR_RED, "[  FAILED  ] ");
      printf("%s.%s", test_case.name(), test_info.name());
      PrintFullTestCommentIfPresent(test_info);
      printf("\n");
    }
  }
}

void PrettyUnitTestResultPrinter::OnTestIterationEnd(const UnitTest& unit_test,
                                                     int /*iteration*/) {
  ColoredPrintf(COLOR_GREEN,  "[==========] ");
  printf("%s from %s ran.",
         FormatTestCount(unit_test.test_to_run_count()).c_str(),
         FormatTestCaseCount(unit_test.test_case_to_run_count()).c_str());
  if (GTEST_FLAG(print_time)) {
    printf(" (%s ms total)",
           internal::StreamableToString(unit_test.elapsed_time()).c_str());
  }
  printf("\n");
  ColoredPrintf(COLOR_GREEN,  "[  PASSED  ] ");
  printf("%s.\n", FormatTestCount(unit_test.successful_test_count()).c_str());

  int num_failures = unit_test.failed_test_count();
  if (!unit_test.Passed()) {
    const int failed_test_count = unit_test.failed_test_count();
    ColoredPrintf(COLOR_RED,  "[  FAILED  ] ");
    printf("%s, listed below:\n", FormatTestCount(failed_test_count).c_str());
    PrintFailedTests(unit_test);
    printf("\n%2d FAILED %s\n", num_failures,
                        num_failures == 1 ? "TEST" : "TESTS");
  }

  int num_disabled = unit_test.reportable_disabled_test_count();
  if (num_disabled && !GTEST_FLAG(also_run_disabled_tests)) {
    if (!num_failures) {
      printf("\n");  // Add a spacer if no FAILURE banner is displayed.
    }
    ColoredPrintf(COLOR_YELLOW,
                  "  YOU HAVE %d DISABLED %s\n\n",
                  num_disabled,
                  num_disabled == 1 ? "TEST" : "TESTS");
  }
  // Ensure that Google Test output is printed before, e.g., heapchecker output.
  fflush(stdout);
}

// End PrettyUnitTestResultPrinter

// class TestEventRepeater
//
// This class forwards events to other event listeners.
class TestEventRepeater : public TestEventListener {
 public:
  TestEventRepeater() : forwarding_enabled_(true) {}
  virtual ~TestEventRepeater();
  void Append(TestEventListener *listener);
  TestEventListener* Release(TestEventListener* listener);

  // Controls whether events will be forwarded to listeners_. Set to false
  // in death test child processes.
  bool forwarding_enabled() const { return forwarding_enabled_; }
  void set_forwarding_enabled(bool enable) { forwarding_enabled_ = enable; }

  virtual void OnTestProgramStart(const UnitTest& unit_test);
  virtual void OnTestIterationStart(const UnitTest& unit_test, int iteration);
  virtual void OnEnvironmentsSetUpStart(const UnitTest& unit_test);
  virtual void OnEnvironmentsSetUpEnd(const UnitTest& unit_test);
  virtual void OnTestCaseStart(const TestCase& test_case);
  virtual void OnTestStart(const TestInfo& test_info);
  virtual void OnTestPartResult(const TestPartResult& result);
  virtual void OnTestEnd(const TestInfo& test_info);
  virtual void OnTestCaseEnd(const TestCase& test_case);
  virtual void OnEnvironmentsTearDownStart(const UnitTest& unit_test);
  virtual void OnEnvironmentsTearDownEnd(const UnitTest& unit_test);
  virtual void OnTestIterationEnd(const UnitTest& unit_test, int iteration);
  virtual void OnTestProgramEnd(const UnitTest& unit_test);

 private:
  // Controls whether events will be forwarded to listeners_. Set to false
  // in death test child processes.
  bool forwarding_enabled_;
  // The list of listeners that receive events.
  std::vector<TestEventListener*> listeners_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(TestEventRepeater);
};

TestEventRepeater::~TestEventRepeater() {
  ForEach(listeners_, Delete<TestEventListener>);
}

void TestEventRepeater::Append(TestEventListener *listener) {
  listeners_.push_back(listener);
}

// TODO(vladl@google.com): Factor the search functionality into Vector::Find.
TestEventListener* TestEventRepeater::Release(TestEventListener *listener) {
  for (size_t i = 0; i < listeners_.size(); ++i) {
    if (listeners_[i] == listener) {
      listeners_.erase(listeners_.begin() + i);
      return listener;
    }
  }

  return NULL;
}

// Since most methods are very similar, use macros to reduce boilerplate.
// This defines a member that forwards the call to all listeners.
#define GTEST_REPEATER_METHOD_(Name, Type) \
void TestEventRepeater::Name(const Type& parameter) { \
  if (forwarding_enabled_) { \
    for (size_t i = 0; i < listeners_.size(); i++) { \
      listeners_[i]->Name(parameter); \
    } \
  } \
}
// This defines a member that forwards the call to all listeners in reverse
// order.
#define GTEST_REVERSE_REPEATER_METHOD_(Name, Type) \
void TestEventRepeater::Name(const Type& parameter) { \
  if (forwarding_enabled_) { \
    for (int i = static_cast<int>(listeners_.size()) - 1; i >= 0; i--) { \
      listeners_[i]->Name(parameter); \
    } \
  } \
}

GTEST_REPEATER_METHOD_(OnTestProgramStart, UnitTest)
GTEST_REPEATER_METHOD_(OnEnvironmentsSetUpStart, UnitTest)
GTEST_REPEATER_METHOD_(OnTestCaseStart, TestCase)
GTEST_REPEATER_METHOD_(OnTestStart, TestInfo)
GTEST_REPEATER_METHOD_(OnTestPartResult, TestPartResult)
GTEST_REPEATER_METHOD_(OnEnvironmentsTearDownStart, UnitTest)
GTEST_REVERSE_REPEATER_METHOD_(OnEnvironmentsSetUpEnd, UnitTest)
GTEST_REVERSE_REPEATER_METHOD_(OnEnvironmentsTearDownEnd, UnitTest)
GTEST_REVERSE_REPEATER_METHOD_(OnTestEnd, TestInfo)
GTEST_REVERSE_REPEATER_METHOD_(OnTestCaseEnd, TestCase)
GTEST_REVERSE_REPEATER_METHOD_(OnTestProgramEnd, UnitTest)

#undef GTEST_REPEATER_METHOD_
#undef GTEST_REVERSE_REPEATER_METHOD_

void TestEventRepeater::OnTestIterationStart(const UnitTest& unit_test,
                                             int iteration) {
  if (forwarding_enabled_) {
    for (size_t i = 0; i < listeners_.size(); i++) {
      listeners_[i]->OnTestIterationStart(unit_test, iteration);
    }
  }
}

void TestEventRepeater::OnTestIterationEnd(const UnitTest& unit_test,
                                           int iteration) {
  if (forwarding_enabled_) {
    for (int i = static_cast<int>(listeners_.size()) - 1; i >= 0; i--) {
      listeners_[i]->OnTestIterationEnd(unit_test, iteration);
    }
  }
}

// End TestEventRepeater

// This class generates an XML output file.
class XmlUnitTestResultPrinter : public EmptyTestEventListener {
 public:
  explicit XmlUnitTestResultPrinter(const char* output_file);

  virtual void OnTestIterationEnd(const UnitTest& unit_test, int iteration);

 private:
  // Is c a whitespace character that is normalized to a space character
  // when it appears in an XML attribute value?
  static bool IsNormalizableWhitespace(char c) {
    return c == 0x9 || c == 0xA || c == 0xD;
  }

  // May c appear in a well-formed XML document?
  static bool IsValidXmlCharacter(char c) {
    return IsNormalizableWhitespace(c) || c >= 0x20;
  }

  // Returns an XML-escaped copy of the input string str.  If
  // is_attribute is true, the text is meant to appear as an attribute
  // value, and normalizable whitespace is preserved by replacing it
  // with character references.
  static std::string EscapeXml(const std::string& str, bool is_attribute);

  // Returns the given string with all characters invalid in XML removed.
  static std::string RemoveInvalidXmlCharacters(const std::string& str);

  // Convenience wrapper around EscapeXml when str is an attribute value.
  static std::string EscapeXmlAttribute(const std::string& str) {
    return EscapeXml(str, true);
  }

  // Convenience wrapper around EscapeXml when str is not an attribute value.
  static std::string EscapeXmlText(const char* str) {
    return EscapeXml(str, false);
  }

  // Verifies that the given attribute belongs to the given element and
  // streams the attribute as XML.
  static void OutputXmlAttribute(std::ostream* stream,
                                 const std::string& element_name,
                                 const std::string& name,
                                 const std::string& value);

  // Streams an XML CDATA section, escaping invalid CDATA sequences as needed.
  static void OutputXmlCDataSection(::std::ostream* stream, const char* data);

  // Streams an XML representation of a TestInfo object.
  static void OutputXmlTestInfo(::std::ostream* stream,
                                const char* test_case_name,
                                const TestInfo& test_info);

  // Prints an XML representation of a TestCase object
  static void PrintXmlTestCase(::std::ostream* stream,
                               const TestCase& test_case);

  // Prints an XML summary of unit_test to output stream out.
  static void PrintXmlUnitTest(::std::ostream* stream,
                               const UnitTest& unit_test);

  // Produces a string representing the test properties in a result as space
  // delimited XML attributes based on the property key="value" pairs.
  // When the std::string is not empty, it includes a space at the beginning,
  // to delimit this attribute from prior attributes.
  static std::string TestPropertiesAsXmlAttributes(const TestResult& result);

  // The output file.
  const std::string output_file_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(XmlUnitTestResultPrinter);
};

// Creates a new XmlUnitTestResultPrinter.
XmlUnitTestResultPrinter::XmlUnitTestResultPrinter(const char* output_file)
    : output_file_(output_file) {
  if (output_file_.c_str() == NULL || output_file_.empty()) {
    fprintf(stderr, "XML output file may not be null\n");
    fflush(stderr);
    exit(EXIT_FAILURE);
  }
}

// Called after the unit test ends.
void XmlUnitTestResultPrinter::OnTestIterationEnd(const UnitTest& unit_test,
                                                  int /*iteration*/) {
  FILE* xmlout = NULL;
  FilePath output_file(output_file_);
  FilePath output_dir(output_file.RemoveFileName());

  if (output_dir.CreateDirectoriesRecursively()) {
    xmlout = posix::FOpen(output_file_.c_str(), "w");
  }
  if (xmlout == NULL) {
    // TODO(wan): report the reason of the failure.
    //
    // We don't do it for now as:
    //
    //   1. There is no urgent need for it.
    //   2. It's a bit involved to make the errno variable thread-safe on
    //      all three operating systems (Linux, Windows, and Mac OS).
    //   3. To interpret the meaning of errno in a thread-safe way,
    //      we need the strerror_r() function, which is not available on
    //      Windows.
    fprintf(stderr,
            "Unable to open file \"%s\"\n",
            output_file_.c_str());
    fflush(stderr);
    exit(EXIT_FAILURE);
  }
  std::stringstream stream;
  PrintXmlUnitTest(&stream, unit_test);
  fprintf(xmlout, "%s", StringStreamToString(&stream).c_str());
  fclose(xmlout);
}

// Returns an XML-escaped copy of the input string str.  If is_attribute
// is true, the text is meant to appear as an attribute value, and
// normalizable whitespace is preserved by replacing it with character
// references.
//
// Invalid XML characters in str, if any, are stripped from the output.
// It is expected that most, if not all, of the text processed by this
// module will consist of ordinary English text.
// If this module is ever modified to produce version 1.1 XML output,
// most invalid characters can be retained using character references.
// TODO(wan): It might be nice to have a minimally invasive, human-readable
// escaping scheme for invalid characters, rather than dropping them.
std::string XmlUnitTestResultPrinter::EscapeXml(
    const std::string& str, bool is_attribute) {
  Message m;

  for (size_t i = 0; i < str.size(); ++i) {
    const char ch = str[i];
    switch (ch) {
      case '<':
        m << "&lt;";
        break;
      case '>':
        m << "&gt;";
        break;
      case '&':
        m << "&amp;";
        break;
      case '\'':
        if (is_attribute)
          m << "&apos;";
        else
          m << '\'';
        break;
      case '"':
        if (is_attribute)
          m << "&quot;";
        else
          m << '"';
        break;
      default:
        if (IsValidXmlCharacter(ch)) {
          if (is_attribute && IsNormalizableWhitespace(ch))
            m << "&#x" << String::FormatByte(static_cast<unsigned char>(ch))
              << ";";
          else
            m << ch;
        }
        break;
    }
  }

  return m.GetString();
}

// Returns the given string with all characters invalid in XML removed.
// Currently invalid characters are dropped from the string. An
// alternative is to replace them with certain characters such as . or ?.
std::string XmlUnitTestResultPrinter::RemoveInvalidXmlCharacters(
    const std::string& str) {
  std::string output;
  output.reserve(str.size());
  for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
    if (IsValidXmlCharacter(*it))
      output.push_back(*it);

  return output;
}

// The following routines generate an XML representation of a UnitTest
// object.
//
// This is how Google Test concepts map to the DTD:
//
// <testsuites name="AllTests">        <-- corresponds to a UnitTest object
//   <testsuite name="testcase-name">  <-- corresponds to a TestCase object
//     <testcase name="test-name">     <-- corresponds to a TestInfo object
//       <failure message="...">...</failure>
//       <failure message="...">...</failure>
//       <failure message="...">...</failure>
//                                     <-- individual assertion failures
//     </testcase>
//   </testsuite>
// </testsuites>

// Formats the given time in milliseconds as seconds.
std::string FormatTimeInMillisAsSeconds(TimeInMillis ms) {
  ::std::stringstream ss;
  ss << ms/1000.0;
  return ss.str();
}

// Converts the given epoch time in milliseconds to a date string in the ISO
// 8601 format, without the timezone information.
std::string FormatEpochTimeInMillisAsIso8601(TimeInMillis ms) {
  // Using non-reentrant version as localtime_r is not portable.
  time_t seconds = static_cast<time_t>(ms / 1000);
#ifdef _MSC_VER
# pragma warning(push)          // Saves the current warning state.
# pragma warning(disable:4996)  // Temporarily disables warning 4996
                                // (function or variable may be unsafe).
  const struct tm* const time_struct = localtime(&seconds);  // NOLINT
# pragma warning(pop)           // Restores the warning state again.
#else
  const struct tm* const time_struct = localtime(&seconds);  // NOLINT
#endif
  if (time_struct == NULL)
    return "";  // Invalid ms value

  // YYYY-MM-DDThh:mm:ss
  return StreamableToString(time_struct->tm_year + 1900) + "-" +
      String::FormatIntWidth2(time_struct->tm_mon + 1) + "-" +
      String::FormatIntWidth2(time_struct->tm_mday) + "T" +
      String::FormatIntWidth2(time_struct->tm_hour) + ":" +
      String::FormatIntWidth2(time_struct->tm_min) + ":" +
      String::FormatIntWidth2(time_struct->tm_sec);
}

// Streams an XML CDATA section, escaping invalid CDATA sequences as needed.
void XmlUnitTestResultPrinter::OutputXmlCDataSection(::std::ostream* stream,
                                                     const char* data) {
  const char* segment = data;
  *stream << "<![CDATA[";
  for (;;) {
    const char* const next_segment = strstr(segment, "]]>");
    if (next_segment != NULL) {
      stream->write(
          segment, static_cast<std::streamsize>(next_segment - segment));
      *stream << "]]>]]&gt;<![CDATA[";
      segment = next_segment + strlen("]]>");
    } else {
      *stream << segment;
      break;
    }
  }
  *stream << "]]>";
}

void XmlUnitTestResultPrinter::OutputXmlAttribute(
    std::ostream* stream,
    const std::string& element_name,
    const std::string& name,
    const std::string& value) {
  const std::vector<std::string>& allowed_names =
      GetReservedAttributesForElement(element_name);

  GTEST_CHECK_(std::find(allowed_names.begin(), allowed_names.end(), name) !=
                   allowed_names.end())
      << "Attribute " << name << " is not allowed for element <" << element_name
      << ">.";

  *stream << " " << name << "=\"" << EscapeXmlAttribute(value) << "\"";
}

// Prints an XML representation of a TestInfo object.
// TODO(wan): There is also value in printing properties with the plain printer.
void XmlUnitTestResultPrinter::OutputXmlTestInfo(::std::ostream* stream,
                                                 const char* test_case_name,
                                                 const TestInfo& test_info) {
  const TestResult& result = *test_info.result();
  const std::string kTestcase = "testcase";

  *stream << "    <testcase";
  OutputXmlAttribute(stream, kTestcase, "name", test_info.name());

  if (test_info.value_param() != NULL) {
    OutputXmlAttribute(stream, kTestcase, "value_param",
                       test_info.value_param());
  }
  if (test_info.type_param() != NULL) {
    OutputXmlAttribute(stream, kTestcase, "type_param", test_info.type_param());
  }

  OutputXmlAttribute(stream, kTestcase, "status",
                     test_info.should_run() ? "run" : "notrun");
  OutputXmlAttribute(stream, kTestcase, "time",
                     FormatTimeInMillisAsSeconds(result.elapsed_time()));
  OutputXmlAttribute(stream, kTestcase, "classname", test_case_name);
  *stream << TestPropertiesAsXmlAttributes(result);

  int failures = 0;
  for (int i = 0; i < result.total_part_count(); ++i) {
    const TestPartResult& part = result.GetTestPartResult(i);
    if (part.failed()) {
      if (++failures == 1) {
        *stream << ">\n";
      }
      const string location = internal::FormatCompilerIndependentFileLocation(
          part.file_name(), part.line_number());
      const string summary = location + "\n" + part.summary();
      *stream << "      <failure message=\""
              << EscapeXmlAttribute(summary.c_str())
              << "\" type=\"\">";
      const string detail = location + "\n" + part.message();
      OutputXmlCDataSection(stream, RemoveInvalidXmlCharacters(detail).c_str());
      *stream << "</failure>\n";
    }
  }

  if (failures == 0)
    *stream << " />\n";
  else
    *stream << "    </testcase>\n";
}

// Prints an XML representation of a TestCase object
void XmlUnitTestResultPrinter::PrintXmlTestCase(std::ostream* stream,
                                                const TestCase& test_case) {
  const std::string kTestsuite = "testsuite";
  *stream << "  <" << kTestsuite;
  OutputXmlAttribute(stream, kTestsuite, "name", test_case.name());
  OutputXmlAttribute(stream, kTestsuite, "tests",
                     StreamableToString(test_case.reportable_test_count()));
  OutputXmlAttribute(stream, kTestsuite, "failures",
                     StreamableToString(test_case.failed_test_count()));
  OutputXmlAttribute(
      stream, kTestsuite, "disabled",
      StreamableToString(test_case.reportable_disabled_test_count()));
  OutputXmlAttribute(stream, kTestsuite, "errors", "0");
  OutputXmlAttribute(stream, kTestsuite, "time",
                     FormatTimeInMillisAsSeconds(test_case.elapsed_time()));
  *stream << TestPropertiesAsXmlAttributes(test_case.ad_hoc_test_result())
          << ">\n";

  for (int i = 0; i < test_case.total_test_count(); ++i) {
    if (test_case.GetTestInfo(i)->is_reportable())
      OutputXmlTestInfo(stream, test_case.name(), *test_case.GetTestInfo(i));
  }
  *stream << "  </" << kTestsuite << ">\n";
}

// Prints an XML summary of unit_test to output stream out.
void XmlUnitTestResultPrinter::PrintXmlUnitTest(std::ostream* stream,
                                                const UnitTest& unit_test) {
  const std::string kTestsuites = "testsuites";

  *stream << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  *stream << "<" << kTestsuites;

  OutputXmlAttribute(stream, kTestsuites, "tests",
                     StreamableToString(unit_test.reportable_test_count()));
  OutputXmlAttribute(stream, kTestsuites, "failures",
                     StreamableToString(unit_test.failed_test_count()));
  OutputXmlAttribute(
      stream, kTestsuites, "disabled",
      StreamableToString(unit_test.reportable_disabled_test_count()));
  OutputXmlAttribute(stream, kTestsuites, "errors", "0");
  OutputXmlAttribute(
      stream, kTestsuites, "timestamp",
      FormatEpochTimeInMillisAsIso8601(unit_test.start_timestamp()));
  OutputXmlAttribute(stream, kTestsuites, "time",
                     FormatTimeInMillisAsSeconds(unit_test.elapsed_time()));

  if (GTEST_FLAG(shuffle)) {
    OutputXmlAttribute(stream, kTestsuites, "random_seed",
                       StreamableToString(unit_test.random_seed()));
  }

  *stream << TestPropertiesAsXmlAttributes(unit_test.ad_hoc_test_result());

  OutputXmlAttribute(stream, kTestsuites, "name", "AllTests");
  *stream << ">\n";

  for (int i = 0; i < unit_test.total_test_case_count(); ++i) {
    if (unit_test.GetTestCase(i)->reportable_test_count() > 0)
      PrintXmlTestCase(stream, *unit_test.GetTestCase(i));
  }
  *stream << "</" << kTestsuites << ">\n";
}

// Produces a string representing the test properties in a result as space
// delimited XML attributes based on the property key="value" pairs.
std::string XmlUnitTestResultPrinter::TestPropertiesAsXmlAttributes(
    const TestResult& result) {
  Message attributes;
  for (int i = 0; i < result.test_property_count(); ++i) {
    const TestProperty& property = result.GetTestProperty(i);
    attributes << " " << property.key() << "="
        << "\"" << EscapeXmlAttribute(property.value()) << "\"";
  }
  return attributes.GetString();
}

// End XmlUnitTestResultPrinter

#if GTEST_CAN_STREAM_RESULTS_

// Checks if str contains '=', '&', '%' or '\n' characters. If yes,
// replaces them by "%xx" where xx is their hexadecimal value. For
// example, replaces "=" with "%3D".  This algorithm is O(strlen(str))
// in both time and space -- important as the input str may contain an
// arbitrarily long test failure message and stack trace.
string StreamingListener::UrlEncode(const char* str) {
  string result;
  result.reserve(strlen(str) + 1);
  for (char ch = *str; ch != '\0'; ch = *++str) {
    switch (ch) {
      case '%':
      case '=':
      case '&':
      case '\n':
        result.append("%" + String::FormatByte(static_cast<unsigned char>(ch)));
        break;
      default:
        result.push_back(ch);
        break;
    }
  }
  return result;
}

void StreamingListener::SocketWriter::MakeConnection() {
  GTEST_CHECK_(sockfd_ == -1)
      << "MakeConnection() can't be called when there is already a connection.";

  addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;    // To allow both IPv4 and IPv6 addresses.
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* servinfo = NULL;

  // Use the getaddrinfo() to get a linked list of IP addresses for
  // the given host name.
  const int error_num = getaddrinfo(
      host_name_.c_str(), port_num_.c_str(), &hints, &servinfo);
  if (error_num != 0) {
    GTEST_LOG_(WARNING) << "stream_result_to: getaddrinfo() failed: "
                        << gai_strerror(error_num);
  }

  // Loop through all the results and connect to the first we can.
  for (addrinfo* cur_addr = servinfo; sockfd_ == -1 && cur_addr != NULL;
       cur_addr = cur_addr->ai_next) {
    sockfd_ = socket(
        cur_addr->ai_family, cur_addr->ai_socktype, cur_addr->ai_protocol);
    if (sockfd_ != -1) {
      // Connect the client socket to the server socket.
      if (connect(sockfd_, cur_addr->ai_addr, cur_addr->ai_addrlen) == -1) {
        close(sockfd_);
        sockfd_ = -1;
      }
    }
  }

  freeaddrinfo(servinfo);  // all done with this structure

  if (sockfd_ == -1) {
    GTEST_LOG_(WARNING) << "stream_result_to: failed to connect to "
                        << host_name_ << ":" << port_num_;
  }
}

// End of class Streaming Listener
#endif  // GTEST_CAN_STREAM_RESULTS__

// Class ScopedTrace

// Pushes the given source file location and message onto a per-thread
// trace stack maintained by Google Test.
ScopedTrace::ScopedTrace(const char* file, int line, const Message& message)
    GTEST_LOCK_EXCLUDED_(&UnitTest::mutex_) {
  TraceInfo trace;
  trace.file = file;
  trace.line = line;
  trace.message = message.GetString();

  UnitTest::GetInstance()->PushGTestTrace(trace);
}

// Pops the info pushed by the c'tor.
ScopedTrace::~ScopedTrace()
    GTEST_LOCK_EXCLUDED_(&UnitTest::mutex_) {
  UnitTest::GetInstance()->PopGTestTrace();
}


// class OsStackTraceGetter

// Returns the current OS stack trace as an std::string.  Parameters:
//
//   max_depth  - the maximum number of stack frames to be included
//                in the trace.
//   skip_count - the number of top frames to be skipped; doesn't count
//                against max_depth.
//
string OsStackTraceGetter::CurrentStackTrace(int /* max_depth */,
                                             int /* skip_count */)
    GTEST_LOCK_EXCLUDED_(mutex_) {
  return "";
}

void OsStackTraceGetter::UponLeavingGTest()
    GTEST_LOCK_EXCLUDED_(mutex_) {
}

const char* const
OsStackTraceGetter::kElidedFramesMarker =
    "... " GTEST_NAME_ " internal frames ...";

// A helper class that creates the premature-exit file in its
// constructor and deletes the file in its destructor.
class ScopedPrematureExitFile {
 public:
  explicit ScopedPrematureExitFile(const char* premature_exit_filepath)
      : premature_exit_filepath_(premature_exit_filepath) {
    // If a path to the premature-exit file is specified...
    if (premature_exit_filepath != NULL && *premature_exit_filepath != '\0') {
      // create the file with a single "0" character in it.  I/O
      // errors are ignored as there's nothing better we can do and we
      // don't want to fail the test because of this.
      FILE* pfile = posix::FOpen(premature_exit_filepath, "w");
      fwrite("0", 1, 1, pfile);
      fclose(pfile);
    }
  }

  ~ScopedPrematureExitFile() {
    if (premature_exit_filepath_ != NULL && *premature_exit_filepath_ != '\0') {
      remove(premature_exit_filepath_);
    }
  }

 private:
  const char* const premature_exit_filepath_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(ScopedPrematureExitFile);
};

}  // namespace internal

// class TestEventListeners

TestEventListeners::TestEventListeners()
    : repeater_(new internal::TestEventRepeater()),
      default_result_printer_(NULL),
      default_xml_generator_(NULL) {
}

TestEventListeners::~TestEventListeners() { delete repeater_; }

// Returns the standard listener responsible for the default console
// output.  Can be removed from the listeners list to shut down default
// console output.  Note that removing this object from the listener list
// with Release transfers its ownership to the user.
void TestEventListeners::Append(TestEventListener* listener) {
  repeater_->Append(listener);
}

// Removes the given event listener from the list and returns it.  It then
// becomes the caller's responsibility to delete the listener. Returns
// NULL if the listener is not found in the list.
TestEventListener* TestEventListeners::Release(TestEventListener* listener) {
  if (listener == default_result_printer_)
    default_result_printer_ = NULL;
  else if (listener == default_xml_generator_)
    default_xml_generator_ = NULL;
  return repeater_->Release(listener);
}

// Returns repeater that broadcasts the TestEventListener events to all
// subscribers.
TestEventListener* TestEventListeners::repeater() { return repeater_; }

// Sets the default_result_printer attribute to the provided listener.
// The listener is also added to the listener list and previous
// default_result_printer is removed from it and deleted. The listener can
// also be NULL in which case it will not be added to the list. Does
// nothing if the previous and the current listener objects are the same.
void TestEventListeners::SetDefaultResultPrinter(TestEventListener* listener) {
  if (default_result_printer_ != listener) {
    // It is an error to pass this method a listener that is already in the
    // list.
    delete Release(default_result_printer_);
    default_result_printer_ = listener;
    if (listener != NULL)
      Append(listener);
  }
}

// Sets the default_xml_generator attribute to the provided listener.  The
// listener is also added to the listener list and previous
// default_xml_generator is removed from it and deleted. The listener can
// also be NULL in which case it will not be added to the list. Does
// nothing if the previous and the current listener objects are the same.
void TestEventListeners::SetDefaultXmlGenerator(TestEventListener* listener) {
  if (default_xml_generator_ != listener) {
    // It is an error to pass this method a listener that is already in the
    // list.
    delete Release(default_xml_generator_);
    default_xml_generator_ = listener;
    if (listener != NULL)
      Append(listener);
  }
}

// Controls whether events will be forwarded by the repeater to the
// listeners in the list.
bool TestEventListeners::EventForwardingEnabled() const {
  return repeater_->forwarding_enabled();
}

void TestEventListeners::SuppressEventForwarding() {
  repeater_->set_forwarding_enabled(false);
}

// class UnitTest

// Gets the singleton UnitTest object.  The first time this method is
// called, a UnitTest object is constructed and returned.  Consecutive
// calls will return the same object.
//
// We don't protect this under mutex_ as a user is not supposed to
// call this before main() starts, from which point on the return
// value will never change.
UnitTest* UnitTest::GetInstance() {
  // When compiled with MSVC 7.1 in optimized mode, destroying the
  // UnitTest object upon exiting the program messes up the exit code,
  // causing successful tests to appear failed.  We have to use a
  // different implementation in this case to bypass the compiler bug.
  // This implementation makes the compiler happy, at the cost of
  // leaking the UnitTest object.

  // CodeGear C++Builder insists on a public destructor for the
  // default implementation.  Use this implementation to keep good OO
  // design with private destructor.

#if (_MSC_VER == 1310 && !defined(_DEBUG)) || defined(__BORLANDC__)
  static UnitTest* const instance = new UnitTest;
  return instance;
#else
  static UnitTest instance;
  return &instance;
#endif  // (_MSC_VER == 1310 && !defined(_DEBUG)) || defined(__BORLANDC__)
}

// Gets the number of successful test cases.
int UnitTest::successful_test_case_count() const {
  return impl()->successful_test_case_count();
}

// Gets the number of failed test cases.
int UnitTest::failed_test_case_count() const {
  return impl()->failed_test_case_count();
}

// Gets the number of all test cases.
int UnitTest::total_test_case_count() const {
  return impl()->total_test_case_count();
}

// Gets the number of all test cases that contain at least one test
// that should run.
int UnitTest::test_case_to_run_count() const {
  return impl()->test_case_to_run_count();
}

// Gets the number of successful tests.
int UnitTest::successful_test_count() const {
  return impl()->successful_test_count();
}

// Gets the number of failed tests.
int UnitTest::failed_test_count() const { return impl()->failed_test_count(); }

// Gets the number of disabled tests that will be reported in the XML report.
int UnitTest::reportable_disabled_test_count() const {
  return impl()->reportable_disabled_test_count();
}

// Gets the number of disabled tests.
int UnitTest::disabled_test_count() const {
  return impl()->disabled_test_count();
}

// Gets the number of tests to be printed in the XML report.
int UnitTest::reportable_test_count() const {
  return impl()->reportable_test_count();
}

// Gets the number of all tests.
int UnitTest::total_test_count() const { return impl()->total_test_count(); }

// Gets the number of tests that should run.
int UnitTest::test_to_run_count() const { return impl()->test_to_run_count(); }

// Gets the time of the test program start, in ms from the start of the
// UNIX epoch.
internal::TimeInMillis UnitTest::start_timestamp() const {
    return impl()->start_timestamp();
}

// Gets the elapsed time, in milliseconds.
internal::TimeInMillis UnitTest::elapsed_time() const {
  return impl()->elapsed_time();
}

// Returns true iff the unit test passed (i.e. all test cases passed).
bool UnitTest::Passed() const { return impl()->Passed(); }

// Returns true iff the unit test failed (i.e. some test case failed
// or something outside of all tests failed).
bool UnitTest::Failed() const { return impl()->Failed(); }

// Gets the i-th test case among all the test cases. i can range from 0 to
// total_test_case_count() - 1. If i is not in that range, returns NULL.
const TestCase* UnitTest::GetTestCase(int i) const {
  return impl()->GetTestCase(i);
}

// Returns the TestResult containing information on test failures and
// properties logged outside of individual test cases.
const TestResult& UnitTest::ad_hoc_test_result() const {
  return *impl()->ad_hoc_test_result();
}

// Gets the i-th test case among all the test cases. i can range from 0 to
// total_test_case_count() - 1. If i is not in that range, returns NULL.
TestCase* UnitTest::GetMutableTestCase(int i) {
  return impl()->GetMutableTestCase(i);
}

// Returns the list of event listeners that can be used to track events
// inside Google Test.
TestEventListeners& UnitTest::listeners() {
  return *impl()->listeners();
}

// Registers and returns a global test environment.  When a test
// program is run, all global test environments will be set-up in the
// order they were registered.  After all tests in the program have
// finished, all global test environments will be torn-down in the
// *reverse* order they were registered.
//
// The UnitTest object takes ownership of the given environment.
//
// We don't protect this under mutex_, as we only support calling it
// from the main thread.
Environment* UnitTest::AddEnvironment(Environment* env) {
  if (env == NULL) {
    return NULL;
  }

  impl_->environments().push_back(env);
  return env;
}

// Adds a TestPartResult to the current TestResult object.  All Google Test
// assertion macros (e.g. ASSERT_TRUE, EXPECT_EQ, etc) eventually call
// this to report their results.  The user code should use the
// assertion macros instead of calling this directly.
void UnitTest::AddTestPartResult(
    TestPartResult::Type result_type,
    const char* file_name,
    int line_number,
    const std::string& message,
    const std::string& os_stack_trace) GTEST_LOCK_EXCLUDED_(mutex_) {
  Message msg;
  msg << message;

  internal::MutexLock lock(&mutex_);
  if (impl_->gtest_trace_stack().size() > 0) {
    msg << "\n" << GTEST_NAME_ << " trace:";

    for (int i = static_cast<int>(impl_->gtest_trace_stack().size());
         i > 0; --i) {
      const internal::TraceInfo& trace = impl_->gtest_trace_stack()[i - 1];
      msg << "\n" << internal::FormatFileLocation(trace.file, trace.line)
          << " " << trace.message;
    }
  }

  if (os_stack_trace.c_str() != NULL && !os_stack_trace.empty()) {
    msg << internal::kStackTraceMarker << os_stack_trace;
  }

  const TestPartResult result =
    TestPartResult(result_type, file_name, line_number,
                   msg.GetString().c_str());
  impl_->GetTestPartResultReporterForCurrentThread()->
      ReportTestPartResult(result);

  if (result_type != TestPartResult::kSuccess) {
    // gtest_break_on_failure takes precedence over
    // gtest_throw_on_failure.  This allows a user to set the latter
    // in the code (perhaps in order to use Google Test assertions
    // with another testing framework) and specify the former on the
    // command line for debugging.
    if (GTEST_FLAG(break_on_failure)) {
#if GTEST_OS_WINDOWS
      // Using DebugBreak on Windows allows gtest to still break into a debugger
      // when a failure happens and both the --gtest_break_on_failure and
      // the --gtest_catch_exceptions flags are specified.
      DebugBreak();
#else
      // Dereference NULL through a volatile pointer to prevent the compiler
      // from removing. We use this rather than abort() or __builtin_trap() for
      // portability: Symbian doesn't implement abort() well, and some debuggers
      // don't correctly trap abort().
      *static_cast<volatile int*>(NULL) = 1;
#endif  // GTEST_OS_WINDOWS
    } else if (GTEST_FLAG(throw_on_failure)) {
#if GTEST_HAS_EXCEPTIONS
      throw internal::GoogleTestFailureException(result);
#else
      // We cannot call abort() as it generates a pop-up in debug mode
      // that cannot be suppressed in VC 7.1 or below.
      exit(1);
#endif
    }
  }
}

// Adds a TestProperty to the current TestResult object when invoked from
// inside a test, to current TestCase's ad_hoc_test_result_ when invoked
// from SetUpTestCase or TearDownTestCase, or to the global property set
// when invoked elsewhere.  If the result already contains a property with
// the same key, the value will be updated.
void UnitTest::RecordProperty(const std::string& key,
                              const std::string& value) {
  impl_->RecordProperty(TestProperty(key, value));
}

// Runs all tests in this UnitTest object and prints the result.
// Returns 0 if successful, or 1 otherwise.
//
// We don't protect this under mutex_, as we only support calling it
// from the main thread.
int UnitTest::Run() {
  const bool in_death_test_child_process =
      internal::GTEST_FLAG(internal_run_death_test).length() > 0;

  // Google Test implements this protocol for catching that a test
  // program exits before returning control to Google Test:
  //
  //   1. Upon start, Google Test creates a file whose absolute path
  //      is specified by the environment variable
  //      TEST_PREMATURE_EXIT_FILE.
  //   2. When Google Test has finished its work, it deletes the file.
  //
  // This allows a test runner to set TEST_PREMATURE_EXIT_FILE before
  // running a Google-Test-based test program and check the existence
  // of the file at the end of the test execution to see if it has
  // exited prematurely.

  // If we are in the child process of a death test, don't
  // create/delete the premature exit file, as doing so is unnecessary
  // and will confuse the parent process.  Otherwise, create/delete
  // the file upon entering/leaving this function.  If the program
  // somehow exits before this function has a chance to return, the
  // premature-exit file will be left undeleted, causing a test runner
  // that understands the premature-exit-file protocol to report the
  // test as having failed.
  const internal::ScopedPrematureExitFile premature_exit_file(
      in_death_test_child_process ?
      NULL : internal::posix::GetEnv("TEST_PREMATURE_EXIT_FILE"));

  // Captures the value of GTEST_FLAG(catch_exceptions).  This value will be
  // used for the duration of the program.
  impl()->set_catch_exceptions(GTEST_FLAG(catch_exceptions));

#if GTEST_HAS_SEH
  // Either the user wants Google Test to catch exceptions thrown by the
  // tests or this is executing in the context of death test child
  // process. In either case the user does not want to see pop-up dialogs
  // about crashes - they are expected.
  if (impl()->catch_exceptions() || in_death_test_child_process) {
# if !GTEST_OS_WINDOWS_MOBILE
    // SetErrorMode doesn't exist on CE.
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOALIGNMENTFAULTEXCEPT |
                 SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
# endif  // !GTEST_OS_WINDOWS_MOBILE

# if (defined(_MSC_VER) || GTEST_OS_WINDOWS_MINGW) && !GTEST_OS_WINDOWS_MOBILE
    // Death test children can be terminated with _abort().  On Windows,
    // _abort() can show a dialog with a warning message.  This forces the
    // abort message to go to stderr instead.
    _set_error_mode(_OUT_TO_STDERR);
# endif

# if _MSC_VER >= 1400 && !GTEST_OS_WINDOWS_MOBILE
    // In the debug version, Visual Studio pops up a separate dialog
    // offering a choice to debug the aborted program. We need to suppress
    // this dialog or it will pop up for every EXPECT/ASSERT_DEATH statement
    // executed. Google Test will notify the user of any unexpected
    // failure via stderr.
    //
    // VC++ doesn't define _set_abort_behavior() prior to the version 8.0.
    // Users of prior VC versions shall suffer the agony and pain of
    // clicking through the countless debug dialogs.
    // TODO(vladl@google.com): find a way to suppress the abort dialog() in the
    // debug mode when compiled with VC 7.1 or lower.
    if (!GTEST_FLAG(break_on_failure))
      _set_abort_behavior(
          0x0,                                    // Clear the following flags:
          _WRITE_ABORT_MSG | _CALL_REPORTFAULT);  // pop-up window, core dump.
# endif
  }
#endif  // GTEST_HAS_SEH

  return internal::HandleExceptionsInMethodIfSupported(
      impl(),
      &internal::UnitTestImpl::RunAllTests,
      "auxiliary test code (environments or event listeners)") ? 0 : 1;
}

// Returns the working directory when the first TEST() or TEST_F() was
// executed.
const char* UnitTest::original_working_dir() const {
  return impl_->original_working_dir_.c_str();
}

// Returns the TestCase object for the test that's currently running,
// or NULL if no test is running.
const TestCase* UnitTest::current_test_case() const
    GTEST_LOCK_EXCLUDED_(mutex_) {
  internal::MutexLock lock(&mutex_);
  return impl_->current_test_case();
}

// Returns the TestInfo object for the test that's currently running,
// or NULL if no test is running.
const TestInfo* UnitTest::current_test_info() const
    GTEST_LOCK_EXCLUDED_(mutex_) {
  internal::MutexLock lock(&mutex_);
  return impl_->current_test_info();
}

// Returns the random seed used at the start of the current test run.
int UnitTest::random_seed() const { return impl_->random_seed(); }

#if GTEST_HAS_PARAM_TEST
// Returns ParameterizedTestCaseRegistry object used to keep track of
// value-parameterized tests and instantiate and register them.
internal::ParameterizedTestCaseRegistry&
    UnitTest::parameterized_test_registry()
        GTEST_LOCK_EXCLUDED_(mutex_) {
  return impl_->parameterized_test_registry();
}
#endif  // GTEST_HAS_PARAM_TEST

// Creates an empty UnitTest.
UnitTest::UnitTest() {
  impl_ = new internal::UnitTestImpl(this);
}

// Destructor of UnitTest.
UnitTest::~UnitTest() {
  delete impl_;
}

// Pushes a trace defined by SCOPED_TRACE() on to the per-thread
// Google Test trace stack.
void UnitTest::PushGTestTrace(const internal::TraceInfo& trace)
    GTEST_LOCK_EXCLUDED_(mutex_) {
  internal::MutexLock lock(&mutex_);
  impl_->gtest_trace_stack().push_back(trace);
}

// Pops a trace from the per-thread Google Test trace stack.
void UnitTest::PopGTestTrace()
    GTEST_LOCK_EXCLUDED_(mutex_) {
  internal::MutexLock lock(&mutex_);
  impl_->gtest_trace_stack().pop_back();
}

namespace internal {

UnitTestImpl::UnitTestImpl(UnitTest* parent)
    : parent_(parent),
#ifdef _MSC_VER
# pragma warning(push)                    // Saves the current warning state.
# pragma warning(disable:4355)            // Temporarily disables warning 4355
                                         // (using this in initializer).
      default_global_test_part_result_reporter_(this),
      default_per_thread_test_part_result_reporter_(this),
# pragma warning(pop)                     // Restores the warning state again.
#else
      default_global_test_part_result_reporter_(this),
      default_per_thread_test_part_result_reporter_(this),
#endif  // _MSC_VER
      global_test_part_result_repoter_(
          &default_global_test_part_result_reporter_),
      per_thread_test_part_result_reporter_(
          &default_per_thread_test_part_result_reporter_),
#if GTEST_HAS_PARAM_TEST
      parameterized_test_registry_(),
      parameterized_tests_registered_(false),
#endif  // GTEST_HAS_PARAM_TEST
      last_death_test_case_(-1),
      current_test_case_(NULL),
      current_test_info_(NULL),
      ad_hoc_test_result_(),
      os_stack_trace_getter_(NULL),
      post_flag_parse_init_performed_(false),
      random_seed_(0),  // Will be overridden by the flag before first use.
      random_(0),  // Will be reseeded before first use.
      start_timestamp_(0),
      elapsed_time_(0),
#if GTEST_HAS_DEATH_TEST
      death_test_factory_(new DefaultDeathTestFactory),
#endif
      // Will be overridden by the flag before first use.
      catch_exceptions_(false) {
  listeners()->SetDefaultResultPrinter(new PrettyUnitTestResultPrinter);
}

UnitTestImpl::~UnitTestImpl() {
  // Deletes every TestCase.
  ForEach(test_cases_, internal::Delete<TestCase>);

  // Deletes every Environment.
  ForEach(environments_, internal::Delete<Environment>);

  delete os_stack_trace_getter_;
}

// Adds a TestProperty to the current TestResult object when invoked in a
// context of a test, to current test case's ad_hoc_test_result when invoke
// from SetUpTestCase/TearDownTestCase, or to the global property set
// otherwise.  If the result already contains a property with the same key,
// the value will be updated.
void UnitTestImpl::RecordProperty(const TestProperty& test_property) {
  std::string xml_element;
  TestResult* test_result;  // TestResult appropriate for property recording.

  if (current_test_info_ != NULL) {
    xml_element = "testcase";
    test_result = &(current_test_info_->result_);
  } else if (current_test_case_ != NULL) {
    xml_element = "testsuite";
    test_result = &(current_test_case_->ad_hoc_test_result_);
  } else {
    xml_element = "testsuites";
    test_result = &ad_hoc_test_result_;
  }
  test_result->RecordProperty(xml_element, test_property);
}

#if GTEST_HAS_DEATH_TEST
// Disables event forwarding if the control is currently in a death test
// subprocess. Must not be called before InitGoogleTest.
void UnitTestImpl::SuppressTestEventsIfInSubprocess() {
  if (internal_run_death_test_flag_.get() != NULL)
    listeners()->SuppressEventForwarding();
}
#endif  // GTEST_HAS_DEATH_TEST

// Initializes event listeners performing XML output as specified by
// UnitTestOptions. Must not be called before InitGoogleTest.
void UnitTestImpl::ConfigureXmlOutput() {
  const std::string& output_format = UnitTestOptions::GetOutputFormat();
  if (output_format == "xml") {
    listeners()->SetDefaultXmlGenerator(new XmlUnitTestResultPrinter(
        UnitTestOptions::GetAbsolutePathToOutputFile().c_str()));
  } else if (output_format != "") {
    printf("WARNING: unrecognized output format \"%s\" ignored.\n",
           output_format.c_str());
    fflush(stdout);
  }
}

#if GTEST_CAN_STREAM_RESULTS_
// Initializes event listeners for streaming test results in string form.
// Must not be called before InitGoogleTest.
void UnitTestImpl::ConfigureStreamingOutput() {
  const std::string& target = GTEST_FLAG(stream_result_to);
  if (!target.empty()) {
    const size_t pos = target.find(':');
    if (pos != std::string::npos) {
      listeners()->Append(new StreamingListener(target.substr(0, pos),
                                                target.substr(pos+1)));
    } else {
      printf("WARNING: unrecognized streaming target \"%s\" ignored.\n",
             target.c_str());
      fflush(stdout);
    }
  }
}
#endif  // GTEST_CAN_STREAM_RESULTS_

// Performs initialization dependent upon flag values obtained in
// ParseGoogleTestFlagsOnly.  Is called from InitGoogleTest after the call to
// ParseGoogleTestFlagsOnly.  In case a user neglects to call InitGoogleTest
// this function is also called from RunAllTests.  Since this function can be
// called more than once, it has to be idempotent.
void UnitTestImpl::PostFlagParsingInit() {
  // Ensures that this function does not execute more than once.
  if (!post_flag_parse_init_performed_) {
    post_flag_parse_init_performed_ = true;

#if GTEST_HAS_DEATH_TEST
    InitDeathTestSubprocessControlInfo();
    SuppressTestEventsIfInSubprocess();
#endif  // GTEST_HAS_DEATH_TEST

    // Registers parameterized tests. This makes parameterized tests
    // available to the UnitTest reflection API without running
    // RUN_ALL_TESTS.
    RegisterParameterizedTests();

    // Configures listeners for XML output. This makes it possible for users
    // to shut down the default XML output before invoking RUN_ALL_TESTS.
    ConfigureXmlOutput();

#if GTEST_CAN_STREAM_RESULTS_
    // Configures listeners for streaming test results to the specified server.
    ConfigureStreamingOutput();
#endif  // GTEST_CAN_STREAM_RESULTS_
  }
}

// A predicate that checks the name of a TestCase against a known
// value.
//
// This is used for implementation of the UnitTest class only.  We put
// it in the anonymous namespace to prevent polluting the outer
// namespace.
//
// TestCaseNameIs is copyable.
class TestCaseNameIs {
 public:
  // Constructor.
  explicit TestCaseNameIs(const std::string& name)
      : name_(name) {}

  // Returns true iff the name of test_case matches name_.
  bool operator()(const TestCase* test_case) const {
    return test_case != NULL && strcmp(test_case->name(), name_.c_str()) == 0;
  }

 private:
  std::string name_;
};

// Finds and returns a TestCase with the given name.  If one doesn't
// exist, creates one and returns it.  It's the CALLER'S
// RESPONSIBILITY to ensure that this function is only called WHEN THE
// TESTS ARE NOT SHUFFLED.
//
// Arguments:
//
//   test_case_name: name of the test case
//   type_param:     the name of the test case's type parameter, or NULL if
//                   this is not a typed or a type-parameterized test case.
//   set_up_tc:      pointer to the function that sets up the test case
//   tear_down_tc:   pointer to the function that tears down the test case
TestCase* UnitTestImpl::GetTestCase(const char* test_case_name,
                                    const char* type_param,
                                    Test::SetUpTestCaseFunc set_up_tc,
                                    Test::TearDownTestCaseFunc tear_down_tc) {
  // Can we find a TestCase with the given name?
  const std::vector<TestCase*>::const_iterator test_case =
      std::find_if(test_cases_.begin(), test_cases_.end(),
                   TestCaseNameIs(test_case_name));

  if (test_case != test_cases_.end())
    return *test_case;

  // No.  Let's create one.
  TestCase* const new_test_case =
      new TestCase(test_case_name, type_param, set_up_tc, tear_down_tc);

  // Is this a death test case?
  if (internal::UnitTestOptions::MatchesFilter(test_case_name,
                                               kDeathTestCaseFilter)) {
    // Yes.  Inserts the test case after the last death test case
    // defined so far.  This only works when the test cases haven't
    // been shuffled.  Otherwise we may end up running a death test
    // after a non-death test.
    ++last_death_test_case_;
    test_cases_.insert(test_cases_.begin() + last_death_test_case_,
                       new_test_case);
  } else {
    // No.  Appends to the end of the list.
    test_cases_.push_back(new_test_case);
  }

  test_case_indices_.push_back(static_cast<int>(test_case_indices_.size()));
  return new_test_case;
}

// Helpers for setting up / tearing down the given environment.  They
// are for use in the ForEach() function.
static void SetUpEnvironment(Environment* env) { env->SetUp(); }
static void TearDownEnvironment(Environment* env) { env->TearDown(); }

// Runs all tests in this UnitTest object, prints the result, and
// returns true if all tests are successful.  If any exception is
// thrown during a test, the test is considered to be failed, but the
// rest of the tests will still be run.
//
// When parameterized tests are enabled, it expands and registers
// parameterized tests first in RegisterParameterizedTests().
// All other functions called from RunAllTests() may safely assume that
// parameterized tests are ready to be counted and run.
bool UnitTestImpl::RunAllTests() {
  // Makes sure InitGoogleTest() was called.
  if (!GTestIsInitialized()) {
    printf("%s",
           "\nThis test program did NOT call ::testing::InitGoogleTest "
           "before calling RUN_ALL_TESTS().  Please fix it.\n");
    return false;
  }

  // Do not run any test if the --help flag was specified.
  if (g_help_flag)
    return true;

  // Repeats the call to the post-flag parsing initialization in case the
  // user didn't call InitGoogleTest.
  PostFlagParsingInit();

  // Even if sharding is not on, test runners may want to use the
  // GTEST_SHARD_STATUS_FILE to query whether the test supports the sharding
  // protocol.
  internal::WriteToShardStatusFileIfNeeded();

  // True iff we are in a subprocess for running a thread-safe-style
  // death test.
  bool in_subprocess_for_death_test = false;

#if GTEST_HAS_DEATH_TEST
  in_subprocess_for_death_test = (internal_run_death_test_flag_.get() != NULL);
#endif  // GTEST_HAS_DEATH_TEST

  const bool should_shard = ShouldShard(kTestTotalShards, kTestShardIndex,
                                        in_subprocess_for_death_test);

  // Compares the full test names with the filter to decide which
  // tests to run.
  const bool has_tests_to_run = FilterTests(should_shard
                                              ? HONOR_SHARDING_PROTOCOL
                                              : IGNORE_SHARDING_PROTOCOL) > 0;

  // Lists the tests and exits if the --gtest_list_tests flag was specified.
  if (GTEST_FLAG(list_tests)) {
    // This must be called *after* FilterTests() has been called.
    ListTestsMatchingFilter();
    return true;
  }

  random_seed_ = GTEST_FLAG(shuffle) ?
      GetRandomSeedFromFlag(GTEST_FLAG(random_seed)) : 0;

  // True iff at least one test has failed.
  bool failed = false;

  TestEventListener* repeater = listeners()->repeater();

  start_timestamp_ = GetTimeInMillis();
  repeater->OnTestProgramStart(*parent_);

  // How many times to repeat the tests?  We don't want to repeat them
  // when we are inside the subprocess of a death test.
  const int repeat = in_subprocess_for_death_test ? 1 : GTEST_FLAG(repeat);
  // Repeats forever if the repeat count is negative.
  const bool forever = repeat < 0;
  for (int i = 0; forever || i != repeat; i++) {
    // We want to preserve failures generated by ad-hoc test
    // assertions executed before RUN_ALL_TESTS().
    ClearNonAdHocTestResult();

    const TimeInMillis start = GetTimeInMillis();

    // Shuffles test cases and tests if requested.
    if (has_tests_to_run && GTEST_FLAG(shuffle)) {
      random()->Reseed(random_seed_);
      // This should be done before calling OnTestIterationStart(),
      // such that a test event listener can see the actual test order
      // in the event.
      ShuffleTests();
    }

    // Tells the unit test event listeners that the tests are about to start.
    repeater->OnTestIterationStart(*parent_, i);

    // Runs each test case if there is at least one test to run.
    if (has_tests_to_run) {
      // Sets up all environments beforehand.
      repeater->OnEnvironmentsSetUpStart(*parent_);
      ForEach(environments_, SetUpEnvironment);
      repeater->OnEnvironmentsSetUpEnd(*parent_);

      // Runs the tests only if there was no fatal failure during global
      // set-up.
      if (!Test::HasFatalFailure()) {
        for (int test_index = 0; test_index < total_test_case_count();
             test_index++) {
          GetMutableTestCase(test_index)->Run();
        }
      }

      // Tears down all environments in reverse order afterwards.
      repeater->OnEnvironmentsTearDownStart(*parent_);
      std::for_each(environments_.rbegin(), environments_.rend(),
                    TearDownEnvironment);
      repeater->OnEnvironmentsTearDownEnd(*parent_);
    }

    elapsed_time_ = GetTimeInMillis() - start;

    // Tells the unit test event listener that the tests have just finished.
    repeater->OnTestIterationEnd(*parent_, i);

    // Gets the result and clears it.
    if (!Passed()) {
      failed = true;
    }

    // Restores the original test order after the iteration.  This
    // allows the user to quickly repro a failure that happens in the
    // N-th iteration without repeating the first (N - 1) iterations.
    // This is not enclosed in "if (GTEST_FLAG(shuffle)) { ... }", in
    // case the user somehow changes the value of the flag somewhere
    // (it's always safe to unshuffle the tests).
    UnshuffleTests();

    if (GTEST_FLAG(shuffle)) {
      // Picks a new random seed for each iteration.
      random_seed_ = GetNextRandomSeed(random_seed_);
    }
  }

  repeater->OnTestProgramEnd(*parent_);

  return !failed;
}

// Reads the GTEST_SHARD_STATUS_FILE environment variable, and creates the file
// if the variable is present. If a file already exists at this location, this
// function will write over it. If the variable is present, but the file cannot
// be created, prints an error and exits.
void WriteToShardStatusFileIfNeeded() {
  const char* const test_shard_file = posix::GetEnv(kTestShardStatusFile);
  if (test_shard_file != NULL) {
    FILE* const file = posix::FOpen(test_shard_file, "w");
    if (file == NULL) {
      ColoredPrintf(COLOR_RED,
                    "Could not write to the test shard status file \"%s\" "
                    "specified by the %s environment variable.\n",
                    test_shard_file, kTestShardStatusFile);
      fflush(stdout);
      exit(EXIT_FAILURE);
    }
    fclose(file);
  }
}

// Checks whether sharding is enabled by examining the relevant
// environment variable values. If the variables are present,
// but inconsistent (i.e., shard_index >= total_shards), prints
// an error and exits. If in_subprocess_for_death_test, sharding is
// disabled because it must only be applied to the original test
// process. Otherwise, we could filter out death tests we intended to execute.
bool ShouldShard(const char* total_shards_env,
                 const char* shard_index_env,
                 bool in_subprocess_for_death_test) {
  if (in_subprocess_for_death_test) {
    return false;
  }

  const Int32 total_shards = Int32FromEnvOrDie(total_shards_env, -1);
  const Int32 shard_index = Int32FromEnvOrDie(shard_index_env, -1);

  if (total_shards == -1 && shard_index == -1) {
    return false;
  } else if (total_shards == -1 && shard_index != -1) {
    const Message msg = Message()
      << "Invalid environment variables: you have "
      << kTestShardIndex << " = " << shard_index
      << ", but have left " << kTestTotalShards << " unset.\n";
    ColoredPrintf(COLOR_RED, msg.GetString().c_str());
    fflush(stdout);
    exit(EXIT_FAILURE);
  } else if (total_shards != -1 && shard_index == -1) {
    const Message msg = Message()
      << "Invalid environment variables: you have "
      << kTestTotalShards << " = " << total_shards
      << ", but have left " << kTestShardIndex << " unset.\n";
    ColoredPrintf(COLOR_RED, msg.GetString().c_str());
    fflush(stdout);
    exit(EXIT_FAILURE);
  } else if (shard_index < 0 || shard_index >= total_shards) {
    const Message msg = Message()
      << "Invalid environment variables: we require 0 <= "
      << kTestShardIndex << " < " << kTestTotalShards
      << ", but you have " << kTestShardIndex << "=" << shard_index
      << ", " << kTestTotalShards << "=" << total_shards << ".\n";
    ColoredPrintf(COLOR_RED, msg.GetString().c_str());
    fflush(stdout);
    exit(EXIT_FAILURE);
  }

  return total_shards > 1;
}

// Parses the environment variable var as an Int32. If it is unset,
// returns default_val. If it is not an Int32, prints an error
// and aborts.
Int32 Int32FromEnvOrDie(const char* var, Int32 default_val) {
  const char* str_val = posix::GetEnv(var);
  if (str_val == NULL) {
    return default_val;
  }

  Int32 result;
  if (!ParseInt32(Message() << "The value of environment variable " << var,
                  str_val, &result)) {
    exit(EXIT_FAILURE);
  }
  return result;
}

// Given the total number of shards, the shard index, and the test id,
// returns true iff the test should be run on this shard. The test id is
// some arbitrary but unique non-negative integer assigned to each test
// method. Assumes that 0 <= shard_index < total_shards.
bool ShouldRunTestOnShard(int total_shards, int shard_index, int test_id) {
  return (test_id % total_shards) == shard_index;
}

// Compares the name of each test with the user-specified filter to
// decide whether the test should be run, then records the result in
// each TestCase and TestInfo object.
// If shard_tests == true, further filters tests based on sharding
// variables in the environment - see
// http://code.google.com/p/googletest/wiki/GoogleTestAdvancedGuide.
// Returns the number of tests that should run.
int UnitTestImpl::FilterTests(ReactionToSharding shard_tests) {
  const Int32 total_shards = shard_tests == HONOR_SHARDING_PROTOCOL ?
      Int32FromEnvOrDie(kTestTotalShards, -1) : -1;
  const Int32 shard_index = shard_tests == HONOR_SHARDING_PROTOCOL ?
      Int32FromEnvOrDie(kTestShardIndex, -1) : -1;

  // num_runnable_tests are the number of tests that will
  // run across all shards (i.e., match filter and are not disabled).
  // num_selected_tests are the number of tests to be run on
  // this shard.
  int num_runnable_tests = 0;
  int num_selected_tests = 0;
  for (size_t i = 0; i < test_cases_.size(); i++) {
    TestCase* const test_case = test_cases_[i];
    const std::string &test_case_name = test_case->name();
    test_case->set_should_run(false);

    for (size_t j = 0; j < test_case->test_info_list().size(); j++) {
      TestInfo* const test_info = test_case->test_info_list()[j];
      const std::string test_name(test_info->name());
      // A test is disabled if test case name or test name matches
      // kDisableTestFilter.
      const bool is_disabled =
          internal::UnitTestOptions::MatchesFilter(test_case_name,
                                                   kDisableTestFilter) ||
          internal::UnitTestOptions::MatchesFilter(test_name,
                                                   kDisableTestFilter);
      test_info->is_disabled_ = is_disabled;

      const bool matches_filter =
          internal::UnitTestOptions::FilterMatchesTest(test_case_name,
                                                       test_name);
      test_info->matches_filter_ = matches_filter;

      const bool is_runnable =
          (GTEST_FLAG(also_run_disabled_tests) || !is_disabled) &&
          matches_filter;

      const bool is_selected = is_runnable &&
          (shard_tests == IGNORE_SHARDING_PROTOCOL ||
           ShouldRunTestOnShard(total_shards, shard_index,
                                num_runnable_tests));

      num_runnable_tests += is_runnable;
      num_selected_tests += is_selected;

      test_info->should_run_ = is_selected;
      test_case->set_should_run(test_case->should_run() || is_selected);
    }
  }
  return num_selected_tests;
}

// Prints the given C-string on a single line by replacing all '\n'
// characters with string "\\n".  If the output takes more than
// max_length characters, only prints the first max_length characters
// and "...".
static void PrintOnOneLine(const char* str, int max_length) {
  if (str != NULL) {
    for (int i = 0; *str != '\0'; ++str) {
      if (i >= max_length) {
        printf("...");
        break;
      }
      if (*str == '\n') {
        printf("\\n");
        i += 2;
      } else {
        printf("%c", *str);
        ++i;
      }
    }
  }
}

// Prints the names of the tests matching the user-specified filter flag.
void UnitTestImpl::ListTestsMatchingFilter() {
  // Print at most this many characters for each type/value parameter.
  const int kMaxParamLength = 250;

  for (size_t i = 0; i < test_cases_.size(); i++) {
    const TestCase* const test_case = test_cases_[i];
    bool printed_test_case_name = false;

    for (size_t j = 0; j < test_case->test_info_list().size(); j++) {
      const TestInfo* const test_info =
          test_case->test_info_list()[j];
      if (test_info->matches_filter_) {
        if (!printed_test_case_name) {
          printed_test_case_name = true;
          printf("%s.", test_case->name());
          if (test_case->type_param() != NULL) {
            printf("  # %s = ", kTypeParamLabel);
            // We print the type parameter on a single line to make
            // the output easy to parse by a program.
            PrintOnOneLine(test_case->type_param(), kMaxParamLength);
          }
          printf("\n");
        }
        printf("  %s", test_info->name());
        if (test_info->value_param() != NULL) {
          printf("  # %s = ", kValueParamLabel);
          // We print the value parameter on a single line to make the
          // output easy to parse by a program.
          PrintOnOneLine(test_info->value_param(), kMaxParamLength);
        }
        printf("\n");
      }
    }
  }
  fflush(stdout);
}

// Sets the OS stack trace getter.
//
// Does nothing if the input and the current OS stack trace getter are
// the same; otherwise, deletes the old getter and makes the input the
// current getter.
void UnitTestImpl::set_os_stack_trace_getter(
    OsStackTraceGetterInterface* getter) {
  if (os_stack_trace_getter_ != getter) {
    delete os_stack_trace_getter_;
    os_stack_trace_getter_ = getter;
  }
}

// Returns the current OS stack trace getter if it is not NULL;
// otherwise, creates an OsStackTraceGetter, makes it the current
// getter, and returns it.
OsStackTraceGetterInterface* UnitTestImpl::os_stack_trace_getter() {
  if (os_stack_trace_getter_ == NULL) {
    os_stack_trace_getter_ = new OsStackTraceGetter;
  }

  return os_stack_trace_getter_;
}

// Returns the TestResult for the test that's currently running, or
// the TestResult for the ad hoc test if no test is running.
TestResult* UnitTestImpl::current_test_result() {
  return current_test_info_ ?
      &(current_test_info_->result_) : &ad_hoc_test_result_;
}

// Shuffles all test cases, and the tests within each test case,
// making sure that death tests are still run first.
void UnitTestImpl::ShuffleTests() {
  // Shuffles the death test cases.
  ShuffleRange(random(), 0, last_death_test_case_ + 1, &test_case_indices_);

  // Shuffles the non-death test cases.
  ShuffleRange(random(), last_death_test_case_ + 1,
               static_cast<int>(test_cases_.size()), &test_case_indices_);

  // Shuffles the tests inside each test case.
  for (size_t i = 0; i < test_cases_.size(); i++) {
    test_cases_[i]->ShuffleTests(random());
  }
}

// Restores the test cases and tests to their order before the first shuffle.
void UnitTestImpl::UnshuffleTests() {
  for (size_t i = 0; i < test_cases_.size(); i++) {
    // Unshuffles the tests in each test case.
    test_cases_[i]->UnshuffleTests();
    // Resets the index of each test case.
    test_case_indices_[i] = static_cast<int>(i);
  }
}

// Returns the current OS stack trace as an std::string.
//
// The maximum number of stack frames to be included is specified by
// the gtest_stack_trace_depth flag.  The skip_count parameter
// specifies the number of top frames to be skipped, which doesn't
// count against the number of frames to be included.
//
// For example, if Foo() calls Bar(), which in turn calls
// GetCurrentOsStackTraceExceptTop(..., 1), Foo() will be included in
// the trace but Bar() and GetCurrentOsStackTraceExceptTop() won't.
std::string GetCurrentOsStackTraceExceptTop(UnitTest* /*unit_test*/,
                                            int skip_count) {
  // We pass skip_count + 1 to skip this wrapper function in addition
  // to what the user really wants to skip.
  return GetUnitTestImpl()->CurrentOsStackTraceExceptTop(skip_count + 1);
}

// Used by the GTEST_SUPPRESS_UNREACHABLE_CODE_WARNING_BELOW_ macro to
// suppress unreachable code warnings.
namespace {
class ClassUniqueToAlwaysTrue {};
}

bool IsTrue(bool condition) { return condition; }

bool AlwaysTrue() {
#if GTEST_HAS_EXCEPTIONS
  // This condition is always false so AlwaysTrue() never actually throws,
  // but it makes the compiler think that it may throw.
  if (IsTrue(false))
    throw ClassUniqueToAlwaysTrue();
#endif  // GTEST_HAS_EXCEPTIONS
  return true;
}

// If *pstr starts with the given prefix, modifies *pstr to be right
// past the prefix and returns true; otherwise leaves *pstr unchanged
// and returns false.  None of pstr, *pstr, and prefix can be NULL.
bool SkipPrefix(const char* prefix, const char** pstr) {
  const size_t prefix_len = strlen(prefix);
  if (strncmp(*pstr, prefix, prefix_len) == 0) {
    *pstr += prefix_len;
    return true;
  }
  return false;
}

// Parses a string as a command line flag.  The string should have
// the format "--flag=value".  When def_optional is true, the "=value"
// part can be omitted.
//
// Returns the value of the flag, or NULL if the parsing failed.
const char* ParseFlagValue(const char* str,
                           const char* flag,
                           bool def_optional) {
  // str and flag must not be NULL.
  if (str == NULL || flag == NULL) return NULL;

  // The flag must start with "--" followed by GTEST_FLAG_PREFIX_.
  const std::string flag_str = std::string("--") + GTEST_FLAG_PREFIX_ + flag;
  const size_t flag_len = flag_str.length();
  if (strncmp(str, flag_str.c_str(), flag_len) != 0) return NULL;

  // Skips the flag name.
  const char* flag_end = str + flag_len;

  // When def_optional is true, it's OK to not have a "=value" part.
  if (def_optional && (flag_end[0] == '\0')) {
    return flag_end;
  }

  // If def_optional is true and there are more characters after the
  // flag name, or if def_optional is false, there must be a '=' after
  // the flag name.
  if (flag_end[0] != '=') return NULL;

  // Returns the string after "=".
  return flag_end + 1;
}

// Parses a string for a bool flag, in the form of either
// "--flag=value" or "--flag".
//
// In the former case, the value is taken as true as long as it does
// not start with '0', 'f', or 'F'.
//
// In the latter case, the value is taken as true.
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
bool ParseBoolFlag(const char* str, const char* flag, bool* value) {
  // Gets the value of the flag as a string.
  const char* const value_str = ParseFlagValue(str, flag, true);

  // Aborts if the parsing failed.
  if (value_str == NULL) return false;

  // Converts the string value to a bool.
  *value = !(*value_str == '0' || *value_str == 'f' || *value_str == 'F');
  return true;
}

// Parses a string for an Int32 flag, in the form of
// "--flag=value".
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
bool ParseInt32Flag(const char* str, const char* flag, Int32* value) {
  // Gets the value of the flag as a string.
  const char* const value_str = ParseFlagValue(str, flag, false);

  // Aborts if the parsing failed.
  if (value_str == NULL) return false;

  // Sets *value to the value of the flag.
  return ParseInt32(Message() << "The value of flag --" << flag,
                    value_str, value);
}

// Parses a string for a string flag, in the form of
// "--flag=value".
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
bool ParseStringFlag(const char* str, const char* flag, std::string* value) {
  // Gets the value of the flag as a string.
  const char* const value_str = ParseFlagValue(str, flag, false);

  // Aborts if the parsing failed.
  if (value_str == NULL) return false;

  // Sets *value to the value of the flag.
  *value = value_str;
  return true;
}

// Determines whether a string has a prefix that Google Test uses for its
// flags, i.e., starts with GTEST_FLAG_PREFIX_ or GTEST_FLAG_PREFIX_DASH_.
// If Google Test detects that a command line flag has its prefix but is not
// recognized, it will print its help message. Flags starting with
// GTEST_INTERNAL_PREFIX_ followed by "internal_" are considered Google Test
// internal flags and do not trigger the help message.
static bool HasGoogleTestFlagPrefix(const char* str) {
  return (SkipPrefix("--", &str) ||
          SkipPrefix("-", &str) ||
          SkipPrefix("/", &str)) &&
         !SkipPrefix(GTEST_FLAG_PREFIX_ "internal_", &str) &&
         (SkipPrefix(GTEST_FLAG_PREFIX_, &str) ||
          SkipPrefix(GTEST_FLAG_PREFIX_DASH_, &str));
}

// Prints a string containing code-encoded text.  The following escape
// sequences can be used in the string to control the text color:
//
//   @@    prints a single '@' character.
//   @R    changes the color to red.
//   @G    changes the color to green.
//   @Y    changes the color to yellow.
//   @D    changes to the default terminal text color.
//
// TODO(wan@google.com): Write tests for this once we add stdout
// capturing to Google Test.
static void PrintColorEncoded(const char* str) {
  GTestColor color = COLOR_DEFAULT;  // The current color.

  // Conceptually, we split the string into segments divided by escape
  // sequences.  Then we print one segment at a time.  At the end of
  // each iteration, the str pointer advances to the beginning of the
  // next segment.
  for (;;) {
    const char* p = strchr(str, '@');
    if (p == NULL) {
      ColoredPrintf(color, "%s", str);
      return;
    }

    ColoredPrintf(color, "%s", std::string(str, p).c_str());

    const char ch = p[1];
    str = p + 2;
    if (ch == '@') {
      ColoredPrintf(color, "@");
    } else if (ch == 'D') {
      color = COLOR_DEFAULT;
    } else if (ch == 'R') {
      color = COLOR_RED;
    } else if (ch == 'G') {
      color = COLOR_GREEN;
    } else if (ch == 'Y') {
      color = COLOR_YELLOW;
    } else {
      --str;
    }
  }
}

static const char kColorEncodedHelpMessage[] =
"This program contains tests written using " GTEST_NAME_ ". You can use the\n"
"following command line flags to control its behavior:\n"
"\n"
"Test Selection:\n"
"  @G--" GTEST_FLAG_PREFIX_ "list_tests@D\n"
"      List the names of all tests instead of running them. The name of\n"
"      TEST(Foo, Bar) is \"Foo.Bar\".\n"
"  @G--" GTEST_FLAG_PREFIX_ "filter=@YPOSTIVE_PATTERNS"
    "[@G-@YNEGATIVE_PATTERNS]@D\n"
"      Run only the tests whose name matches one of the positive patterns but\n"
"      none of the negative patterns. '?' matches any single character; '*'\n"
"      matches any substring; ':' separates two patterns.\n"
"  @G--" GTEST_FLAG_PREFIX_ "also_run_disabled_tests@D\n"
"      Run all disabled tests too.\n"
"\n"
"Test Execution:\n"
"  @G--" GTEST_FLAG_PREFIX_ "repeat=@Y[COUNT]@D\n"
"      Run the tests repeatedly; use a negative count to repeat forever.\n"
"  @G--" GTEST_FLAG_PREFIX_ "shuffle@D\n"
"      Randomize tests' orders on every iteration.\n"
"  @G--" GTEST_FLAG_PREFIX_ "random_seed=@Y[NUMBER]@D\n"
"      Random number seed to use for shuffling test orders (between 1 and\n"
"      99999, or 0 to use a seed based on the current time).\n"
"\n"
"Test Output:\n"
"  @G--" GTEST_FLAG_PREFIX_ "color=@Y(@Gyes@Y|@Gno@Y|@Gauto@Y)@D\n"
"      Enable/disable colored output. The default is @Gauto@D.\n"
"  -@G-" GTEST_FLAG_PREFIX_ "print_time=0@D\n"
"      Don't print the elapsed time of each test.\n"
"  @G--" GTEST_FLAG_PREFIX_ "output=xml@Y[@G:@YDIRECTORY_PATH@G"
    GTEST_PATH_SEP_ "@Y|@G:@YFILE_PATH]@D\n"
"      Generate an XML report in the given directory or with the given file\n"
"      name. @YFILE_PATH@D defaults to @Gtest_details.xml@D.\n"
#if GTEST_CAN_STREAM_RESULTS_
"  @G--" GTEST_FLAG_PREFIX_ "stream_result_to=@YHOST@G:@YPORT@D\n"
"      Stream test results to the given server.\n"
#endif  // GTEST_CAN_STREAM_RESULTS_
"\n"
"Assertion Behavior:\n"
#if GTEST_HAS_DEATH_TEST && !GTEST_OS_WINDOWS
"  @G--" GTEST_FLAG_PREFIX_ "death_test_style=@Y(@Gfast@Y|@Gthreadsafe@Y)@D\n"
"      Set the default death test style.\n"
#endif  // GTEST_HAS_DEATH_TEST && !GTEST_OS_WINDOWS
"  @G--" GTEST_FLAG_PREFIX_ "break_on_failure@D\n"
"      Turn assertion failures into debugger break-points.\n"
"  @G--" GTEST_FLAG_PREFIX_ "throw_on_failure@D\n"
"      Turn assertion failures into C++ exceptions.\n"
"  @G--" GTEST_FLAG_PREFIX_ "catch_exceptions=0@D\n"
"      Do not report exceptions as test failures. Instead, allow them\n"
"      to crash the program or throw a pop-up (on Windows).\n"
"\n"
"Except for @G--" GTEST_FLAG_PREFIX_ "list_tests@D, you can alternatively set "
    "the corresponding\n"
"environment variable of a flag (all letters in upper-case). For example, to\n"
"disable colored text output, you can either specify @G--" GTEST_FLAG_PREFIX_
    "color=no@D or set\n"
"the @G" GTEST_FLAG_PREFIX_UPPER_ "COLOR@D environment variable to @Gno@D.\n"
"\n"
"For more information, please read the " GTEST_NAME_ " documentation at\n"
"@G" GTEST_PROJECT_URL_ "@D. If you find a bug in " GTEST_NAME_ "\n"
"(not one in your own code or tests), please report it to\n"
"@G<" GTEST_DEV_EMAIL_ ">@D.\n";

// Parses the command line for Google Test flags, without initializing
// other parts of Google Test.  The type parameter CharType can be
// instantiated to either char or wchar_t.
template <typename CharType>
void ParseGoogleTestFlagsOnlyImpl(int* argc, CharType** argv) {
  for (int i = 1; i < *argc; i++) {
    const std::string arg_string = StreamableToString(argv[i]);
    const char* const arg = arg_string.c_str();

    using internal::ParseBoolFlag;
    using internal::ParseInt32Flag;
    using internal::ParseStringFlag;

    // Do we see a Google Test flag?
    if (ParseBoolFlag(arg, kAlsoRunDisabledTestsFlag,
                      &GTEST_FLAG(also_run_disabled_tests)) ||
        ParseBoolFlag(arg, kBreakOnFailureFlag,
                      &GTEST_FLAG(break_on_failure)) ||
        ParseBoolFlag(arg, kCatchExceptionsFlag,
                      &GTEST_FLAG(catch_exceptions)) ||
        ParseStringFlag(arg, kColorFlag, &GTEST_FLAG(color)) ||
        ParseStringFlag(arg, kDeathTestStyleFlag,
                        &GTEST_FLAG(death_test_style)) ||
        ParseBoolFlag(arg, kDeathTestUseFork,
                      &GTEST_FLAG(death_test_use_fork)) ||
        ParseStringFlag(arg, kFilterFlag, &GTEST_FLAG(filter)) ||
        ParseStringFlag(arg, kInternalRunDeathTestFlag,
                        &GTEST_FLAG(internal_run_death_test)) ||
        ParseBoolFlag(arg, kListTestsFlag, &GTEST_FLAG(list_tests)) ||
        ParseStringFlag(arg, kOutputFlag, &GTEST_FLAG(output)) ||
        ParseBoolFlag(arg, kPrintTimeFlag, &GTEST_FLAG(print_time)) ||
        ParseInt32Flag(arg, kRandomSeedFlag, &GTEST_FLAG(random_seed)) ||
        ParseInt32Flag(arg, kRepeatFlag, &GTEST_FLAG(repeat)) ||
        ParseBoolFlag(arg, kShuffleFlag, &GTEST_FLAG(shuffle)) ||
        ParseInt32Flag(arg, kStackTraceDepthFlag,
                       &GTEST_FLAG(stack_trace_depth)) ||
        ParseStringFlag(arg, kStreamResultToFlag,
                        &GTEST_FLAG(stream_result_to)) ||
        ParseBoolFlag(arg, kThrowOnFailureFlag,
                      &GTEST_FLAG(throw_on_failure))
        ) {
      // Yes.  Shift the remainder of the argv list left by one.  Note
      // that argv has (*argc + 1) elements, the last one always being
      // NULL.  The following loop moves the trailing NULL element as
      // well.
      for (int j = i; j != *argc; j++) {
        argv[j] = argv[j + 1];
      }

      // Decrements the argument count.
      (*argc)--;

      // We also need to decrement the iterator as we just removed
      // an element.
      i--;
    } else if (arg_string == "--help" || arg_string == "-h" ||
               arg_string == "-?" || arg_string == "/?" ||
               HasGoogleTestFlagPrefix(arg)) {
      // Both help flag and unrecognized Google Test flags (excluding
      // internal ones) trigger help display.
      g_help_flag = true;
    }
  }

  if (g_help_flag) {
    // We print the help here instead of in RUN_ALL_TESTS(), as the
    // latter may not be called at all if the user is using Google
    // Test with another testing framework.
    PrintColorEncoded(kColorEncodedHelpMessage);
  }
}

// Parses the command line for Google Test flags, without initializing
// other parts of Google Test.
void ParseGoogleTestFlagsOnly(int* argc, char** argv) {
  ParseGoogleTestFlagsOnlyImpl(argc, argv);
}
void ParseGoogleTestFlagsOnly(int* argc, wchar_t** argv) {
  ParseGoogleTestFlagsOnlyImpl(argc, argv);
}

// The internal implementation of InitGoogleTest().
//
// The type parameter CharType can be instantiated to either char or
// wchar_t.
template <typename CharType>
void InitGoogleTestImpl(int* argc, CharType** argv) {
  g_init_gtest_count++;

  // We don't want to run the initialization code twice.
  if (g_init_gtest_count != 1) return;

  if (*argc <= 0) return;

  internal::g_executable_path = internal::StreamableToString(argv[0]);

#if GTEST_HAS_DEATH_TEST

  g_argvs.clear();
  for (int i = 0; i != *argc; i++) {
    g_argvs.push_back(StreamableToString(argv[i]));
  }

#endif  // GTEST_HAS_DEATH_TEST

  ParseGoogleTestFlagsOnly(argc, argv);
  GetUnitTestImpl()->PostFlagParsingInit();
}

}  // namespace internal

// Initializes Google Test.  This must be called before calling
// RUN_ALL_TESTS().  In particular, it parses a command line for the
// flags that Google Test recognizes.  Whenever a Google Test flag is
// seen, it is removed from argv, and *argc is decremented.
//
// No value is returned.  Instead, the Google Test flag variables are
// updated.
//
// Calling the function for the second time has no user-visible effect.
void InitGoogleTest(int* argc, char** argv) {
  internal::InitGoogleTestImpl(argc, argv);
}

// This overloaded version can be used in Windows programs compiled in
// UNICODE mode.
void InitGoogleTest(int* argc, wchar_t** argv) {
  internal::InitGoogleTestImpl(argc, argv);
}

}  // namespace testing
// Copyright 2005, Google Inc.
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
// Author: wan@google.com (Zhanyong Wan), vladl@google.com (Vlad Losev)
//
// This file implements death tests.


#if GTEST_HAS_DEATH_TEST

# if GTEST_OS_MAC
#  include <crt_externs.h>
# endif  // GTEST_OS_MAC

# include <errno.h>
# include <fcntl.h>
# include <limits.h>

# if GTEST_OS_LINUX
#  include <signal.h>
# endif  // GTEST_OS_LINUX

# include <stdarg.h>

# if GTEST_OS_WINDOWS
#  include <windows.h>
# else
#  include <sys/mman.h>
#  include <sys/wait.h>
# endif  // GTEST_OS_WINDOWS

# if GTEST_OS_QNX
#  include <spawn.h>
# endif  // GTEST_OS_QNX

#endif  // GTEST_HAS_DEATH_TEST


// Indicates that this translation unit is part of Google Test's
// implementation.  It must come before gtest-internal-inl.h is
// included, or there will be a compiler error.  This trick is to
// prevent a user from accidentally including gtest-internal-inl.h in
// his code.
#define GTEST_IMPLEMENTATION_ 1
#undef GTEST_IMPLEMENTATION_

namespace testing {

// Constants.

// The default death test style.
static const char kDefaultDeathTestStyle[] = "fast";

GTEST_DEFINE_string_(
    death_test_style,
    internal::StringFromGTestEnv("death_test_style", kDefaultDeathTestStyle),
    "Indicates how to run a death test in a forked child process: "
    "\"threadsafe\" (child process re-executes the test binary "
    "from the beginning, running only the specific death test) or "
    "\"fast\" (child process runs the death test immediately "
    "after forking).");

GTEST_DEFINE_bool_(
    death_test_use_fork,
    internal::BoolFromGTestEnv("death_test_use_fork", false),
    "Instructs to use fork()/_exit() instead of clone() in death tests. "
    "Ignored and always uses fork() on POSIX systems where clone() is not "
    "implemented. Useful when running under valgrind or similar tools if "
    "those do not support clone(). Valgrind 3.3.1 will just fail if "
    "it sees an unsupported combination of clone() flags. "
    "It is not recommended to use this flag w/o valgrind though it will "
    "work in 99% of the cases. Once valgrind is fixed, this flag will "
    "most likely be removed.");

namespace internal {
GTEST_DEFINE_string_(
    internal_run_death_test, "",
    "Indicates the file, line number, temporal index of "
    "the single death test to run, and a file descriptor to "
    "which a success code may be sent, all separated by "
    "the '|' characters.  This flag is specified if and only if the current "
    "process is a sub-process launched for running a thread-safe "
    "death test.  FOR INTERNAL USE ONLY.");
}  // namespace internal

#if GTEST_HAS_DEATH_TEST

namespace internal {

// Valid only for fast death tests. Indicates the code is running in the
// child process of a fast style death test.
static bool g_in_fast_death_test_child = false;

// Returns a Boolean value indicating whether the caller is currently
// executing in the context of the death test child process.  Tools such as
// Valgrind heap checkers may need this to modify their behavior in death
// tests.  IMPORTANT: This is an internal utility.  Using it may break the
// implementation of death tests.  User code MUST NOT use it.
bool InDeathTestChild() {
# if GTEST_OS_WINDOWS

  // On Windows, death tests are thread-safe regardless of the value of the
  // death_test_style flag.
  return !GTEST_FLAG(internal_run_death_test).empty();

# else

  if (GTEST_FLAG(death_test_style) == "threadsafe")
    return !GTEST_FLAG(internal_run_death_test).empty();
  else
    return g_in_fast_death_test_child;
#endif
}

}  // namespace internal

// ExitedWithCode constructor.
ExitedWithCode::ExitedWithCode(int exit_code) : exit_code_(exit_code) {
}

// ExitedWithCode function-call operator.
bool ExitedWithCode::operator()(int exit_status) const {
# if GTEST_OS_WINDOWS

  return exit_status == exit_code_;

# else

  return WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == exit_code_;

# endif  // GTEST_OS_WINDOWS
}

# if !GTEST_OS_WINDOWS
// KilledBySignal constructor.
KilledBySignal::KilledBySignal(int signum) : signum_(signum) {
}

// KilledBySignal function-call operator.
bool KilledBySignal::operator()(int exit_status) const {
  return WIFSIGNALED(exit_status) && WTERMSIG(exit_status) == signum_;
}
# endif  // !GTEST_OS_WINDOWS

namespace internal {

// Utilities needed for death tests.

// Generates a textual description of a given exit code, in the format
// specified by wait(2).
static std::string ExitSummary(int exit_code) {
  Message m;

# if GTEST_OS_WINDOWS

  m << "Exited with exit status " << exit_code;

# else

  if (WIFEXITED(exit_code)) {
    m << "Exited with exit status " << WEXITSTATUS(exit_code);
  } else if (WIFSIGNALED(exit_code)) {
    m << "Terminated by signal " << WTERMSIG(exit_code);
  }
#  ifdef WCOREDUMP
  if (WCOREDUMP(exit_code)) {
    m << " (core dumped)";
  }
#  endif
# endif  // GTEST_OS_WINDOWS

  return m.GetString();
}

// Returns true if exit_status describes a process that was terminated
// by a signal, or exited normally with a nonzero exit code.
bool ExitedUnsuccessfully(int exit_status) {
  return !ExitedWithCode(0)(exit_status);
}

# if !GTEST_OS_WINDOWS
// Generates a textual failure message when a death test finds more than
// one thread running, or cannot determine the number of threads, prior
// to executing the given statement.  It is the responsibility of the
// caller not to pass a thread_count of 1.
static std::string DeathTestThreadWarning(size_t thread_count) {
  Message msg;
  msg << "Death tests use fork(), which is unsafe particularly"
      << " in a threaded context. For this test, " << GTEST_NAME_ << " ";
  if (thread_count == 0)
    msg << "couldn't detect the number of threads.";
  else
    msg << "detected " << thread_count << " threads.";
  return msg.GetString();
}
# endif  // !GTEST_OS_WINDOWS

// Flag characters for reporting a death test that did not die.
static const char kDeathTestLived = 'L';
static const char kDeathTestReturned = 'R';
static const char kDeathTestThrew = 'T';
static const char kDeathTestInternalError = 'I';

// An enumeration describing all of the possible ways that a death test can
// conclude.  DIED means that the process died while executing the test
// code; LIVED means that process lived beyond the end of the test code;
// RETURNED means that the test statement attempted to execute a return
// statement, which is not allowed; THREW means that the test statement
// returned control by throwing an exception.  IN_PROGRESS means the test
// has not yet concluded.
// TODO(vladl@google.com): Unify names and possibly values for
// AbortReason, DeathTestOutcome, and flag characters above.
enum DeathTestOutcome { IN_PROGRESS, DIED, LIVED, RETURNED, THREW };

// Routine for aborting the program which is safe to call from an
// exec-style death test child process, in which case the error
// message is propagated back to the parent process.  Otherwise, the
// message is simply printed to stderr.  In either case, the program
// then exits with status 1.
void DeathTestAbort(const std::string& message) {
  // On a POSIX system, this function may be called from a threadsafe-style
  // death test child process, which operates on a very small stack.  Use
  // the heap for any additional non-minuscule memory requirements.
  const InternalRunDeathTestFlag* const flag =
      GetUnitTestImpl()->internal_run_death_test_flag();
  if (flag != NULL) {
    FILE* parent = posix::FDOpen(flag->write_fd(), "w");
    fputc(kDeathTestInternalError, parent);
    fprintf(parent, "%s", message.c_str());
    fflush(parent);
    _exit(1);
  } else {
    fprintf(stderr, "%s", message.c_str());
    fflush(stderr);
    posix::Abort();
  }
}

// A replacement for CHECK that calls DeathTestAbort if the assertion
// fails.
# define GTEST_DEATH_TEST_CHECK_(expression) \
  do { \
    if (!::testing::internal::IsTrue(expression)) { \
      DeathTestAbort( \
          ::std::string("CHECK failed: File ") + __FILE__ +  ", line " \
          + ::testing::internal::StreamableToString(__LINE__) + ": " \
          + #expression); \
    } \
  } while (::testing::internal::AlwaysFalse())

// This macro is similar to GTEST_DEATH_TEST_CHECK_, but it is meant for
// evaluating any system call that fulfills two conditions: it must return
// -1 on failure, and set errno to EINTR when it is interrupted and
// should be tried again.  The macro expands to a loop that repeatedly
// evaluates the expression as long as it evaluates to -1 and sets
// errno to EINTR.  If the expression evaluates to -1 but errno is
// something other than EINTR, DeathTestAbort is called.
# define GTEST_DEATH_TEST_CHECK_SYSCALL_(expression) \
  do { \
    int gtest_retval; \
    do { \
      gtest_retval = (expression); \
    } while (gtest_retval == -1 && errno == EINTR); \
    if (gtest_retval == -1) { \
      DeathTestAbort( \
          ::std::string("CHECK failed: File ") + __FILE__ + ", line " \
          + ::testing::internal::StreamableToString(__LINE__) + ": " \
          + #expression + " != -1"); \
    } \
  } while (::testing::internal::AlwaysFalse())

// Returns the message describing the last system error in errno.
std::string GetLastErrnoDescription() {
    return errno == 0 ? "" : posix::StrError(errno);
}

// This is called from a death test parent process to read a failure
// message from the death test child process and log it with the FATAL
// severity. On Windows, the message is read from a pipe handle. On other
// platforms, it is read from a file descriptor.
static void FailFromInternalError(int fd) {
  Message error;
  char buffer[256];
  int num_read;

  do {
    while ((num_read = posix::Read(fd, buffer, 255)) > 0) {
      buffer[num_read] = '\0';
      error << buffer;
    }
  } while (num_read == -1 && errno == EINTR);

  if (num_read == 0) {
    GTEST_LOG_(FATAL) << error.GetString();
  } else {
    const int last_error = errno;
    GTEST_LOG_(FATAL) << "Error while reading death test internal: "
                      << GetLastErrnoDescription() << " [" << last_error << "]";
  }
}

// Death test constructor.  Increments the running death test count
// for the current test.
DeathTest::DeathTest() {
  TestInfo* const info = GetUnitTestImpl()->current_test_info();
  if (info == NULL) {
    DeathTestAbort("Cannot run a death test outside of a TEST or "
                   "TEST_F construct");
  }
}

// Creates and returns a death test by dispatching to the current
// death test factory.
bool DeathTest::Create(const char* statement, const RE* regex,
                       const char* file, int line, DeathTest** test) {
  return GetUnitTestImpl()->death_test_factory()->Create(
      statement, regex, file, line, test);
}

const char* DeathTest::LastMessage() {
  return last_death_test_message_.c_str();
}

void DeathTest::set_last_death_test_message(const std::string& message) {
  last_death_test_message_ = message;
}

std::string DeathTest::last_death_test_message_;

// Provides cross platform implementation for some death functionality.
class DeathTestImpl : public DeathTest {
 protected:
  DeathTestImpl(const char* a_statement, const RE* a_regex)
      : statement_(a_statement),
        regex_(a_regex),
        spawned_(false),
        status_(-1),
        outcome_(IN_PROGRESS),
        read_fd_(-1),
        write_fd_(-1) {}

  // read_fd_ is expected to be closed and cleared by a derived class.
  ~DeathTestImpl() { GTEST_DEATH_TEST_CHECK_(read_fd_ == -1); }

  void Abort(AbortReason reason);
  virtual bool Passed(bool status_ok);

  const char* statement() const { return statement_; }
  const RE* regex() const { return regex_; }
  bool spawned() const { return spawned_; }
  void set_spawned(bool is_spawned) { spawned_ = is_spawned; }
  int status() const { return status_; }
  void set_status(int a_status) { status_ = a_status; }
  DeathTestOutcome outcome() const { return outcome_; }
  void set_outcome(DeathTestOutcome an_outcome) { outcome_ = an_outcome; }
  int read_fd() const { return read_fd_; }
  void set_read_fd(int fd) { read_fd_ = fd; }
  int write_fd() const { return write_fd_; }
  void set_write_fd(int fd) { write_fd_ = fd; }

  // Called in the parent process only. Reads the result code of the death
  // test child process via a pipe, interprets it to set the outcome_
  // member, and closes read_fd_.  Outputs diagnostics and terminates in
  // case of unexpected codes.
  void ReadAndInterpretStatusByte();

 private:
  // The textual content of the code this object is testing.  This class
  // doesn't own this string and should not attempt to delete it.
  const char* const statement_;
  // The regular expression which test output must match.  DeathTestImpl
  // doesn't own this object and should not attempt to delete it.
  const RE* const regex_;
  // True if the death test child process has been successfully spawned.
  bool spawned_;
  // The exit status of the child process.
  int status_;
  // How the death test concluded.
  DeathTestOutcome outcome_;
  // Descriptor to the read end of the pipe to the child process.  It is
  // always -1 in the child process.  The child keeps its write end of the
  // pipe in write_fd_.
  int read_fd_;
  // Descriptor to the child's write end of the pipe to the parent process.
  // It is always -1 in the parent process.  The parent keeps its end of the
  // pipe in read_fd_.
  int write_fd_;
};

// Called in the parent process only. Reads the result code of the death
// test child process via a pipe, interprets it to set the outcome_
// member, and closes read_fd_.  Outputs diagnostics and terminates in
// case of unexpected codes.
void DeathTestImpl::ReadAndInterpretStatusByte() {
  char flag;
  int bytes_read;

  // The read() here blocks until data is available (signifying the
  // failure of the death test) or until the pipe is closed (signifying
  // its success), so it's okay to call this in the parent before
  // the child process has exited.
  do {
    bytes_read = posix::Read(read_fd(), &flag, 1);
  } while (bytes_read == -1 && errno == EINTR);

  if (bytes_read == 0) {
    set_outcome(DIED);
  } else if (bytes_read == 1) {
    switch (flag) {
      case kDeathTestReturned:
        set_outcome(RETURNED);
        break;
      case kDeathTestThrew:
        set_outcome(THREW);
        break;
      case kDeathTestLived:
        set_outcome(LIVED);
        break;
      case kDeathTestInternalError:
        FailFromInternalError(read_fd());  // Does not return.
        break;
      default:
        GTEST_LOG_(FATAL) << "Death test child process reported "
                          << "unexpected status byte ("
                          << static_cast<unsigned int>(flag) << ")";
    }
  } else {
    GTEST_LOG_(FATAL) << "Read from death test child process failed: "
                      << GetLastErrnoDescription();
  }
  GTEST_DEATH_TEST_CHECK_SYSCALL_(posix::Close(read_fd()));
  set_read_fd(-1);
}

// Signals that the death test code which should have exited, didn't.
// Should be called only in a death test child process.
// Writes a status byte to the child's status file descriptor, then
// calls _exit(1).
void DeathTestImpl::Abort(AbortReason reason) {
  // The parent process considers the death test to be a failure if
  // it finds any data in our pipe.  So, here we write a single flag byte
  // to the pipe, then exit.
  const char status_ch =
      reason == TEST_DID_NOT_DIE ? kDeathTestLived :
      reason == TEST_THREW_EXCEPTION ? kDeathTestThrew : kDeathTestReturned;

  GTEST_DEATH_TEST_CHECK_SYSCALL_(posix::Write(write_fd(), &status_ch, 1));
  // We are leaking the descriptor here because on some platforms (i.e.,
  // when built as Windows DLL), destructors of global objects will still
  // run after calling _exit(). On such systems, write_fd_ will be
  // indirectly closed from the destructor of UnitTestImpl, causing double
  // close if it is also closed here. On debug configurations, double close
  // may assert. As there are no in-process buffers to flush here, we are
  // relying on the OS to close the descriptor after the process terminates
  // when the destructors are not run.
  _exit(1);  // Exits w/o any normal exit hooks (we were supposed to crash)
}

// Returns an indented copy of stderr output for a death test.
// This makes distinguishing death test output lines from regular log lines
// much easier.
static ::std::string FormatDeathTestOutput(const ::std::string& output) {
  ::std::string ret;
  for (size_t at = 0; ; ) {
    const size_t line_end = output.find('\n', at);
    ret += "[  DEATH   ] ";
    if (line_end == ::std::string::npos) {
      ret += output.substr(at);
      break;
    }
    ret += output.substr(at, line_end + 1 - at);
    at = line_end + 1;
  }
  return ret;
}

// Assesses the success or failure of a death test, using both private
// members which have previously been set, and one argument:
//
// Private data members:
//   outcome:  An enumeration describing how the death test
//             concluded: DIED, LIVED, THREW, or RETURNED.  The death test
//             fails in the latter three cases.
//   status:   The exit status of the child process. On *nix, it is in the
//             in the format specified by wait(2). On Windows, this is the
//             value supplied to the ExitProcess() API or a numeric code
//             of the exception that terminated the program.
//   regex:    A regular expression object to be applied to
//             the test's captured standard error output; the death test
//             fails if it does not match.
//
// Argument:
//   status_ok: true if exit_status is acceptable in the context of
//              this particular death test, which fails if it is false
//
// Returns true iff all of the above conditions are met.  Otherwise, the
// first failing condition, in the order given above, is the one that is
// reported. Also sets the last death test message string.
bool DeathTestImpl::Passed(bool status_ok) {
  if (!spawned())
    return false;

  const std::string error_message = GetCapturedStderr();

  bool success = false;
  Message buffer;

  buffer << "Death test: " << statement() << "\n";
  switch (outcome()) {
    case LIVED:
      buffer << "    Result: failed to die.\n"
             << " Error msg:\n" << FormatDeathTestOutput(error_message);
      break;
    case THREW:
      buffer << "    Result: threw an exception.\n"
             << " Error msg:\n" << FormatDeathTestOutput(error_message);
      break;
    case RETURNED:
      buffer << "    Result: illegal return in test statement.\n"
             << " Error msg:\n" << FormatDeathTestOutput(error_message);
      break;
    case DIED:
      if (status_ok) {
        const bool matched = RE::PartialMatch(error_message.c_str(), *regex());
        if (matched) {
          success = true;
        } else {
          buffer << "    Result: died but not with expected error.\n"
                 << "  Expected: " << regex()->pattern() << "\n"
                 << "Actual msg:\n" << FormatDeathTestOutput(error_message);
        }
      } else {
        buffer << "    Result: died but not with expected exit code:\n"
               << "            " << ExitSummary(status()) << "\n"
               << "Actual msg:\n" << FormatDeathTestOutput(error_message);
      }
      break;
    case IN_PROGRESS:
    default:
      GTEST_LOG_(FATAL)
          << "DeathTest::Passed somehow called before conclusion of test";
  }

  DeathTest::set_last_death_test_message(buffer.GetString());
  return success;
}

# if GTEST_OS_WINDOWS
// WindowsDeathTest implements death tests on Windows. Due to the
// specifics of starting new processes on Windows, death tests there are
// always threadsafe, and Google Test considers the
// --gtest_death_test_style=fast setting to be equivalent to
// --gtest_death_test_style=threadsafe there.
//
// A few implementation notes:  Like the Linux version, the Windows
// implementation uses pipes for child-to-parent communication. But due to
// the specifics of pipes on Windows, some extra steps are required:
//
// 1. The parent creates a communication pipe and stores handles to both
//    ends of it.
// 2. The parent starts the child and provides it with the information
//    necessary to acquire the handle to the write end of the pipe.
// 3. The child acquires the write end of the pipe and signals the parent
//    using a Windows event.
// 4. Now the parent can release the write end of the pipe on its side. If
//    this is done before step 3, the object's reference count goes down to
//    0 and it is destroyed, preventing the child from acquiring it. The
//    parent now has to release it, or read operations on the read end of
//    the pipe will not return when the child terminates.
// 5. The parent reads child's output through the pipe (outcome code and
//    any possible error messages) from the pipe, and its stderr and then
//    determines whether to fail the test.
//
// Note: to distinguish Win32 API calls from the local method and function
// calls, the former are explicitly resolved in the global namespace.
//
class WindowsDeathTest : public DeathTestImpl {
 public:
  WindowsDeathTest(const char* a_statement,
                   const RE* a_regex,
                   const char* file,
                   int line)
      : DeathTestImpl(a_statement, a_regex), file_(file), line_(line) {}

  // All of these virtual functions are inherited from DeathTest.
  virtual int Wait();
  virtual TestRole AssumeRole();

 private:
  // The name of the file in which the death test is located.
  const char* const file_;
  // The line number on which the death test is located.
  const int line_;
  // Handle to the write end of the pipe to the child process.
  AutoHandle write_handle_;
  // Child process handle.
  AutoHandle child_handle_;
  // Event the child process uses to signal the parent that it has
  // acquired the handle to the write end of the pipe. After seeing this
  // event the parent can release its own handles to make sure its
  // ReadFile() calls return when the child terminates.
  AutoHandle event_handle_;
};

// Waits for the child in a death test to exit, returning its exit
// status, or 0 if no child process exists.  As a side effect, sets the
// outcome data member.
int WindowsDeathTest::Wait() {
  if (!spawned())
    return 0;

  // Wait until the child either signals that it has acquired the write end
  // of the pipe or it dies.
  const HANDLE wait_handles[2] = { child_handle_.Get(), event_handle_.Get() };
  switch (::WaitForMultipleObjects(2,
                                   wait_handles,
                                   FALSE,  // Waits for any of the handles.
                                   INFINITE)) {
    case WAIT_OBJECT_0:
    case WAIT_OBJECT_0 + 1:
      break;
    default:
      GTEST_DEATH_TEST_CHECK_(false);  // Should not get here.
  }

  // The child has acquired the write end of the pipe or exited.
  // We release the handle on our side and continue.
  write_handle_.Reset();
  event_handle_.Reset();

  ReadAndInterpretStatusByte();

  // Waits for the child process to exit if it haven't already. This
  // returns immediately if the child has already exited, regardless of
  // whether previous calls to WaitForMultipleObjects synchronized on this
  // handle or not.
  GTEST_DEATH_TEST_CHECK_(
      WAIT_OBJECT_0 == ::WaitForSingleObject(child_handle_.Get(),
                                             INFINITE));
  DWORD status_code;
  GTEST_DEATH_TEST_CHECK_(
      ::GetExitCodeProcess(child_handle_.Get(), &status_code) != FALSE);
  child_handle_.Reset();
  set_status(static_cast<int>(status_code));
  return status();
}

// The AssumeRole process for a Windows death test.  It creates a child
// process with the same executable as the current process to run the
// death test.  The child process is given the --gtest_filter and
// --gtest_internal_run_death_test flags such that it knows to run the
// current death test only.
DeathTest::TestRole WindowsDeathTest::AssumeRole() {
  const UnitTestImpl* const impl = GetUnitTestImpl();
  const InternalRunDeathTestFlag* const flag =
      impl->internal_run_death_test_flag();
  const TestInfo* const info = impl->current_test_info();
  const int death_test_index = info->result()->death_test_count();

  if (flag != NULL) {
    // ParseInternalRunDeathTestFlag() has performed all the necessary
    // processing.
    set_write_fd(flag->write_fd());
    return EXECUTE_TEST;
  }

  // WindowsDeathTest uses an anonymous pipe to communicate results of
  // a death test.
  SECURITY_ATTRIBUTES handles_are_inheritable = {
    sizeof(SECURITY_ATTRIBUTES), NULL, TRUE };
  HANDLE read_handle, write_handle;
  GTEST_DEATH_TEST_CHECK_(
      ::CreatePipe(&read_handle, &write_handle, &handles_are_inheritable,
                   0)  // Default buffer size.
      != FALSE);
  set_read_fd(::_open_osfhandle(reinterpret_cast<intptr_t>(read_handle),
                                O_RDONLY));
  write_handle_.Reset(write_handle);
  event_handle_.Reset(::CreateEvent(
      &handles_are_inheritable,
      TRUE,    // The event will automatically reset to non-signaled state.
      FALSE,   // The initial state is non-signalled.
      NULL));  // The even is unnamed.
  GTEST_DEATH_TEST_CHECK_(event_handle_.Get() != NULL);
  const std::string filter_flag =
      std::string("--") + GTEST_FLAG_PREFIX_ + kFilterFlag + "=" +
      info->test_case_name() + "." + info->name();
  const std::string internal_flag =
      std::string("--") + GTEST_FLAG_PREFIX_ + kInternalRunDeathTestFlag +
      "=" + file_ + "|" + StreamableToString(line_) + "|" +
      StreamableToString(death_test_index) + "|" +
      StreamableToString(static_cast<unsigned int>(::GetCurrentProcessId())) +
      // size_t has the same width as pointers on both 32-bit and 64-bit
      // Windows platforms.
      // See http://msdn.microsoft.com/en-us/library/tcxf1dw6.aspx.
      "|" + StreamableToString(reinterpret_cast<size_t>(write_handle)) +
      "|" + StreamableToString(reinterpret_cast<size_t>(event_handle_.Get()));

  char executable_path[_MAX_PATH + 1];  // NOLINT
  GTEST_DEATH_TEST_CHECK_(
      _MAX_PATH + 1 != ::GetModuleFileNameA(NULL,
                                            executable_path,
                                            _MAX_PATH));

  std::string command_line =
      std::string(::GetCommandLineA()) + " " + filter_flag + " \"" +
      internal_flag + "\"";

  DeathTest::set_last_death_test_message("");

  CaptureStderr();
  // Flush the log buffers since the log streams are shared with the child.
  FlushInfoLog();

  // The child process will share the standard handles with the parent.
  STARTUPINFOA startup_info;
  memset(&startup_info, 0, sizeof(STARTUPINFO));
  startup_info.dwFlags = STARTF_USESTDHANDLES;
  startup_info.hStdInput = ::GetStdHandle(STD_INPUT_HANDLE);
  startup_info.hStdOutput = ::GetStdHandle(STD_OUTPUT_HANDLE);
  startup_info.hStdError = ::GetStdHandle(STD_ERROR_HANDLE);

  PROCESS_INFORMATION process_info;
  GTEST_DEATH_TEST_CHECK_(::CreateProcessA(
      executable_path,
      const_cast<char*>(command_line.c_str()),
      NULL,   // Retuned process handle is not inheritable.
      NULL,   // Retuned thread handle is not inheritable.
      TRUE,   // Child inherits all inheritable handles (for write_handle_).
      0x0,    // Default creation flags.
      NULL,   // Inherit the parent's environment.
      UnitTest::GetInstance()->original_working_dir(),
      &startup_info,
      &process_info) != FALSE);
  child_handle_.Reset(process_info.hProcess);
  ::CloseHandle(process_info.hThread);
  set_spawned(true);
  return OVERSEE_TEST;
}
# else  // We are not on Windows.

// ForkingDeathTest provides implementations for most of the abstract
// methods of the DeathTest interface.  Only the AssumeRole method is
// left undefined.
class ForkingDeathTest : public DeathTestImpl {
 public:
  ForkingDeathTest(const char* statement, const RE* regex);

  // All of these virtual functions are inherited from DeathTest.
  virtual int Wait();

 protected:
  void set_child_pid(pid_t child_pid) { child_pid_ = child_pid; }

 private:
  // PID of child process during death test; 0 in the child process itself.
  pid_t child_pid_;
};

// Constructs a ForkingDeathTest.
ForkingDeathTest::ForkingDeathTest(const char* a_statement, const RE* a_regex)
    : DeathTestImpl(a_statement, a_regex),
      child_pid_(-1) {}

// Waits for the child in a death test to exit, returning its exit
// status, or 0 if no child process exists.  As a side effect, sets the
// outcome data member.
int ForkingDeathTest::Wait() {
  if (!spawned())
    return 0;

  ReadAndInterpretStatusByte();

  int status_value;
  GTEST_DEATH_TEST_CHECK_SYSCALL_(waitpid(child_pid_, &status_value, 0));
  set_status(status_value);
  return status_value;
}

// A concrete death test class that forks, then immediately runs the test
// in the child process.
class NoExecDeathTest : public ForkingDeathTest {
 public:
  NoExecDeathTest(const char* a_statement, const RE* a_regex) :
      ForkingDeathTest(a_statement, a_regex) { }
  virtual TestRole AssumeRole();
};

// The AssumeRole process for a fork-and-run death test.  It implements a
// straightforward fork, with a simple pipe to transmit the status byte.
DeathTest::TestRole NoExecDeathTest::AssumeRole() {
  const size_t thread_count = GetThreadCount();
  if (thread_count != 1) {
    GTEST_LOG_(WARNING) << DeathTestThreadWarning(thread_count);
  }

  int pipe_fd[2];
  GTEST_DEATH_TEST_CHECK_(pipe(pipe_fd) != -1);

  DeathTest::set_last_death_test_message("");
  CaptureStderr();
  // When we fork the process below, the log file buffers are copied, but the
  // file descriptors are shared.  We flush all log files here so that closing
  // the file descriptors in the child process doesn't throw off the
  // synchronization between descriptors and buffers in the parent process.
  // This is as close to the fork as possible to avoid a race condition in case
  // there are multiple threads running before the death test, and another
  // thread writes to the log file.
  FlushInfoLog();

  const pid_t child_pid = fork();
  GTEST_DEATH_TEST_CHECK_(child_pid != -1);
  set_child_pid(child_pid);
  if (child_pid == 0) {
    GTEST_DEATH_TEST_CHECK_SYSCALL_(close(pipe_fd[0]));
    set_write_fd(pipe_fd[1]);
    // Redirects all logging to stderr in the child process to prevent
    // concurrent writes to the log files.  We capture stderr in the parent
    // process and append the child process' output to a log.
    LogToStderr();
    // Event forwarding to the listeners of event listener API mush be shut
    // down in death test subprocesses.
    GetUnitTestImpl()->listeners()->SuppressEventForwarding();
    g_in_fast_death_test_child = true;
    return EXECUTE_TEST;
  } else {
    GTEST_DEATH_TEST_CHECK_SYSCALL_(close(pipe_fd[1]));
    set_read_fd(pipe_fd[0]);
    set_spawned(true);
    return OVERSEE_TEST;
  }
}

// A concrete death test class that forks and re-executes the main
// program from the beginning, with command-line flags set that cause
// only this specific death test to be run.
class ExecDeathTest : public ForkingDeathTest {
 public:
  ExecDeathTest(const char* a_statement, const RE* a_regex,
                const char* file, int line) :
      ForkingDeathTest(a_statement, a_regex), file_(file), line_(line) { }
  virtual TestRole AssumeRole();
 private:
  static ::std::vector<testing::internal::string>
  GetArgvsForDeathTestChildProcess() {
    ::std::vector<testing::internal::string> args = GetInjectableArgvs();
    return args;
  }
  // The name of the file in which the death test is located.
  const char* const file_;
  // The line number on which the death test is located.
  const int line_;
};

// Utility class for accumulating command-line arguments.
class Arguments {
 public:
  Arguments() {
    args_.push_back(NULL);
  }

  ~Arguments() {
    for (std::vector<char*>::iterator i = args_.begin(); i != args_.end();
         ++i) {
      free(*i);
    }
  }
  void AddArgument(const char* argument) {
    args_.insert(args_.end() - 1, posix::StrDup(argument));
  }

  template <typename Str>
  void AddArguments(const ::std::vector<Str>& arguments) {
    for (typename ::std::vector<Str>::const_iterator i = arguments.begin();
         i != arguments.end();
         ++i) {
      args_.insert(args_.end() - 1, posix::StrDup(i->c_str()));
    }
  }
  char* const* Argv() {
    return &args_[0];
  }

 private:
  std::vector<char*> args_;
};

// A struct that encompasses the arguments to the child process of a
// threadsafe-style death test process.
struct ExecDeathTestArgs {
  char* const* argv;  // Command-line arguments for the child's call to exec
  int close_fd;       // File descriptor to close; the read end of a pipe
};

#  if GTEST_OS_MAC
inline char** GetEnviron() {
  // When Google Test is built as a framework on MacOS X, the environ variable
  // is unavailable. Apple's documentation (man environ) recommends using
  // _NSGetEnviron() instead.
  return *_NSGetEnviron();
}
#  else
// Some POSIX platforms expect you to declare environ. extern "C" makes
// it reside in the global namespace.
extern "C" char** environ;
inline char** GetEnviron() { return environ; }
#  endif  // GTEST_OS_MAC

#  if !GTEST_OS_QNX
// The main function for a threadsafe-style death test child process.
// This function is called in a clone()-ed process and thus must avoid
// any potentially unsafe operations like malloc or libc functions.
static int ExecDeathTestChildMain(void* child_arg) {
  ExecDeathTestArgs* const args = static_cast<ExecDeathTestArgs*>(child_arg);
  GTEST_DEATH_TEST_CHECK_SYSCALL_(close(args->close_fd));

  // We need to execute the test program in the same environment where
  // it was originally invoked.  Therefore we change to the original
  // working directory first.
  const char* const original_dir =
      UnitTest::GetInstance()->original_working_dir();
  // We can safely call chdir() as it's a direct system call.
  if (chdir(original_dir) != 0) {
    DeathTestAbort(std::string("chdir(\"") + original_dir + "\") failed: " +
                   GetLastErrnoDescription());
    return EXIT_FAILURE;
  }

  // We can safely call execve() as it's a direct system call.  We
  // cannot use execvp() as it's a libc function and thus potentially
  // unsafe.  Since execve() doesn't search the PATH, the user must
  // invoke the test program via a valid path that contains at least
  // one path separator.
  execve(args->argv[0], args->argv, GetEnviron());
  DeathTestAbort(std::string("execve(") + args->argv[0] + ", ...) in " +
                 original_dir + " failed: " +
                 GetLastErrnoDescription());
  return EXIT_FAILURE;
}
#  endif  // !GTEST_OS_QNX

// Two utility routines that together determine the direction the stack
// grows.
// This could be accomplished more elegantly by a single recursive
// function, but we want to guard against the unlikely possibility of
// a smart compiler optimizing the recursion away.
//
// GTEST_NO_INLINE_ is required to prevent GCC 4.6 from inlining
// StackLowerThanAddress into StackGrowsDown, which then doesn't give
// correct answer.
void StackLowerThanAddress(const void* ptr, bool* result) GTEST_NO_INLINE_;
void StackLowerThanAddress(const void* ptr, bool* result) {
  int dummy;
  *result = (&dummy < ptr);
}

bool StackGrowsDown() {
  int dummy;
  bool result;
  StackLowerThanAddress(&dummy, &result);
  return result;
}

// Spawns a child process with the same executable as the current process in
// a thread-safe manner and instructs it to run the death test.  The
// implementation uses fork(2) + exec.  On systems where clone(2) is
// available, it is used instead, being slightly more thread-safe.  On QNX,
// fork supports only single-threaded environments, so this function uses
// spawn(2) there instead.  The function dies with an error message if
// anything goes wrong.
static pid_t ExecDeathTestSpawnChild(char* const* argv, int close_fd) {
  ExecDeathTestArgs args = { argv, close_fd };
  pid_t child_pid = -1;

#  if GTEST_OS_QNX
  // Obtains the current directory and sets it to be closed in the child
  // process.
  const int cwd_fd = open(".", O_RDONLY);
  GTEST_DEATH_TEST_CHECK_(cwd_fd != -1);
  GTEST_DEATH_TEST_CHECK_SYSCALL_(fcntl(cwd_fd, F_SETFD, FD_CLOEXEC));
  // We need to execute the test program in the same environment where
  // it was originally invoked.  Therefore we change to the original
  // working directory first.
  const char* const original_dir =
      UnitTest::GetInstance()->original_working_dir();
  // We can safely call chdir() as it's a direct system call.
  if (chdir(original_dir) != 0) {
    DeathTestAbort(std::string("chdir(\"") + original_dir + "\") failed: " +
                   GetLastErrnoDescription());
    return EXIT_FAILURE;
  }

  int fd_flags;
  // Set close_fd to be closed after spawn.
  GTEST_DEATH_TEST_CHECK_SYSCALL_(fd_flags = fcntl(close_fd, F_GETFD));
  GTEST_DEATH_TEST_CHECK_SYSCALL_(fcntl(close_fd, F_SETFD,
                                        fd_flags | FD_CLOEXEC));
  struct inheritance inherit = {0};
  // spawn is a system call.
  child_pid = spawn(args.argv[0], 0, NULL, &inherit, args.argv, GetEnviron());
  // Restores the current working directory.
  GTEST_DEATH_TEST_CHECK_(fchdir(cwd_fd) != -1);
  GTEST_DEATH_TEST_CHECK_SYSCALL_(close(cwd_fd));

#  else   // GTEST_OS_QNX
#   if GTEST_OS_LINUX
  // When a SIGPROF signal is received while fork() or clone() are executing,
  // the process may hang. To avoid this, we ignore SIGPROF here and re-enable
  // it after the call to fork()/clone() is complete.
  struct sigaction saved_sigprof_action;
  struct sigaction ignore_sigprof_action;
  memset(&ignore_sigprof_action, 0, sizeof(ignore_sigprof_action));
  sigemptyset(&ignore_sigprof_action.sa_mask);
  ignore_sigprof_action.sa_handler = SIG_IGN;
  GTEST_DEATH_TEST_CHECK_SYSCALL_(sigaction(
      SIGPROF, &ignore_sigprof_action, &saved_sigprof_action));
#   endif  // GTEST_OS_LINUX

#   if GTEST_HAS_CLONE
  const bool use_fork = GTEST_FLAG(death_test_use_fork);

  if (!use_fork) {
    static const bool stack_grows_down = StackGrowsDown();
    const size_t stack_size = getpagesize();
    // MMAP_ANONYMOUS is not defined on Mac, so we use MAP_ANON instead.
    void* const stack = mmap(NULL, stack_size, PROT_READ | PROT_WRITE,
                             MAP_ANON | MAP_PRIVATE, -1, 0);
    GTEST_DEATH_TEST_CHECK_(stack != MAP_FAILED);

    // Maximum stack alignment in bytes:  For a downward-growing stack, this
    // amount is subtracted from size of the stack space to get an address
    // that is within the stack space and is aligned on all systems we care
    // about.  As far as I know there is no ABI with stack alignment greater
    // than 64.  We assume stack and stack_size already have alignment of
    // kMaxStackAlignment.
    const size_t kMaxStackAlignment = 64;
    void* const stack_top =
        static_cast<char*>(stack) +
            (stack_grows_down ? stack_size - kMaxStackAlignment : 0);
    GTEST_DEATH_TEST_CHECK_(stack_size > kMaxStackAlignment &&
        reinterpret_cast<intptr_t>(stack_top) % kMaxStackAlignment == 0);

    child_pid = clone(&ExecDeathTestChildMain, stack_top, SIGCHLD, &args);

    GTEST_DEATH_TEST_CHECK_(munmap(stack, stack_size) != -1);
  }
#   else
  const bool use_fork = true;
#   endif  // GTEST_HAS_CLONE

  if (use_fork && (child_pid = fork()) == 0) {
      ExecDeathTestChildMain(&args);
      _exit(0);
  }
#  endif  // GTEST_OS_QNX
#  if GTEST_OS_LINUX
  GTEST_DEATH_TEST_CHECK_SYSCALL_(
      sigaction(SIGPROF, &saved_sigprof_action, NULL));
#  endif  // GTEST_OS_LINUX

  GTEST_DEATH_TEST_CHECK_(child_pid != -1);
  return child_pid;
}

// The AssumeRole process for a fork-and-exec death test.  It re-executes the
// main program from the beginning, setting the --gtest_filter
// and --gtest_internal_run_death_test flags to cause only the current
// death test to be re-run.
DeathTest::TestRole ExecDeathTest::AssumeRole() {
  const UnitTestImpl* const impl = GetUnitTestImpl();
  const InternalRunDeathTestFlag* const flag =
      impl->internal_run_death_test_flag();
  const TestInfo* const info = impl->current_test_info();
  const int death_test_index = info->result()->death_test_count();

  if (flag != NULL) {
    set_write_fd(flag->write_fd());
    return EXECUTE_TEST;
  }

  int pipe_fd[2];
  GTEST_DEATH_TEST_CHECK_(pipe(pipe_fd) != -1);
  // Clear the close-on-exec flag on the write end of the pipe, lest
  // it be closed when the child process does an exec:
  GTEST_DEATH_TEST_CHECK_(fcntl(pipe_fd[1], F_SETFD, 0) != -1);

  const std::string filter_flag =
      std::string("--") + GTEST_FLAG_PREFIX_ + kFilterFlag + "="
      + info->test_case_name() + "." + info->name();
  const std::string internal_flag =
      std::string("--") + GTEST_FLAG_PREFIX_ + kInternalRunDeathTestFlag + "="
      + file_ + "|" + StreamableToString(line_) + "|"
      + StreamableToString(death_test_index) + "|"
      + StreamableToString(pipe_fd[1]);
  Arguments args;
  args.AddArguments(GetArgvsForDeathTestChildProcess());
  args.AddArgument(filter_flag.c_str());
  args.AddArgument(internal_flag.c_str());

  DeathTest::set_last_death_test_message("");

  CaptureStderr();
  // See the comment in NoExecDeathTest::AssumeRole for why the next line
  // is necessary.
  FlushInfoLog();

  const pid_t child_pid = ExecDeathTestSpawnChild(args.Argv(), pipe_fd[0]);
  GTEST_DEATH_TEST_CHECK_SYSCALL_(close(pipe_fd[1]));
  set_child_pid(child_pid);
  set_read_fd(pipe_fd[0]);
  set_spawned(true);
  return OVERSEE_TEST;
}

# endif  // !GTEST_OS_WINDOWS

// Creates a concrete DeathTest-derived class that depends on the
// --gtest_death_test_style flag, and sets the pointer pointed to
// by the "test" argument to its address.  If the test should be
// skipped, sets that pointer to NULL.  Returns true, unless the
// flag is set to an invalid value.
bool DefaultDeathTestFactory::Create(const char* statement, const RE* regex,
                                     const char* file, int line,
                                     DeathTest** test) {
  UnitTestImpl* const impl = GetUnitTestImpl();
  const InternalRunDeathTestFlag* const flag =
      impl->internal_run_death_test_flag();
  const int death_test_index = impl->current_test_info()
      ->increment_death_test_count();

  if (flag != NULL) {
    if (death_test_index > flag->index()) {
      DeathTest::set_last_death_test_message(
          "Death test count (" + StreamableToString(death_test_index)
          + ") somehow exceeded expected maximum ("
          + StreamableToString(flag->index()) + ")");
      return false;
    }

    if (!(flag->file() == file && flag->line() == line &&
          flag->index() == death_test_index)) {
      *test = NULL;
      return true;
    }
  }

# if GTEST_OS_WINDOWS

  if (GTEST_FLAG(death_test_style) == "threadsafe" ||
      GTEST_FLAG(death_test_style) == "fast") {
    *test = new WindowsDeathTest(statement, regex, file, line);
  }

# else

  if (GTEST_FLAG(death_test_style) == "threadsafe") {
    *test = new ExecDeathTest(statement, regex, file, line);
  } else if (GTEST_FLAG(death_test_style) == "fast") {
    *test = new NoExecDeathTest(statement, regex);
  }

# endif  // GTEST_OS_WINDOWS

  else {  // NOLINT - this is more readable than unbalanced brackets inside #if.
    DeathTest::set_last_death_test_message(
        "Unknown death test style \"" + GTEST_FLAG(death_test_style)
        + "\" encountered");
    return false;
  }

  return true;
}

// Splits a given string on a given delimiter, populating a given
// vector with the fields.  GTEST_HAS_DEATH_TEST implies that we have
// ::std::string, so we can use it here.
static void SplitString(const ::std::string& str, char delimiter,
                        ::std::vector< ::std::string>* dest) {
  ::std::vector< ::std::string> parsed;
  ::std::string::size_type pos = 0;
  while (::testing::internal::AlwaysTrue()) {
    const ::std::string::size_type colon = str.find(delimiter, pos);
    if (colon == ::std::string::npos) {
      parsed.push_back(str.substr(pos));
      break;
    } else {
      parsed.push_back(str.substr(pos, colon - pos));
      pos = colon + 1;
    }
  }
  dest->swap(parsed);
}

# if GTEST_OS_WINDOWS
// Recreates the pipe and event handles from the provided parameters,
// signals the event, and returns a file descriptor wrapped around the pipe
// handle. This function is called in the child process only.
int GetStatusFileDescriptor(unsigned int parent_process_id,
                            size_t write_handle_as_size_t,
                            size_t event_handle_as_size_t) {
  AutoHandle parent_process_handle(::OpenProcess(PROCESS_DUP_HANDLE,
                                                   FALSE,  // Non-inheritable.
                                                   parent_process_id));
  if (parent_process_handle.Get() == INVALID_HANDLE_VALUE) {
    DeathTestAbort("Unable to open parent process " +
                   StreamableToString(parent_process_id));
  }

  // TODO(vladl@google.com): Replace the following check with a
  // compile-time assertion when available.
  GTEST_CHECK_(sizeof(HANDLE) <= sizeof(size_t));

  const HANDLE write_handle =
      reinterpret_cast<HANDLE>(write_handle_as_size_t);
  HANDLE dup_write_handle;

  // The newly initialized handle is accessible only in in the parent
  // process. To obtain one accessible within the child, we need to use
  // DuplicateHandle.
  if (!::DuplicateHandle(parent_process_handle.Get(), write_handle,
                         ::GetCurrentProcess(), &dup_write_handle,
                         0x0,    // Requested privileges ignored since
                                 // DUPLICATE_SAME_ACCESS is used.
                         FALSE,  // Request non-inheritable handler.
                         DUPLICATE_SAME_ACCESS)) {
    DeathTestAbort("Unable to duplicate the pipe handle " +
                   StreamableToString(write_handle_as_size_t) +
                   " from the parent process " +
                   StreamableToString(parent_process_id));
  }

  const HANDLE event_handle = reinterpret_cast<HANDLE>(event_handle_as_size_t);
  HANDLE dup_event_handle;

  if (!::DuplicateHandle(parent_process_handle.Get(), event_handle,
                         ::GetCurrentProcess(), &dup_event_handle,
                         0x0,
                         FALSE,
                         DUPLICATE_SAME_ACCESS)) {
    DeathTestAbort("Unable to duplicate the event handle " +
                   StreamableToString(event_handle_as_size_t) +
                   " from the parent process " +
                   StreamableToString(parent_process_id));
  }

  const int write_fd =
      ::_open_osfhandle(reinterpret_cast<intptr_t>(dup_write_handle), O_APPEND);
  if (write_fd == -1) {
    DeathTestAbort("Unable to convert pipe handle " +
                   StreamableToString(write_handle_as_size_t) +
                   " to a file descriptor");
  }

  // Signals the parent that the write end of the pipe has been acquired
  // so the parent can release its own write end.
  ::SetEvent(dup_event_handle);

  return write_fd;
}
# endif  // GTEST_OS_WINDOWS

// Returns a newly created InternalRunDeathTestFlag object with fields
// initialized from the GTEST_FLAG(internal_run_death_test) flag if
// the flag is specified; otherwise returns NULL.
InternalRunDeathTestFlag* ParseInternalRunDeathTestFlag() {
  if (GTEST_FLAG(internal_run_death_test) == "") return NULL;

  // GTEST_HAS_DEATH_TEST implies that we have ::std::string, so we
  // can use it here.
  int line = -1;
  int index = -1;
  ::std::vector< ::std::string> fields;
  SplitString(GTEST_FLAG(internal_run_death_test).c_str(), '|', &fields);
  int write_fd = -1;

# if GTEST_OS_WINDOWS

  unsigned int parent_process_id = 0;
  size_t write_handle_as_size_t = 0;
  size_t event_handle_as_size_t = 0;

  if (fields.size() != 6
      || !ParseNaturalNumber(fields[1], &line)
      || !ParseNaturalNumber(fields[2], &index)
      || !ParseNaturalNumber(fields[3], &parent_process_id)
      || !ParseNaturalNumber(fields[4], &write_handle_as_size_t)
      || !ParseNaturalNumber(fields[5], &event_handle_as_size_t)) {
    DeathTestAbort("Bad --gtest_internal_run_death_test flag: " +
                   GTEST_FLAG(internal_run_death_test));
  }
  write_fd = GetStatusFileDescriptor(parent_process_id,
                                     write_handle_as_size_t,
                                     event_handle_as_size_t);
# else

  if (fields.size() != 4
      || !ParseNaturalNumber(fields[1], &line)
      || !ParseNaturalNumber(fields[2], &index)
      || !ParseNaturalNumber(fields[3], &write_fd)) {
    DeathTestAbort("Bad --gtest_internal_run_death_test flag: "
        + GTEST_FLAG(internal_run_death_test));
  }

# endif  // GTEST_OS_WINDOWS

  return new InternalRunDeathTestFlag(fields[0], line, index, write_fd);
}

}  // namespace internal

#endif  // GTEST_HAS_DEATH_TEST

}  // namespace testing
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
// Authors: keith.ray@gmail.com (Keith Ray)


#include <stdlib.h>

#if GTEST_OS_WINDOWS_MOBILE
# include <windows.h>
#elif GTEST_OS_WINDOWS
# include <direct.h>
# include <io.h>
#elif GTEST_OS_SYMBIAN
// Symbian OpenC has PATH_MAX in sys/syslimits.h
# include <sys/syslimits.h>
#else
# include <limits.h>
# include <climits>  // Some Linux distributions define PATH_MAX here.
#endif  // GTEST_OS_WINDOWS_MOBILE

#if GTEST_OS_WINDOWS
# define GTEST_PATH_MAX_ _MAX_PATH
#elif defined(PATH_MAX)
# define GTEST_PATH_MAX_ PATH_MAX
#elif defined(_XOPEN_PATH_MAX)
# define GTEST_PATH_MAX_ _XOPEN_PATH_MAX
#else
# define GTEST_PATH_MAX_ _POSIX_PATH_MAX
#endif  // GTEST_OS_WINDOWS


namespace testing {
namespace internal {

#if GTEST_OS_WINDOWS
// On Windows, '\\' is the standard path separator, but many tools and the
// Windows API also accept '/' as an alternate path separator. Unless otherwise
// noted, a file path can contain either kind of path separators, or a mixture
// of them.
const char kPathSeparator = '\\';
const char kAlternatePathSeparator = '/';
const char kPathSeparatorString[] = "\\";
const char kAlternatePathSeparatorString[] = "/";
# if GTEST_OS_WINDOWS_MOBILE
// Windows CE doesn't have a current directory. You should not use
// the current directory in tests on Windows CE, but this at least
// provides a reasonable fallback.
const char kCurrentDirectoryString[] = "\\";
// Windows CE doesn't define INVALID_FILE_ATTRIBUTES
const DWORD kInvalidFileAttributes = 0xffffffff;
# else
const char kCurrentDirectoryString[] = ".\\";
# endif  // GTEST_OS_WINDOWS_MOBILE
#else
const char kPathSeparator = '/';
const char kPathSeparatorString[] = "/";
const char kCurrentDirectoryString[] = "./";
#endif  // GTEST_OS_WINDOWS

// Returns whether the given character is a valid path separator.
static bool IsPathSeparator(char c) {
#if GTEST_HAS_ALT_PATH_SEP_
  return (c == kPathSeparator) || (c == kAlternatePathSeparator);
#else
  return c == kPathSeparator;
#endif
}

// Returns the current working directory, or "" if unsuccessful.
FilePath FilePath::GetCurrentDir() {
#if GTEST_OS_WINDOWS_MOBILE
  // Windows CE doesn't have a current directory, so we just return
  // something reasonable.
  return FilePath(kCurrentDirectoryString);
#elif GTEST_OS_WINDOWS
  char cwd[GTEST_PATH_MAX_ + 1] = { '\0' };
  return FilePath(_getcwd(cwd, sizeof(cwd)) == NULL ? "" : cwd);
#else
  char cwd[GTEST_PATH_MAX_ + 1] = { '\0' };
  return FilePath(getcwd(cwd, sizeof(cwd)) == NULL ? "" : cwd);
#endif  // GTEST_OS_WINDOWS_MOBILE
}

// Returns a copy of the FilePath with the case-insensitive extension removed.
// Example: FilePath("dir/file.exe").RemoveExtension("EXE") returns
// FilePath("dir/file"). If a case-insensitive extension is not
// found, returns a copy of the original FilePath.
FilePath FilePath::RemoveExtension(const char* extension) const {
  const std::string dot_extension = std::string(".") + extension;
  if (String::EndsWithCaseInsensitive(pathname_, dot_extension)) {
    return FilePath(pathname_.substr(
        0, pathname_.length() - dot_extension.length()));
  }
  return *this;
}

// Returns a pointer to the last occurence of a valid path separator in
// the FilePath. On Windows, for example, both '/' and '\' are valid path
// separators. Returns NULL if no path separator was found.
const char* FilePath::FindLastPathSeparator() const {
  const char* const last_sep = strrchr(c_str(), kPathSeparator);
#if GTEST_HAS_ALT_PATH_SEP_
  const char* const last_alt_sep = strrchr(c_str(), kAlternatePathSeparator);
  // Comparing two pointers of which only one is NULL is undefined.
  if (last_alt_sep != NULL &&
      (last_sep == NULL || last_alt_sep > last_sep)) {
    return last_alt_sep;
  }
#endif
  return last_sep;
}

// Returns a copy of the FilePath with the directory part removed.
// Example: FilePath("path/to/file").RemoveDirectoryName() returns
// FilePath("file"). If there is no directory part ("just_a_file"), it returns
// the FilePath unmodified. If there is no file part ("just_a_dir/") it
// returns an empty FilePath ("").
// On Windows platform, '\' is the path separator, otherwise it is '/'.
FilePath FilePath::RemoveDirectoryName() const {
  const char* const last_sep = FindLastPathSeparator();
  return last_sep ? FilePath(last_sep + 1) : *this;
}

// RemoveFileName returns the directory path with the filename removed.
// Example: FilePath("path/to/file").RemoveFileName() returns "path/to/".
// If the FilePath is "a_file" or "/a_file", RemoveFileName returns
// FilePath("./") or, on Windows, FilePath(".\\"). If the filepath does
// not have a file, like "just/a/dir/", it returns the FilePath unmodified.
// On Windows platform, '\' is the path separator, otherwise it is '/'.
FilePath FilePath::RemoveFileName() const {
  const char* const last_sep = FindLastPathSeparator();
  std::string dir;
  if (last_sep) {
    dir = std::string(c_str(), last_sep + 1 - c_str());
  } else {
    dir = kCurrentDirectoryString;
  }
  return FilePath(dir);
}

// Helper functions for naming files in a directory for xml output.

// Given directory = "dir", base_name = "test", number = 0,
// extension = "xml", returns "dir/test.xml". If number is greater
// than zero (e.g., 12), returns "dir/test_12.xml".
// On Windows platform, uses \ as the separator rather than /.
FilePath FilePath::MakeFileName(const FilePath& directory,
                                const FilePath& base_name,
                                int number,
                                const char* extension) {
  std::string file;
  if (number == 0) {
    file = base_name.string() + "." + extension;
  } else {
    file = base_name.string() + "_" + StreamableToString(number)
        + "." + extension;
  }
  return ConcatPaths(directory, FilePath(file));
}

// Given directory = "dir", relative_path = "test.xml", returns "dir/test.xml".
// On Windows, uses \ as the separator rather than /.
FilePath FilePath::ConcatPaths(const FilePath& directory,
                               const FilePath& relative_path) {
  if (directory.IsEmpty())
    return relative_path;
  const FilePath dir(directory.RemoveTrailingPathSeparator());
  return FilePath(dir.string() + kPathSeparator + relative_path.string());
}

// Returns true if pathname describes something findable in the file-system,
// either a file, directory, or whatever.
bool FilePath::FileOrDirectoryExists() const {
#if GTEST_OS_WINDOWS_MOBILE
  LPCWSTR unicode = String::AnsiToUtf16(pathname_.c_str());
  const DWORD attributes = GetFileAttributes(unicode);
  delete [] unicode;
  return attributes != kInvalidFileAttributes;
#else
  posix::StatStruct file_stat;
  return posix::Stat(pathname_.c_str(), &file_stat) == 0;
#endif  // GTEST_OS_WINDOWS_MOBILE
}

// Returns true if pathname describes a directory in the file-system
// that exists.
bool FilePath::DirectoryExists() const {
  bool result = false;
#if GTEST_OS_WINDOWS
  // Don't strip off trailing separator if path is a root directory on
  // Windows (like "C:\\").
  const FilePath& path(IsRootDirectory() ? *this :
                                           RemoveTrailingPathSeparator());
#else
  const FilePath& path(*this);
#endif

#if GTEST_OS_WINDOWS_MOBILE
  LPCWSTR unicode = String::AnsiToUtf16(path.c_str());
  const DWORD attributes = GetFileAttributes(unicode);
  delete [] unicode;
  if ((attributes != kInvalidFileAttributes) &&
      (attributes & FILE_ATTRIBUTE_DIRECTORY)) {
    result = true;
  }
#else
  posix::StatStruct file_stat;
  result = posix::Stat(path.c_str(), &file_stat) == 0 &&
      posix::IsDir(file_stat);
#endif  // GTEST_OS_WINDOWS_MOBILE

  return result;
}

// Returns true if pathname describes a root directory. (Windows has one
// root directory per disk drive.)
bool FilePath::IsRootDirectory() const {
#if GTEST_OS_WINDOWS
  // TODO(wan@google.com): on Windows a network share like
  // \\server\share can be a root directory, although it cannot be the
  // current directory.  Handle this properly.
  return pathname_.length() == 3 && IsAbsolutePath();
#else
  return pathname_.length() == 1 && IsPathSeparator(pathname_.c_str()[0]);
#endif
}

// Returns true if pathname describes an absolute path.
bool FilePath::IsAbsolutePath() const {
  const char* const name = pathname_.c_str();
#if GTEST_OS_WINDOWS
  return pathname_.length() >= 3 &&
     ((name[0] >= 'a' && name[0] <= 'z') ||
      (name[0] >= 'A' && name[0] <= 'Z')) &&
     name[1] == ':' &&
     IsPathSeparator(name[2]);
#else
  return IsPathSeparator(name[0]);
#endif
}

// Returns a pathname for a file that does not currently exist. The pathname
// will be directory/base_name.extension or
// directory/base_name_<number>.extension if directory/base_name.extension
// already exists. The number will be incremented until a pathname is found
// that does not already exist.
// Examples: 'dir/foo_test.xml' or 'dir/foo_test_1.xml'.
// There could be a race condition if two or more processes are calling this
// function at the same time -- they could both pick the same filename.
FilePath FilePath::GenerateUniqueFileName(const FilePath& directory,
                                          const FilePath& base_name,
                                          const char* extension) {
  FilePath full_pathname;
  int number = 0;
  do {
    full_pathname.Set(MakeFileName(directory, base_name, number++, extension));
  } while (full_pathname.FileOrDirectoryExists());
  return full_pathname;
}

// Returns true if FilePath ends with a path separator, which indicates that
// it is intended to represent a directory. Returns false otherwise.
// This does NOT check that a directory (or file) actually exists.
bool FilePath::IsDirectory() const {
  return !pathname_.empty() &&
         IsPathSeparator(pathname_.c_str()[pathname_.length() - 1]);
}

// Create directories so that path exists. Returns true if successful or if
// the directories already exist; returns false if unable to create directories
// for any reason.
bool FilePath::CreateDirectoriesRecursively() const {
  if (!this->IsDirectory()) {
    return false;
  }

  if (pathname_.length() == 0 || this->DirectoryExists()) {
    return true;
  }

  const FilePath parent(this->RemoveTrailingPathSeparator().RemoveFileName());
  return parent.CreateDirectoriesRecursively() && this->CreateFolder();
}

// Create the directory so that path exists. Returns true if successful or
// if the directory already exists; returns false if unable to create the
// directory for any reason, including if the parent directory does not
// exist. Not named "CreateDirectory" because that's a macro on Windows.
bool FilePath::CreateFolder() const {
#if GTEST_OS_WINDOWS_MOBILE
  FilePath removed_sep(this->RemoveTrailingPathSeparator());
  LPCWSTR unicode = String::AnsiToUtf16(removed_sep.c_str());
  int result = CreateDirectory(unicode, NULL) ? 0 : -1;
  delete [] unicode;
#elif GTEST_OS_WINDOWS
  int result = _mkdir(pathname_.c_str());
#else
  int result = mkdir(pathname_.c_str(), 0777);
#endif  // GTEST_OS_WINDOWS_MOBILE

  if (result == -1) {
    return this->DirectoryExists();  // An error is OK if the directory exists.
  }
  return true;  // No error.
}

// If input name has a trailing separator character, remove it and return the
// name, otherwise return the name string unmodified.
// On Windows platform, uses \ as the separator, other platforms use /.
FilePath FilePath::RemoveTrailingPathSeparator() const {
  return IsDirectory()
      ? FilePath(pathname_.substr(0, pathname_.length() - 1))
      : *this;
}

// Removes any redundant separators that might be in the pathname.
// For example, "bar///foo" becomes "bar/foo". Does not eliminate other
// redundancies that might be in a pathname involving "." or "..".
// TODO(wan@google.com): handle Windows network shares (e.g. \\server\share).
void FilePath::Normalize() {
  if (pathname_.c_str() == NULL) {
    pathname_ = "";
    return;
  }
  const char* src = pathname_.c_str();
  char* const dest = new char[pathname_.length() + 1];
  char* dest_ptr = dest;
  memset(dest_ptr, 0, pathname_.length() + 1);

  while (*src != '\0') {
    *dest_ptr = *src;
    if (!IsPathSeparator(*src)) {
      src++;
    } else {
#if GTEST_HAS_ALT_PATH_SEP_
      if (*dest_ptr == kAlternatePathSeparator) {
        *dest_ptr = kPathSeparator;
      }
#endif
      while (IsPathSeparator(*src))
        src++;
    }
    dest_ptr++;
  }
  *dest_ptr = '\0';
  pathname_ = dest;
  delete[] dest;
}

}  // namespace internal
}  // namespace testing
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


#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if GTEST_OS_WINDOWS_MOBILE
# include <windows.h>  // For TerminateProcess()
#elif GTEST_OS_WINDOWS
# include <io.h>
# include <sys/stat.h>
#else
# include <unistd.h>
#endif  // GTEST_OS_WINDOWS_MOBILE

#if GTEST_OS_MAC
# include <mach/mach_init.h>
# include <mach/task.h>
# include <mach/vm_map.h>
#endif  // GTEST_OS_MAC

#if GTEST_OS_QNX
# include <devctl.h>
# include <sys/procfs.h>
#endif  // GTEST_OS_QNX


// Indicates that this translation unit is part of Google Test's
// implementation.  It must come before gtest-internal-inl.h is
// included, or there will be a compiler error.  This trick is to
// prevent a user from accidentally including gtest-internal-inl.h in
// his code.
#define GTEST_IMPLEMENTATION_ 1
#undef GTEST_IMPLEMENTATION_

namespace testing {
namespace internal {

#if defined(_MSC_VER) || defined(__BORLANDC__)
// MSVC and C++Builder do not provide a definition of STDERR_FILENO.
const int kStdOutFileno = 1;
const int kStdErrFileno = 2;
#else
const int kStdOutFileno = STDOUT_FILENO;
const int kStdErrFileno = STDERR_FILENO;
#endif  // _MSC_VER

#if GTEST_OS_MAC

// Returns the number of threads running in the process, or 0 to indicate that
// we cannot detect it.
size_t GetThreadCount() {
  const task_t task = mach_task_self();
  mach_msg_type_number_t thread_count;
  thread_act_array_t thread_list;
  const kern_return_t status = task_threads(task, &thread_list, &thread_count);
  if (status == KERN_SUCCESS) {
    // task_threads allocates resources in thread_list and we need to free them
    // to avoid leaks.
    vm_deallocate(task,
                  reinterpret_cast<vm_address_t>(thread_list),
                  sizeof(thread_t) * thread_count);
    return static_cast<size_t>(thread_count);
  } else {
    return 0;
  }
}

#elif GTEST_OS_QNX

// Returns the number of threads running in the process, or 0 to indicate that
// we cannot detect it.
size_t GetThreadCount() {
  const int fd = open("/proc/self/as", O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  procfs_info process_info;
  const int status =
      devctl(fd, DCMD_PROC_INFO, &process_info, sizeof(process_info), NULL);
  close(fd);
  if (status == EOK) {
    return static_cast<size_t>(process_info.num_threads);
  } else {
    return 0;
  }
}

#else

size_t GetThreadCount() {
  // There's no portable way to detect the number of threads, so we just
  // return 0 to indicate that we cannot detect it.
  return 0;
}

#endif  // GTEST_OS_MAC

#if GTEST_USES_POSIX_RE

// Implements RE.  Currently only needed for death tests.

RE::~RE() {
  if (is_valid_) {
    // regfree'ing an invalid regex might crash because the content
    // of the regex is undefined. Since the regex's are essentially
    // the same, one cannot be valid (or invalid) without the other
    // being so too.
    regfree(&partial_regex_);
    regfree(&full_regex_);
  }
  free(const_cast<char*>(pattern_));
}

// Returns true iff regular expression re matches the entire str.
bool RE::FullMatch(const char* str, const RE& re) {
  if (!re.is_valid_) return false;

  regmatch_t match;
  return regexec(&re.full_regex_, str, 1, &match, 0) == 0;
}

// Returns true iff regular expression re matches a substring of str
// (including str itself).
bool RE::PartialMatch(const char* str, const RE& re) {
  if (!re.is_valid_) return false;

  regmatch_t match;
  return regexec(&re.partial_regex_, str, 1, &match, 0) == 0;
}

// Initializes an RE from its string representation.
void RE::Init(const char* regex) {
  pattern_ = posix::StrDup(regex);

  // Reserves enough bytes to hold the regular expression used for a
  // full match.
  const size_t full_regex_len = strlen(regex) + 10;
  char* const full_pattern = new char[full_regex_len];

  snprintf(full_pattern, full_regex_len, "^(%s)$", regex);
  is_valid_ = regcomp(&full_regex_, full_pattern, REG_EXTENDED) == 0;
  // We want to call regcomp(&partial_regex_, ...) even if the
  // previous expression returns false.  Otherwise partial_regex_ may
  // not be properly initialized can may cause trouble when it's
  // freed.
  //
  // Some implementation of POSIX regex (e.g. on at least some
  // versions of Cygwin) doesn't accept the empty string as a valid
  // regex.  We change it to an equivalent form "()" to be safe.
  if (is_valid_) {
    const char* const partial_regex = (*regex == '\0') ? "()" : regex;
    is_valid_ = regcomp(&partial_regex_, partial_regex, REG_EXTENDED) == 0;
  }
  EXPECT_TRUE(is_valid_)
      << "Regular expression \"" << regex
      << "\" is not a valid POSIX Extended regular expression.";

  delete[] full_pattern;
}

#elif GTEST_USES_SIMPLE_RE

// Returns true iff ch appears anywhere in str (excluding the
// terminating '\0' character).
bool IsInSet(char ch, const char* str) {
  return ch != '\0' && strchr(str, ch) != NULL;
}

// Returns true iff ch belongs to the given classification.  Unlike
// similar functions in <ctype.h>, these aren't affected by the
// current locale.
bool IsAsciiDigit(char ch) { return '0' <= ch && ch <= '9'; }
bool IsAsciiPunct(char ch) {
  return IsInSet(ch, "^-!\"#$%&'()*+,./:;<=>?@[\\]_`{|}~");
}
bool IsRepeat(char ch) { return IsInSet(ch, "?*+"); }
bool IsAsciiWhiteSpace(char ch) { return IsInSet(ch, " \f\n\r\t\v"); }
bool IsAsciiWordChar(char ch) {
  return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') ||
      ('0' <= ch && ch <= '9') || ch == '_';
}

// Returns true iff "\\c" is a supported escape sequence.
bool IsValidEscape(char c) {
  return (IsAsciiPunct(c) || IsInSet(c, "dDfnrsStvwW"));
}

// Returns true iff the given atom (specified by escaped and pattern)
// matches ch.  The result is undefined if the atom is invalid.
bool AtomMatchesChar(bool escaped, char pattern_char, char ch) {
  if (escaped) {  // "\\p" where p is pattern_char.
    switch (pattern_char) {
      case 'd': return IsAsciiDigit(ch);
      case 'D': return !IsAsciiDigit(ch);
      case 'f': return ch == '\f';
      case 'n': return ch == '\n';
      case 'r': return ch == '\r';
      case 's': return IsAsciiWhiteSpace(ch);
      case 'S': return !IsAsciiWhiteSpace(ch);
      case 't': return ch == '\t';
      case 'v': return ch == '\v';
      case 'w': return IsAsciiWordChar(ch);
      case 'W': return !IsAsciiWordChar(ch);
    }
    return IsAsciiPunct(pattern_char) && pattern_char == ch;
  }

  return (pattern_char == '.' && ch != '\n') || pattern_char == ch;
}

// Helper function used by ValidateRegex() to format error messages.
std::string FormatRegexSyntaxError(const char* regex, int index) {
  return (Message() << "Syntax error at index " << index
          << " in simple regular expression \"" << regex << "\": ").GetString();
}

// Generates non-fatal failures and returns false if regex is invalid;
// otherwise returns true.
bool ValidateRegex(const char* regex) {
  if (regex == NULL) {
    // TODO(wan@google.com): fix the source file location in the
    // assertion failures to match where the regex is used in user
    // code.
    ADD_FAILURE() << "NULL is not a valid simple regular expression.";
    return false;
  }

  bool is_valid = true;

  // True iff ?, *, or + can follow the previous atom.
  bool prev_repeatable = false;
  for (int i = 0; regex[i]; i++) {
    if (regex[i] == '\\') {  // An escape sequence
      i++;
      if (regex[i] == '\0') {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i - 1)
                      << "'\\' cannot appear at the end.";
        return false;
      }

      if (!IsValidEscape(regex[i])) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i - 1)
                      << "invalid escape sequence \"\\" << regex[i] << "\".";
        is_valid = false;
      }
      prev_repeatable = true;
    } else {  // Not an escape sequence.
      const char ch = regex[i];

      if (ch == '^' && i > 0) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'^' can only appear at the beginning.";
        is_valid = false;
      } else if (ch == '$' && regex[i + 1] != '\0') {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'$' can only appear at the end.";
        is_valid = false;
      } else if (IsInSet(ch, "()[]{}|")) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'" << ch << "' is unsupported.";
        is_valid = false;
      } else if (IsRepeat(ch) && !prev_repeatable) {
        ADD_FAILURE() << FormatRegexSyntaxError(regex, i)
                      << "'" << ch << "' can only follow a repeatable token.";
        is_valid = false;
      }

      prev_repeatable = !IsInSet(ch, "^$?*+");
    }
  }

  return is_valid;
}

// Matches a repeated regex atom followed by a valid simple regular
// expression.  The regex atom is defined as c if escaped is false,
// or \c otherwise.  repeat is the repetition meta character (?, *,
// or +).  The behavior is undefined if str contains too many
// characters to be indexable by size_t, in which case the test will
// probably time out anyway.  We are fine with this limitation as
// std::string has it too.
bool MatchRepetitionAndRegexAtHead(
    bool escaped, char c, char repeat, const char* regex,
    const char* str) {
  const size_t min_count = (repeat == '+') ? 1 : 0;
  const size_t max_count = (repeat == '?') ? 1 :
      static_cast<size_t>(-1) - 1;
  // We cannot call numeric_limits::max() as it conflicts with the
  // max() macro on Windows.

  for (size_t i = 0; i <= max_count; ++i) {
    // We know that the atom matches each of the first i characters in str.
    if (i >= min_count && MatchRegexAtHead(regex, str + i)) {
      // We have enough matches at the head, and the tail matches too.
      // Since we only care about *whether* the pattern matches str
      // (as opposed to *how* it matches), there is no need to find a
      // greedy match.
      return true;
    }
    if (str[i] == '\0' || !AtomMatchesChar(escaped, c, str[i]))
      return false;
  }
  return false;
}

// Returns true iff regex matches a prefix of str.  regex must be a
// valid simple regular expression and not start with "^", or the
// result is undefined.
bool MatchRegexAtHead(const char* regex, const char* str) {
  if (*regex == '\0')  // An empty regex matches a prefix of anything.
    return true;

  // "$" only matches the end of a string.  Note that regex being
  // valid guarantees that there's nothing after "$" in it.
  if (*regex == '$')
    return *str == '\0';

  // Is the first thing in regex an escape sequence?
  const bool escaped = *regex == '\\';
  if (escaped)
    ++regex;
  if (IsRepeat(regex[1])) {
    // MatchRepetitionAndRegexAtHead() calls MatchRegexAtHead(), so
    // here's an indirect recursion.  It terminates as the regex gets
    // shorter in each recursion.
    return MatchRepetitionAndRegexAtHead(
        escaped, regex[0], regex[1], regex + 2, str);
  } else {
    // regex isn't empty, isn't "$", and doesn't start with a
    // repetition.  We match the first atom of regex with the first
    // character of str and recurse.
    return (*str != '\0') && AtomMatchesChar(escaped, *regex, *str) &&
        MatchRegexAtHead(regex + 1, str + 1);
  }
}

// Returns true iff regex matches any substring of str.  regex must be
// a valid simple regular expression, or the result is undefined.
//
// The algorithm is recursive, but the recursion depth doesn't exceed
// the regex length, so we won't need to worry about running out of
// stack space normally.  In rare cases the time complexity can be
// exponential with respect to the regex length + the string length,
// but usually it's must faster (often close to linear).
bool MatchRegexAnywhere(const char* regex, const char* str) {
  if (regex == NULL || str == NULL)
    return false;

  if (*regex == '^')
    return MatchRegexAtHead(regex + 1, str);

  // A successful match can be anywhere in str.
  do {
    if (MatchRegexAtHead(regex, str))
      return true;
  } while (*str++ != '\0');
  return false;
}

// Implements the RE class.

RE::~RE() {
  free(const_cast<char*>(pattern_));
  free(const_cast<char*>(full_pattern_));
}

// Returns true iff regular expression re matches the entire str.
bool RE::FullMatch(const char* str, const RE& re) {
  return re.is_valid_ && MatchRegexAnywhere(re.full_pattern_, str);
}

// Returns true iff regular expression re matches a substring of str
// (including str itself).
bool RE::PartialMatch(const char* str, const RE& re) {
  return re.is_valid_ && MatchRegexAnywhere(re.pattern_, str);
}

// Initializes an RE from its string representation.
void RE::Init(const char* regex) {
  pattern_ = full_pattern_ = NULL;
  if (regex != NULL) {
    pattern_ = posix::StrDup(regex);
  }

  is_valid_ = ValidateRegex(regex);
  if (!is_valid_) {
    // No need to calculate the full pattern when the regex is invalid.
    return;
  }

  const size_t len = strlen(regex);
  // Reserves enough bytes to hold the regular expression used for a
  // full match: we need space to prepend a '^', append a '$', and
  // terminate the string with '\0'.
  char* buffer = static_cast<char*>(malloc(len + 3));
  full_pattern_ = buffer;

  if (*regex != '^')
    *buffer++ = '^';  // Makes sure full_pattern_ starts with '^'.

  // We don't use snprintf or strncpy, as they trigger a warning when
  // compiled with VC++ 8.0.
  memcpy(buffer, regex, len);
  buffer += len;

  if (len == 0 || regex[len - 1] != '$')
    *buffer++ = '$';  // Makes sure full_pattern_ ends with '$'.

  *buffer = '\0';
}

#endif  // GTEST_USES_POSIX_RE

const char kUnknownFile[] = "unknown file";

// Formats a source file path and a line number as they would appear
// in an error message from the compiler used to compile this code.
GTEST_API_ ::std::string FormatFileLocation(const char* file, int line) {
  const std::string file_name(file == NULL ? kUnknownFile : file);

  if (line < 0) {
    return file_name + ":";
  }
#ifdef _MSC_VER
  return file_name + "(" + StreamableToString(line) + "):";
#else
  return file_name + ":" + StreamableToString(line) + ":";
#endif  // _MSC_VER
}

// Formats a file location for compiler-independent XML output.
// Although this function is not platform dependent, we put it next to
// FormatFileLocation in order to contrast the two functions.
// Note that FormatCompilerIndependentFileLocation() does NOT append colon
// to the file location it produces, unlike FormatFileLocation().
GTEST_API_ ::std::string FormatCompilerIndependentFileLocation(
    const char* file, int line) {
  const std::string file_name(file == NULL ? kUnknownFile : file);

  if (line < 0)
    return file_name;
  else
    return file_name + ":" + StreamableToString(line);
}


GTestLog::GTestLog(GTestLogSeverity severity, const char* file, int line)
    : severity_(severity) {
  const char* const marker =
      severity == GTEST_INFO ?    "[  INFO ]" :
      severity == GTEST_WARNING ? "[WARNING]" :
      severity == GTEST_ERROR ?   "[ ERROR ]" : "[ FATAL ]";
  GetStream() << ::std::endl << marker << " "
              << FormatFileLocation(file, line).c_str() << ": ";
}

// Flushes the buffers and, if severity is GTEST_FATAL, aborts the program.
GTestLog::~GTestLog() {
  GetStream() << ::std::endl;
  if (severity_ == GTEST_FATAL) {
    fflush(stderr);
    posix::Abort();
  }
}
// Disable Microsoft deprecation warnings for POSIX functions called from
// this class (creat, dup, dup2, and close)
#ifdef _MSC_VER
# pragma warning(push)
# pragma warning(disable: 4996)
#endif  // _MSC_VER

#if GTEST_HAS_STREAM_REDIRECTION

// Object that captures an output stream (stdout/stderr).
class CapturedStream {
 public:
  // The ctor redirects the stream to a temporary file.
  explicit CapturedStream(int fd) : fd_(fd), uncaptured_fd_(dup(fd)) {
# if GTEST_OS_WINDOWS
    char temp_dir_path[MAX_PATH + 1] = { '\0' };  // NOLINT
    char temp_file_path[MAX_PATH + 1] = { '\0' };  // NOLINT

    ::GetTempPathA(sizeof(temp_dir_path), temp_dir_path);
    const UINT success = ::GetTempFileNameA(temp_dir_path,
                                            "gtest_redir",
                                            0,  // Generate unique file name.
                                            temp_file_path);
    GTEST_CHECK_(success != 0)
        << "Unable to create a temporary file in " << temp_dir_path;
    const int captured_fd = creat(temp_file_path, _S_IREAD | _S_IWRITE);
    GTEST_CHECK_(captured_fd != -1) << "Unable to open temporary file "
                                    << temp_file_path;
    filename_ = temp_file_path;
# else
    // There's no guarantee that a test has write access to the current
    // directory, so we create the temporary file in the /tmp directory
    // instead. We use /tmp on most systems, and /sdcard on Android.
    // That's because Android doesn't have /tmp.
#  if GTEST_OS_LINUX_ANDROID
    // Note: Android applications are expected to call the framework's
    // Context.getExternalStorageDirectory() method through JNI to get
    // the location of the world-writable SD Card directory. However,
    // this requires a Context handle, which cannot be retrieved
    // globally from native code. Doing so also precludes running the
    // code as part of a regular standalone executable, which doesn't
    // run in a Dalvik process (e.g. when running it through 'adb shell').
    //
    // The location /sdcard is directly accessible from native code
    // and is the only location (unofficially) supported by the Android
    // team. It's generally a symlink to the real SD Card mount point
    // which can be /mnt/sdcard, /mnt/sdcard0, /system/media/sdcard, or
    // other OEM-customized locations. Never rely on these, and always
    // use /sdcard.
    char name_template[] = "/sdcard/gtest_captured_stream.XXXXXX";
#  else
    char name_template[] = "/tmp/captured_stream.XXXXXX";
#  endif  // GTEST_OS_LINUX_ANDROID
    const int captured_fd = mkstemp(name_template);
    filename_ = name_template;
# endif  // GTEST_OS_WINDOWS
    fflush(NULL);
    dup2(captured_fd, fd_);
    close(captured_fd);
  }

  ~CapturedStream() {
    remove(filename_.c_str());
  }

  std::string GetCapturedString() {
    if (uncaptured_fd_ != -1) {
      // Restores the original stream.
      fflush(NULL);
      dup2(uncaptured_fd_, fd_);
      close(uncaptured_fd_);
      uncaptured_fd_ = -1;
    }

    FILE* const file = posix::FOpen(filename_.c_str(), "r");
    const std::string content = ReadEntireFile(file);
    posix::FClose(file);
    return content;
  }

 private:
  // Reads the entire content of a file as an std::string.
  static std::string ReadEntireFile(FILE* file);

  // Returns the size (in bytes) of a file.
  static size_t GetFileSize(FILE* file);

  const int fd_;  // A stream to capture.
  int uncaptured_fd_;
  // Name of the temporary file holding the stderr output.
  ::std::string filename_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(CapturedStream);
};

// Returns the size (in bytes) of a file.
size_t CapturedStream::GetFileSize(FILE* file) {
  fseek(file, 0, SEEK_END);
  return static_cast<size_t>(ftell(file));
}

// Reads the entire content of a file as a string.
std::string CapturedStream::ReadEntireFile(FILE* file) {
  const size_t file_size = GetFileSize(file);
  char* const buffer = new char[file_size];

  size_t bytes_last_read = 0;  // # of bytes read in the last fread()
  size_t bytes_read = 0;       // # of bytes read so far

  fseek(file, 0, SEEK_SET);

  // Keeps reading the file until we cannot read further or the
  // pre-determined file size is reached.
  do {
    bytes_last_read = fread(buffer+bytes_read, 1, file_size-bytes_read, file);
    bytes_read += bytes_last_read;
  } while (bytes_last_read > 0 && bytes_read < file_size);

  const std::string content(buffer, bytes_read);
  delete[] buffer;

  return content;
}

# ifdef _MSC_VER
#  pragma warning(pop)
# endif  // _MSC_VER

static CapturedStream* g_captured_stderr = NULL;
static CapturedStream* g_captured_stdout = NULL;

// Starts capturing an output stream (stdout/stderr).
void CaptureStream(int fd, const char* stream_name, CapturedStream** stream) {
  if (*stream != NULL) {
    GTEST_LOG_(FATAL) << "Only one " << stream_name
                      << " capturer can exist at a time.";
  }
  *stream = new CapturedStream(fd);
}

// Stops capturing the output stream and returns the captured string.
std::string GetCapturedStream(CapturedStream** captured_stream) {
  const std::string content = (*captured_stream)->GetCapturedString();

  delete *captured_stream;
  *captured_stream = NULL;

  return content;
}

// Starts capturing stdout.
void CaptureStdout() {
  CaptureStream(kStdOutFileno, "stdout", &g_captured_stdout);
}

// Starts capturing stderr.
void CaptureStderr() {
  CaptureStream(kStdErrFileno, "stderr", &g_captured_stderr);
}

// Stops capturing stdout and returns the captured string.
std::string GetCapturedStdout() {
  return GetCapturedStream(&g_captured_stdout);
}

// Stops capturing stderr and returns the captured string.
std::string GetCapturedStderr() {
  return GetCapturedStream(&g_captured_stderr);
}

#endif  // GTEST_HAS_STREAM_REDIRECTION

#if GTEST_HAS_DEATH_TEST

// A copy of all command line arguments.  Set by InitGoogleTest().
::std::vector<testing::internal::string> g_argvs;

static const ::std::vector<testing::internal::string>* g_injected_test_argvs =
                                        NULL;  // Owned.

void SetInjectableArgvs(const ::std::vector<testing::internal::string>* argvs) {
  if (g_injected_test_argvs != argvs)
    delete g_injected_test_argvs;
  g_injected_test_argvs = argvs;
}

const ::std::vector<testing::internal::string>& GetInjectableArgvs() {
  if (g_injected_test_argvs != NULL) {
    return *g_injected_test_argvs;
  }
  return g_argvs;
}
#endif  // GTEST_HAS_DEATH_TEST

#if GTEST_OS_WINDOWS_MOBILE
namespace posix {
void Abort() {
  DebugBreak();
  TerminateProcess(GetCurrentProcess(), 1);
}
}  // namespace posix
#endif  // GTEST_OS_WINDOWS_MOBILE

// Returns the name of the environment variable corresponding to the
// given flag.  For example, FlagToEnvVar("foo") will return
// "GTEST_FOO" in the open-source version.
static std::string FlagToEnvVar(const char* flag) {
  const std::string full_flag =
      (Message() << GTEST_FLAG_PREFIX_ << flag).GetString();

  Message env_var;
  for (size_t i = 0; i != full_flag.length(); i++) {
    env_var << ToUpper(full_flag.c_str()[i]);
  }

  return env_var.GetString();
}

// Parses 'str' for a 32-bit signed integer.  If successful, writes
// the result to *value and returns true; otherwise leaves *value
// unchanged and returns false.
bool ParseInt32(const Message& src_text, const char* str, Int32* value) {
  // Parses the environment variable as a decimal integer.
  char* end = NULL;
  const long long_value = strtol(str, &end, 10);  // NOLINT

  // Has strtol() consumed all characters in the string?
  if (*end != '\0') {
    // No - an invalid character was encountered.
    Message msg;
    msg << "WARNING: " << src_text
        << " is expected to be a 32-bit integer, but actually"
        << " has value \"" << str << "\".\n";
    printf("%s", msg.GetString().c_str());
    fflush(stdout);
    return false;
  }

  // Is the parsed value in the range of an Int32?
  const Int32 result = static_cast<Int32>(long_value);
  if (long_value == LONG_MAX || long_value == LONG_MIN ||
      // The parsed value overflows as a long.  (strtol() returns
      // LONG_MAX or LONG_MIN when the input overflows.)
      result != long_value
      // The parsed value overflows as an Int32.
      ) {
    Message msg;
    msg << "WARNING: " << src_text
        << " is expected to be a 32-bit integer, but actually"
        << " has value " << str << ", which overflows.\n";
    printf("%s", msg.GetString().c_str());
    fflush(stdout);
    return false;
  }

  *value = result;
  return true;
}

// Reads and returns the Boolean environment variable corresponding to
// the given flag; if it's not set, returns default_value.
//
// The value is considered true iff it's not "0".
bool BoolFromGTestEnv(const char* flag, bool default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const string_value = posix::GetEnv(env_var.c_str());
  return string_value == NULL ?
      default_value : strcmp(string_value, "0") != 0;
}

// Reads and returns a 32-bit integer stored in the environment
// variable corresponding to the given flag; if it isn't set or
// doesn't represent a valid 32-bit integer, returns default_value.
Int32 Int32FromGTestEnv(const char* flag, Int32 default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const string_value = posix::GetEnv(env_var.c_str());
  if (string_value == NULL) {
    // The environment variable is not set.
    return default_value;
  }

  Int32 result = default_value;
  if (!ParseInt32(Message() << "Environment variable " << env_var,
                  string_value, &result)) {
    printf("The default value %s is used.\n",
           (Message() << default_value).GetString().c_str());
    fflush(stdout);
    return default_value;
  }

  return result;
}

// Reads and returns the string environment variable corresponding to
// the given flag; if it's not set, returns default_value.
const char* StringFromGTestEnv(const char* flag, const char* default_value) {
  const std::string env_var = FlagToEnvVar(flag);
  const char* const value = posix::GetEnv(env_var.c_str());
  return value == NULL ? default_value : value;
}

}  // namespace internal
}  // namespace testing
// Copyright 2007, Google Inc.
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

// Google Test - The Google C++ Testing Framework
//
// This file implements a universal value printer that can print a
// value of any type T:
//
//   void ::testing::internal::UniversalPrinter<T>::Print(value, ostream_ptr);
//
// It uses the << operator when possible, and prints the bytes in the
// object otherwise.  A user can override its behavior for a class
// type Foo by defining either operator<<(::std::ostream&, const Foo&)
// or void PrintTo(const Foo&, ::std::ostream*) in the namespace that
// defines Foo.

#include <ctype.h>
#include <stdio.h>
#include <ostream>  // NOLINT
#include <string>

namespace testing {

namespace {

using ::std::ostream;

// Prints a segment of bytes in the given object.
void PrintByteSegmentInObjectTo(const unsigned char* obj_bytes, size_t start,
                                size_t count, ostream* os) {
  char text[5] = "";
  for (size_t i = 0; i != count; i++) {
    const size_t j = start + i;
    if (i != 0) {
      // Organizes the bytes into groups of 2 for easy parsing by
      // human.
      if ((j % 2) == 0)
        *os << ' ';
      else
        *os << '-';
    }
    GTEST_SNPRINTF_(text, sizeof(text), "%02X", obj_bytes[j]);
    *os << text;
  }
}

// Prints the bytes in the given value to the given ostream.
void PrintBytesInObjectToImpl(const unsigned char* obj_bytes, size_t count,
                              ostream* os) {
  // Tells the user how big the object is.
  *os << count << "-byte object <";

  const size_t kThreshold = 132;
  const size_t kChunkSize = 64;
  // If the object size is bigger than kThreshold, we'll have to omit
  // some details by printing only the first and the last kChunkSize
  // bytes.
  // TODO(wan): let the user control the threshold using a flag.
  if (count < kThreshold) {
    PrintByteSegmentInObjectTo(obj_bytes, 0, count, os);
  } else {
    PrintByteSegmentInObjectTo(obj_bytes, 0, kChunkSize, os);
    *os << " ... ";
    // Rounds up to 2-byte boundary.
    const size_t resume_pos = (count - kChunkSize + 1)/2*2;
    PrintByteSegmentInObjectTo(obj_bytes, resume_pos, count - resume_pos, os);
  }
  *os << ">";
}

}  // namespace

namespace internal2 {

// Delegates to PrintBytesInObjectToImpl() to print the bytes in the
// given object.  The delegation simplifies the implementation, which
// uses the << operator and thus is easier done outside of the
// ::testing::internal namespace, which contains a << operator that
// sometimes conflicts with the one in STL.
void PrintBytesInObjectTo(const unsigned char* obj_bytes, size_t count,
                          ostream* os) {
  PrintBytesInObjectToImpl(obj_bytes, count, os);
}

}  // namespace internal2

namespace internal {

// Depending on the value of a char (or wchar_t), we print it in one
// of three formats:
//   - as is if it's a printable ASCII (e.g. 'a', '2', ' '),
//   - as a hexidecimal escape sequence (e.g. '\x7F'), or
//   - as a special escape sequence (e.g. '\r', '\n').
enum CharFormat {
  kAsIs,
  kHexEscape,
  kSpecialEscape
};

// Returns true if c is a printable ASCII character.  We test the
// value of c directly instead of calling isprint(), which is buggy on
// Windows Mobile.
inline bool IsPrintableAscii(wchar_t c) {
  return 0x20 <= c && c <= 0x7E;
}

// Prints a wide or narrow char c as a character literal without the
// quotes, escaping it when necessary; returns how c was formatted.
// The template argument UnsignedChar is the unsigned version of Char,
// which is the type of c.
template <typename UnsignedChar, typename Char>
static CharFormat PrintAsCharLiteralTo(Char c, ostream* os) {
  switch (static_cast<wchar_t>(c)) {
    case L'\0':
      *os << "\\0";
      break;
    case L'\'':
      *os << "\\'";
      break;
    case L'\\':
      *os << "\\\\";
      break;
    case L'\a':
      *os << "\\a";
      break;
    case L'\b':
      *os << "\\b";
      break;
    case L'\f':
      *os << "\\f";
      break;
    case L'\n':
      *os << "\\n";
      break;
    case L'\r':
      *os << "\\r";
      break;
    case L'\t':
      *os << "\\t";
      break;
    case L'\v':
      *os << "\\v";
      break;
    default:
      if (IsPrintableAscii(c)) {
        *os << static_cast<char>(c);
        return kAsIs;
      } else {
        *os << "\\x" + String::FormatHexInt(static_cast<UnsignedChar>(c));
        return kHexEscape;
      }
  }
  return kSpecialEscape;
}

// Prints a wchar_t c as if it's part of a string literal, escaping it when
// necessary; returns how c was formatted.
static CharFormat PrintAsStringLiteralTo(wchar_t c, ostream* os) {
  switch (c) {
    case L'\'':
      *os << "'";
      return kAsIs;
    case L'"':
      *os << "\\\"";
      return kSpecialEscape;
    default:
      return PrintAsCharLiteralTo<wchar_t>(c, os);
  }
}

// Prints a char c as if it's part of a string literal, escaping it when
// necessary; returns how c was formatted.
static CharFormat PrintAsStringLiteralTo(char c, ostream* os) {
  return PrintAsStringLiteralTo(
      static_cast<wchar_t>(static_cast<unsigned char>(c)), os);
}

// Prints a wide or narrow character c and its code.  '\0' is printed
// as "'\\0'", other unprintable characters are also properly escaped
// using the standard C++ escape sequence.  The template argument
// UnsignedChar is the unsigned version of Char, which is the type of c.
template <typename UnsignedChar, typename Char>
void PrintCharAndCodeTo(Char c, ostream* os) {
  // First, print c as a literal in the most readable form we can find.
  *os << ((sizeof(c) > 1) ? "L'" : "'");
  const CharFormat format = PrintAsCharLiteralTo<UnsignedChar>(c, os);
  *os << "'";

  // To aid user debugging, we also print c's code in decimal, unless
  // it's 0 (in which case c was printed as '\\0', making the code
  // obvious).
  if (c == 0)
    return;
  *os << " (" << static_cast<int>(c);

  // For more convenience, we print c's code again in hexidecimal,
  // unless c was already printed in the form '\x##' or the code is in
  // [1, 9].
  if (format == kHexEscape || (1 <= c && c <= 9)) {
    // Do nothing.
  } else {
    *os << ", 0x" << String::FormatHexInt(static_cast<UnsignedChar>(c));
  }
  *os << ")";
}

void PrintTo(unsigned char c, ::std::ostream* os) {
  PrintCharAndCodeTo<unsigned char>(c, os);
}
void PrintTo(signed char c, ::std::ostream* os) {
  PrintCharAndCodeTo<unsigned char>(c, os);
}

// Prints a wchar_t as a symbol if it is printable or as its internal
// code otherwise and also as its code.  L'\0' is printed as "L'\\0'".
void PrintTo(wchar_t wc, ostream* os) {
  PrintCharAndCodeTo<wchar_t>(wc, os);
}

// Prints the given array of characters to the ostream.  CharType must be either
// char or wchar_t.
// The array starts at begin, the length is len, it may include '\0' characters
// and may not be NUL-terminated.
template <typename CharType>
static void PrintCharsAsStringTo(
    const CharType* begin, size_t len, ostream* os) {
  const char* const kQuoteBegin = sizeof(CharType) == 1 ? "\"" : "L\"";
  *os << kQuoteBegin;
  bool is_previous_hex = false;
  for (size_t index = 0; index < len; ++index) {
    const CharType cur = begin[index];
    if (is_previous_hex && IsXDigit(cur)) {
      // Previous character is of '\x..' form and this character can be
      // interpreted as another hexadecimal digit in its number. Break string to
      // disambiguate.
      *os << "\" " << kQuoteBegin;
    }
    is_previous_hex = PrintAsStringLiteralTo(cur, os) == kHexEscape;
  }
  *os << "\"";
}

// Prints a (const) char/wchar_t array of 'len' elements, starting at address
// 'begin'.  CharType must be either char or wchar_t.
template <typename CharType>
static void UniversalPrintCharArray(
    const CharType* begin, size_t len, ostream* os) {
  // The code
  //   const char kFoo[] = "foo";
  // generates an array of 4, not 3, elements, with the last one being '\0'.
  //
  // Therefore when printing a char array, we don't print the last element if
  // it's '\0', such that the output matches the string literal as it's
  // written in the source code.
  if (len > 0 && begin[len - 1] == '\0') {
    PrintCharsAsStringTo(begin, len - 1, os);
    return;
  }

  // If, however, the last element in the array is not '\0', e.g.
  //    const char kFoo[] = { 'f', 'o', 'o' };
  // we must print the entire array.  We also print a message to indicate
  // that the array is not NUL-terminated.
  PrintCharsAsStringTo(begin, len, os);
  *os << " (no terminating NUL)";
}

// Prints a (const) char array of 'len' elements, starting at address 'begin'.
void UniversalPrintArray(const char* begin, size_t len, ostream* os) {
  UniversalPrintCharArray(begin, len, os);
}

// Prints a (const) wchar_t array of 'len' elements, starting at address
// 'begin'.
void UniversalPrintArray(const wchar_t* begin, size_t len, ostream* os) {
  UniversalPrintCharArray(begin, len, os);
}

// Prints the given C string to the ostream.
void PrintTo(const char* s, ostream* os) {
  if (s == NULL) {
    *os << "NULL";
  } else {
    *os << ImplicitCast_<const void*>(s) << " pointing to ";
    PrintCharsAsStringTo(s, strlen(s), os);
  }
}

// MSVC compiler can be configured to define whar_t as a typedef
// of unsigned short. Defining an overload for const wchar_t* in that case
// would cause pointers to unsigned shorts be printed as wide strings,
// possibly accessing more memory than intended and causing invalid
// memory accesses. MSVC defines _NATIVE_WCHAR_T_DEFINED symbol when
// wchar_t is implemented as a native type.
#if !defined(_MSC_VER) || defined(_NATIVE_WCHAR_T_DEFINED)
// Prints the given wide C string to the ostream.
void PrintTo(const wchar_t* s, ostream* os) {
  if (s == NULL) {
    *os << "NULL";
  } else {
    *os << ImplicitCast_<const void*>(s) << " pointing to ";
    PrintCharsAsStringTo(s, wcslen(s), os);
  }
}
#endif  // wchar_t is native

// Prints a ::string object.
#if GTEST_HAS_GLOBAL_STRING
void PrintStringTo(const ::string& s, ostream* os) {
  PrintCharsAsStringTo(s.data(), s.size(), os);
}
#endif  // GTEST_HAS_GLOBAL_STRING

void PrintStringTo(const ::std::string& s, ostream* os) {
  PrintCharsAsStringTo(s.data(), s.size(), os);
}

// Prints a ::wstring object.
#if GTEST_HAS_GLOBAL_WSTRING
void PrintWideStringTo(const ::wstring& s, ostream* os) {
  PrintCharsAsStringTo(s.data(), s.size(), os);
}
#endif  // GTEST_HAS_GLOBAL_WSTRING

#if GTEST_HAS_STD_WSTRING
void PrintWideStringTo(const ::std::wstring& s, ostream* os) {
  PrintCharsAsStringTo(s.data(), s.size(), os);
}
#endif  // GTEST_HAS_STD_WSTRING

}  // namespace internal

}  // namespace testing
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
// Author: mheule@google.com (Markus Heule)
//
// The Google C++ Testing Framework (Google Test)


// Indicates that this translation unit is part of Google Test's
// implementation.  It must come before gtest-internal-inl.h is
// included, or there will be a compiler error.  This trick is to
// prevent a user from accidentally including gtest-internal-inl.h in
// his code.
#define GTEST_IMPLEMENTATION_ 1
#undef GTEST_IMPLEMENTATION_

namespace testing {

using internal::GetUnitTestImpl;

// Gets the summary of the failure message by omitting the stack trace
// in it.
std::string TestPartResult::ExtractSummary(const char* message) {
  const char* const stack_trace = strstr(message, internal::kStackTraceMarker);
  return stack_trace == NULL ? message :
      std::string(message, stack_trace);
}

// Prints a TestPartResult object.
std::ostream& operator<<(std::ostream& os, const TestPartResult& result) {
  return os
      << result.file_name() << ":" << result.line_number() << ": "
      << (result.type() == TestPartResult::kSuccess ? "Success" :
          result.type() == TestPartResult::kFatalFailure ? "Fatal failure" :
          "Non-fatal failure") << ":\n"
      << result.message() << std::endl;
}

// Appends a TestPartResult to the array.
void TestPartResultArray::Append(const TestPartResult& result) {
  array_.push_back(result);
}

// Returns the TestPartResult at the given index (0-based).
const TestPartResult& TestPartResultArray::GetTestPartResult(int index) const {
  if (index < 0 || index >= size()) {
    printf("\nInvalid index (%d) into TestPartResultArray.\n", index);
    internal::posix::Abort();
  }

  return array_[index];
}

// Returns the number of TestPartResult objects in the array.
int TestPartResultArray::size() const {
  return static_cast<int>(array_.size());
}

namespace internal {

HasNewFatalFailureHelper::HasNewFatalFailureHelper()
    : has_new_fatal_failure_(false),
      original_reporter_(GetUnitTestImpl()->
                         GetTestPartResultReporterForCurrentThread()) {
  GetUnitTestImpl()->SetTestPartResultReporterForCurrentThread(this);
}

HasNewFatalFailureHelper::~HasNewFatalFailureHelper() {
  GetUnitTestImpl()->SetTestPartResultReporterForCurrentThread(
      original_reporter_);
}

void HasNewFatalFailureHelper::ReportTestPartResult(
    const TestPartResult& result) {
  if (result.fatally_failed())
    has_new_fatal_failure_ = true;
  original_reporter_->ReportTestPartResult(result);
}

}  // namespace internal

}  // namespace testing
// Copyright 2008 Google Inc.
// All Rights Reserved.
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


namespace testing {
namespace internal {

#if GTEST_HAS_TYPED_TEST_P

// Skips to the first non-space char in str. Returns an empty string if str
// contains only whitespace characters.
static const char* SkipSpaces(const char* str) {
  while (IsSpace(*str))
    str++;
  return str;
}

// Verifies that registered_tests match the test names in
// defined_test_names_; returns registered_tests if successful, or
// aborts the program otherwise.
const char* TypedTestCasePState::VerifyRegisteredTestNames(
    const char* file, int line, const char* registered_tests) {
  typedef ::std::set<const char*>::const_iterator DefinedTestIter;
  registered_ = true;

  // Skip initial whitespace in registered_tests since some
  // preprocessors prefix stringizied literals with whitespace.
  registered_tests = SkipSpaces(registered_tests);

  Message errors;
  ::std::set<std::string> tests;
  for (const char* names = registered_tests; names != NULL;
       names = SkipComma(names)) {
    const std::string name = GetPrefixUntilComma(names);
    if (tests.count(name) != 0) {
      errors << "Test " << name << " is listed more than once.\n";
      continue;
    }

    bool found = false;
    for (DefinedTestIter it = defined_test_names_.begin();
         it != defined_test_names_.end();
         ++it) {
      if (name == *it) {
        found = true;
        break;
      }
    }

    if (found) {
      tests.insert(name);
    } else {
      errors << "No test named " << name
             << " can be found in this test case.\n";
    }
  }

  for (DefinedTestIter it = defined_test_names_.begin();
       it != defined_test_names_.end();
       ++it) {
    if (tests.count(*it) == 0) {
      errors << "You forgot to list test " << *it << ".\n";
    }
  }

  const std::string& errors_str = errors.GetString();
  if (errors_str != "") {
    fprintf(stderr, "%s %s", FormatFileLocation(file, line).c_str(),
            errors_str.c_str());
    fflush(stderr);
    posix::Abort();
  }

  return registered_tests;
}

#endif  // GTEST_HAS_TYPED_TEST_P

}  // namespace internal
}  // namespace testing
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
//
// Google C++ Mocking Framework (Google Mock)
//
// This file #includes all Google Mock implementation .cc files.  The
// purpose is to allow a user to build Google Mock by compiling this
// file alone.

// This line ensures that gmock.h can be compiled on its own, even
// when it's fused.
#include "gmock/gmock.h"

// The following lines pull in the real gmock *.cc files.
// Copyright 2007, Google Inc.
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
// This file implements cardinalities.


#include <limits.h>
#include <ostream>  // NOLINT
#include <sstream>
#include <string>

namespace testing {

namespace {

// Implements the Between(m, n) cardinality.
class BetweenCardinalityImpl : public CardinalityInterface {
 public:
  BetweenCardinalityImpl(int min, int max)
      : min_(min >= 0 ? min : 0),
        max_(max >= min_ ? max : min_) {
    std::stringstream ss;
    if (min < 0) {
      ss << "The invocation lower bound must be >= 0, "
         << "but is actually " << min << ".";
      internal::Expect(false, __FILE__, __LINE__, ss.str());
    } else if (max < 0) {
      ss << "The invocation upper bound must be >= 0, "
         << "but is actually " << max << ".";
      internal::Expect(false, __FILE__, __LINE__, ss.str());
    } else if (min > max) {
      ss << "The invocation upper bound (" << max
         << ") must be >= the invocation lower bound (" << min
         << ").";
      internal::Expect(false, __FILE__, __LINE__, ss.str());
    }
  }

  // Conservative estimate on the lower/upper bound of the number of
  // calls allowed.
  virtual int ConservativeLowerBound() const { return min_; }
  virtual int ConservativeUpperBound() const { return max_; }

  virtual bool IsSatisfiedByCallCount(int call_count) const {
    return min_ <= call_count && call_count <= max_;
  }

  virtual bool IsSaturatedByCallCount(int call_count) const {
    return call_count >= max_;
  }

  virtual void DescribeTo(::std::ostream* os) const;

 private:
  const int min_;
  const int max_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(BetweenCardinalityImpl);
};

// Formats "n times" in a human-friendly way.
inline internal::string FormatTimes(int n) {
  if (n == 1) {
    return "once";
  } else if (n == 2) {
    return "twice";
  } else {
    std::stringstream ss;
    ss << n << " times";
    return ss.str();
  }
}

// Describes the Between(m, n) cardinality in human-friendly text.
void BetweenCardinalityImpl::DescribeTo(::std::ostream* os) const {
  if (min_ == 0) {
    if (max_ == 0) {
      *os << "never called";
    } else if (max_ == INT_MAX) {
      *os << "called any number of times";
    } else {
      *os << "called at most " << FormatTimes(max_);
    }
  } else if (min_ == max_) {
    *os << "called " << FormatTimes(min_);
  } else if (max_ == INT_MAX) {
    *os << "called at least " << FormatTimes(min_);
  } else {
    // 0 < min_ < max_ < INT_MAX
    *os << "called between " << min_ << " and " << max_ << " times";
  }
}

}  // Unnamed namespace

// Describes the given call count to an ostream.
void Cardinality::DescribeActualCallCountTo(int actual_call_count,
                                            ::std::ostream* os) {
  if (actual_call_count > 0) {
    *os << "called " << FormatTimes(actual_call_count);
  } else {
    *os << "never called";
  }
}

// Creates a cardinality that allows at least n calls.
GTEST_API_ Cardinality AtLeast(int n) { return Between(n, INT_MAX); }

// Creates a cardinality that allows at most n calls.
GTEST_API_ Cardinality AtMost(int n) { return Between(0, n); }

// Creates a cardinality that allows any number of calls.
GTEST_API_ Cardinality AnyNumber() { return AtLeast(0); }

// Creates a cardinality that allows between min and max calls.
GTEST_API_ Cardinality Between(int min, int max) {
  return Cardinality(new BetweenCardinalityImpl(min, max));
}

// Creates a cardinality that allows exactly n calls.
GTEST_API_ Cardinality Exactly(int n) { return Between(n, n); }

}  // namespace testing
// Copyright 2007, Google Inc.
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
// This file defines some utilities useful for implementing Google
// Mock.  They are subject to change without notice, so please DO NOT
// USE THEM IN USER CODE.


#include <ctype.h>
#include <ostream>  // NOLINT
#include <string>

namespace testing {
namespace internal {

// Converts an identifier name to a space-separated list of lower-case
// words.  Each maximum substring of the form [A-Za-z][a-z]*|\d+ is
// treated as one word.  For example, both "FooBar123" and
// "foo_bar_123" are converted to "foo bar 123".
GTEST_API_ string ConvertIdentifierNameToWords(const char* id_name) {
  string result;
  char prev_char = '\0';
  for (const char* p = id_name; *p != '\0'; prev_char = *(p++)) {
    // We don't care about the current locale as the input is
    // guaranteed to be a valid C++ identifier name.
    const bool starts_new_word = IsUpper(*p) ||
        (!IsAlpha(prev_char) && IsLower(*p)) ||
        (!IsDigit(prev_char) && IsDigit(*p));

    if (IsAlNum(*p)) {
      if (starts_new_word && result != "")
        result += ' ';
      result += ToLower(*p);
    }
  }
  return result;
}

// This class reports Google Mock failures as Google Test failures.  A
// user can define another class in a similar fashion if he intends to
// use Google Mock with a testing framework other than Google Test.
class GoogleTestFailureReporter : public FailureReporterInterface {
 public:
  virtual void ReportFailure(FailureType type, const char* file, int line,
                             const string& message) {
    AssertHelper(type == kFatal ?
                 TestPartResult::kFatalFailure :
                 TestPartResult::kNonFatalFailure,
                 file,
                 line,
                 message.c_str()) = Message();
    if (type == kFatal) {
      posix::Abort();
    }
  }
};

// Returns the global failure reporter.  Will create a
// GoogleTestFailureReporter and return it the first time called.
GTEST_API_ FailureReporterInterface* GetFailureReporter() {
  // Points to the global failure reporter used by Google Mock.  gcc
  // guarantees that the following use of failure_reporter is
  // thread-safe.  We may need to add additional synchronization to
  // protect failure_reporter if we port Google Mock to other
  // compilers.
  static FailureReporterInterface* const failure_reporter =
      new GoogleTestFailureReporter();
  return failure_reporter;
}

// Protects global resources (stdout in particular) used by Log().
static GTEST_DEFINE_STATIC_MUTEX_(g_log_mutex);

// Returns true iff a log with the given severity is visible according
// to the --gmock_verbose flag.
GTEST_API_ bool LogIsVisible(LogSeverity severity) {
  if (GMOCK_FLAG(verbose) == kInfoVerbosity) {
    // Always show the log if --gmock_verbose=info.
    return true;
  } else if (GMOCK_FLAG(verbose) == kErrorVerbosity) {
    // Always hide it if --gmock_verbose=error.
    return false;
  } else {
    // If --gmock_verbose is neither "info" nor "error", we treat it
    // as "warning" (its default value).
    return severity == kWarning;
  }
}

// Prints the given message to stdout iff 'severity' >= the level
// specified by the --gmock_verbose flag.  If stack_frames_to_skip >=
// 0, also prints the stack trace excluding the top
// stack_frames_to_skip frames.  In opt mode, any positive
// stack_frames_to_skip is treated as 0, since we don't know which
// function calls will be inlined by the compiler and need to be
// conservative.
GTEST_API_ void Log(LogSeverity severity,
                    const string& message,
                    int stack_frames_to_skip) {
  if (!LogIsVisible(severity))
    return;

  // Ensures that logs from different threads don't interleave.
  MutexLock l(&g_log_mutex);

  // "using ::std::cout;" doesn't work with Symbian's STLport, where cout is a
  // macro.

  if (severity == kWarning) {
    // Prints a GMOCK WARNING marker to make the warnings easily searchable.
    std::cout << "\nGMOCK WARNING:";
  }
  // Pre-pends a new-line to message if it doesn't start with one.
  if (message.empty() || message[0] != '\n') {
    std::cout << "\n";
  }
  std::cout << message;
  if (stack_frames_to_skip >= 0) {
#ifdef NDEBUG
    // In opt mode, we have to be conservative and skip no stack frame.
    const int actual_to_skip = 0;
#else
    // In dbg mode, we can do what the caller tell us to do (plus one
    // for skipping this function's stack frame).
    const int actual_to_skip = stack_frames_to_skip + 1;
#endif  // NDEBUG

    // Appends a new-line to message if it doesn't end with one.
    if (!message.empty() && *message.rbegin() != '\n') {
      std::cout << "\n";
    }
    std::cout << "Stack trace:\n"
         << ::testing::internal::GetCurrentOsStackTraceExceptTop(
             ::testing::UnitTest::GetInstance(), actual_to_skip);
  }
  std::cout << ::std::flush;
}

}  // namespace internal
}  // namespace testing
// Copyright 2007, Google Inc.
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
// This file implements Matcher<const string&>, Matcher<string>, and
// utilities for defining matchers.


#include <string.h>
#include <sstream>
#include <string>

namespace testing {

// Constructs a matcher that matches a const string& whose value is
// equal to s.
Matcher<const internal::string&>::Matcher(const internal::string& s) {
  *this = Eq(s);
}

// Constructs a matcher that matches a const string& whose value is
// equal to s.
Matcher<const internal::string&>::Matcher(const char* s) {
  *this = Eq(internal::string(s));
}

// Constructs a matcher that matches a string whose value is equal to s.
Matcher<internal::string>::Matcher(const internal::string& s) { *this = Eq(s); }

// Constructs a matcher that matches a string whose value is equal to s.
Matcher<internal::string>::Matcher(const char* s) {
  *this = Eq(internal::string(s));
}

#if GTEST_HAS_STRING_PIECE_
// Constructs a matcher that matches a const StringPiece& whose value is
// equal to s.
Matcher<const StringPiece&>::Matcher(const internal::string& s) {
  *this = Eq(s);
}

// Constructs a matcher that matches a const StringPiece& whose value is
// equal to s.
Matcher<const StringPiece&>::Matcher(const char* s) {
  *this = Eq(internal::string(s));
}

// Constructs a matcher that matches a const StringPiece& whose value is
// equal to s.
Matcher<const StringPiece&>::Matcher(StringPiece s) {
  *this = Eq(s.ToString());
}

// Constructs a matcher that matches a StringPiece whose value is equal to s.
Matcher<StringPiece>::Matcher(const internal::string& s) {
  *this = Eq(s);
}

// Constructs a matcher that matches a StringPiece whose value is equal to s.
Matcher<StringPiece>::Matcher(const char* s) {
  *this = Eq(internal::string(s));
}

// Constructs a matcher that matches a StringPiece whose value is equal to s.
Matcher<StringPiece>::Matcher(StringPiece s) {
  *this = Eq(s.ToString());
}
#endif  // GTEST_HAS_STRING_PIECE_

namespace internal {

// Joins a vector of strings as if they are fields of a tuple; returns
// the joined string.
GTEST_API_ string JoinAsTuple(const Strings& fields) {
  switch (fields.size()) {
    case 0:
      return "";
    case 1:
      return fields[0];
    default:
      string result = "(" + fields[0];
      for (size_t i = 1; i < fields.size(); i++) {
        result += ", ";
        result += fields[i];
      }
      result += ")";
      return result;
  }
}

// Returns the description for a matcher defined using the MATCHER*()
// macro where the user-supplied description string is "", if
// 'negation' is false; otherwise returns the description of the
// negation of the matcher.  'param_values' contains a list of strings
// that are the print-out of the matcher's parameters.
GTEST_API_ string FormatMatcherDescription(bool negation,
                                           const char* matcher_name,
                                           const Strings& param_values) {
  string result = ConvertIdentifierNameToWords(matcher_name);
  if (param_values.size() >= 1)
    result += " " + JoinAsTuple(param_values);
  return negation ? "not (" + result + ")" : result;
}

// FindMaxBipartiteMatching and its helper class.
//
// Uses the well-known Ford-Fulkerson max flow method to find a maximum
// bipartite matching. Flow is considered to be from left to right.
// There is an implicit source node that is connected to all of the left
// nodes, and an implicit sink node that is connected to all of the
// right nodes. All edges have unit capacity.
//
// Neither the flow graph nor the residual flow graph are represented
// explicitly. Instead, they are implied by the information in 'graph' and
// a vector<int> called 'left_' whose elements are initialized to the
// value kUnused. This represents the initial state of the algorithm,
// where the flow graph is empty, and the residual flow graph has the
// following edges:
//   - An edge from source to each left_ node
//   - An edge from each right_ node to sink
//   - An edge from each left_ node to each right_ node, if the
//     corresponding edge exists in 'graph'.
//
// When the TryAugment() method adds a flow, it sets left_[l] = r for some
// nodes l and r. This induces the following changes:
//   - The edges (source, l), (l, r), and (r, sink) are added to the
//     flow graph.
//   - The same three edges are removed from the residual flow graph.
//   - The reverse edges (l, source), (r, l), and (sink, r) are added
//     to the residual flow graph, which is a directional graph
//     representing unused flow capacity.
//
// When the method augments a flow (moving left_[l] from some r1 to some
// other r2), this can be thought of as "undoing" the above steps with
// respect to r1 and "redoing" them with respect to r2.
//
// It bears repeating that the flow graph and residual flow graph are
// never represented explicitly, but can be derived by looking at the
// information in 'graph' and in left_.
//
// As an optimization, there is a second vector<int> called right_ which
// does not provide any new information. Instead, it enables more
// efficient queries about edges entering or leaving the right-side nodes
// of the flow or residual flow graphs. The following invariants are
// maintained:
//
// left[l] == kUnused or right[left[l]] == l
// right[r] == kUnused or left[right[r]] == r
//
// . [ source ]                                        .
// .   |||                                             .
// .   |||                                             .
// .   ||\--> left[0]=1  ---\    right[0]=-1 ----\     .
// .   ||                   |                    |     .
// .   |\---> left[1]=-1    \--> right[1]=0  ---\|     .
// .   |                                        ||     .
// .   \----> left[2]=2  ------> right[2]=2  --\||     .
// .                                           |||     .
// .         elements           matchers       vvv     .
// .                                         [ sink ]  .
//
// See Also:
//   [1] Cormen, et al (2001). "Section 26.2: The Ford–Fulkerson method".
//       "Introduction to Algorithms (Second ed.)", pp. 651–664.
//   [2] "Ford–Fulkerson algorithm", Wikipedia,
//       'http://en.wikipedia.org/wiki/Ford%E2%80%93Fulkerson_algorithm'
class MaxBipartiteMatchState {
 public:
  explicit MaxBipartiteMatchState(const MatchMatrix& graph)
      : graph_(&graph),
        left_(graph_->LhsSize(), kUnused),
        right_(graph_->RhsSize(), kUnused) {
  }

  // Returns the edges of a maximal match, each in the form {left, right}.
  ElementMatcherPairs Compute() {
    // 'seen' is used for path finding { 0: unseen, 1: seen }.
    ::std::vector<char> seen;
    // Searches the residual flow graph for a path from each left node to
    // the sink in the residual flow graph, and if one is found, add flow
    // to the graph. It's okay to search through the left nodes once. The
    // edge from the implicit source node to each previously-visited left
    // node will have flow if that left node has any path to the sink
    // whatsoever. Subsequent augmentations can only add flow to the
    // network, and cannot take away that previous flow unit from the source.
    // Since the source-to-left edge can only carry one flow unit (or,
    // each element can be matched to only one matcher), there is no need
    // to visit the left nodes more than once looking for augmented paths.
    // The flow is known to be possible or impossible by looking at the
    // node once.
    for (size_t ilhs = 0; ilhs < graph_->LhsSize(); ++ilhs) {
      // Reset the path-marking vector and try to find a path from
      // source to sink starting at the left_[ilhs] node.
      GTEST_CHECK_(left_[ilhs] == kUnused)
          << "ilhs: " << ilhs << ", left_[ilhs]: " << left_[ilhs];
      // 'seen' initialized to 'graph_->RhsSize()' copies of 0.
      seen.assign(graph_->RhsSize(), 0);
      TryAugment(ilhs, &seen);
    }
    ElementMatcherPairs result;
    for (size_t ilhs = 0; ilhs < left_.size(); ++ilhs) {
      size_t irhs = left_[ilhs];
      if (irhs == kUnused) continue;
      result.push_back(ElementMatcherPair(ilhs, irhs));
    }
    return result;
  }

 private:
  static const size_t kUnused = static_cast<size_t>(-1);

  // Perform a depth-first search from left node ilhs to the sink.  If a
  // path is found, flow is added to the network by linking the left and
  // right vector elements corresponding each segment of the path.
  // Returns true if a path to sink was found, which means that a unit of
  // flow was added to the network. The 'seen' vector elements correspond
  // to right nodes and are marked to eliminate cycles from the search.
  //
  // Left nodes will only be explored at most once because they
  // are accessible from at most one right node in the residual flow
  // graph.
  //
  // Note that left_[ilhs] is the only element of left_ that TryAugment will
  // potentially transition from kUnused to another value. Any other
  // left_ element holding kUnused before TryAugment will be holding it
  // when TryAugment returns.
  //
  bool TryAugment(size_t ilhs, ::std::vector<char>* seen) {
    for (size_t irhs = 0; irhs < graph_->RhsSize(); ++irhs) {
      if ((*seen)[irhs])
        continue;
      if (!graph_->HasEdge(ilhs, irhs))
        continue;
      // There's an available edge from ilhs to irhs.
      (*seen)[irhs] = 1;
      // Next a search is performed to determine whether
      // this edge is a dead end or leads to the sink.
      //
      // right_[irhs] == kUnused means that there is residual flow from
      // right node irhs to the sink, so we can use that to finish this
      // flow path and return success.
      //
      // Otherwise there is residual flow to some ilhs. We push flow
      // along that path and call ourselves recursively to see if this
      // ultimately leads to sink.
      if (right_[irhs] == kUnused || TryAugment(right_[irhs], seen)) {
        // Add flow from left_[ilhs] to right_[irhs].
        left_[ilhs] = irhs;
        right_[irhs] = ilhs;
        return true;
      }
    }
    return false;
  }

  const MatchMatrix* graph_;  // not owned
  // Each element of the left_ vector represents a left hand side node
  // (i.e. an element) and each element of right_ is a right hand side
  // node (i.e. a matcher). The values in the left_ vector indicate
  // outflow from that node to a node on the the right_ side. The values
  // in the right_ indicate inflow, and specify which left_ node is
  // feeding that right_ node, if any. For example, left_[3] == 1 means
  // there's a flow from element #3 to matcher #1. Such a flow would also
  // be redundantly represented in the right_ vector as right_[1] == 3.
  // Elements of left_ and right_ are either kUnused or mutually
  // referent. Mutually referent means that left_[right_[i]] = i and
  // right_[left_[i]] = i.
  ::std::vector<size_t> left_;
  ::std::vector<size_t> right_;

  GTEST_DISALLOW_ASSIGN_(MaxBipartiteMatchState);
};

const size_t MaxBipartiteMatchState::kUnused;

GTEST_API_ ElementMatcherPairs
FindMaxBipartiteMatching(const MatchMatrix& g) {
  return MaxBipartiteMatchState(g).Compute();
}

static void LogElementMatcherPairVec(const ElementMatcherPairs& pairs,
                                     ::std::ostream* stream) {
  typedef ElementMatcherPairs::const_iterator Iter;
  ::std::ostream& os = *stream;
  os << "{";
  const char *sep = "";
  for (Iter it = pairs.begin(); it != pairs.end(); ++it) {
    os << sep << "\n  ("
       << "element #" << it->first << ", "
       << "matcher #" << it->second << ")";
    sep = ",";
  }
  os << "\n}";
}

// Tries to find a pairing, and explains the result.
GTEST_API_ bool FindPairing(const MatchMatrix& matrix,
                            MatchResultListener* listener) {
  ElementMatcherPairs matches = FindMaxBipartiteMatching(matrix);

  size_t max_flow = matches.size();
  bool result = (max_flow == matrix.RhsSize());

  if (!result) {
    if (listener->IsInterested()) {
      *listener << "where no permutation of the elements can "
                   "satisfy all matchers, and the closest match is "
                << max_flow << " of " << matrix.RhsSize()
                << " matchers with the pairings:\n";
      LogElementMatcherPairVec(matches, listener->stream());
    }
    return false;
  }

  if (matches.size() > 1) {
    if (listener->IsInterested()) {
      const char *sep = "where:\n";
      for (size_t mi = 0; mi < matches.size(); ++mi) {
        *listener << sep << " - element #" << matches[mi].first
                  << " is matched by matcher #" << matches[mi].second;
        sep = ",\n";
      }
    }
  }
  return true;
}

bool MatchMatrix::NextGraph() {
  for (size_t ilhs = 0; ilhs < LhsSize(); ++ilhs) {
    for (size_t irhs = 0; irhs < RhsSize(); ++irhs) {
      char& b = matched_[SpaceIndex(ilhs, irhs)];
      if (!b) {
        b = 1;
        return true;
      }
      b = 0;
    }
  }
  return false;
}

void MatchMatrix::Randomize() {
  for (size_t ilhs = 0; ilhs < LhsSize(); ++ilhs) {
    for (size_t irhs = 0; irhs < RhsSize(); ++irhs) {
      char& b = matched_[SpaceIndex(ilhs, irhs)];
      b = static_cast<char>(rand() & 1);  // NOLINT
    }
  }
}

string MatchMatrix::DebugString() const {
  ::std::stringstream ss;
  const char *sep = "";
  for (size_t i = 0; i < LhsSize(); ++i) {
    ss << sep;
    for (size_t j = 0; j < RhsSize(); ++j) {
      ss << HasEdge(i, j);
    }
    sep = ";";
  }
  return ss.str();
}

void UnorderedElementsAreMatcherImplBase::DescribeToImpl(
    ::std::ostream* os) const {
  if (matcher_describers_.empty()) {
    *os << "is empty";
    return;
  }
  if (matcher_describers_.size() == 1) {
    *os << "has " << Elements(1) << " and that element ";
    matcher_describers_[0]->DescribeTo(os);
    return;
  }
  *os << "has " << Elements(matcher_describers_.size())
      << " and there exists some permutation of elements such that:\n";
  const char* sep = "";
  for (size_t i = 0; i != matcher_describers_.size(); ++i) {
    *os << sep << " - element #" << i << " ";
    matcher_describers_[i]->DescribeTo(os);
    sep = ", and\n";
  }
}

void UnorderedElementsAreMatcherImplBase::DescribeNegationToImpl(
    ::std::ostream* os) const {
  if (matcher_describers_.empty()) {
    *os << "isn't empty";
    return;
  }
  if (matcher_describers_.size() == 1) {
    *os << "doesn't have " << Elements(1)
        << ", or has " << Elements(1) << " that ";
    matcher_describers_[0]->DescribeNegationTo(os);
    return;
  }
  *os << "doesn't have " << Elements(matcher_describers_.size())
      << ", or there exists no permutation of elements such that:\n";
  const char* sep = "";
  for (size_t i = 0; i != matcher_describers_.size(); ++i) {
    *os << sep << " - element #" << i << " ";
    matcher_describers_[i]->DescribeTo(os);
    sep = ", and\n";
  }
}

// Checks that all matchers match at least one element, and that all
// elements match at least one matcher. This enables faster matching
// and better error reporting.
// Returns false, writing an explanation to 'listener', if and only
// if the success criteria are not met.
bool UnorderedElementsAreMatcherImplBase::
VerifyAllElementsAndMatchersAreMatched(
    const ::std::vector<string>& element_printouts,
    const MatchMatrix& matrix,
    MatchResultListener* listener) const {
  bool result = true;
  ::std::vector<char> element_matched(matrix.LhsSize(), 0);
  ::std::vector<char> matcher_matched(matrix.RhsSize(), 0);

  for (size_t ilhs = 0; ilhs < matrix.LhsSize(); ilhs++) {
    for (size_t irhs = 0; irhs < matrix.RhsSize(); irhs++) {
      char matched = matrix.HasEdge(ilhs, irhs);
      element_matched[ilhs] |= matched;
      matcher_matched[irhs] |= matched;
    }
  }

  {
    const char* sep =
        "where the following matchers don't match any elements:\n";
    for (size_t mi = 0; mi < matcher_matched.size(); ++mi) {
      if (matcher_matched[mi])
        continue;
      result = false;
      if (listener->IsInterested()) {
        *listener << sep << "matcher #" << mi << ": ";
        matcher_describers_[mi]->DescribeTo(listener->stream());
        sep = ",\n";
      }
    }
  }

  {
    const char* sep =
        "where the following elements don't match any matchers:\n";
    const char* outer_sep = "";
    if (!result) {
      outer_sep = "\nand ";
    }
    for (size_t ei = 0; ei < element_matched.size(); ++ei) {
      if (element_matched[ei])
        continue;
      result = false;
      if (listener->IsInterested()) {
        *listener << outer_sep << sep << "element #" << ei << ": "
                  << element_printouts[ei];
        sep = ",\n";
        outer_sep = "";
      }
    }
  }
  return result;
}

}  // namespace internal
}  // namespace testing
// Copyright 2007, Google Inc.
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
// This file implements the spec builder syntax (ON_CALL and
// EXPECT_CALL).


#include <stdlib.h>
#include <iostream>  // NOLINT
#include <map>
#include <set>
#include <string>

#if GTEST_OS_CYGWIN || GTEST_OS_LINUX || GTEST_OS_MAC
# include <unistd.h>  // NOLINT
#endif

namespace testing {
namespace internal {

// Protects the mock object registry (in class Mock), all function
// mockers, and all expectations.
GTEST_API_ GTEST_DEFINE_STATIC_MUTEX_(g_gmock_mutex);

// Logs a message including file and line number information.
GTEST_API_ void LogWithLocation(testing::internal::LogSeverity severity,
                                const char* file, int line,
                                const string& message) {
  ::std::ostringstream s;
  s << file << ":" << line << ": " << message << ::std::endl;
  Log(severity, s.str(), 0);
}

// Constructs an ExpectationBase object.
ExpectationBase::ExpectationBase(const char* a_file,
                                 int a_line,
                                 const string& a_source_text)
    : file_(a_file),
      line_(a_line),
      source_text_(a_source_text),
      cardinality_specified_(false),
      cardinality_(Exactly(1)),
      call_count_(0),
      retired_(false),
      extra_matcher_specified_(false),
      repeated_action_specified_(false),
      retires_on_saturation_(false),
      last_clause_(kNone),
      action_count_checked_(false) {}

// Destructs an ExpectationBase object.
ExpectationBase::~ExpectationBase() {}

// Explicitly specifies the cardinality of this expectation.  Used by
// the subclasses to implement the .Times() clause.
void ExpectationBase::SpecifyCardinality(const Cardinality& a_cardinality) {
  cardinality_specified_ = true;
  cardinality_ = a_cardinality;
}

// Retires all pre-requisites of this expectation.
void ExpectationBase::RetireAllPreRequisites()
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
  if (is_retired()) {
    // We can take this short-cut as we never retire an expectation
    // until we have retired all its pre-requisites.
    return;
  }

  for (ExpectationSet::const_iterator it = immediate_prerequisites_.begin();
       it != immediate_prerequisites_.end(); ++it) {
    ExpectationBase* const prerequisite = it->expectation_base().get();
    if (!prerequisite->is_retired()) {
      prerequisite->RetireAllPreRequisites();
      prerequisite->Retire();
    }
  }
}

// Returns true iff all pre-requisites of this expectation have been
// satisfied.
bool ExpectationBase::AllPrerequisitesAreSatisfied() const
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
  g_gmock_mutex.AssertHeld();
  for (ExpectationSet::const_iterator it = immediate_prerequisites_.begin();
       it != immediate_prerequisites_.end(); ++it) {
    if (!(it->expectation_base()->IsSatisfied()) ||
        !(it->expectation_base()->AllPrerequisitesAreSatisfied()))
      return false;
  }
  return true;
}

// Adds unsatisfied pre-requisites of this expectation to 'result'.
void ExpectationBase::FindUnsatisfiedPrerequisites(ExpectationSet* result) const
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
  g_gmock_mutex.AssertHeld();
  for (ExpectationSet::const_iterator it = immediate_prerequisites_.begin();
       it != immediate_prerequisites_.end(); ++it) {
    if (it->expectation_base()->IsSatisfied()) {
      // If *it is satisfied and has a call count of 0, some of its
      // pre-requisites may not be satisfied yet.
      if (it->expectation_base()->call_count_ == 0) {
        it->expectation_base()->FindUnsatisfiedPrerequisites(result);
      }
    } else {
      // Now that we know *it is unsatisfied, we are not so interested
      // in whether its pre-requisites are satisfied.  Therefore we
      // don't recursively call FindUnsatisfiedPrerequisites() here.
      *result += *it;
    }
  }
}

// Describes how many times a function call matching this
// expectation has occurred.
void ExpectationBase::DescribeCallCountTo(::std::ostream* os) const
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
  g_gmock_mutex.AssertHeld();

  // Describes how many times the function is expected to be called.
  *os << "         Expected: to be ";
  cardinality().DescribeTo(os);
  *os << "\n           Actual: ";
  Cardinality::DescribeActualCallCountTo(call_count(), os);

  // Describes the state of the expectation (e.g. is it satisfied?
  // is it active?).
  *os << " - " << (IsOverSaturated() ? "over-saturated" :
                   IsSaturated() ? "saturated" :
                   IsSatisfied() ? "satisfied" : "unsatisfied")
      << " and "
      << (is_retired() ? "retired" : "active");
}

// Checks the action count (i.e. the number of WillOnce() and
// WillRepeatedly() clauses) against the cardinality if this hasn't
// been done before.  Prints a warning if there are too many or too
// few actions.
void ExpectationBase::CheckActionCountIfNotDone() const
    GTEST_LOCK_EXCLUDED_(mutex_) {
  bool should_check = false;
  {
    MutexLock l(&mutex_);
    if (!action_count_checked_) {
      action_count_checked_ = true;
      should_check = true;
    }
  }

  if (should_check) {
    if (!cardinality_specified_) {
      // The cardinality was inferred - no need to check the action
      // count against it.
      return;
    }

    // The cardinality was explicitly specified.
    const int action_count = static_cast<int>(untyped_actions_.size());
    const int upper_bound = cardinality().ConservativeUpperBound();
    const int lower_bound = cardinality().ConservativeLowerBound();
    bool too_many;  // True if there are too many actions, or false
    // if there are too few.
    if (action_count > upper_bound ||
        (action_count == upper_bound && repeated_action_specified_)) {
      too_many = true;
    } else if (0 < action_count && action_count < lower_bound &&
               !repeated_action_specified_) {
      too_many = false;
    } else {
      return;
    }

    ::std::stringstream ss;
    DescribeLocationTo(&ss);
    ss << "Too " << (too_many ? "many" : "few")
       << " actions specified in " << source_text() << "...\n"
       << "Expected to be ";
    cardinality().DescribeTo(&ss);
    ss << ", but has " << (too_many ? "" : "only ")
       << action_count << " WillOnce()"
       << (action_count == 1 ? "" : "s");
    if (repeated_action_specified_) {
      ss << " and a WillRepeatedly()";
    }
    ss << ".";
    Log(kWarning, ss.str(), -1);  // -1 means "don't print stack trace".
  }
}

// Implements the .Times() clause.
void ExpectationBase::UntypedTimes(const Cardinality& a_cardinality) {
  if (last_clause_ == kTimes) {
    ExpectSpecProperty(false,
                       ".Times() cannot appear "
                       "more than once in an EXPECT_CALL().");
  } else {
    ExpectSpecProperty(last_clause_ < kTimes,
                       ".Times() cannot appear after "
                       ".InSequence(), .WillOnce(), .WillRepeatedly(), "
                       "or .RetiresOnSaturation().");
  }
  last_clause_ = kTimes;

  SpecifyCardinality(a_cardinality);
}

// Points to the implicit sequence introduced by a living InSequence
// object (if any) in the current thread or NULL.
GTEST_API_ ThreadLocal<Sequence*> g_gmock_implicit_sequence;

// Reports an uninteresting call (whose description is in msg) in the
// manner specified by 'reaction'.
void ReportUninterestingCall(CallReaction reaction, const string& msg) {
  switch (reaction) {
    case kAllow:
      Log(kInfo, msg, 3);
      break;
    case kWarn:
      Log(kWarning, msg, 3);
      break;
    default:  // FAIL
      Expect(false, NULL, -1, msg);
  }
}

UntypedFunctionMockerBase::UntypedFunctionMockerBase()
    : mock_obj_(NULL), name_("") {}

UntypedFunctionMockerBase::~UntypedFunctionMockerBase() {}

// Sets the mock object this mock method belongs to, and registers
// this information in the global mock registry.  Will be called
// whenever an EXPECT_CALL() or ON_CALL() is executed on this mock
// method.
void UntypedFunctionMockerBase::RegisterOwner(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
  {
    MutexLock l(&g_gmock_mutex);
    mock_obj_ = mock_obj;
  }
  Mock::Register(mock_obj, this);
}

// Sets the mock object this mock method belongs to, and sets the name
// of the mock function.  Will be called upon each invocation of this
// mock function.
void UntypedFunctionMockerBase::SetOwnerAndName(const void* mock_obj,
                                                const char* name)
    GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
  // We protect name_ under g_gmock_mutex in case this mock function
  // is called from two threads concurrently.
  MutexLock l(&g_gmock_mutex);
  mock_obj_ = mock_obj;
  name_ = name;
}

// Returns the name of the function being mocked.  Must be called
// after RegisterOwner() or SetOwnerAndName() has been called.
const void* UntypedFunctionMockerBase::MockObject() const
    GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
  const void* mock_obj;
  {
    // We protect mock_obj_ under g_gmock_mutex in case this mock
    // function is called from two threads concurrently.
    MutexLock l(&g_gmock_mutex);
    Assert(mock_obj_ != NULL, __FILE__, __LINE__,
           "MockObject() must not be called before RegisterOwner() or "
           "SetOwnerAndName() has been called.");
    mock_obj = mock_obj_;
  }
  return mock_obj;
}

// Returns the name of this mock method.  Must be called after
// SetOwnerAndName() has been called.
const char* UntypedFunctionMockerBase::Name() const
    GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
  const char* name;
  {
    // We protect name_ under g_gmock_mutex in case this mock
    // function is called from two threads concurrently.
    MutexLock l(&g_gmock_mutex);
    Assert(name_ != NULL, __FILE__, __LINE__,
           "Name() must not be called before SetOwnerAndName() has "
           "been called.");
    name = name_;
  }
  return name;
}

// Calculates the result of invoking this mock function with the given
// arguments, prints it, and returns it.  The caller is responsible
// for deleting the result.
const UntypedActionResultHolderBase*
UntypedFunctionMockerBase::UntypedInvokeWith(const void* const untyped_args)
    GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
  if (untyped_expectations_.size() == 0) {
    // No expectation is set on this mock method - we have an
    // uninteresting call.

    // We must get Google Mock's reaction on uninteresting calls
    // made on this mock object BEFORE performing the action,
    // because the action may DELETE the mock object and make the
    // following expression meaningless.
    const CallReaction reaction =
        Mock::GetReactionOnUninterestingCalls(MockObject());

    // True iff we need to print this call's arguments and return
    // value.  This definition must be kept in sync with
    // the behavior of ReportUninterestingCall().
    const bool need_to_report_uninteresting_call =
        // If the user allows this uninteresting call, we print it
        // only when he wants informational messages.
        reaction == kAllow ? LogIsVisible(kInfo) :
        // If the user wants this to be a warning, we print it only
        // when he wants to see warnings.
        reaction == kWarn ? LogIsVisible(kWarning) :
        // Otherwise, the user wants this to be an error, and we
        // should always print detailed information in the error.
        true;

    if (!need_to_report_uninteresting_call) {
      // Perform the action without printing the call information.
      return this->UntypedPerformDefaultAction(untyped_args, "");
    }

    // Warns about the uninteresting call.
    ::std::stringstream ss;
    this->UntypedDescribeUninterestingCall(untyped_args, &ss);

    // Calculates the function result.
    const UntypedActionResultHolderBase* const result =
        this->UntypedPerformDefaultAction(untyped_args, ss.str());

    // Prints the function result.
    if (result != NULL)
      result->PrintAsActionResult(&ss);

    ReportUninterestingCall(reaction, ss.str());
    return result;
  }

  bool is_excessive = false;
  ::std::stringstream ss;
  ::std::stringstream why;
  ::std::stringstream loc;
  const void* untyped_action = NULL;

  // The UntypedFindMatchingExpectation() function acquires and
  // releases g_gmock_mutex.
  const ExpectationBase* const untyped_expectation =
      this->UntypedFindMatchingExpectation(
          untyped_args, &untyped_action, &is_excessive,
          &ss, &why);
  const bool found = untyped_expectation != NULL;

  // True iff we need to print the call's arguments and return value.
  // This definition must be kept in sync with the uses of Expect()
  // and Log() in this function.
  const bool need_to_report_call =
      !found || is_excessive || LogIsVisible(kInfo);
  if (!need_to_report_call) {
    // Perform the action without printing the call information.
    return
        untyped_action == NULL ?
        this->UntypedPerformDefaultAction(untyped_args, "") :
        this->UntypedPerformAction(untyped_action, untyped_args);
  }

  ss << "    Function call: " << Name();
  this->UntypedPrintArgs(untyped_args, &ss);

  // In case the action deletes a piece of the expectation, we
  // generate the message beforehand.
  if (found && !is_excessive) {
    untyped_expectation->DescribeLocationTo(&loc);
  }

  const UntypedActionResultHolderBase* const result =
      untyped_action == NULL ?
      this->UntypedPerformDefaultAction(untyped_args, ss.str()) :
      this->UntypedPerformAction(untyped_action, untyped_args);
  if (result != NULL)
    result->PrintAsActionResult(&ss);
  ss << "\n" << why.str();

  if (!found) {
    // No expectation matches this call - reports a failure.
    Expect(false, NULL, -1, ss.str());
  } else if (is_excessive) {
    // We had an upper-bound violation and the failure message is in ss.
    Expect(false, untyped_expectation->file(),
           untyped_expectation->line(), ss.str());
  } else {
    // We had an expected call and the matching expectation is
    // described in ss.
    Log(kInfo, loc.str() + ss.str(), 2);
  }

  return result;
}

// Returns an Expectation object that references and co-owns exp,
// which must be an expectation on this mock function.
Expectation UntypedFunctionMockerBase::GetHandleOf(ExpectationBase* exp) {
  for (UntypedExpectations::const_iterator it =
           untyped_expectations_.begin();
       it != untyped_expectations_.end(); ++it) {
    if (it->get() == exp) {
      return Expectation(*it);
    }
  }

  Assert(false, __FILE__, __LINE__, "Cannot find expectation.");
  return Expectation();
  // The above statement is just to make the code compile, and will
  // never be executed.
}

// Verifies that all expectations on this mock function have been
// satisfied.  Reports one or more Google Test non-fatal failures
// and returns false if not.
bool UntypedFunctionMockerBase::VerifyAndClearExpectationsLocked()
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
  g_gmock_mutex.AssertHeld();
  bool expectations_met = true;
  for (UntypedExpectations::const_iterator it =
           untyped_expectations_.begin();
       it != untyped_expectations_.end(); ++it) {
    ExpectationBase* const untyped_expectation = it->get();
    if (untyped_expectation->IsOverSaturated()) {
      // There was an upper-bound violation.  Since the error was
      // already reported when it occurred, there is no need to do
      // anything here.
      expectations_met = false;
    } else if (!untyped_expectation->IsSatisfied()) {
      expectations_met = false;
      ::std::stringstream ss;
      ss  << "Actual function call count doesn't match "
          << untyped_expectation->source_text() << "...\n";
      // No need to show the source file location of the expectation
      // in the description, as the Expect() call that follows already
      // takes care of it.
      untyped_expectation->MaybeDescribeExtraMatcherTo(&ss);
      untyped_expectation->DescribeCallCountTo(&ss);
      Expect(false, untyped_expectation->file(),
             untyped_expectation->line(), ss.str());
    }
  }

  // Deleting our expectations may trigger other mock objects to be deleted, for
  // example if an action contains a reference counted smart pointer to that
  // mock object, and that is the last reference. So if we delete our
  // expectations within the context of the global mutex we may deadlock when
  // this method is called again. Instead, make a copy of the set of
  // expectations to delete, clear our set within the mutex, and then clear the
  // copied set outside of it.
  UntypedExpectations expectations_to_delete;
  untyped_expectations_.swap(expectations_to_delete);

  g_gmock_mutex.Unlock();
  expectations_to_delete.clear();
  g_gmock_mutex.Lock();

  return expectations_met;
}

}  // namespace internal

// Class Mock.

namespace {

typedef std::set<internal::UntypedFunctionMockerBase*> FunctionMockers;

// The current state of a mock object.  Such information is needed for
// detecting leaked mock objects and explicitly verifying a mock's
// expectations.
struct MockObjectState {
  MockObjectState()
      : first_used_file(NULL), first_used_line(-1), leakable(false) {}

  // Where in the source file an ON_CALL or EXPECT_CALL is first
  // invoked on this mock object.
  const char* first_used_file;
  int first_used_line;
  ::std::string first_used_test_case;
  ::std::string first_used_test;
  bool leakable;  // true iff it's OK to leak the object.
  FunctionMockers function_mockers;  // All registered methods of the object.
};

// A global registry holding the state of all mock objects that are
// alive.  A mock object is added to this registry the first time
// Mock::AllowLeak(), ON_CALL(), or EXPECT_CALL() is called on it.  It
// is removed from the registry in the mock object's destructor.
class MockObjectRegistry {
 public:
  // Maps a mock object (identified by its address) to its state.
  typedef std::map<const void*, MockObjectState> StateMap;

  // This destructor will be called when a program exits, after all
  // tests in it have been run.  By then, there should be no mock
  // object alive.  Therefore we report any living object as test
  // failure, unless the user explicitly asked us to ignore it.
  ~MockObjectRegistry() {
    // "using ::std::cout;" doesn't work with Symbian's STLport, where cout is
    // a macro.

    if (!GMOCK_FLAG(catch_leaked_mocks))
      return;

    int leaked_count = 0;
    for (StateMap::const_iterator it = states_.begin(); it != states_.end();
         ++it) {
      if (it->second.leakable)  // The user said it's fine to leak this object.
        continue;

      // TODO(wan@google.com): Print the type of the leaked object.
      // This can help the user identify the leaked object.
      std::cout << "\n";
      const MockObjectState& state = it->second;
      std::cout << internal::FormatFileLocation(state.first_used_file,
                                                state.first_used_line);
      std::cout << " ERROR: this mock object";
      if (state.first_used_test != "") {
        std::cout << " (used in test " << state.first_used_test_case << "."
             << state.first_used_test << ")";
      }
      std::cout << " should be deleted but never is. Its address is @"
           << it->first << ".";
      leaked_count++;
    }
    if (leaked_count > 0) {
      std::cout << "\nERROR: " << leaked_count
           << " leaked mock " << (leaked_count == 1 ? "object" : "objects")
           << " found at program exit.\n";
      std::cout.flush();
      ::std::cerr.flush();
      // RUN_ALL_TESTS() has already returned when this destructor is
      // called.  Therefore we cannot use the normal Google Test
      // failure reporting mechanism.
      _exit(1);  // We cannot call exit() as it is not reentrant and
                 // may already have been called.
    }
  }

  StateMap& states() { return states_; }

 private:
  StateMap states_;
};

// Protected by g_gmock_mutex.
MockObjectRegistry g_mock_object_registry;

// Maps a mock object to the reaction Google Mock should have when an
// uninteresting method is called.  Protected by g_gmock_mutex.
std::map<const void*, internal::CallReaction> g_uninteresting_call_reaction;

// Sets the reaction Google Mock should have when an uninteresting
// method of the given mock object is called.
void SetReactionOnUninterestingCalls(const void* mock_obj,
                                     internal::CallReaction reaction)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  g_uninteresting_call_reaction[mock_obj] = reaction;
}

}  // namespace

// Tells Google Mock to allow uninteresting calls on the given mock
// object.
void Mock::AllowUninterestingCalls(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  SetReactionOnUninterestingCalls(mock_obj, internal::kAllow);
}

// Tells Google Mock to warn the user about uninteresting calls on the
// given mock object.
void Mock::WarnUninterestingCalls(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  SetReactionOnUninterestingCalls(mock_obj, internal::kWarn);
}

// Tells Google Mock to fail uninteresting calls on the given mock
// object.
void Mock::FailUninterestingCalls(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  SetReactionOnUninterestingCalls(mock_obj, internal::kFail);
}

// Tells Google Mock the given mock object is being destroyed and its
// entry in the call-reaction table should be removed.
void Mock::UnregisterCallReaction(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  g_uninteresting_call_reaction.erase(mock_obj);
}

// Returns the reaction Google Mock will have on uninteresting calls
// made on the given mock object.
internal::CallReaction Mock::GetReactionOnUninterestingCalls(
    const void* mock_obj)
        GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  return (g_uninteresting_call_reaction.count(mock_obj) == 0) ?
      internal::kDefault : g_uninteresting_call_reaction[mock_obj];
}

// Tells Google Mock to ignore mock_obj when checking for leaked mock
// objects.
void Mock::AllowLeak(const void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  g_mock_object_registry.states()[mock_obj].leakable = true;
}

// Verifies and clears all expectations on the given mock object.  If
// the expectations aren't satisfied, generates one or more Google
// Test non-fatal failures and returns false.
bool Mock::VerifyAndClearExpectations(void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  return VerifyAndClearExpectationsLocked(mock_obj);
}

// Verifies all expectations on the given mock object and clears its
// default actions and expectations.  Returns true iff the
// verification was successful.
bool Mock::VerifyAndClear(void* mock_obj)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  ClearDefaultActionsLocked(mock_obj);
  return VerifyAndClearExpectationsLocked(mock_obj);
}

// Verifies and clears all expectations on the given mock object.  If
// the expectations aren't satisfied, generates one or more Google
// Test non-fatal failures and returns false.
bool Mock::VerifyAndClearExpectationsLocked(void* mock_obj)
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex) {
  internal::g_gmock_mutex.AssertHeld();
  if (g_mock_object_registry.states().count(mock_obj) == 0) {
    // No EXPECT_CALL() was set on the given mock object.
    return true;
  }

  // Verifies and clears the expectations on each mock method in the
  // given mock object.
  bool expectations_met = true;
  FunctionMockers& mockers =
      g_mock_object_registry.states()[mock_obj].function_mockers;
  for (FunctionMockers::const_iterator it = mockers.begin();
       it != mockers.end(); ++it) {
    if (!(*it)->VerifyAndClearExpectationsLocked()) {
      expectations_met = false;
    }
  }

  // We don't clear the content of mockers, as they may still be
  // needed by ClearDefaultActionsLocked().
  return expectations_met;
}

// Registers a mock object and a mock method it owns.
void Mock::Register(const void* mock_obj,
                    internal::UntypedFunctionMockerBase* mocker)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  g_mock_object_registry.states()[mock_obj].function_mockers.insert(mocker);
}

// Tells Google Mock where in the source code mock_obj is used in an
// ON_CALL or EXPECT_CALL.  In case mock_obj is leaked, this
// information helps the user identify which object it is.
void Mock::RegisterUseByOnCallOrExpectCall(const void* mock_obj,
                                           const char* file, int line)
    GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex) {
  internal::MutexLock l(&internal::g_gmock_mutex);
  MockObjectState& state = g_mock_object_registry.states()[mock_obj];
  if (state.first_used_file == NULL) {
    state.first_used_file = file;
    state.first_used_line = line;
    const TestInfo* const test_info =
        UnitTest::GetInstance()->current_test_info();
    if (test_info != NULL) {
      // TODO(wan@google.com): record the test case name when the
      // ON_CALL or EXPECT_CALL is invoked from SetUpTestCase() or
      // TearDownTestCase().
      state.first_used_test_case = test_info->test_case_name();
      state.first_used_test = test_info->name();
    }
  }
}

// Unregisters a mock method; removes the owning mock object from the
// registry when the last mock method associated with it has been
// unregistered.  This is called only in the destructor of
// FunctionMockerBase.
void Mock::UnregisterLocked(internal::UntypedFunctionMockerBase* mocker)
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex) {
  internal::g_gmock_mutex.AssertHeld();
  for (MockObjectRegistry::StateMap::iterator it =
           g_mock_object_registry.states().begin();
       it != g_mock_object_registry.states().end(); ++it) {
    FunctionMockers& mockers = it->second.function_mockers;
    if (mockers.erase(mocker) > 0) {
      // mocker was in mockers and has been just removed.
      if (mockers.empty()) {
        g_mock_object_registry.states().erase(it);
      }
      return;
    }
  }
}

// Clears all ON_CALL()s set on the given mock object.
void Mock::ClearDefaultActionsLocked(void* mock_obj)
    GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex) {
  internal::g_gmock_mutex.AssertHeld();

  if (g_mock_object_registry.states().count(mock_obj) == 0) {
    // No ON_CALL() was set on the given mock object.
    return;
  }

  // Clears the default actions for each mock method in the given mock
  // object.
  FunctionMockers& mockers =
      g_mock_object_registry.states()[mock_obj].function_mockers;
  for (FunctionMockers::const_iterator it = mockers.begin();
       it != mockers.end(); ++it) {
    (*it)->ClearDefaultActionsLocked();
  }

  // We don't clear the content of mockers, as they may still be
  // needed by VerifyAndClearExpectationsLocked().
}

Expectation::Expectation() {}

Expectation::Expectation(
    const internal::linked_ptr<internal::ExpectationBase>& an_expectation_base)
    : expectation_base_(an_expectation_base) {}

Expectation::~Expectation() {}

// Adds an expectation to a sequence.
void Sequence::AddExpectation(const Expectation& expectation) const {
  if (*last_expectation_ != expectation) {
    if (last_expectation_->expectation_base() != NULL) {
      expectation.expectation_base()->immediate_prerequisites_
          += *last_expectation_;
    }
    *last_expectation_ = expectation;
  }
}

// Creates the implicit sequence if there isn't one.
InSequence::InSequence() {
  if (internal::g_gmock_implicit_sequence.get() == NULL) {
    internal::g_gmock_implicit_sequence.set(new Sequence);
    sequence_created_ = true;
  } else {
    sequence_created_ = false;
  }
}

// Deletes the implicit sequence if it was created by the constructor
// of this object.
InSequence::~InSequence() {
  if (sequence_created_) {
    delete internal::g_gmock_implicit_sequence.get();
    internal::g_gmock_implicit_sequence.set(NULL);
  }
}

}  // namespace testing
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


namespace testing {

// TODO(wan@google.com): support using environment variables to
// control the flag values, like what Google Test does.

GMOCK_DEFINE_bool_(catch_leaked_mocks, true,
                   "true iff Google Mock should report leaked mock objects "
                   "as failures.");

GMOCK_DEFINE_string_(verbose, internal::kWarningVerbosity,
                     "Controls how verbose Google Mock's output is."
                     "  Valid values:\n"
                     "  info    - prints all messages.\n"
                     "  warning - prints warnings and errors.\n"
                     "  error   - prints errors only.");

namespace internal {

// Parses a string as a command line flag.  The string should have the
// format "--gmock_flag=value".  When def_optional is true, the
// "=value" part can be omitted.
//
// Returns the value of the flag, or NULL if the parsing failed.
static const char* ParseGoogleMockFlagValue(const char* str,
                                            const char* flag,
                                            bool def_optional) {
  // str and flag must not be NULL.
  if (str == NULL || flag == NULL) return NULL;

  // The flag must start with "--gmock_".
  const std::string flag_str = std::string("--gmock_") + flag;
  const size_t flag_len = flag_str.length();
  if (strncmp(str, flag_str.c_str(), flag_len) != 0) return NULL;

  // Skips the flag name.
  const char* flag_end = str + flag_len;

  // When def_optional is true, it's OK to not have a "=value" part.
  if (def_optional && (flag_end[0] == '\0')) {
    return flag_end;
  }

  // If def_optional is true and there are more characters after the
  // flag name, or if def_optional is false, there must be a '=' after
  // the flag name.
  if (flag_end[0] != '=') return NULL;

  // Returns the string after "=".
  return flag_end + 1;
}

// Parses a string for a Google Mock bool flag, in the form of
// "--gmock_flag=value".
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
static bool ParseGoogleMockBoolFlag(const char* str, const char* flag,
                                    bool* value) {
  // Gets the value of the flag as a string.
  const char* const value_str = ParseGoogleMockFlagValue(str, flag, true);

  // Aborts if the parsing failed.
  if (value_str == NULL) return false;

  // Converts the string value to a bool.
  *value = !(*value_str == '0' || *value_str == 'f' || *value_str == 'F');
  return true;
}

// Parses a string for a Google Mock string flag, in the form of
// "--gmock_flag=value".
//
// On success, stores the value of the flag in *value, and returns
// true.  On failure, returns false without changing *value.
static bool ParseGoogleMockStringFlag(const char* str, const char* flag,
                                      std::string* value) {
  // Gets the value of the flag as a string.
  const char* const value_str = ParseGoogleMockFlagValue(str, flag, false);

  // Aborts if the parsing failed.
  if (value_str == NULL) return false;

  // Sets *value to the value of the flag.
  *value = value_str;
  return true;
}

// The internal implementation of InitGoogleMock().
//
// The type parameter CharType can be instantiated to either char or
// wchar_t.
template <typename CharType>
void InitGoogleMockImpl(int* argc, CharType** argv) {
  // Makes sure Google Test is initialized.  InitGoogleTest() is
  // idempotent, so it's fine if the user has already called it.
  InitGoogleTest(argc, argv);
  if (*argc <= 0) return;

  for (int i = 1; i != *argc; i++) {
    const std::string arg_string = StreamableToString(argv[i]);
    const char* const arg = arg_string.c_str();

    // Do we see a Google Mock flag?
    if (ParseGoogleMockBoolFlag(arg, "catch_leaked_mocks",
                                &GMOCK_FLAG(catch_leaked_mocks)) ||
        ParseGoogleMockStringFlag(arg, "verbose", &GMOCK_FLAG(verbose))) {
      // Yes.  Shift the remainder of the argv list left by one.  Note
      // that argv has (*argc + 1) elements, the last one always being
      // NULL.  The following loop moves the trailing NULL element as
      // well.
      for (int j = i; j != *argc; j++) {
        argv[j] = argv[j + 1];
      }

      // Decrements the argument count.
      (*argc)--;

      // We also need to decrement the iterator as we just removed
      // an element.
      i--;
    }
  }
}

}  // namespace internal

// Initializes Google Mock.  This must be called before running the
// tests.  In particular, it parses a command line for the flags that
// Google Mock recognizes.  Whenever a Google Mock flag is seen, it is
// removed from argv, and *argc is decremented.
//
// No value is returned.  Instead, the Google Mock flag variables are
// updated.
//
// Since Google Test is needed for Google Mock to work, this function
// also initializes Google Test and parses its flags, if that hasn't
// been done.
GTEST_API_ void InitGoogleMock(int* argc, char** argv) {
  internal::InitGoogleMockImpl(argc, argv);
}

// This overloaded version can be used in Windows programs compiled in
// UNICODE mode.
GTEST_API_ void InitGoogleMock(int* argc, wchar_t** argv) {
  internal::InitGoogleMockImpl(argc, argv);
}

}  // namespace testing
