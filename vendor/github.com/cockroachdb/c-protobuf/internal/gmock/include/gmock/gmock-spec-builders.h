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
// This file implements the ON_CALL() and EXPECT_CALL() macros.
//
// A user can use the ON_CALL() macro to specify the default action of
// a mock method.  The syntax is:
//
//   ON_CALL(mock_object, Method(argument-matchers))
//       .With(multi-argument-matcher)
//       .WillByDefault(action);
//
//  where the .With() clause is optional.
//
// A user can use the EXPECT_CALL() macro to specify an expectation on
// a mock method.  The syntax is:
//
//   EXPECT_CALL(mock_object, Method(argument-matchers))
//       .With(multi-argument-matchers)
//       .Times(cardinality)
//       .InSequence(sequences)
//       .After(expectations)
//       .WillOnce(action)
//       .WillRepeatedly(action)
//       .RetiresOnSaturation();
//
// where all clauses are optional, and .InSequence()/.After()/
// .WillOnce() can appear any number of times.

#ifndef GMOCK_INCLUDE_GMOCK_GMOCK_SPEC_BUILDERS_H_
#define GMOCK_INCLUDE_GMOCK_GMOCK_SPEC_BUILDERS_H_

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#if GTEST_HAS_EXCEPTIONS
# include <stdexcept>  // NOLINT
#endif

#include "gmock/gmock-actions.h"
#include "gmock/gmock-cardinalities.h"
#include "gmock/gmock-matchers.h"
#include "gmock/internal/gmock-internal-utils.h"
#include "gmock/internal/gmock-port.h"
#include "gtest/gtest.h"

namespace testing {

// An abstract handle of an expectation.
class Expectation;

// A set of expectation handles.
class ExpectationSet;

// Anything inside the 'internal' namespace IS INTERNAL IMPLEMENTATION
// and MUST NOT BE USED IN USER CODE!!!
namespace internal {

// Implements a mock function.
template <typename F> class FunctionMocker;

// Base class for expectations.
class ExpectationBase;

// Implements an expectation.
template <typename F> class TypedExpectation;

// Helper class for testing the Expectation class template.
class ExpectationTester;

// Base class for function mockers.
template <typename F> class FunctionMockerBase;

// Protects the mock object registry (in class Mock), all function
// mockers, and all expectations.
//
// The reason we don't use more fine-grained protection is: when a
// mock function Foo() is called, it needs to consult its expectations
// to see which one should be picked.  If another thread is allowed to
// call a mock function (either Foo() or a different one) at the same
// time, it could affect the "retired" attributes of Foo()'s
// expectations when InSequence() is used, and thus affect which
// expectation gets picked.  Therefore, we sequence all mock function
// calls to ensure the integrity of the mock objects' states.
GTEST_API_ GTEST_DECLARE_STATIC_MUTEX_(g_gmock_mutex);

// Untyped base class for ActionResultHolder<R>.
class UntypedActionResultHolderBase;

// Abstract base class of FunctionMockerBase.  This is the
// type-agnostic part of the function mocker interface.  Its pure
// virtual methods are implemented by FunctionMockerBase.
class GTEST_API_ UntypedFunctionMockerBase {
 public:
  UntypedFunctionMockerBase();
  virtual ~UntypedFunctionMockerBase();

  // Verifies that all expectations on this mock function have been
  // satisfied.  Reports one or more Google Test non-fatal failures
  // and returns false if not.
  bool VerifyAndClearExpectationsLocked()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex);

  // Clears the ON_CALL()s set on this mock function.
  virtual void ClearDefaultActionsLocked()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) = 0;

  // In all of the following Untyped* functions, it's the caller's
  // responsibility to guarantee the correctness of the arguments'
  // types.

  // Performs the default action with the given arguments and returns
  // the action's result.  The call description string will be used in
  // the error message to describe the call in the case the default
  // action fails.
  // L = *
  virtual UntypedActionResultHolderBase* UntypedPerformDefaultAction(
      const void* untyped_args,
      const string& call_description) const = 0;

  // Performs the given action with the given arguments and returns
  // the action's result.
  // L = *
  virtual UntypedActionResultHolderBase* UntypedPerformAction(
      const void* untyped_action,
      const void* untyped_args) const = 0;

  // Writes a message that the call is uninteresting (i.e. neither
  // explicitly expected nor explicitly unexpected) to the given
  // ostream.
  virtual void UntypedDescribeUninterestingCall(
      const void* untyped_args,
      ::std::ostream* os) const
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) = 0;

  // Returns the expectation that matches the given function arguments
  // (or NULL is there's no match); when a match is found,
  // untyped_action is set to point to the action that should be
  // performed (or NULL if the action is "do default"), and
  // is_excessive is modified to indicate whether the call exceeds the
  // expected number.
  virtual const ExpectationBase* UntypedFindMatchingExpectation(
      const void* untyped_args,
      const void** untyped_action, bool* is_excessive,
      ::std::ostream* what, ::std::ostream* why)
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) = 0;

  // Prints the given function arguments to the ostream.
  virtual void UntypedPrintArgs(const void* untyped_args,
                                ::std::ostream* os) const = 0;

  // Sets the mock object this mock method belongs to, and registers
  // this information in the global mock registry.  Will be called
  // whenever an EXPECT_CALL() or ON_CALL() is executed on this mock
  // method.
  // TODO(wan@google.com): rename to SetAndRegisterOwner().
  void RegisterOwner(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(g_gmock_mutex);

  // Sets the mock object this mock method belongs to, and sets the
  // name of the mock function.  Will be called upon each invocation
  // of this mock function.
  void SetOwnerAndName(const void* mock_obj, const char* name)
      GTEST_LOCK_EXCLUDED_(g_gmock_mutex);

  // Returns the mock object this mock method belongs to.  Must be
  // called after RegisterOwner() or SetOwnerAndName() has been
  // called.
  const void* MockObject() const
      GTEST_LOCK_EXCLUDED_(g_gmock_mutex);

  // Returns the name of this mock method.  Must be called after
  // SetOwnerAndName() has been called.
  const char* Name() const
      GTEST_LOCK_EXCLUDED_(g_gmock_mutex);

  // Returns the result of invoking this mock function with the given
  // arguments.  This function can be safely called from multiple
  // threads concurrently.  The caller is responsible for deleting the
  // result.
  const UntypedActionResultHolderBase* UntypedInvokeWith(
      const void* untyped_args)
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex);

 protected:
  typedef std::vector<const void*> UntypedOnCallSpecs;

  typedef std::vector<internal::linked_ptr<ExpectationBase> >
  UntypedExpectations;

  // Returns an Expectation object that references and co-owns exp,
  // which must be an expectation on this mock function.
  Expectation GetHandleOf(ExpectationBase* exp);

  // Address of the mock object this mock method belongs to.  Only
  // valid after this mock method has been called or
  // ON_CALL/EXPECT_CALL has been invoked on it.
  const void* mock_obj_;  // Protected by g_gmock_mutex.

  // Name of the function being mocked.  Only valid after this mock
  // method has been called.
  const char* name_;  // Protected by g_gmock_mutex.

  // All default action specs for this function mocker.
  UntypedOnCallSpecs untyped_on_call_specs_;

  // All expectations for this function mocker.
  UntypedExpectations untyped_expectations_;
};  // class UntypedFunctionMockerBase

// Untyped base class for OnCallSpec<F>.
class UntypedOnCallSpecBase {
 public:
  // The arguments are the location of the ON_CALL() statement.
  UntypedOnCallSpecBase(const char* a_file, int a_line)
      : file_(a_file), line_(a_line), last_clause_(kNone) {}

  // Where in the source file was the default action spec defined?
  const char* file() const { return file_; }
  int line() const { return line_; }

 protected:
  // Gives each clause in the ON_CALL() statement a name.
  enum Clause {
    // Do not change the order of the enum members!  The run-time
    // syntax checking relies on it.
    kNone,
    kWith,
    kWillByDefault
  };

  // Asserts that the ON_CALL() statement has a certain property.
  void AssertSpecProperty(bool property, const string& failure_message) const {
    Assert(property, file_, line_, failure_message);
  }

  // Expects that the ON_CALL() statement has a certain property.
  void ExpectSpecProperty(bool property, const string& failure_message) const {
    Expect(property, file_, line_, failure_message);
  }

  const char* file_;
  int line_;

  // The last clause in the ON_CALL() statement as seen so far.
  // Initially kNone and changes as the statement is parsed.
  Clause last_clause_;
};  // class UntypedOnCallSpecBase

// This template class implements an ON_CALL spec.
template <typename F>
class OnCallSpec : public UntypedOnCallSpecBase {
 public:
  typedef typename Function<F>::ArgumentTuple ArgumentTuple;
  typedef typename Function<F>::ArgumentMatcherTuple ArgumentMatcherTuple;

  // Constructs an OnCallSpec object from the information inside
  // the parenthesis of an ON_CALL() statement.
  OnCallSpec(const char* a_file, int a_line,
             const ArgumentMatcherTuple& matchers)
      : UntypedOnCallSpecBase(a_file, a_line),
        matchers_(matchers),
        // By default, extra_matcher_ should match anything.  However,
        // we cannot initialize it with _ as that triggers a compiler
        // bug in Symbian's C++ compiler (cannot decide between two
        // overloaded constructors of Matcher<const ArgumentTuple&>).
        extra_matcher_(A<const ArgumentTuple&>()) {
  }

  // Implements the .With() clause.
  OnCallSpec& With(const Matcher<const ArgumentTuple&>& m) {
    // Makes sure this is called at most once.
    ExpectSpecProperty(last_clause_ < kWith,
                       ".With() cannot appear "
                       "more than once in an ON_CALL().");
    last_clause_ = kWith;

    extra_matcher_ = m;
    return *this;
  }

  // Implements the .WillByDefault() clause.
  OnCallSpec& WillByDefault(const Action<F>& action) {
    ExpectSpecProperty(last_clause_ < kWillByDefault,
                       ".WillByDefault() must appear "
                       "exactly once in an ON_CALL().");
    last_clause_ = kWillByDefault;

    ExpectSpecProperty(!action.IsDoDefault(),
                       "DoDefault() cannot be used in ON_CALL().");
    action_ = action;
    return *this;
  }

  // Returns true iff the given arguments match the matchers.
  bool Matches(const ArgumentTuple& args) const {
    return TupleMatches(matchers_, args) && extra_matcher_.Matches(args);
  }

  // Returns the action specified by the user.
  const Action<F>& GetAction() const {
    AssertSpecProperty(last_clause_ == kWillByDefault,
                       ".WillByDefault() must appear exactly "
                       "once in an ON_CALL().");
    return action_;
  }

 private:
  // The information in statement
  //
  //   ON_CALL(mock_object, Method(matchers))
  //       .With(multi-argument-matcher)
  //       .WillByDefault(action);
  //
  // is recorded in the data members like this:
  //
  //   source file that contains the statement => file_
  //   line number of the statement            => line_
  //   matchers                                => matchers_
  //   multi-argument-matcher                  => extra_matcher_
  //   action                                  => action_
  ArgumentMatcherTuple matchers_;
  Matcher<const ArgumentTuple&> extra_matcher_;
  Action<F> action_;
};  // class OnCallSpec

// Possible reactions on uninteresting calls.
enum CallReaction {
  kAllow,
  kWarn,
  kFail,
  kDefault = kWarn  // By default, warn about uninteresting calls.
};

}  // namespace internal

// Utilities for manipulating mock objects.
class GTEST_API_ Mock {
 public:
  // The following public methods can be called concurrently.

  // Tells Google Mock to ignore mock_obj when checking for leaked
  // mock objects.
  static void AllowLeak(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Verifies and clears all expectations on the given mock object.
  // If the expectations aren't satisfied, generates one or more
  // Google Test non-fatal failures and returns false.
  static bool VerifyAndClearExpectations(void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Verifies all expectations on the given mock object and clears its
  // default actions and expectations.  Returns true iff the
  // verification was successful.
  static bool VerifyAndClear(void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

 private:
  friend class internal::UntypedFunctionMockerBase;

  // Needed for a function mocker to register itself (so that we know
  // how to clear a mock object).
  template <typename F>
  friend class internal::FunctionMockerBase;

  template <typename M>
  friend class NiceMock;

  template <typename M>
  friend class NaggyMock;

  template <typename M>
  friend class StrictMock;

  // Tells Google Mock to allow uninteresting calls on the given mock
  // object.
  static void AllowUninterestingCalls(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Tells Google Mock to warn the user about uninteresting calls on
  // the given mock object.
  static void WarnUninterestingCalls(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Tells Google Mock to fail uninteresting calls on the given mock
  // object.
  static void FailUninterestingCalls(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Tells Google Mock the given mock object is being destroyed and
  // its entry in the call-reaction table should be removed.
  static void UnregisterCallReaction(const void* mock_obj)
      GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Returns the reaction Google Mock will have on uninteresting calls
  // made on the given mock object.
  static internal::CallReaction GetReactionOnUninterestingCalls(
      const void* mock_obj)
          GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Verifies that all expectations on the given mock object have been
  // satisfied.  Reports one or more Google Test non-fatal failures
  // and returns false if not.
  static bool VerifyAndClearExpectationsLocked(void* mock_obj)
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex);

  // Clears all ON_CALL()s set on the given mock object.
  static void ClearDefaultActionsLocked(void* mock_obj)
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex);

  // Registers a mock object and a mock method it owns.
  static void Register(
      const void* mock_obj,
      internal::UntypedFunctionMockerBase* mocker)
          GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Tells Google Mock where in the source code mock_obj is used in an
  // ON_CALL or EXPECT_CALL.  In case mock_obj is leaked, this
  // information helps the user identify which object it is.
  static void RegisterUseByOnCallOrExpectCall(
      const void* mock_obj, const char* file, int line)
          GTEST_LOCK_EXCLUDED_(internal::g_gmock_mutex);

  // Unregisters a mock method; removes the owning mock object from
  // the registry when the last mock method associated with it has
  // been unregistered.  This is called only in the destructor of
  // FunctionMockerBase.
  static void UnregisterLocked(internal::UntypedFunctionMockerBase* mocker)
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(internal::g_gmock_mutex);
};  // class Mock

// An abstract handle of an expectation.  Useful in the .After()
// clause of EXPECT_CALL() for setting the (partial) order of
// expectations.  The syntax:
//
//   Expectation e1 = EXPECT_CALL(...)...;
//   EXPECT_CALL(...).After(e1)...;
//
// sets two expectations where the latter can only be matched after
// the former has been satisfied.
//
// Notes:
//   - This class is copyable and has value semantics.
//   - Constness is shallow: a const Expectation object itself cannot
//     be modified, but the mutable methods of the ExpectationBase
//     object it references can be called via expectation_base().
//   - The constructors and destructor are defined out-of-line because
//     the Symbian WINSCW compiler wants to otherwise instantiate them
//     when it sees this class definition, at which point it doesn't have
//     ExpectationBase available yet, leading to incorrect destruction
//     in the linked_ptr (or compilation errors if using a checking
//     linked_ptr).
class GTEST_API_ Expectation {
 public:
  // Constructs a null object that doesn't reference any expectation.
  Expectation();

  ~Expectation();

  // This single-argument ctor must not be explicit, in order to support the
  //   Expectation e = EXPECT_CALL(...);
  // syntax.
  //
  // A TypedExpectation object stores its pre-requisites as
  // Expectation objects, and needs to call the non-const Retire()
  // method on the ExpectationBase objects they reference.  Therefore
  // Expectation must receive a *non-const* reference to the
  // ExpectationBase object.
  Expectation(internal::ExpectationBase& exp);  // NOLINT

  // The compiler-generated copy ctor and operator= work exactly as
  // intended, so we don't need to define our own.

  // Returns true iff rhs references the same expectation as this object does.
  bool operator==(const Expectation& rhs) const {
    return expectation_base_ == rhs.expectation_base_;
  }

  bool operator!=(const Expectation& rhs) const { return !(*this == rhs); }

 private:
  friend class ExpectationSet;
  friend class Sequence;
  friend class ::testing::internal::ExpectationBase;
  friend class ::testing::internal::UntypedFunctionMockerBase;

  template <typename F>
  friend class ::testing::internal::FunctionMockerBase;

  template <typename F>
  friend class ::testing::internal::TypedExpectation;

  // This comparator is needed for putting Expectation objects into a set.
  class Less {
   public:
    bool operator()(const Expectation& lhs, const Expectation& rhs) const {
      return lhs.expectation_base_.get() < rhs.expectation_base_.get();
    }
  };

  typedef ::std::set<Expectation, Less> Set;

  Expectation(
      const internal::linked_ptr<internal::ExpectationBase>& expectation_base);

  // Returns the expectation this object references.
  const internal::linked_ptr<internal::ExpectationBase>&
  expectation_base() const {
    return expectation_base_;
  }

  // A linked_ptr that co-owns the expectation this handle references.
  internal::linked_ptr<internal::ExpectationBase> expectation_base_;
};

// A set of expectation handles.  Useful in the .After() clause of
// EXPECT_CALL() for setting the (partial) order of expectations.  The
// syntax:
//
//   ExpectationSet es;
//   es += EXPECT_CALL(...)...;
//   es += EXPECT_CALL(...)...;
//   EXPECT_CALL(...).After(es)...;
//
// sets three expectations where the last one can only be matched
// after the first two have both been satisfied.
//
// This class is copyable and has value semantics.
class ExpectationSet {
 public:
  // A bidirectional iterator that can read a const element in the set.
  typedef Expectation::Set::const_iterator const_iterator;

  // An object stored in the set.  This is an alias of Expectation.
  typedef Expectation::Set::value_type value_type;

  // Constructs an empty set.
  ExpectationSet() {}

  // This single-argument ctor must not be explicit, in order to support the
  //   ExpectationSet es = EXPECT_CALL(...);
  // syntax.
  ExpectationSet(internal::ExpectationBase& exp) {  // NOLINT
    *this += Expectation(exp);
  }

  // This single-argument ctor implements implicit conversion from
  // Expectation and thus must not be explicit.  This allows either an
  // Expectation or an ExpectationSet to be used in .After().
  ExpectationSet(const Expectation& e) {  // NOLINT
    *this += e;
  }

  // The compiler-generator ctor and operator= works exactly as
  // intended, so we don't need to define our own.

  // Returns true iff rhs contains the same set of Expectation objects
  // as this does.
  bool operator==(const ExpectationSet& rhs) const {
    return expectations_ == rhs.expectations_;
  }

  bool operator!=(const ExpectationSet& rhs) const { return !(*this == rhs); }

  // Implements the syntax
  //   expectation_set += EXPECT_CALL(...);
  ExpectationSet& operator+=(const Expectation& e) {
    expectations_.insert(e);
    return *this;
  }

  int size() const { return static_cast<int>(expectations_.size()); }

  const_iterator begin() const { return expectations_.begin(); }
  const_iterator end() const { return expectations_.end(); }

 private:
  Expectation::Set expectations_;
};


// Sequence objects are used by a user to specify the relative order
// in which the expectations should match.  They are copyable (we rely
// on the compiler-defined copy constructor and assignment operator).
class GTEST_API_ Sequence {
 public:
  // Constructs an empty sequence.
  Sequence() : last_expectation_(new Expectation) {}

  // Adds an expectation to this sequence.  The caller must ensure
  // that no other thread is accessing this Sequence object.
  void AddExpectation(const Expectation& expectation) const;

 private:
  // The last expectation in this sequence.  We use a linked_ptr here
  // because Sequence objects are copyable and we want the copies to
  // be aliases.  The linked_ptr allows the copies to co-own and share
  // the same Expectation object.
  internal::linked_ptr<Expectation> last_expectation_;
};  // class Sequence

// An object of this type causes all EXPECT_CALL() statements
// encountered in its scope to be put in an anonymous sequence.  The
// work is done in the constructor and destructor.  You should only
// create an InSequence object on the stack.
//
// The sole purpose for this class is to support easy definition of
// sequential expectations, e.g.
//
//   {
//     InSequence dummy;  // The name of the object doesn't matter.
//
//     // The following expectations must match in the order they appear.
//     EXPECT_CALL(a, Bar())...;
//     EXPECT_CALL(a, Baz())...;
//     ...
//     EXPECT_CALL(b, Xyz())...;
//   }
//
// You can create InSequence objects in multiple threads, as long as
// they are used to affect different mock objects.  The idea is that
// each thread can create and set up its own mocks as if it's the only
// thread.  However, for clarity of your tests we recommend you to set
// up mocks in the main thread unless you have a good reason not to do
// so.
class GTEST_API_ InSequence {
 public:
  InSequence();
  ~InSequence();
 private:
  bool sequence_created_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(InSequence);  // NOLINT
} GTEST_ATTRIBUTE_UNUSED_;

namespace internal {

// Points to the implicit sequence introduced by a living InSequence
// object (if any) in the current thread or NULL.
GTEST_API_ extern ThreadLocal<Sequence*> g_gmock_implicit_sequence;

// Base class for implementing expectations.
//
// There are two reasons for having a type-agnostic base class for
// Expectation:
//
//   1. We need to store collections of expectations of different
//   types (e.g. all pre-requisites of a particular expectation, all
//   expectations in a sequence).  Therefore these expectation objects
//   must share a common base class.
//
//   2. We can avoid binary code bloat by moving methods not depending
//   on the template argument of Expectation to the base class.
//
// This class is internal and mustn't be used by user code directly.
class GTEST_API_ ExpectationBase {
 public:
  // source_text is the EXPECT_CALL(...) source that created this Expectation.
  ExpectationBase(const char* file, int line, const string& source_text);

  virtual ~ExpectationBase();

  // Where in the source file was the expectation spec defined?
  const char* file() const { return file_; }
  int line() const { return line_; }
  const char* source_text() const { return source_text_.c_str(); }
  // Returns the cardinality specified in the expectation spec.
  const Cardinality& cardinality() const { return cardinality_; }

  // Describes the source file location of this expectation.
  void DescribeLocationTo(::std::ostream* os) const {
    *os << FormatFileLocation(file(), line()) << " ";
  }

  // Describes how many times a function call matching this
  // expectation has occurred.
  void DescribeCallCountTo(::std::ostream* os) const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex);

  // If this mock method has an extra matcher (i.e. .With(matcher)),
  // describes it to the ostream.
  virtual void MaybeDescribeExtraMatcherTo(::std::ostream* os) = 0;

 protected:
  friend class ::testing::Expectation;
  friend class UntypedFunctionMockerBase;

  enum Clause {
    // Don't change the order of the enum members!
    kNone,
    kWith,
    kTimes,
    kInSequence,
    kAfter,
    kWillOnce,
    kWillRepeatedly,
    kRetiresOnSaturation
  };

  typedef std::vector<const void*> UntypedActions;

  // Returns an Expectation object that references and co-owns this
  // expectation.
  virtual Expectation GetHandle() = 0;

  // Asserts that the EXPECT_CALL() statement has the given property.
  void AssertSpecProperty(bool property, const string& failure_message) const {
    Assert(property, file_, line_, failure_message);
  }

  // Expects that the EXPECT_CALL() statement has the given property.
  void ExpectSpecProperty(bool property, const string& failure_message) const {
    Expect(property, file_, line_, failure_message);
  }

  // Explicitly specifies the cardinality of this expectation.  Used
  // by the subclasses to implement the .Times() clause.
  void SpecifyCardinality(const Cardinality& cardinality);

  // Returns true iff the user specified the cardinality explicitly
  // using a .Times().
  bool cardinality_specified() const { return cardinality_specified_; }

  // Sets the cardinality of this expectation spec.
  void set_cardinality(const Cardinality& a_cardinality) {
    cardinality_ = a_cardinality;
  }

  // The following group of methods should only be called after the
  // EXPECT_CALL() statement, and only when g_gmock_mutex is held by
  // the current thread.

  // Retires all pre-requisites of this expectation.
  void RetireAllPreRequisites()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex);

  // Returns true iff this expectation is retired.
  bool is_retired() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return retired_;
  }

  // Retires this expectation.
  void Retire()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    retired_ = true;
  }

  // Returns true iff this expectation is satisfied.
  bool IsSatisfied() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return cardinality().IsSatisfiedByCallCount(call_count_);
  }

  // Returns true iff this expectation is saturated.
  bool IsSaturated() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return cardinality().IsSaturatedByCallCount(call_count_);
  }

  // Returns true iff this expectation is over-saturated.
  bool IsOverSaturated() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return cardinality().IsOverSaturatedByCallCount(call_count_);
  }

  // Returns true iff all pre-requisites of this expectation are satisfied.
  bool AllPrerequisitesAreSatisfied() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex);

  // Adds unsatisfied pre-requisites of this expectation to 'result'.
  void FindUnsatisfiedPrerequisites(ExpectationSet* result) const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex);

  // Returns the number this expectation has been invoked.
  int call_count() const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return call_count_;
  }

  // Increments the number this expectation has been invoked.
  void IncrementCallCount()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    call_count_++;
  }

  // Checks the action count (i.e. the number of WillOnce() and
  // WillRepeatedly() clauses) against the cardinality if this hasn't
  // been done before.  Prints a warning if there are too many or too
  // few actions.
  void CheckActionCountIfNotDone() const
      GTEST_LOCK_EXCLUDED_(mutex_);

  friend class ::testing::Sequence;
  friend class ::testing::internal::ExpectationTester;

  template <typename Function>
  friend class TypedExpectation;

  // Implements the .Times() clause.
  void UntypedTimes(const Cardinality& a_cardinality);

  // This group of fields are part of the spec and won't change after
  // an EXPECT_CALL() statement finishes.
  const char* file_;          // The file that contains the expectation.
  int line_;                  // The line number of the expectation.
  const string source_text_;  // The EXPECT_CALL(...) source text.
  // True iff the cardinality is specified explicitly.
  bool cardinality_specified_;
  Cardinality cardinality_;            // The cardinality of the expectation.
  // The immediate pre-requisites (i.e. expectations that must be
  // satisfied before this expectation can be matched) of this
  // expectation.  We use linked_ptr in the set because we want an
  // Expectation object to be co-owned by its FunctionMocker and its
  // successors.  This allows multiple mock objects to be deleted at
  // different times.
  ExpectationSet immediate_prerequisites_;

  // This group of fields are the current state of the expectation,
  // and can change as the mock function is called.
  int call_count_;  // How many times this expectation has been invoked.
  bool retired_;    // True iff this expectation has retired.
  UntypedActions untyped_actions_;
  bool extra_matcher_specified_;
  bool repeated_action_specified_;  // True if a WillRepeatedly() was specified.
  bool retires_on_saturation_;
  Clause last_clause_;
  mutable bool action_count_checked_;  // Under mutex_.
  mutable Mutex mutex_;  // Protects action_count_checked_.

  GTEST_DISALLOW_ASSIGN_(ExpectationBase);
};  // class ExpectationBase

// Impements an expectation for the given function type.
template <typename F>
class TypedExpectation : public ExpectationBase {
 public:
  typedef typename Function<F>::ArgumentTuple ArgumentTuple;
  typedef typename Function<F>::ArgumentMatcherTuple ArgumentMatcherTuple;
  typedef typename Function<F>::Result Result;

  TypedExpectation(FunctionMockerBase<F>* owner,
                   const char* a_file, int a_line, const string& a_source_text,
                   const ArgumentMatcherTuple& m)
      : ExpectationBase(a_file, a_line, a_source_text),
        owner_(owner),
        matchers_(m),
        // By default, extra_matcher_ should match anything.  However,
        // we cannot initialize it with _ as that triggers a compiler
        // bug in Symbian's C++ compiler (cannot decide between two
        // overloaded constructors of Matcher<const ArgumentTuple&>).
        extra_matcher_(A<const ArgumentTuple&>()),
        repeated_action_(DoDefault()) {}

  virtual ~TypedExpectation() {
    // Check the validity of the action count if it hasn't been done
    // yet (for example, if the expectation was never used).
    CheckActionCountIfNotDone();
    for (UntypedActions::const_iterator it = untyped_actions_.begin();
         it != untyped_actions_.end(); ++it) {
      delete static_cast<const Action<F>*>(*it);
    }
  }

  // Implements the .With() clause.
  TypedExpectation& With(const Matcher<const ArgumentTuple&>& m) {
    if (last_clause_ == kWith) {
      ExpectSpecProperty(false,
                         ".With() cannot appear "
                         "more than once in an EXPECT_CALL().");
    } else {
      ExpectSpecProperty(last_clause_ < kWith,
                         ".With() must be the first "
                         "clause in an EXPECT_CALL().");
    }
    last_clause_ = kWith;

    extra_matcher_ = m;
    extra_matcher_specified_ = true;
    return *this;
  }

  // Implements the .Times() clause.
  TypedExpectation& Times(const Cardinality& a_cardinality) {
    ExpectationBase::UntypedTimes(a_cardinality);
    return *this;
  }

  // Implements the .Times() clause.
  TypedExpectation& Times(int n) {
    return Times(Exactly(n));
  }

  // Implements the .InSequence() clause.
  TypedExpectation& InSequence(const Sequence& s) {
    ExpectSpecProperty(last_clause_ <= kInSequence,
                       ".InSequence() cannot appear after .After(),"
                       " .WillOnce(), .WillRepeatedly(), or "
                       ".RetiresOnSaturation().");
    last_clause_ = kInSequence;

    s.AddExpectation(GetHandle());
    return *this;
  }
  TypedExpectation& InSequence(const Sequence& s1, const Sequence& s2) {
    return InSequence(s1).InSequence(s2);
  }
  TypedExpectation& InSequence(const Sequence& s1, const Sequence& s2,
                               const Sequence& s3) {
    return InSequence(s1, s2).InSequence(s3);
  }
  TypedExpectation& InSequence(const Sequence& s1, const Sequence& s2,
                               const Sequence& s3, const Sequence& s4) {
    return InSequence(s1, s2, s3).InSequence(s4);
  }
  TypedExpectation& InSequence(const Sequence& s1, const Sequence& s2,
                               const Sequence& s3, const Sequence& s4,
                               const Sequence& s5) {
    return InSequence(s1, s2, s3, s4).InSequence(s5);
  }

  // Implements that .After() clause.
  TypedExpectation& After(const ExpectationSet& s) {
    ExpectSpecProperty(last_clause_ <= kAfter,
                       ".After() cannot appear after .WillOnce(),"
                       " .WillRepeatedly(), or "
                       ".RetiresOnSaturation().");
    last_clause_ = kAfter;

    for (ExpectationSet::const_iterator it = s.begin(); it != s.end(); ++it) {
      immediate_prerequisites_ += *it;
    }
    return *this;
  }
  TypedExpectation& After(const ExpectationSet& s1, const ExpectationSet& s2) {
    return After(s1).After(s2);
  }
  TypedExpectation& After(const ExpectationSet& s1, const ExpectationSet& s2,
                          const ExpectationSet& s3) {
    return After(s1, s2).After(s3);
  }
  TypedExpectation& After(const ExpectationSet& s1, const ExpectationSet& s2,
                          const ExpectationSet& s3, const ExpectationSet& s4) {
    return After(s1, s2, s3).After(s4);
  }
  TypedExpectation& After(const ExpectationSet& s1, const ExpectationSet& s2,
                          const ExpectationSet& s3, const ExpectationSet& s4,
                          const ExpectationSet& s5) {
    return After(s1, s2, s3, s4).After(s5);
  }

  // Implements the .WillOnce() clause.
  TypedExpectation& WillOnce(const Action<F>& action) {
    ExpectSpecProperty(last_clause_ <= kWillOnce,
                       ".WillOnce() cannot appear after "
                       ".WillRepeatedly() or .RetiresOnSaturation().");
    last_clause_ = kWillOnce;

    untyped_actions_.push_back(new Action<F>(action));
    if (!cardinality_specified()) {
      set_cardinality(Exactly(static_cast<int>(untyped_actions_.size())));
    }
    return *this;
  }

  // Implements the .WillRepeatedly() clause.
  TypedExpectation& WillRepeatedly(const Action<F>& action) {
    if (last_clause_ == kWillRepeatedly) {
      ExpectSpecProperty(false,
                         ".WillRepeatedly() cannot appear "
                         "more than once in an EXPECT_CALL().");
    } else {
      ExpectSpecProperty(last_clause_ < kWillRepeatedly,
                         ".WillRepeatedly() cannot appear "
                         "after .RetiresOnSaturation().");
    }
    last_clause_ = kWillRepeatedly;
    repeated_action_specified_ = true;

    repeated_action_ = action;
    if (!cardinality_specified()) {
      set_cardinality(AtLeast(static_cast<int>(untyped_actions_.size())));
    }

    // Now that no more action clauses can be specified, we check
    // whether their count makes sense.
    CheckActionCountIfNotDone();
    return *this;
  }

  // Implements the .RetiresOnSaturation() clause.
  TypedExpectation& RetiresOnSaturation() {
    ExpectSpecProperty(last_clause_ < kRetiresOnSaturation,
                       ".RetiresOnSaturation() cannot appear "
                       "more than once.");
    last_clause_ = kRetiresOnSaturation;
    retires_on_saturation_ = true;

    // Now that no more action clauses can be specified, we check
    // whether their count makes sense.
    CheckActionCountIfNotDone();
    return *this;
  }

  // Returns the matchers for the arguments as specified inside the
  // EXPECT_CALL() macro.
  const ArgumentMatcherTuple& matchers() const {
    return matchers_;
  }

  // Returns the matcher specified by the .With() clause.
  const Matcher<const ArgumentTuple&>& extra_matcher() const {
    return extra_matcher_;
  }

  // Returns the action specified by the .WillRepeatedly() clause.
  const Action<F>& repeated_action() const { return repeated_action_; }

  // If this mock method has an extra matcher (i.e. .With(matcher)),
  // describes it to the ostream.
  virtual void MaybeDescribeExtraMatcherTo(::std::ostream* os) {
    if (extra_matcher_specified_) {
      *os << "    Expected args: ";
      extra_matcher_.DescribeTo(os);
      *os << "\n";
    }
  }

 private:
  template <typename Function>
  friend class FunctionMockerBase;

  // Returns an Expectation object that references and co-owns this
  // expectation.
  virtual Expectation GetHandle() {
    return owner_->GetHandleOf(this);
  }

  // The following methods will be called only after the EXPECT_CALL()
  // statement finishes and when the current thread holds
  // g_gmock_mutex.

  // Returns true iff this expectation matches the given arguments.
  bool Matches(const ArgumentTuple& args) const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    return TupleMatches(matchers_, args) && extra_matcher_.Matches(args);
  }

  // Returns true iff this expectation should handle the given arguments.
  bool ShouldHandleArguments(const ArgumentTuple& args) const
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();

    // In case the action count wasn't checked when the expectation
    // was defined (e.g. if this expectation has no WillRepeatedly()
    // or RetiresOnSaturation() clause), we check it when the
    // expectation is used for the first time.
    CheckActionCountIfNotDone();
    return !is_retired() && AllPrerequisitesAreSatisfied() && Matches(args);
  }

  // Describes the result of matching the arguments against this
  // expectation to the given ostream.
  void ExplainMatchResultTo(
      const ArgumentTuple& args,
      ::std::ostream* os) const
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();

    if (is_retired()) {
      *os << "         Expected: the expectation is active\n"
          << "           Actual: it is retired\n";
    } else if (!Matches(args)) {
      if (!TupleMatches(matchers_, args)) {
        ExplainMatchFailureTupleTo(matchers_, args, os);
      }
      StringMatchResultListener listener;
      if (!extra_matcher_.MatchAndExplain(args, &listener)) {
        *os << "    Expected args: ";
        extra_matcher_.DescribeTo(os);
        *os << "\n           Actual: don't match";

        internal::PrintIfNotEmpty(listener.str(), os);
        *os << "\n";
      }
    } else if (!AllPrerequisitesAreSatisfied()) {
      *os << "         Expected: all pre-requisites are satisfied\n"
          << "           Actual: the following immediate pre-requisites "
          << "are not satisfied:\n";
      ExpectationSet unsatisfied_prereqs;
      FindUnsatisfiedPrerequisites(&unsatisfied_prereqs);
      int i = 0;
      for (ExpectationSet::const_iterator it = unsatisfied_prereqs.begin();
           it != unsatisfied_prereqs.end(); ++it) {
        it->expectation_base()->DescribeLocationTo(os);
        *os << "pre-requisite #" << i++ << "\n";
      }
      *os << "                   (end of pre-requisites)\n";
    } else {
      // This line is here just for completeness' sake.  It will never
      // be executed as currently the ExplainMatchResultTo() function
      // is called only when the mock function call does NOT match the
      // expectation.
      *os << "The call matches the expectation.\n";
    }
  }

  // Returns the action that should be taken for the current invocation.
  const Action<F>& GetCurrentAction(
      const FunctionMockerBase<F>* mocker,
      const ArgumentTuple& args) const
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    const int count = call_count();
    Assert(count >= 1, __FILE__, __LINE__,
           "call_count() is <= 0 when GetCurrentAction() is "
           "called - this should never happen.");

    const int action_count = static_cast<int>(untyped_actions_.size());
    if (action_count > 0 && !repeated_action_specified_ &&
        count > action_count) {
      // If there is at least one WillOnce() and no WillRepeatedly(),
      // we warn the user when the WillOnce() clauses ran out.
      ::std::stringstream ss;
      DescribeLocationTo(&ss);
      ss << "Actions ran out in " << source_text() << "...\n"
         << "Called " << count << " times, but only "
         << action_count << " WillOnce()"
         << (action_count == 1 ? " is" : "s are") << " specified - ";
      mocker->DescribeDefaultActionTo(args, &ss);
      Log(kWarning, ss.str(), 1);
    }

    return count <= action_count ?
        *static_cast<const Action<F>*>(untyped_actions_[count - 1]) :
        repeated_action();
  }

  // Given the arguments of a mock function call, if the call will
  // over-saturate this expectation, returns the default action;
  // otherwise, returns the next action in this expectation.  Also
  // describes *what* happened to 'what', and explains *why* Google
  // Mock does it to 'why'.  This method is not const as it calls
  // IncrementCallCount().  A return value of NULL means the default
  // action.
  const Action<F>* GetActionForArguments(
      const FunctionMockerBase<F>* mocker,
      const ArgumentTuple& args,
      ::std::ostream* what,
      ::std::ostream* why)
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    if (IsSaturated()) {
      // We have an excessive call.
      IncrementCallCount();
      *what << "Mock function called more times than expected - ";
      mocker->DescribeDefaultActionTo(args, what);
      DescribeCallCountTo(why);

      // TODO(wan@google.com): allow the user to control whether
      // unexpected calls should fail immediately or continue using a
      // flag --gmock_unexpected_calls_are_fatal.
      return NULL;
    }

    IncrementCallCount();
    RetireAllPreRequisites();

    if (retires_on_saturation_ && IsSaturated()) {
      Retire();
    }

    // Must be done after IncrementCount()!
    *what << "Mock function call matches " << source_text() <<"...\n";
    return &(GetCurrentAction(mocker, args));
  }

  // All the fields below won't change once the EXPECT_CALL()
  // statement finishes.
  FunctionMockerBase<F>* const owner_;
  ArgumentMatcherTuple matchers_;
  Matcher<const ArgumentTuple&> extra_matcher_;
  Action<F> repeated_action_;

  GTEST_DISALLOW_COPY_AND_ASSIGN_(TypedExpectation);
};  // class TypedExpectation

// A MockSpec object is used by ON_CALL() or EXPECT_CALL() for
// specifying the default behavior of, or expectation on, a mock
// function.

// Note: class MockSpec really belongs to the ::testing namespace.
// However if we define it in ::testing, MSVC will complain when
// classes in ::testing::internal declare it as a friend class
// template.  To workaround this compiler bug, we define MockSpec in
// ::testing::internal and import it into ::testing.

// Logs a message including file and line number information.
GTEST_API_ void LogWithLocation(testing::internal::LogSeverity severity,
                                const char* file, int line,
                                const string& message);

template <typename F>
class MockSpec {
 public:
  typedef typename internal::Function<F>::ArgumentTuple ArgumentTuple;
  typedef typename internal::Function<F>::ArgumentMatcherTuple
      ArgumentMatcherTuple;

  // Constructs a MockSpec object, given the function mocker object
  // that the spec is associated with.
  explicit MockSpec(internal::FunctionMockerBase<F>* function_mocker)
      : function_mocker_(function_mocker) {}

  // Adds a new default action spec to the function mocker and returns
  // the newly created spec.
  internal::OnCallSpec<F>& InternalDefaultActionSetAt(
      const char* file, int line, const char* obj, const char* call) {
    LogWithLocation(internal::kInfo, file, line,
        string("ON_CALL(") + obj + ", " + call + ") invoked");
    return function_mocker_->AddNewOnCallSpec(file, line, matchers_);
  }

  // Adds a new expectation spec to the function mocker and returns
  // the newly created spec.
  internal::TypedExpectation<F>& InternalExpectedAt(
      const char* file, int line, const char* obj, const char* call) {
    const string source_text(string("EXPECT_CALL(") + obj + ", " + call + ")");
    LogWithLocation(internal::kInfo, file, line, source_text + " invoked");
    return function_mocker_->AddNewExpectation(
        file, line, source_text, matchers_);
  }

 private:
  template <typename Function>
  friend class internal::FunctionMocker;

  void SetMatchers(const ArgumentMatcherTuple& matchers) {
    matchers_ = matchers;
  }

  // The function mocker that owns this spec.
  internal::FunctionMockerBase<F>* const function_mocker_;
  // The argument matchers specified in the spec.
  ArgumentMatcherTuple matchers_;

  GTEST_DISALLOW_ASSIGN_(MockSpec);
};  // class MockSpec

// MSVC warns about using 'this' in base member initializer list, so
// we need to temporarily disable the warning.  We have to do it for
// the entire class to suppress the warning, even though it's about
// the constructor only.

#ifdef _MSC_VER
# pragma warning(push)          // Saves the current warning state.
# pragma warning(disable:4355)  // Temporarily disables warning 4355.
#endif  // _MSV_VER

// C++ treats the void type specially.  For example, you cannot define
// a void-typed variable or pass a void value to a function.
// ActionResultHolder<T> holds a value of type T, where T must be a
// copyable type or void (T doesn't need to be default-constructable).
// It hides the syntactic difference between void and other types, and
// is used to unify the code for invoking both void-returning and
// non-void-returning mock functions.

// Untyped base class for ActionResultHolder<T>.
class UntypedActionResultHolderBase {
 public:
  virtual ~UntypedActionResultHolderBase() {}

  // Prints the held value as an action's result to os.
  virtual void PrintAsActionResult(::std::ostream* os) const = 0;
};

// This generic definition is used when T is not void.
template <typename T>
class ActionResultHolder : public UntypedActionResultHolderBase {
 public:
  explicit ActionResultHolder(T a_value) : value_(a_value) {}

  // The compiler-generated copy constructor and assignment operator
  // are exactly what we need, so we don't need to define them.

  // Returns the held value and deletes this object.
  T GetValueAndDelete() const {
    T retval(value_);
    delete this;
    return retval;
  }

  // Prints the held value as an action's result to os.
  virtual void PrintAsActionResult(::std::ostream* os) const {
    *os << "\n          Returns: ";
    // T may be a reference type, so we don't use UniversalPrint().
    UniversalPrinter<T>::Print(value_, os);
  }

  // Performs the given mock function's default action and returns the
  // result in a new-ed ActionResultHolder.
  template <typename F>
  static ActionResultHolder* PerformDefaultAction(
      const FunctionMockerBase<F>* func_mocker,
      const typename Function<F>::ArgumentTuple& args,
      const string& call_description) {
    return new ActionResultHolder(
        func_mocker->PerformDefaultAction(args, call_description));
  }

  // Performs the given action and returns the result in a new-ed
  // ActionResultHolder.
  template <typename F>
  static ActionResultHolder*
  PerformAction(const Action<F>& action,
                const typename Function<F>::ArgumentTuple& args) {
    return new ActionResultHolder(action.Perform(args));
  }

 private:
  T value_;

  // T could be a reference type, so = isn't supported.
  GTEST_DISALLOW_ASSIGN_(ActionResultHolder);
};

// Specialization for T = void.
template <>
class ActionResultHolder<void> : public UntypedActionResultHolderBase {
 public:
  void GetValueAndDelete() const { delete this; }

  virtual void PrintAsActionResult(::std::ostream* /* os */) const {}

  // Performs the given mock function's default action and returns NULL;
  template <typename F>
  static ActionResultHolder* PerformDefaultAction(
      const FunctionMockerBase<F>* func_mocker,
      const typename Function<F>::ArgumentTuple& args,
      const string& call_description) {
    func_mocker->PerformDefaultAction(args, call_description);
    return NULL;
  }

  // Performs the given action and returns NULL.
  template <typename F>
  static ActionResultHolder* PerformAction(
      const Action<F>& action,
      const typename Function<F>::ArgumentTuple& args) {
    action.Perform(args);
    return NULL;
  }
};

// The base of the function mocker class for the given function type.
// We put the methods in this class instead of its child to avoid code
// bloat.
template <typename F>
class FunctionMockerBase : public UntypedFunctionMockerBase {
 public:
  typedef typename Function<F>::Result Result;
  typedef typename Function<F>::ArgumentTuple ArgumentTuple;
  typedef typename Function<F>::ArgumentMatcherTuple ArgumentMatcherTuple;

  FunctionMockerBase() : current_spec_(this) {}

  // The destructor verifies that all expectations on this mock
  // function have been satisfied.  If not, it will report Google Test
  // non-fatal failures for the violations.
  virtual ~FunctionMockerBase()
        GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    MutexLock l(&g_gmock_mutex);
    VerifyAndClearExpectationsLocked();
    Mock::UnregisterLocked(this);
    ClearDefaultActionsLocked();
  }

  // Returns the ON_CALL spec that matches this mock function with the
  // given arguments; returns NULL if no matching ON_CALL is found.
  // L = *
  const OnCallSpec<F>* FindOnCallSpec(
      const ArgumentTuple& args) const {
    for (UntypedOnCallSpecs::const_reverse_iterator it
             = untyped_on_call_specs_.rbegin();
         it != untyped_on_call_specs_.rend(); ++it) {
      const OnCallSpec<F>* spec = static_cast<const OnCallSpec<F>*>(*it);
      if (spec->Matches(args))
        return spec;
    }

    return NULL;
  }

  // Performs the default action of this mock function on the given
  // arguments and returns the result. Asserts (or throws if
  // exceptions are enabled) with a helpful call descrption if there
  // is no valid return value. This method doesn't depend on the
  // mutable state of this object, and thus can be called concurrently
  // without locking.
  // L = *
  Result PerformDefaultAction(const ArgumentTuple& args,
                              const string& call_description) const {
    const OnCallSpec<F>* const spec =
        this->FindOnCallSpec(args);
    if (spec != NULL) {
      return spec->GetAction().Perform(args);
    }
    const string message = call_description +
        "\n    The mock function has no default action "
        "set, and its return type has no default value set.";
#if GTEST_HAS_EXCEPTIONS
    if (!DefaultValue<Result>::Exists()) {
      throw std::runtime_error(message);
    }
#else
    Assert(DefaultValue<Result>::Exists(), "", -1, message);
#endif
    return DefaultValue<Result>::Get();
  }

  // Performs the default action with the given arguments and returns
  // the action's result.  The call description string will be used in
  // the error message to describe the call in the case the default
  // action fails.  The caller is responsible for deleting the result.
  // L = *
  virtual UntypedActionResultHolderBase* UntypedPerformDefaultAction(
      const void* untyped_args,  // must point to an ArgumentTuple
      const string& call_description) const {
    const ArgumentTuple& args =
        *static_cast<const ArgumentTuple*>(untyped_args);
    return ResultHolder::PerformDefaultAction(this, args, call_description);
  }

  // Performs the given action with the given arguments and returns
  // the action's result.  The caller is responsible for deleting the
  // result.
  // L = *
  virtual UntypedActionResultHolderBase* UntypedPerformAction(
      const void* untyped_action, const void* untyped_args) const {
    // Make a copy of the action before performing it, in case the
    // action deletes the mock object (and thus deletes itself).
    const Action<F> action = *static_cast<const Action<F>*>(untyped_action);
    const ArgumentTuple& args =
        *static_cast<const ArgumentTuple*>(untyped_args);
    return ResultHolder::PerformAction(action, args);
  }

  // Implements UntypedFunctionMockerBase::ClearDefaultActionsLocked():
  // clears the ON_CALL()s set on this mock function.
  virtual void ClearDefaultActionsLocked()
      GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();

    // Deleting our default actions may trigger other mock objects to be
    // deleted, for example if an action contains a reference counted smart
    // pointer to that mock object, and that is the last reference. So if we
    // delete our actions within the context of the global mutex we may deadlock
    // when this method is called again. Instead, make a copy of the set of
    // actions to delete, clear our set within the mutex, and then delete the
    // actions outside of the mutex.
    UntypedOnCallSpecs specs_to_delete;
    untyped_on_call_specs_.swap(specs_to_delete);

    g_gmock_mutex.Unlock();
    for (UntypedOnCallSpecs::const_iterator it =
             specs_to_delete.begin();
         it != specs_to_delete.end(); ++it) {
      delete static_cast<const OnCallSpec<F>*>(*it);
    }

    // Lock the mutex again, since the caller expects it to be locked when we
    // return.
    g_gmock_mutex.Lock();
  }

 protected:
  template <typename Function>
  friend class MockSpec;

  typedef ActionResultHolder<Result> ResultHolder;

  // Returns the result of invoking this mock function with the given
  // arguments.  This function can be safely called from multiple
  // threads concurrently.
  Result InvokeWith(const ArgumentTuple& args)
        GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    return static_cast<const ResultHolder*>(
        this->UntypedInvokeWith(&args))->GetValueAndDelete();
  }

  // Adds and returns a default action spec for this mock function.
  OnCallSpec<F>& AddNewOnCallSpec(
      const char* file, int line,
      const ArgumentMatcherTuple& m)
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    Mock::RegisterUseByOnCallOrExpectCall(MockObject(), file, line);
    OnCallSpec<F>* const on_call_spec = new OnCallSpec<F>(file, line, m);
    untyped_on_call_specs_.push_back(on_call_spec);
    return *on_call_spec;
  }

  // Adds and returns an expectation spec for this mock function.
  TypedExpectation<F>& AddNewExpectation(
      const char* file,
      int line,
      const string& source_text,
      const ArgumentMatcherTuple& m)
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    Mock::RegisterUseByOnCallOrExpectCall(MockObject(), file, line);
    TypedExpectation<F>* const expectation =
        new TypedExpectation<F>(this, file, line, source_text, m);
    const linked_ptr<ExpectationBase> untyped_expectation(expectation);
    untyped_expectations_.push_back(untyped_expectation);

    // Adds this expectation into the implicit sequence if there is one.
    Sequence* const implicit_sequence = g_gmock_implicit_sequence.get();
    if (implicit_sequence != NULL) {
      implicit_sequence->AddExpectation(Expectation(untyped_expectation));
    }

    return *expectation;
  }

  // The current spec (either default action spec or expectation spec)
  // being described on this function mocker.
  MockSpec<F>& current_spec() { return current_spec_; }

 private:
  template <typename Func> friend class TypedExpectation;

  // Some utilities needed for implementing UntypedInvokeWith().

  // Describes what default action will be performed for the given
  // arguments.
  // L = *
  void DescribeDefaultActionTo(const ArgumentTuple& args,
                               ::std::ostream* os) const {
    const OnCallSpec<F>* const spec = FindOnCallSpec(args);

    if (spec == NULL) {
      *os << (internal::type_equals<Result, void>::value ?
              "returning directly.\n" :
              "returning default value.\n");
    } else {
      *os << "taking default action specified at:\n"
          << FormatFileLocation(spec->file(), spec->line()) << "\n";
    }
  }

  // Writes a message that the call is uninteresting (i.e. neither
  // explicitly expected nor explicitly unexpected) to the given
  // ostream.
  virtual void UntypedDescribeUninterestingCall(
      const void* untyped_args,
      ::std::ostream* os) const
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    const ArgumentTuple& args =
        *static_cast<const ArgumentTuple*>(untyped_args);
    *os << "Uninteresting mock function call - ";
    DescribeDefaultActionTo(args, os);
    *os << "    Function call: " << Name();
    UniversalPrint(args, os);
  }

  // Returns the expectation that matches the given function arguments
  // (or NULL is there's no match); when a match is found,
  // untyped_action is set to point to the action that should be
  // performed (or NULL if the action is "do default"), and
  // is_excessive is modified to indicate whether the call exceeds the
  // expected number.
  //
  // Critical section: We must find the matching expectation and the
  // corresponding action that needs to be taken in an ATOMIC
  // transaction.  Otherwise another thread may call this mock
  // method in the middle and mess up the state.
  //
  // However, performing the action has to be left out of the critical
  // section.  The reason is that we have no control on what the
  // action does (it can invoke an arbitrary user function or even a
  // mock function) and excessive locking could cause a dead lock.
  virtual const ExpectationBase* UntypedFindMatchingExpectation(
      const void* untyped_args,
      const void** untyped_action, bool* is_excessive,
      ::std::ostream* what, ::std::ostream* why)
          GTEST_LOCK_EXCLUDED_(g_gmock_mutex) {
    const ArgumentTuple& args =
        *static_cast<const ArgumentTuple*>(untyped_args);
    MutexLock l(&g_gmock_mutex);
    TypedExpectation<F>* exp = this->FindMatchingExpectationLocked(args);
    if (exp == NULL) {  // A match wasn't found.
      this->FormatUnexpectedCallMessageLocked(args, what, why);
      return NULL;
    }

    // This line must be done before calling GetActionForArguments(),
    // which will increment the call count for *exp and thus affect
    // its saturation status.
    *is_excessive = exp->IsSaturated();
    const Action<F>* action = exp->GetActionForArguments(this, args, what, why);
    if (action != NULL && action->IsDoDefault())
      action = NULL;  // Normalize "do default" to NULL.
    *untyped_action = action;
    return exp;
  }

  // Prints the given function arguments to the ostream.
  virtual void UntypedPrintArgs(const void* untyped_args,
                                ::std::ostream* os) const {
    const ArgumentTuple& args =
        *static_cast<const ArgumentTuple*>(untyped_args);
    UniversalPrint(args, os);
  }

  // Returns the expectation that matches the arguments, or NULL if no
  // expectation matches them.
  TypedExpectation<F>* FindMatchingExpectationLocked(
      const ArgumentTuple& args) const
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    for (typename UntypedExpectations::const_reverse_iterator it =
             untyped_expectations_.rbegin();
         it != untyped_expectations_.rend(); ++it) {
      TypedExpectation<F>* const exp =
          static_cast<TypedExpectation<F>*>(it->get());
      if (exp->ShouldHandleArguments(args)) {
        return exp;
      }
    }
    return NULL;
  }

  // Returns a message that the arguments don't match any expectation.
  void FormatUnexpectedCallMessageLocked(
      const ArgumentTuple& args,
      ::std::ostream* os,
      ::std::ostream* why) const
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    *os << "\nUnexpected mock function call - ";
    DescribeDefaultActionTo(args, os);
    PrintTriedExpectationsLocked(args, why);
  }

  // Prints a list of expectations that have been tried against the
  // current mock function call.
  void PrintTriedExpectationsLocked(
      const ArgumentTuple& args,
      ::std::ostream* why) const
          GTEST_EXCLUSIVE_LOCK_REQUIRED_(g_gmock_mutex) {
    g_gmock_mutex.AssertHeld();
    const int count = static_cast<int>(untyped_expectations_.size());
    *why << "Google Mock tried the following " << count << " "
         << (count == 1 ? "expectation, but it didn't match" :
             "expectations, but none matched")
         << ":\n";
    for (int i = 0; i < count; i++) {
      TypedExpectation<F>* const expectation =
          static_cast<TypedExpectation<F>*>(untyped_expectations_[i].get());
      *why << "\n";
      expectation->DescribeLocationTo(why);
      if (count > 1) {
        *why << "tried expectation #" << i << ": ";
      }
      *why << expectation->source_text() << "...\n";
      expectation->ExplainMatchResultTo(args, why);
      expectation->DescribeCallCountTo(why);
    }
  }

  // The current spec (either default action spec or expectation spec)
  // being described on this function mocker.
  MockSpec<F> current_spec_;

  // There is no generally useful and implementable semantics of
  // copying a mock object, so copying a mock is usually a user error.
  // Thus we disallow copying function mockers.  If the user really
  // wants to copy a mock object, he should implement his own copy
  // operation, for example:
  //
  //   class MockFoo : public Foo {
  //    public:
  //     // Defines a copy constructor explicitly.
  //     MockFoo(const MockFoo& src) {}
  //     ...
  //   };
  GTEST_DISALLOW_COPY_AND_ASSIGN_(FunctionMockerBase);
};  // class FunctionMockerBase

#ifdef _MSC_VER
# pragma warning(pop)  // Restores the warning state.
#endif  // _MSV_VER

// Implements methods of FunctionMockerBase.

// Verifies that all expectations on this mock function have been
// satisfied.  Reports one or more Google Test non-fatal failures and
// returns false if not.

// Reports an uninteresting call (whose description is in msg) in the
// manner specified by 'reaction'.
void ReportUninterestingCall(CallReaction reaction, const string& msg);

}  // namespace internal

// The style guide prohibits "using" statements in a namespace scope
// inside a header file.  However, the MockSpec class template is
// meant to be defined in the ::testing namespace.  The following line
// is just a trick for working around a bug in MSVC 8.0, which cannot
// handle it if we define MockSpec in ::testing.
using internal::MockSpec;

// Const(x) is a convenient function for obtaining a const reference
// to x.  This is useful for setting expectations on an overloaded
// const mock method, e.g.
//
//   class MockFoo : public FooInterface {
//    public:
//     MOCK_METHOD0(Bar, int());
//     MOCK_CONST_METHOD0(Bar, int&());
//   };
//
//   MockFoo foo;
//   // Expects a call to non-const MockFoo::Bar().
//   EXPECT_CALL(foo, Bar());
//   // Expects a call to const MockFoo::Bar().
//   EXPECT_CALL(Const(foo), Bar());
template <typename T>
inline const T& Const(const T& x) { return x; }

// Constructs an Expectation object that references and co-owns exp.
inline Expectation::Expectation(internal::ExpectationBase& exp)  // NOLINT
    : expectation_base_(exp.GetHandle().expectation_base()) {}

}  // namespace testing

// A separate macro is required to avoid compile errors when the name
// of the method used in call is a result of macro expansion.
// See CompilesWithMethodNameExpandedFromMacro tests in
// internal/gmock-spec-builders_test.cc for more details.
#define GMOCK_ON_CALL_IMPL_(obj, call) \
    ((obj).gmock_##call).InternalDefaultActionSetAt(__FILE__, __LINE__, \
                                                    #obj, #call)
#define ON_CALL(obj, call) GMOCK_ON_CALL_IMPL_(obj, call)

#define GMOCK_EXPECT_CALL_IMPL_(obj, call) \
    ((obj).gmock_##call).InternalExpectedAt(__FILE__, __LINE__, #obj, #call)
#define EXPECT_CALL(obj, call) GMOCK_EXPECT_CALL_IMPL_(obj, call)

#endif  // GMOCK_INCLUDE_GMOCK_GMOCK_SPEC_BUILDERS_H_
