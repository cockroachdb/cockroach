// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecerror

import (
	"context"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

const (
	panicLineSubstring            = "runtime/panic.go"
	runtimePanicFileSubstring     = "runtime"
	runtimePanicFunctionSubstring = "runtime."
)

var testingKnobShouldCatchPanic bool

// ProductionBehaviorForTests reinstates the release-build behavior for
// CatchVectorizedRuntimeError, which is to catch *all* panics originating from
// within the vectorized execution engine, including runtime panics that are not
// wrapped in InternalError, ExpectedError, or StorageError. (Without calling
// this, in crdb_test builds, CatchVectorizedRuntimeError will only catch panics
// that are wrapped in InternalError, ExpectedError, or StorageError.)
func ProductionBehaviorForTests() func() {
	old := testingKnobShouldCatchPanic
	testingKnobShouldCatchPanic = true
	return func() {
		testingKnobShouldCatchPanic = old
	}
}

// CatchVectorizedRuntimeError executes operation, catches a runtime error if
// it is coming from the vectorized engine, and returns it. If an error not
// related to the vectorized engine occurs, it is not recovered from.
func CatchVectorizedRuntimeError(operation func()) (retErr error) {
	defer func() {
		panicObj := recover()
		if panicObj == nil {
			// No panic happened, so the operation must have been executed
			// successfully.
			return
		}

		// First check for error types that should only come from the vectorized
		// engine.
		if err, ok := panicObj.(error); ok {
			// StorageError was caused by something below SQL, and represents an error
			// that we'd simply like to propagate along.
			var se *StorageError
			// notInternalError is an error from the vectorized engine that we'd
			// simply like to propagate along.
			var nie *notInternalError
			// internalError is an error from the vectorized engine that might need to
			// be returned to the client with a stacktrace, sentry report, and
			// "internal error" designation.
			var ie *internalError
			passthrough := errors.As(err, &se) || errors.As(err, &nie)
			if errors.As(err, &ie) {
				// Unwrap so that internalError doesn't show up in sentry reports.
				retErr = ie.Unwrap()
				// If the internal error doesn't already have an error code, mark it as
				// an assertion error so that we generate a sentry report. (We don't do
				// this for StorageError, notInternalError, or context.Canceled to avoid
				// creating unnecessary sentry reports.)
				if !passthrough && !errors.Is(retErr, context.Canceled) {
					if code := pgerror.GetPGCode(retErr); code == pgcode.Uncategorized {
						retErr = errors.NewAssertionErrorWithWrappedErrf(
							retErr, "unexpected error from the vectorized engine",
						)
					}
				}
				return
			}
			if passthrough {
				retErr = err
				return
			}
		}

		// For other types of errors, we need to check whence the panic originated
		// to know what to do. If the panic originated in the vectorized engine, we
		// can safely return it as a normal error knowing that any illegal state
		// will be cleaned up when the statement finishes. If the panic originated
		// lower in the stack, however, we must treat it as unrecoverable because it
		// could indicate an illegal state that might persist even after this
		// statement finishes.
		//
		// To check whence the panic originated, we find the frame just before the
		// panic frame.
		var panicLineFound bool
		var panicEmittedFrom string
		// Usually, we'll find the offending frame within 3 program counters,
		// starting with the caller of this deferred function (2 above the
		// runtime.Callers frame). However, we also want to catch panics
		// originating in the Go runtime with the runtime frames being returned
		// by CallersFrames, so we allow for 5 more program counters for that
		// case (e.g. invalid interface conversions use 2 counters).
		pc := make([]uintptr, 8)
		n := runtime.Callers(2, pc)
		if n >= 1 {
			frames := runtime.CallersFrames(pc[:n])
			// A fixed number of program counters can expand to any number of frames.
			for {
				frame, more := frames.Next()
				if strings.Contains(frame.File, panicLineSubstring) {
					panicLineFound = true
				} else if panicLineFound {
					if strings.HasPrefix(frame.Function, runtimePanicFunctionSubstring) &&
						strings.Contains(frame.File, runtimePanicFileSubstring) {
						// This frame is from the Go runtime, so we simply
						// ignore it to get to the offending one within the
						// CRDB.
						continue
					}
					panicEmittedFrom = frame.Function
					break
				}
				if !more {
					break
				}
			}
		}
		if !shouldCatchPanic(panicEmittedFrom) {
			// The panic is from outside the vectorized engine (or we didn't find it
			// in the stack). We treat it as unrecoverable because it could indicate
			// an illegal state that might persist even after this statement finishes.
			panic(panicObj)
		}

		err, ok := panicObj.(error)
		if !ok {
			// Not an error object. Definitely unexpected.
			retErr = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", panicObj)
			return
		}
		retErr = err

		annotateErrorWithoutCode := true
		if errors.Is(err, context.Canceled) {
			// We don't want to annotate the context cancellation errors in case they
			// don't have a valid PG code so that the sentry report is not sent
			// (errors with failed assertions get sentry reports).
			annotateErrorWithoutCode = false
		}
		if code := pgerror.GetPGCode(err); annotateErrorWithoutCode && code == pgcode.Uncategorized {
			// Any error without a code already is "surprising" and
			// needs to be annotated to indicate that it was
			// unexpected.
			retErr = errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error from the vectorized engine")
		}
	}()
	operation()
	return retErr
}

// We use the approach of allow-listing the packages the panics from which are
// safe to catch (which is the case when the code doesn't update shared state
// and doesn't manipulate locks).
//
// Multiple actual packages can have the same prefix as a single constant string
// defined below, but all of such packages are allowed to be caught from.
const (
	colPackagesPrefix      = "github.com/cockroachdb/cockroach/pkg/col"
	encodingPackagePrefix  = "github.com/cockroachdb/cockroach/pkg/util/encoding"
	execinfraPackagePrefix = "github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	sqlColPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/col"
	sqlRowPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/row"
	sqlSemPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/sem"
	// When running BenchmarkCatchVectorizedRuntimeError under bazel, the
	// repository prefix is missing.
	testSqlColPackagesPrefix = "pkg/sql/col"
)

// shouldCatchPanic checks whether the panic that was emitted from
// panicEmittedFrom line of code (which contains the full path to the function
// where the panic originated) should be caught by the vectorized engine.
//
// The vectorized engine uses the panic-catch mechanism of error propagation, so
// we need to catch all of its errors. We also want to catch any panics that
// occur because of internal errors in some execution component (e.g. builtins).
//
// panicEmittedFrom must be trimmed to not have any white spaces in the prefix.
func shouldCatchPanic(panicEmittedFrom string) bool {
	// In crdb_test builds we do not catch runtime panics even if they originate
	// from within the vectorized execution engine. These panics probably
	// represent bugs in vectorized execution, and we want them to fail loudly in
	// tests (but not in production clusters).
	if buildutil.CrdbTestBuild && !testingKnobShouldCatchPanic {
		return false
	}

	const panicFromTheCatcherItselfPrefix = "github.com/cockroachdb/cockroach/pkg/sql/colexecerror.CatchVectorizedRuntimeError"
	if strings.HasPrefix(panicEmittedFrom, panicFromTheCatcherItselfPrefix) {
		// This panic came from the catcher itself, so we will propagate it
		// unchanged by the higher-level catchers.
		return false
	}
	const nonCatchablePanicPrefix = "github.com/cockroachdb/cockroach/pkg/sql/colexecerror.NonCatchablePanic"
	if strings.HasPrefix(panicEmittedFrom, nonCatchablePanicPrefix) {
		// This panic came from NonCatchablePanic() method and should not be
		// caught.
		return false
	}
	return strings.HasPrefix(panicEmittedFrom, colPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, encodingPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, execinfraPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlColPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlRowPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlSemPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, testSqlColPackagesPrefix)
}

// StorageError is an error that was created by a component below the sql
// stack, such as the network or storage layers. A StorageError will be bubbled
// up all the way past the SQL layer unchanged.
type StorageError struct {
	cause error
}

func (s *StorageError) Error() string { return s.cause.Error() }
func (s *StorageError) Cause() error  { return s.cause }
func (s *StorageError) Unwrap() error { return s.cause }

// NewStorageError returns a new storage error. This can be used to propagate
// an error through the exec subsystem unchanged.
func NewStorageError(err error) *StorageError {
	return &StorageError{cause: err}
}

// notInternalError is an error that occurs not because the vectorized engine
// happens to be in an unexpected state (for example, it was caused by a
// non-columnar builtin).
// notInternalError will be returned to the client not as an
// "internal error" and without the stack trace.
type notInternalError struct {
	cause error
}

func newNotInternalError(err error) *notInternalError {
	return &notInternalError{cause: err}
}

var (
	_ errors.Wrapper = &notInternalError{}
)

func (e *notInternalError) Error() string { return e.cause.Error() }
func (e *notInternalError) Cause() error  { return e.cause }
func (e *notInternalError) Unwrap() error { return e.Cause() }

func decodeNotInternalError(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return newNotInternalError(cause)
}

// internalError is an error that occurs because the vectorized engine is in an
// unexpected state. Usually it wraps an assertion error.
type internalError struct {
	cause error
}

func newInternalError(err error) *internalError {
	return &internalError{cause: err}
}

var (
	_ errors.Wrapper = &internalError{}
)

func (e *internalError) Error() string { return e.cause.Error() }
func (e *internalError) Cause() error  { return e.cause }
func (e *internalError) Unwrap() error { return e.Cause() }

func decodeInternalError(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return newInternalError(cause)
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*notInternalError)(nil)), decodeNotInternalError)
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*internalError)(nil)), decodeInternalError)
}

// InternalError panics with the error wrapped by internalError. It will always
// be caught and returned as internal error to the client with the corresponding
// stack trace. This method should be called to propagate errors that resulted
// in the vectorized engine being in an *unexpected* state.
func InternalError(err error) {
	panic(newInternalError(err))
}

// ExpectedError panics with the error that is wrapped by
// notInternalError which will not be treated as internal error and
// will not have a printed out stack trace. This method should be called to
// propagate errors that the vectorized engine *expects* to occur.
func ExpectedError(err error) {
	panic(newNotInternalError(err))
}

// NonCatchablePanic is the equivalent of Golang's 'panic' word that can be used
// in order to crash the goroutine. It could be used by the testing code within
// the vectorized engine to simulate a panic that occurs outside of the engine
// (and, thus, should not be caught).
func NonCatchablePanic(object interface{}) {
	panic(object)
}
