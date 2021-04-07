// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecerror

import (
	"bufio"
	"context"
	"runtime/debug"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

const panicLineSubstring = "runtime/panic.go"

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

		// Find where the panic came from and only proceed if it is related to the
		// vectorized engine.
		stackTrace := string(debug.Stack())
		scanner := bufio.NewScanner(strings.NewReader(stackTrace))
		panicLineFound := false
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), panicLineSubstring) {
				panicLineFound = true
				break
			}
		}
		if !panicLineFound {
			panic(errors.AssertionFailedf("panic line %q not found in the stack trace\n%s", panicLineSubstring, stackTrace))
		}
		if !scanner.Scan() {
			panic(errors.AssertionFailedf("unexpectedly there is no line below the panic line in the stack trace\n%s", stackTrace))
		}
		panicEmittedFrom := strings.TrimSpace(scanner.Text())
		if !shouldCatchPanic(panicEmittedFrom) {
			panic(panicObj)
		}

		err, ok := panicObj.(error)
		if !ok {
			// Not an error object. Definitely unexpected.
			retErr = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", panicObj)
			return
		}
		retErr = err

		if _, ok := panicObj.(*StorageError); ok {
			// A StorageError was caused by something below SQL, and represents
			// an error that we'd simply like to propagate along.
			// Do nothing.
			return
		}

		annotateErrorWithoutCode := true
		var nie *notInternalError
		if errors.As(err, &nie) {
			// A notInternalError was not caused by the vectorized engine and
			// represents an error that we don't want to annotate in case it
			// doesn't have a valid PG code.
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
	execinfraPackagePrefix = "github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	sqlColPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/col"
	sqlRowPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/row"
	sqlSemPackagesPrefix   = "github.com/cockroachdb/cockroach/pkg/sql/sem"
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
		strings.HasPrefix(panicEmittedFrom, execinfraPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlColPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlRowPackagesPrefix) ||
		strings.HasPrefix(panicEmittedFrom, sqlSemPackagesPrefix)
}

// StorageError is an error that was created by a component below the sql
// stack, such as the network or storage layers. A StorageError will be bubbled
// up all the way past the SQL layer unchanged.
type StorageError struct {
	error
}

// Cause implements the Causer interface.
func (s *StorageError) Cause() error {
	return s.error
}

// NewStorageError returns a new storage error. This can be used to propagate
// an error through the exec subsystem unchanged.
func NewStorageError(err error) *StorageError {
	return &StorageError{error: err}
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

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*notInternalError)(nil)), decodeNotInternalError)
}

// InternalError simply panics with the provided object. It will always be
// caught and returned as internal error to the client with the corresponding
// stack trace. This method should be called to propagate errors that resulted
// in the vectorized engine being in an *unexpected* state.
func InternalError(err error) {
	panic(err)
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
