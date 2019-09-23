// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execerror

import (
	"bufio"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

const panicLineSubstring = "runtime/panic.go"

// CatchVectorizedRuntimeError executes operation, catches a runtime error if
// it is coming from the vectorized engine, and returns it. If an error not
// related to the vectorized engine occurs, it is not recovered from.
func CatchVectorizedRuntimeError(operation func()) (retErr error) {
	defer func() {
		if err := recover(); err != nil {
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
				panic(fmt.Sprintf("panic line %q not found in the stack trace\n%s", panicLineSubstring, stackTrace))
			}
			if scanner.Scan() {
				panicEmittedFrom := strings.TrimSpace(scanner.Text())
				if isPanicFromVectorizedEngine(panicEmittedFrom) {
					// We only want to catch runtime errors coming from the vectorized
					// engine.
					if e, ok := err.(error); ok {
						if _, ok := err.(*StorageError); ok {
							// A StorageError was caused by something below SQL, and represents
							// an error that we'd simply like to propagate along.
							// Do nothing.
						} else {
							doNotAnnotate := false
							if nvie, ok := e.(*notVectorizedInternalError); ok {
								// A notVectorizedInternalError was not caused by the
								// vectorized engine and represents an error that we don't
								// want to annotate in case it doesn't have a valid PG code.
								doNotAnnotate = true
								// We want to unwrap notVectorizedInternalError so that in case
								// the original error does have a valid PG code, the code is
								// correctly propagated.
								e = nvie.error
							}
							if code := pgerror.GetPGCode(e); !doNotAnnotate && code == pgcode.Uncategorized {
								// Any error without a code already is "surprising" and
								// needs to be annotated to indicate that it was
								// unexpected.
								e = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", e)
							}
						}
						retErr = e
					} else {
						// Not an error object. Definitely unexpected.
						surprisingObject := err
						retErr = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", surprisingObject)
					}
				} else {
					// Do not recover from the panic not related to the vectorized
					// engine.
					panic(err)
				}
			} else {
				panic(fmt.Sprintf("unexpectedly there is no line below the panic line in the stack trace\n%s", stackTrace))
			}
		}
		// No panic happened, so the operation must have been executed
		// successfully.
	}()
	operation()
	return retErr
}

const (
	colPackagePrefix          = "github.com/cockroachdb/cockroach/pkg/col"
	colexecPackagePrefix      = "github.com/cockroachdb/cockroach/pkg/sql/colexec"
	colflowsetupPackagePrefix = "github.com/cockroachdb/cockroach/pkg/sql/colflow"
	treePackagePrefix         = "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// isPanicFromVectorizedEngine checks whether the panic that was emitted from
// panicEmittedFrom line of code (which includes package name as well as the
// file name and the line number) came from the vectorized engine.
// panicEmittedFrom must be trimmed to not have any white spaces in the prefix.
func isPanicFromVectorizedEngine(panicEmittedFrom string) bool {
	const testExceptionPrefix = "github.com/cockroachdb/cockroach/pkg/sql/colflow.(*testNonVectorizedPanicEmitter)"
	if strings.HasPrefix(panicEmittedFrom, testExceptionPrefix) {
		// Although the panic appears to be coming from the vectorized engine, it
		// is intended to not be caught in order to test the panic propagation, so
		// we say that the panic is not from the vectorized engine.
		return false
	}
	return strings.HasPrefix(panicEmittedFrom, colPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colexecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colflowsetupPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, treePackagePrefix)
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

// notVectorizedInternalError is an error that originated outside of the
// vectorized engine (for example, it was caused by a non-columnar builtin).
// notVectorizedInternalError will be returned to the client not as an
// "internal error" and without the stack trace.
type notVectorizedInternalError struct {
	error
}

func newNotVectorizedInternalError(err error) *notVectorizedInternalError {
	return &notVectorizedInternalError{error: err}
}

// VectorizedInternalPanic simply panics with the provided object. It will
// always be returned as internal error to the client with the corresponding
// stack trace. This method should be called to propagate all unexpected errors
// that originated within the vectorized engine.
func VectorizedInternalPanic(err interface{}) {
	panic(err)
}

// NonVectorizedPanic panics with the error that is wrapped by
// notVectorizedInternalError which will not be treated as internal error and
// will not have a printed out stack trace. This method should be called to
// propagate all errors that originated outside of the vectorized engine and
// all expected errors from the vectorized engine.
func NonVectorizedPanic(err error) {
	panic(newNotVectorizedInternalError(err))
}
