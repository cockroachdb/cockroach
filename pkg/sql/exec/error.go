// Copyright 2019 The Cockroach Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/pkg/errors"
)

// VectorizedRuntimeErrorMarker is a marker interface used to distinguish
// VectorizedRuntimeErrors from others.
type VectorizedRuntimeErrorMarker interface {
	error

	// isVectorizedRuntimeError is used as a mark on a struct that implements it.
	// It will never be called.
	isVectorizedRuntimeError() bool
}

// VectorizedRuntimeError is an error that can occur during execution of the
// vectorized engine.
type VectorizedRuntimeError struct {
	error
}

var _ VectorizedRuntimeErrorMarker = VectorizedRuntimeError{}

func (v VectorizedRuntimeError) isVectorizedRuntimeError() bool {
	return true
}

// ThrowExecError panics with a VectorizedRuntimeError that encompasses the
// provided err.
func ThrowExecError(err error) {
	panic(VectorizedRuntimeError{err})
}

// CatchVectorizedRuntimeError executes operation and returns caught
// VectorizedRuntimeError while not recovering from other errors.
func CatchVectorizedRuntimeError(operation func()) (retErr error) {
	defer func() {
		if err := recover(); err != nil {
			if vecErr, ok := err.(VectorizedRuntimeErrorMarker); ok {
				// We only want to catch runtime errors related to the
				// vectorized engine.
				retErr = vecErr
			} else {
				// Do not recover from the non related to the vectorized engine
				// error.
				panic(err)
			}
		} else {
			// No panic happened, so the operation must have been executed
			// successfully.
			retErr = nil
		}
	}()
	operation()
	return retErr
}

// TestVectorizedErrorEmitter is an Operator that panics on every odd-numbered
// invocation of Next() and returns the next batch from the input on every
// even-numbered (i.e. it becomes a noop for those iterations). execError
// determines whether a VectorizedRuntimeError is emitted. Used for tests only.
type TestVectorizedErrorEmitter struct {
	input     Operator
	emitBatch bool
	execError bool
}

var _ Operator = &TestVectorizedErrorEmitter{}

// NewTestVectorizedErrorEmitter creates a new TestVectorizedErrorEmitter.
func NewTestVectorizedErrorEmitter(input Operator, execError bool) Operator {
	return &TestVectorizedErrorEmitter{input: input, execError: execError}
}

// Init is part of Operator interface.
func (e *TestVectorizedErrorEmitter) Init() {
	e.input.Init()
}

// Next is part of Operator interface.
func (e *TestVectorizedErrorEmitter) Next() coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		if e.execError {
			ThrowExecError(errors.New("Test VectorizedRuntimeError"))
		} else {
			panic(errors.New("Test NON VectorizedRuntimeError"))
		}

	}
	e.emitBatch = false
	return e.input.Next()
}
