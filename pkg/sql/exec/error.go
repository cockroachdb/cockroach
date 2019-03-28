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

// VectorizedRuntimeError is an error that can occur during execution of the
// vectorized engine.
type VectorizedRuntimeError struct {
	error
}

func makeVectorizedRuntimeError(err error) VectorizedRuntimeError {
	return VectorizedRuntimeError{err}
}

// TestVectorizedErrorEmitter is an Operator that panics with a
// VectorizedRuntimeError on every odd-numbered invocation of Next() and
// returns the next batch from the input on every even-numbered (i.e. it
// becomes a noop for those iterations). Used for tests only.
type TestVectorizedErrorEmitter struct {
	input     Operator
	emitBatch bool
}

var _ Operator = &TestVectorizedErrorEmitter{}

func NewTestVectorizedErrorEmitter(input Operator) Operator {
	return &TestVectorizedErrorEmitter{input: input}
}

func (e *TestVectorizedErrorEmitter) Init() {
	e.input.Init()
}

func (e *TestVectorizedErrorEmitter) Next() coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic(makeVectorizedRuntimeError(errors.New("Test VectorizedRuntimeError")))
	}
	e.emitBatch = false
	return e.input.Next()
}

// TestNonVectorizedErrorEmitter is an Operator that panics with a NON
// VectorizedRuntimeError on every odd-numbered invocation of Next() and
// returns the next batch from the input on every even-numbered (i.e. it
// becomes a noop for those iterations). Used for tests only.
type TestNonVectorizedErrorEmitter struct {
	input     Operator
	emitBatch bool
}

var _ Operator = &TestNonVectorizedErrorEmitter{}

func NewTestNonVectorizedErrorEmitter(input Operator) Operator {
	return &TestNonVectorizedErrorEmitter{input: input}
}

func (e *TestNonVectorizedErrorEmitter) Init() {
	e.input.Init()
}

func (e *TestNonVectorizedErrorEmitter) Next() coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic(errors.New("Test NON VectorizedRuntimeError"))
	}
	e.emitBatch = false
	return e.input.Next()
}
