// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import (
	"bufio"
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
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
						// Any error without a code already is "surprising" and
						// needs to be annotated to indicate that it was
						// unexpected.
						if code := pgerror.GetPGCode(e); code == pgcode.Uncategorized {
							e = errors.Wrap(e, "unexpected error from the vectorized runtime")
						}
						retErr = e
					} else {
						// Not an error object. Definitely unexpected.
						retErr = pgerror.AssertionFailedf("unexpected error from the vectorized runtime: %v", err)
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
	execPackagePrefix  = "github.com/cockroachdb/cockroach/pkg/sql/exec"
	colBatchScanPrefix = "github.com/cockroachdb/cockroach/pkg/sql/distsqlrun.(*colBatchScan)"
)

// isPanicFromVectorizedEngine checks whether the panic that was emitted from
// panicEmittedFrom line of code (which includes package name as well as the
// file name and the line number) came from the vectorized engine.
// panicEmittedFrom must be trimmed to not have any white spaces in the prefix.
func isPanicFromVectorizedEngine(panicEmittedFrom string) bool {
	return strings.HasPrefix(panicEmittedFrom, execPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colBatchScanPrefix)
}

// TestVectorizedErrorEmitter is an Operator that panics on every odd-numbered
// invocation of Next() and returns the next batch from the input on every
// even-numbered (i.e. it becomes a noop for those iterations). Used for tests
// only.
type TestVectorizedErrorEmitter struct {
	input     Operator
	emitBatch bool
}

var _ Operator = &TestVectorizedErrorEmitter{}

// NewTestVectorizedErrorEmitter creates a new TestVectorizedErrorEmitter.
func NewTestVectorizedErrorEmitter(input Operator) Operator {
	return &TestVectorizedErrorEmitter{input: input}
}

// Init is part of Operator interface.
func (e *TestVectorizedErrorEmitter) Init() {
	e.input.Init()
}

// Next is part of Operator interface.
func (e *TestVectorizedErrorEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic(errors.New("a panic from exec package"))
	}

	e.emitBatch = false
	return e.input.Next(ctx)
}
