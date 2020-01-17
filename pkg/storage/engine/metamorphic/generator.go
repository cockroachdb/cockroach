// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metamorphic

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Object to store info corresponding to one metamorphic test run. Responsible
// for generating and executing operations.
type metaTestRunner struct {
	ctx         context.Context
	w           io.Writer
	t           *testing.T
	rng         *rand.Rand
	seed        int64
	engine      engine.Engine
	tsGenerator tsGenerator
	managers    map[operandType]operandManager
	nameToOp    map[string]*mvccOp
	weights     []int
	ops         []*mvccOp
}

func (m *metaTestRunner) init() {
	// Use a passed-in seed. Using the same seed for two consecutive metamorphic
	// test runs should guarantee the same operations being generated.
	m.rng = rand.New(rand.NewSource(m.seed))

	m.managers = map[operandType]operandManager{
		OPERAND_TRANSACTION: &txnManager{
			rng:             m.rng,
			tsGenerator:     &m.tsGenerator,
			txnIdMap:        make(map[string]*roachpb.Transaction),
			inFlightBatches: make(map[*roachpb.Transaction][]engine.Batch),
			testRunner:      m,
		},
		OPERAND_READWRITER: &readWriterManager{
			rng:          m.rng,
			eng:          m.engine,
			batchToIdMap: make(map[engine.Batch]int),
		},
		OPERAND_MVCC_KEY: &keyManager{
			rng:         m.rng,
			tsGenerator: &m.tsGenerator,
		},
		OPERAND_VALUE:       &valueManager{m.rng},
		OPERAND_TEST_RUNNER: &testRunnerManager{m},
		OPERAND_ITERATOR: &iteratorManager{
			rng:          m.rng,
			readerToIter: make(map[engine.Reader][]engine.Iterator),
			iterToId:     make(map[engine.Iterator]uint64),
			iterCounter:  0,
		},
	}
	m.nameToOp = make(map[string]*mvccOp)

	m.weights = make([]int, len(operations))
	for i := range operations {
		m.weights[i] = operations[i].weight
		m.nameToOp[operations[i].name] = &operations[i]
	}
	m.ops = nil
}

// generateAndRun generates n operations using a TPCC-style deck shuffle with
// weighted probabilities of each operation appearing.
func (m *metaTestRunner) generateAndRun(n uint64) {
	m.ops = make([]*mvccOp, n)
	deck := NewDeck(m.rng, m.weights...)

	for i := uint64(0); i < n; i++ {
		opToAdd := &operations[deck.Int()]

		m.resolveAndRunOp(opToAdd)
	}

	// Close all open objects. This should let the engine close cleanly.
	closingOrder := []operandType{
		OPERAND_ITERATOR,
		OPERAND_READWRITER,
		OPERAND_TRANSACTION,
	}
	for _, operandType := range closingOrder {
		m.managers[operandType].closeAll()
	}
}
func (m *metaTestRunner) parseFileAndRun(f io.Reader) {
	// TODO(itsbilal): Implement this.
}

func (m *metaTestRunner) runOp(run opRun) string {
	op := run.op

	// This operation might require other operations to run before it runs. Call
	// the dependentOps method to resolve these dependencies.
	if op.dependentOps != nil {
		for _, opRun := range op.dependentOps(m, run.args...) {
			m.runOp(opRun)
		}
	}

	// Running the operation could cause this operand to not exist. Build strings
	// for arguments beforehand.
	argStrings := make([]string, len(op.operands))
	for i, arg := range run.args {
		argStrings[i] = m.managers[op.operands[i]].toString(arg)
	}

	m.ops = append(m.ops, op)
	output := op.run(m.ctx, m, run.args...)
	m.printOp(op, argStrings, output)
	return output
}

// Resolve all operands (including recursively running openers for operands as
// necessary) and run the specified operation.
func (m *metaTestRunner) resolveAndRunOp(op *mvccOp) {
	operandInstances := make([]operand, len(op.operands))

	// Operation op depends on some operands to exist in an open state.
	// If those operands' managers report a zero count for that object's open
	// instances, recursively call addOp with that operand type's opener.
	for i, operand := range op.operands {
		opManager := m.managers[operand]
		if opManager.count() == 0 {
			// Add this operation to the list first, so that it creates the dependency.
			m.resolveAndRunOp(m.nameToOp[opManager.opener()])
		}
		operandInstances[i] = opManager.get()
	}

	m.runOp(opRun{
		op:   op,
		args: operandInstances,
	})
}

// Print passed-in operation, arguments and output string to output file.
func (m *metaTestRunner) printOp(op *mvccOp, argStrings []string, output string) {
	fmt.Fprintf(m.w, "%s(", op.name)
	for i, arg := range argStrings {
		if i > 0 {
			fmt.Fprintf(m.w, ", ")
		}
		fmt.Fprintf(m.w, "%s", arg)
	}
	fmt.Fprintf(m.w, ") -> %s\n", output)
}

// Monotonically increasing timestamp generator.
type tsGenerator struct {
	lastTS hlc.Timestamp
}

func (t *tsGenerator) generate() hlc.Timestamp {
	t.lastTS.WallTime++
	return t.lastTS
}

func (t *tsGenerator) randomPastTimestamp(rng *rand.Rand) hlc.Timestamp {
	var result hlc.Timestamp
	result.WallTime = int64(float64(t.lastTS.WallTime+1) * rng.Float64())
	return result
}
