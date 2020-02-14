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
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
)

const zipfMax uint64 = 100000

func makeStorageConfig(path string) base.StorageConfig {
	return base.StorageConfig{
		Dir:      path,
		Settings: cluster.MakeTestingClusterSettings(),
	}
}

func createTestRocksDBEngine(path string) (engine.Engine, error) {
	cache := engine.NewRocksDBCache(1 << 20)
	defer cache.Release()
	cfg := engine.RocksDBConfig{
		StorageConfig: makeStorageConfig(path),
		ReadOnly:      false,
	}

	return engine.NewRocksDB(cfg, cache)
}

func createTestPebbleEngine(path string) (engine.Engine, error) {
	pebbleConfig := engine.PebbleConfig{
		StorageConfig: makeStorageConfig(path),
		Opts:          engine.DefaultPebbleOptions(),
	}
	pebbleConfig.Opts.Cache = pebble.NewCache(1 << 20)
	defer pebbleConfig.Opts.Cache.Unref()

	return engine.NewPebble(context.Background(), pebbleConfig)
}

type engineImpl struct {
	name   string
	create func(path string) (engine.Engine, error)
}

var _ fmt.Stringer = &engineImpl{}

func (e *engineImpl) String() string {
	return e.name
}

var engineImplRocksDB = engineImpl{"rocksdb", createTestRocksDBEngine}
var engineImplPebble = engineImpl{"pebble", createTestPebbleEngine}

// Object to store info corresponding to one metamorphic test run. Responsible
// for generating and executing operations.
type metaTestRunner struct {
	ctx         context.Context
	w           io.Writer
	t           *testing.T
	rng         *rand.Rand
	seed        int64
	path        string
	engineImpls []engineImpl
	curEngine   int
	restarts    bool
	engine      engine.Engine
	tsGenerator tsGenerator
	managers    map[operandType]operandManager
	nameToOp    map[string]*mvccOp
	weights     []int
}

func (m *metaTestRunner) init() {
	// Use a passed-in seed. Using the same seed for two consecutive metamorphic
	// test runs should guarantee the same operations being generated.
	m.rng = rand.New(rand.NewSource(m.seed))
	m.tsGenerator.init(m.rng)
	m.curEngine = 0

	var err error
	m.engine, err = m.engineImpls[0].create(m.path)
	if err != nil {
		m.t.Fatal(err)
	}

	m.managers = map[operandType]operandManager{
		operandTransaction: &txnManager{
			rng:         m.rng,
			tsGenerator: &m.tsGenerator,
			txnIDMap:    make(map[string]*roachpb.Transaction),
			openBatches: make(map[*roachpb.Transaction]map[engine.Batch]struct{}),
			testRunner:  m,
		},
		operandReadWriter: &readWriterManager{
			rng:          m.rng,
			m:            m,
			batchToIDMap: make(map[engine.Batch]int),
		},
		operandMVCCKey: &keyManager{
			rng:         m.rng,
			tsGenerator: &m.tsGenerator,
		},
		operandPastTS: &tsManager{
			rng:         m.rng,
			tsGenerator: &m.tsGenerator,
		},
		operandValue:      &valueManager{m.rng},
		operandTestRunner: &testRunnerManager{m},
		operandIterator: &iteratorManager{
			rng:          m.rng,
			readerToIter: make(map[engine.Reader][]engine.Iterator),
			iterToInfo:   make(map[engine.Iterator]iteratorInfo),
			iterCounter:  0,
		},
	}

	m.nameToOp = make(map[string]*mvccOp)
	m.weights = make([]int, len(operations))
	for i := range operations {
		m.weights[i] = operations[i].weight
		m.nameToOp[operations[i].name] = &operations[i]
	}
}

// Run this function in a defer to ensure any Fatals on m.t do not cause panics
// due to leaked iterators.
func (m *metaTestRunner) closeAll() {
	if m.engine == nil {
		// Engine already closed; possibly running in a defer after a panic.
		return
	}
	// Close all open objects. This should let the engine close cleanly.
	closingOrder := []operandType{
		operandIterator,
		operandReadWriter,
		operandTransaction,
	}
	for _, operandType := range closingOrder {
		m.managers[operandType].closeAll()
	}
	m.engine.Close()
	m.engine = nil
}

// generateAndRun generates n operations using a TPCC-style deck shuffle with
// weighted probabilities of each operation appearing.
func (m *metaTestRunner) generateAndRun(n int) {
	deck := newDeck(m.rng, m.weights...)
	for i := 0; i < n; i++ {
		op := &operations[deck.Int()]

		m.resolveAndRunOp(op)
	}
}

// Closes the current engine and starts another one up, with the same path.
// Returns the engine transition that
func (m *metaTestRunner) restart() (string, string) {
	m.closeAll()
	oldEngineName := m.engineImpls[m.curEngine].name
	// TODO(itsbilal): Select engines at random instead of cycling through them.
	m.curEngine++
	if m.curEngine >= len(m.engineImpls) {
		// If we're restarting more times than the number of engine implementations
		// specified, loop back around to the first engine type specified.
		m.curEngine = 0
	}

	var err error
	m.engine, err = m.engineImpls[m.curEngine].create(m.path)
	if err != nil {
		m.t.Fatal(err)
	}
	return oldEngineName, m.engineImpls[m.curEngine].name
}

func (m *metaTestRunner) parseFileAndRun(f io.Reader) {
	reader := bufio.NewReader(f)
	lineCount := 0
	for {
		var argList []operand
		var opName, argListString, expectedOutput string
		var firstByte byte
		var err error

		lineCount++
		// Read the first byte to check if this line is a comment.
		firstByte, err = reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return
			}
			m.t.Fatal(err)
		}
		if firstByte == '#' {
			// Advance to the end of the line and continue.
			if _, err := reader.ReadString('\n'); err != nil {
				if err == io.EOF {
					return
				}
				m.t.Fatal(err)
			}
			continue
		}

		if opName, err = reader.ReadString('('); err != nil {
			if err == io.EOF {
				return
			}
			m.t.Fatal(err)
		}
		opName = string(firstByte) + opName[:len(opName)-1]

		if argListString, err = reader.ReadString(')'); err != nil {
			m.t.Fatal(err)
		}

		// Parse argument list
		argStrings := strings.Split(argListString, ", ")
		// Special handling for last element: could end with ), or could just be )
		lastElem := argStrings[len(argStrings)-1]
		if strings.HasSuffix(lastElem, ")") {
			lastElem = lastElem[:len(lastElem)-1]
			if len(lastElem) > 0 {
				argStrings[len(argStrings)-1] = lastElem
			} else {
				argStrings = argStrings[:len(argStrings)-1]
			}
		} else {
			m.t.Fatalf("while parsing: last element %s did not have ) suffix", lastElem)
		}

		if _, err = reader.ReadString('>'); err != nil {
			m.t.Fatal(err)
		}
		// Space after arrow.
		if _, err = reader.Discard(1); err != nil {
			m.t.Fatal(err)
		}
		if expectedOutput, err = reader.ReadString('\n'); err != nil {
			m.t.Fatal(err)
		}

		// Resolve args
		op := m.nameToOp[opName]
		for i, operandType := range op.operands {
			operandInstance := m.managers[operandType].parse(argStrings[i])
			argList = append(argList, operandInstance)
		}

		actualOutput := m.runOp(opRun{
			op:   m.nameToOp[opName],
			args: argList,
		})

		if strings.Compare(strings.TrimSpace(expectedOutput), strings.TrimSpace(actualOutput)) != 0 {
			// Error messages can sometimes mismatch. If both outputs contain "error",
			// consider this a pass.
			if strings.Contains(expectedOutput, "error") && strings.Contains(actualOutput, "error") {
				continue
			}
			m.t.Fatalf("mismatching output at line %d: expected %s, got %s", lineCount, expectedOutput, actualOutput)
		}
	}
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

// printComment prints a comment line into the output file. Supports single-line
// comments only.
func (m *metaTestRunner) printComment(comment string) {
	fmt.Fprintf(m.w, "# %s\n", comment)
}

// Monotonically increasing timestamp generator.
type tsGenerator struct {
	lastTS hlc.Timestamp
	zipf   *rand.Zipf
}

func (t *tsGenerator) init(rng *rand.Rand) {
	t.zipf = rand.NewZipf(rng, 2, 5, zipfMax)
}

func (t *tsGenerator) generate() hlc.Timestamp {
	t.lastTS.WallTime++
	return t.lastTS
}

func (t *tsGenerator) randomPastTimestamp(rng *rand.Rand) hlc.Timestamp {
	var result hlc.Timestamp

	// Return a result that's skewed toward the latest wall time.
	result.WallTime = int64(float64(t.lastTS.WallTime) * float64((zipfMax - t.zipf.Uint64())) / float64(zipfMax))
	return result
}
