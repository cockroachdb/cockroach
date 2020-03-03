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
	ctx             context.Context
	w               io.Writer
	t               *testing.T
	rng             *rand.Rand
	seed            int64
	path            string
	engineImpls     []engineImpl
	curEngine       int
	restarts        bool
	engine          engine.Engine
	tsGenerator     tsGenerator
	opGenerators    map[operandType]operandGenerator
	txnGenerator    *txnGenerator
	rwGenerator     *readWriterGenerator
	iterGenerator   *iteratorGenerator
	keyGenerator    *keyGenerator
	valueGenerator  *valueGenerator
	pastTSGenerator *pastTSGenerator
	nextTSGenerator *nextTSGenerator
	openIters       map[iteratorID]iteratorInfo
	openBatches     map[readWriterID]engine.ReadWriter
	openTxns        map[txnID]*roachpb.Transaction
	nameToGenerator map[string]*opGenerator
	ops             []opRun
	weights         []int
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

	// Initialize opGenerator structs. These retain all generation time
	// state of open objects.
	m.txnGenerator = &txnGenerator{
		rng:         m.rng,
		tsGenerator: &m.tsGenerator,
		txnIDMap:    make(map[txnID]*roachpb.Transaction),
		openBatches: make(map[txnID]map[readWriterID]struct{}),
		testRunner:  m,
	}
	m.rwGenerator = &readWriterGenerator{
		rng:        m.rng,
		m:          m,
		batchIDMap: make(map[readWriterID]engine.Batch),
	}
	m.iterGenerator = &iteratorGenerator{
		rng:          m.rng,
		readerToIter: make(map[readWriterID][]iteratorID),
		iterInfo:     make(map[iteratorID]iteratorInfo),
	}
	m.keyGenerator = &keyGenerator{
		rng:         m.rng,
		tsGenerator: &m.tsGenerator,
	}
	m.valueGenerator = &valueGenerator{m.rng}
	m.pastTSGenerator = &pastTSGenerator{
		rng:         m.rng,
		tsGenerator: &m.tsGenerator,
	}
	m.nextTSGenerator = &nextTSGenerator{
		pastTSGenerator: pastTSGenerator{
			rng:         m.rng,
			tsGenerator: &m.tsGenerator,
		},
	}

	m.opGenerators = map[operandType]operandGenerator{
		operandTransaction: m.txnGenerator,
		operandReadWriter:  m.rwGenerator,
		operandMVCCKey:     m.keyGenerator,
		operandPastTS:      m.pastTSGenerator,
		operandNextTS:      m.nextTSGenerator,
		operandValue:       m.valueGenerator,
		operandIterator:    m.iterGenerator,
	}

	m.nameToGenerator = make(map[string]*opGenerator)
	m.weights = make([]int, len(opGenerators))
	for i := range opGenerators {
		m.weights[i] = opGenerators[i].weight
		m.nameToGenerator[opGenerators[i].name] = &opGenerators[i]
	}
	m.ops = nil
	m.openIters = make(map[iteratorID]iteratorInfo)
	m.openBatches = make(map[readWriterID]engine.ReadWriter)
	m.openTxns = make(map[txnID]*roachpb.Transaction)
}

func (m *metaTestRunner) closeGenerators() {
	closingOrder := []operandGenerator{
		m.iterGenerator,
		m.rwGenerator,
		m.txnGenerator,
	}
	for _, generator := range closingOrder {
		generator.closeAll()
	}
}

// Run this function in a defer to ensure any Fatals on m.t do not cause panics
// due to leaked iterators.
func (m *metaTestRunner) closeAll() {
	if m.engine == nil {
		// Engine already closed; possibly running in a defer after a panic.
		return
	}
	// If there are any open objects, close them.
	for _, iter := range m.openIters {
		iter.iter.Close()
	}
	for _, batch := range m.openBatches {
		batch.Close()
	}
	// TODO(itsbilal): Abort all txns.
	m.openIters = make(map[iteratorID]iteratorInfo)
	m.openBatches = make(map[readWriterID]engine.ReadWriter)
	m.openTxns = make(map[txnID]*roachpb.Transaction)
	m.engine.Close()
	m.engine = nil
}

// Getters and setters for txns, batches, and iterators.
func (m *metaTestRunner) getTxn(id txnID) *roachpb.Transaction {
	txn, ok := m.openTxns[id]
	if !ok {
		panic(fmt.Sprintf("txn with id %s not found", string(id)))
	}
	return txn
}

func (m *metaTestRunner) setTxn(id txnID, txn *roachpb.Transaction) {
	m.openTxns[id] = txn
}

func (m *metaTestRunner) getReadWriter(id readWriterID) engine.ReadWriter {
	if id == "engine" {
		return m.engine
	}

	batch, ok := m.openBatches[id]
	if !ok {
		panic(fmt.Sprintf("batch with id %s not found", string(id)))
	}
	return batch
}

func (m *metaTestRunner) setReadWriter(id readWriterID, rw engine.ReadWriter) {
	if id == "engine" {
		// no-op
		return
	}
	m.openBatches[id] = rw
}

func (m *metaTestRunner) getIterInfo(id iteratorID) iteratorInfo {
	iter, ok := m.openIters[id]
	if !ok {
		panic(fmt.Sprintf("iter with id %s not found", string(id)))
	}
	return iter
}

func (m *metaTestRunner) setIterInfo(id iteratorID, iterInfo iteratorInfo) {
	m.openIters[id] = iterInfo
}

// generateAndRun generates n operations using a TPCC-style deck shuffle with
// weighted probabilities of each operation appearing.
func (m *metaTestRunner) generateAndRun(n int) {
	deck := newDeck(m.rng, m.weights...)
	for i := 0; i < n; i++ {
		op := &opGenerators[deck.Int()]

		m.resolveAndAddOp(op)
	}

	for i := range m.ops {
		opRun := &m.ops[i]
		output := opRun.op.run(m.ctx)
		m.printOp(opRun.name, opRun.args, output)
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
	lineCount := uint64(0)
	for {
		var opName, argListString, expectedOutput string
		var firstByte byte
		var err error

		lineCount++
		// Read the first byte to check if this line is a comment.
		firstByte, err = reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			m.t.Fatal(err)
		}
		if firstByte == '#' {
			// Advance to the end of the line and continue.
			if _, err := reader.ReadString('\n'); err != nil {
				if err == io.EOF {
					break
				}
				m.t.Fatal(err)
			}
			continue
		}

		if opName, err = reader.ReadString('('); err != nil {
			if err == io.EOF {
				break
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
		opGenerator := m.nameToGenerator[opName]
		m.ops = append(m.ops, opRun{
			name:           opGenerator.name,
			op:             opGenerator.generate(m.ctx, m, argStrings...),
			args:           argStrings,
			lineNum:        lineCount,
			expectedOutput: expectedOutput,
		})
	}

	for i := range m.ops {
		op := &m.ops[i]
		actualOutput := op.op.run(m.ctx)
		m.printOp(op.name, op.args, actualOutput)
		if strings.Compare(strings.TrimSpace(op.expectedOutput), strings.TrimSpace(actualOutput)) != 0 {
			// Error messages can sometimes mismatch. If both outputs contain "error",
			// consider this a pass.
			if strings.Contains(op.expectedOutput, "error") && strings.Contains(actualOutput, "error") {
				continue
			}
			m.t.Fatalf("mismatching output at line %d: expected %s, got %s", op.lineNum, op.expectedOutput, actualOutput)
		}
	}
}

func (m *metaTestRunner) generateAndAddOp(run opReference) mvccOp {
	opGenerator := run.generator

	// This operation might require other operations to run before it runs. Call
	// the dependentOps method to resolve these dependencies.
	if opGenerator.dependentOps != nil {
		for _, opReference := range opGenerator.dependentOps(m, run.args...) {
			m.generateAndAddOp(opReference)
		}
	}

	op := opGenerator.generate(m.ctx, m, run.args...)
	m.ops = append(m.ops, opRun{
		name: opGenerator.name,
		op:   op,
		args: run.args,
	})
	return op
}

// Resolve all operands (including recursively queueing openers for operands as
// necessary) and add the specified operation to the operations list.
func (m *metaTestRunner) resolveAndAddOp(op *opGenerator) {
	argStrings := make([]string, len(op.operands))

	// Operation op depends on some operands to exist in an open state.
	// If those operands' opGenerators report a zero count for that object's open
	// instances, recursively call generateAndAddOp with that operand type's
	// opener.
	for i, operand := range op.operands {
		opGenerator := m.opGenerators[operand]
		// Special case: if this is an opener operation, and the operand is the
		// last one in the list of operands, call getNew() to get a new ID instead.
		if i == len(op.operands)-1 && op.isOpener {
			argStrings[i] = opGenerator.getNew()
			continue
		}
		if opGenerator.count() == 0 {
			// Add this operation to the list first, so that it creates the
			// dependency.
			m.resolveAndAddOp(m.nameToGenerator[opGenerator.opener()])
		}
		argStrings[i] = opGenerator.get()
	}

	m.generateAndAddOp(opReference{
		generator: op,
		args:      argStrings,
	})
}

// Print passed-in operation, arguments and output string to output file.
func (m *metaTestRunner) printOp(opName string, argStrings []string, output string) {
	fmt.Fprintf(m.w, "%s(", opName)
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
