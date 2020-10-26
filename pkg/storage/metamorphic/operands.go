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
	"fmt"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type operandType int

const (
	operandTransaction operandType = iota
	operandReadWriter
	operandMVCCKey
	operandPastTS
	operandNextTS
	operandValue
	operandIterator
	operandFloat
)

const (
	maxValueSize = 16
)

// operandGenerator represents an object to generate instances of operands
// that can be passed to an operation as an argument. For simplicity,
// we create operandGenerators for each type of argument, even primitive ones
// like MVCCKeys and values. All state about open objects (iterators,
// transactions, writers, etc) during generation should be stored in an
// operandGenerator.
//
// operandGenerators are strictly for generation-time use only; all info about
// execution time objects is stored directly in the metaTestRunner.
type operandGenerator interface {
	// get retrieves an instance of this operand. Depending on operand type (eg.
	// keys), it could also generate and return a new type of an instance. An
	// operand is represented as a serializable string, that can be converted into
	// a concrete instance type during execution by calling a get<concrete type>()
	// or parse() method on the concrete operand generator.
	get() string
	// getNew retrieves a new instance of this type of operand. Called when an
	// opener operation (with isOpener = true) needs an ID to store its output.
	getNew() string
	// opener returns the name of an operation generator (defined in
	// operations.go) that always creates a new instance of this object. Called by
	// the test runner when an operation requires one instance of this
	// operand to exist, and count() == 0.
	opener() string
	// count returns the number of live objects being managed by this generator.
	// If 0, the opener() operation can be called when necessary.
	count() int
	// closeAll closes all managed operands. Used when the test exits, or when a
	// restart operation executes.
	closeAll()
}

func generateBytes(rng *rand.Rand, min int, max int) []byte {
	// For better readability, stick to lowercase alphabet characters.
	iterations := min + rng.Intn(max-min)
	result := make([]byte, 0, iterations)

	for i := 0; i < iterations; i++ {
		result = append(result, byte(rng.Float64()*float64('z'-'a')+'a'))
	}
	return result
}

type keyGenerator struct {
	liveKeys    []storage.MVCCKey
	rng         *rand.Rand
	tsGenerator *tsGenerator
}

var _ operandGenerator = &keyGenerator{}

func (k *keyGenerator) opener() string {
	return ""
}

func (k *keyGenerator) count() int {
	// Always return a nonzero value so opener() is never called directly.
	return len(k.liveKeys) + 1
}

func (k *keyGenerator) open() storage.MVCCKey {
	var key storage.MVCCKey
	key.Key = generateBytes(k.rng, 8, maxValueSize)
	key.Timestamp = k.tsGenerator.lastTS
	k.liveKeys = append(k.liveKeys, key)

	return key
}

func (k *keyGenerator) toString(key storage.MVCCKey) string {
	return fmt.Sprintf("%s/%d", key.Key, key.Timestamp.WallTime)
}

func (k *keyGenerator) get() string {
	// 15% chance of returning a new key even if some exist.
	if len(k.liveKeys) == 0 || k.rng.Float64() < 0.30 {
		return k.toString(k.open())
	}

	return k.toString(k.liveKeys[k.rng.Intn(len(k.liveKeys))])
}

func (k *keyGenerator) getNew() string {
	return k.get()
}

func (k *keyGenerator) closeAll() {
	// No-op.
}

func (k *keyGenerator) parse(input string) storage.MVCCKey {
	var key storage.MVCCKey
	key.Key = make([]byte, 0, maxValueSize)
	_, err := fmt.Sscanf(input, "%q/%d", &key.Key, &key.Timestamp.WallTime)
	if err != nil {
		panic(err)
	}
	return key
}

type valueGenerator struct {
	rng *rand.Rand
}

var _ operandGenerator = &valueGenerator{}

func (v *valueGenerator) opener() string {
	return ""
}

func (v *valueGenerator) count() int {
	return 1
}

func (v *valueGenerator) get() string {
	return v.toString(generateBytes(v.rng, 4, maxValueSize))
}

func (v *valueGenerator) getNew() string {
	return v.get()
}

func (v *valueGenerator) closeAll() {
	// No-op.
}

func (v *valueGenerator) toString(value []byte) string {
	return fmt.Sprintf("%s", value)
}

func (v *valueGenerator) parse(input string) []byte {
	return []byte(input)
}

type txnID string

type txnGenerator struct {
	rng         *rand.Rand
	testRunner  *metaTestRunner
	tsGenerator *tsGenerator
	liveTxns    []txnID
	txnIDMap    map[txnID]*roachpb.Transaction
	openBatches map[txnID]map[readWriterID]struct{}
	// Counts "generated" transactions - i.e. how many txn_open()s have been
	// inserted so far. Could stay 0 in check mode.
	txnGenCounter uint64
}

var _ operandGenerator = &txnGenerator{}

func (t *txnGenerator) opener() string {
	return "txn_open"
}

func (t *txnGenerator) count() int {
	return len(t.txnIDMap)
}

func (t *txnGenerator) get() string {
	if len(t.liveTxns) == 0 {
		panic("no open txns")
	}
	return string(t.liveTxns[t.rng.Intn(len(t.liveTxns))])
}

// getNew returns a transaction ID, and saves this transaction as a "live"
// transaction for generation purposes. Called only during generation, and
// must be matched with a generateClose call.
func (t *txnGenerator) getNew() string {
	t.txnGenCounter++
	id := txnID(fmt.Sprintf("t%d", t.txnGenCounter))
	// Increment the timestamp.
	t.tsGenerator.generate()
	// This ensures count() is correct as of generation time.
	t.txnIDMap[id] = nil
	t.liveTxns = append(t.liveTxns, id)
	return string(id)
}

// generateClose is called when a transaction closing operation is generated.
func (t *txnGenerator) generateClose(id txnID) {
	delete(t.openBatches, id)
	delete(t.txnIDMap, id)

	for i := range t.liveTxns {
		if t.liveTxns[i] == id {
			t.liveTxns[i] = t.liveTxns[len(t.liveTxns)-1]
			t.liveTxns = t.liveTxns[:len(t.liveTxns)-1]
			break
		}
	}
}

func (t *txnGenerator) clearBatch(batch readWriterID) {
	for _, batches := range t.openBatches {
		delete(batches, batch)
	}
}

func (t *txnGenerator) trackWriteOnBatch(w readWriterID, txn txnID) {
	if w == "engine" {
		return
	}
	openBatches, ok := t.openBatches[txn]
	if !ok {
		t.openBatches[txn] = make(map[readWriterID]struct{})
		openBatches = t.openBatches[txn]
	}
	openBatches[w] = struct{}{}
}

func (t *txnGenerator) closeAll() {
	t.liveTxns = nil
	t.txnIDMap = make(map[txnID]*roachpb.Transaction)
	t.openBatches = make(map[txnID]map[readWriterID]struct{})
}

type pastTSGenerator struct {
	rng         *rand.Rand
	tsGenerator *tsGenerator
}

var _ operandGenerator = &pastTSGenerator{}

func (t *pastTSGenerator) opener() string {
	return ""
}

func (t *pastTSGenerator) count() int {
	// Always return a non-zero count so opener() is never called.
	return int(t.tsGenerator.lastTS.WallTime) + 1
}

func (t *pastTSGenerator) closeAll() {
	// No-op.
}

func (t *pastTSGenerator) toString(ts hlc.Timestamp) string {
	return fmt.Sprintf("%d", ts.WallTime)
}

func (t *pastTSGenerator) parse(input string) hlc.Timestamp {
	var ts hlc.Timestamp
	wallTime, err := strconv.ParseInt(input, 10, 0)
	if err != nil {
		panic(err)
	}
	ts.WallTime = wallTime
	return ts
}

func (t *pastTSGenerator) get() string {
	return t.toString(t.tsGenerator.randomPastTimestamp(t.rng))
}

func (t *pastTSGenerator) getNew() string {
	return t.get()
}

// Similar to pastTSGenerator, except it always increments the "current" timestamp
// and returns the newest one.
type nextTSGenerator struct {
	pastTSGenerator
}

func (t *nextTSGenerator) get() string {
	return t.toString(t.tsGenerator.generate())
}

func (t *nextTSGenerator) getNew() string {
	return t.get()
}

type readWriterID string

type readWriterGenerator struct {
	rng             *rand.Rand
	m               *metaTestRunner
	liveBatches     []readWriterID
	batchIDMap      map[readWriterID]storage.Batch
	batchGenCounter uint64
}

var _ operandGenerator = &readWriterGenerator{}

func (w *readWriterGenerator) get() string {
	// 25% chance of returning the engine, even if there are live batches.
	if len(w.liveBatches) == 0 || w.rng.Float64() < 0.25 {
		return "engine"
	}

	return string(w.liveBatches[w.rng.Intn(len(w.liveBatches))])
}

// getNew is called during generation to generate a batch ID.
func (w *readWriterGenerator) getNew() string {
	w.batchGenCounter++
	id := readWriterID(fmt.Sprintf("batch%d", w.batchGenCounter))
	w.batchIDMap[id] = nil
	w.liveBatches = append(w.liveBatches, id)

	return string(id)
}

func (w *readWriterGenerator) opener() string {
	return "batch_open"
}

// generateClose is called during generation when an operation that closes a
// readWriter is generated.
func (w *readWriterGenerator) generateClose(id readWriterID) {
	if id == "engine" {
		return
	}
	delete(w.batchIDMap, id)
	for i, batch := range w.liveBatches {
		if batch == id {
			w.liveBatches[i] = w.liveBatches[len(w.liveBatches)-1]
			w.liveBatches = w.liveBatches[:len(w.liveBatches)-1]
			break
		}
	}
	w.m.txnGenerator.clearBatch(id)
}

func (w *readWriterGenerator) count() int {
	return len(w.batchIDMap) + 1
}

func (w *readWriterGenerator) closeAll() {
	for _, batch := range w.batchIDMap {
		if batch != nil {
			batch.Close()
		}
	}
	w.liveBatches = w.liveBatches[:0]
	w.batchIDMap = make(map[readWriterID]storage.Batch)
}

type iteratorID string
type iteratorInfo struct {
	id          iteratorID
	iter        storage.MVCCIterator
	lowerBound  roachpb.Key
	isBatchIter bool
}

type iteratorGenerator struct {
	rng            *rand.Rand
	readerToIter   map[readWriterID][]iteratorID
	iterInfo       map[iteratorID]iteratorInfo
	liveIters      []iteratorID
	iterGenCounter uint64
}

var _ operandGenerator = &iteratorGenerator{}

func (i *iteratorGenerator) get() string {
	if len(i.liveIters) == 0 {
		panic("no open iterators")
	}

	return string(i.liveIters[i.rng.Intn(len(i.liveIters))])
}

func (i *iteratorGenerator) getNew() string {
	i.iterGenCounter++
	id := fmt.Sprintf("iter%d", i.iterGenCounter)
	return id
}

// generateOpen is called during generation to generate an iterator ID for the
// specified readWriter.
func (i *iteratorGenerator) generateOpen(rwID readWriterID, id iteratorID) {
	i.iterInfo[id] = iteratorInfo{
		id:          id,
		lowerBound:  nil,
		isBatchIter: rwID != "engine",
	}
	i.readerToIter[rwID] = append(i.readerToIter[rwID], id)
	i.liveIters = append(i.liveIters, id)
}

// generateClose is called during generation when an operation that closes an
// iterator is generated.
func (i *iteratorGenerator) generateClose(id iteratorID) {
	delete(i.iterInfo, id)
	// Clear iter from readerToIter
	for reader, iters := range i.readerToIter {
		for j, id2 := range iters {
			if id == id2 {
				// Delete iters[j]
				iters[j] = iters[len(iters)-1]
				i.readerToIter[reader] = iters[:len(iters)-1]

				break
			}
		}
	}
	// Clear iter from liveIters
	for j, iter := range i.liveIters {
		if id == iter {
			i.liveIters[j] = i.liveIters[len(i.liveIters)-1]
			i.liveIters = i.liveIters[:len(i.liveIters)-1]
			break
		}
	}
}

func (i *iteratorGenerator) opener() string {
	return "iterator_open"
}

func (i *iteratorGenerator) count() int {
	return len(i.iterInfo)
}

func (i *iteratorGenerator) closeAll() {
	i.liveIters = nil
	i.iterInfo = make(map[iteratorID]iteratorInfo)
	i.readerToIter = make(map[readWriterID][]iteratorID)
}

type floatGenerator struct {
	rng *rand.Rand
}

func (f *floatGenerator) get() string {
	return fmt.Sprintf("%.4f", f.rng.Float32())
}

func (f *floatGenerator) getNew() string {
	return f.get()
}

func (f *floatGenerator) parse(input string) float32 {
	var result float32
	if _, err := fmt.Sscanf(input, "%f", &result); err != nil {
		panic(err)
	}
	return result
}

func (f *floatGenerator) opener() string {
	// Not applicable, because count() is always nonzero.
	return ""
}

func (f *floatGenerator) count() int {
	return 1
}

func (f *floatGenerator) closeAll() {
	// No-op.
}
