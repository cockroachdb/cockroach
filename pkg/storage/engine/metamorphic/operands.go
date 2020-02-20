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
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type operandType int

const (
	operandTransaction operandType = iota
	operandReadWriter
	operandMVCCKey
	operandPastTS
	operandValue
	operandIterator
)

const (
	maxValueSize = 16
)

// operandManager represents an object to manage instances of a type of
// object that can be passed as an "operand" to an operation. For simplicity,
// we create operandManagers for each type of argument, even primitive ones like
// MVCCKeys and values. All state about open objects (iterators, transactions,
// writers, etc) should be stored in an operandManager.
//
// Note that all the interface methods, other than closeAll(), are called during
// operation generation time. Some managers implement additional methods, such
// as open(), close(), etc, that are called during operation execution.
type operandManager interface {
	// get retrieves an instance of this operand. Depending on operand type (eg.
	// keys), it could also generate and return a new type of an instance. An
	// operand is represented as a serializable string, that can be converted into
	// a concrete instance type during execution by calling a parse() method on
	// the concrete operand manager.
	get() string
	// opener returns the name of an operation generator (defined in
	// operations.go) that always creates a new instance of this object. Called by
	// the test runner when an operation requires one instance of this
	// operand to exist, and count() == 0.
	opener() string
	// count returns the number of live objects being managed by this manager. If
	// 0, the opener() operation can be called when necessary.
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

type keyManager struct {
	liveKeys    []engine.MVCCKey
	rng         *rand.Rand
	tsGenerator *tsGenerator
}

var _ operandManager = &keyManager{}

func (k *keyManager) opener() string {
	return ""
}

func (k *keyManager) count() int {
	// Always return a nonzero value so opener() is never called directly.
	return len(k.liveKeys) + 1
}

func (k *keyManager) open() engine.MVCCKey {
	var key engine.MVCCKey
	key.Key = generateBytes(k.rng, 8, maxValueSize)
	key.Timestamp = k.tsGenerator.lastTS
	k.liveKeys = append(k.liveKeys, key)

	return key
}

func (k *keyManager) close(engine.MVCCKey) {
	// No-op.
}

func (k *keyManager) toString(key engine.MVCCKey) string {
	return fmt.Sprintf("%s/%d", key.Key, key.Timestamp.WallTime)
}

func (k *keyManager) get() string {
	// 15% chance of returning a new key even if some exist.
	if len(k.liveKeys) == 0 || k.rng.Float64() < 0.30 {
		return k.toString(k.open())
	}

	return k.toString(k.liveKeys[k.rng.Intn(len(k.liveKeys))])
}

func (k *keyManager) closeAll() {
	// No-op.
}

func (k *keyManager) parse(input string) engine.MVCCKey {
	var key engine.MVCCKey
	key.Key = make([]byte, 0, maxValueSize)
	_, err := fmt.Sscanf(input, "%q/%d", &key.Key, &key.Timestamp.WallTime)
	if err != nil {
		panic(err)
	}
	return key
}

type valueManager struct {
	rng *rand.Rand
}

var _ operandManager = &valueManager{}

func (v *valueManager) opener() string {
	return ""
}

func (v *valueManager) count() int {
	return 1
}

func (v *valueManager) get() string {
	return v.toString(generateBytes(v.rng, 4, maxValueSize))
}

func (v *valueManager) close([]byte) {
	// No-op.
}

func (v *valueManager) closeAll() {
	// No-op.
}

func (v *valueManager) toString(value []byte) string {
	return fmt.Sprintf("%s", value)
}

func (v *valueManager) parse(input string) []byte {
	var value = make([]byte, 0, maxValueSize)
	_, err := fmt.Sscanf(input, "%s", &value)
	if err != nil {
		panic(err)
	}
	return value
}

type txnID string

type txnManager struct {
	rng         *rand.Rand
	testRunner  *metaTestRunner
	tsGenerator *tsGenerator
	liveTxns    []txnID
	txnIDMap    map[txnID]*roachpb.Transaction
	openBatches map[txnID]map[readWriterID]struct{}
	// Counts "generated" transactions - i.e. how many txn_open()s have been
	// inserted so far. Could stay 0 in check mode.
	txnGenCounter uint64
	// Counts how many transactions have been opened - a txn is opened when the
	// txnOpenOp is executed.
	txnOpenCounter uint64
}

var _ operandManager = &txnManager{}

func (t *txnManager) opener() string {
	return "txn_open"
}

func (t *txnManager) count() int {
	return len(t.txnIDMap)
}

func (t *txnManager) get() string {
	if len(t.liveTxns) == 0 {
		panic("no open txns")
	}
	return string(t.liveTxns[t.rng.Intn(len(t.liveTxns))])
}

// generateOpen returns a transaction ID, and saves this transaction as a "live"
// transaction for generation purposes. Called only during generation, and
// must be matched with a generateClose call.
func (t *txnManager) generateOpen() txnID {
	t.txnGenCounter++
	id := txnID(fmt.Sprintf("t%d", t.txnGenCounter))
	// This ensures count() is correct as of generation time.
	t.txnIDMap[id] = nil
	t.liveTxns = append(t.liveTxns, id)
	return id
}

// generateClose is called when a transaction closing operation is generated.
func (t *txnManager) generateClose(id txnID) {
	delete(t.openBatches, id)
	delete(t.txnIDMap, id)

	idx := len(t.liveTxns)
	for i := range t.liveTxns {
		if t.liveTxns[i] == id {
			idx = i
			break
		}
	}
	t.liveTxns[idx] = t.liveTxns[len(t.liveTxns)-1]
	t.liveTxns = t.liveTxns[:len(t.liveTxns)-1]
}

// open is called during execution to generate a transaction.
func (t *txnManager) open() *roachpb.Transaction {
	t.txnOpenCounter++
	ts := t.tsGenerator.generate()
	id := txnID(fmt.Sprintf("t%d", t.txnOpenCounter))
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             uuid.FromUint128(uint128.FromInts(0, t.txnOpenCounter)),
			Key:            roachpb.KeyMin,
			WriteTimestamp: ts,
			Sequence:       0,
		},
		Name:                    string(id),
		DeprecatedOrigTimestamp: ts,
		ReadTimestamp:           ts,
		Status:                  roachpb.PENDING,
	}
	t.txnIDMap[id] = txn

	return txn
}

// open is called during execution to close a transaction.
func (t *txnManager) close(txn *roachpb.Transaction) {
	for _, span := range txn.IntentSpans {
		intent := roachpb.MakeIntent(txn, span)
		intent.Status = roachpb.COMMITTED
		_, err := engine.MVCCResolveWriteIntent(context.TODO(), t.testRunner.engine, nil, intent)
		if err != nil {
			panic(err)
		}
	}

	delete(t.txnIDMap, txnID(txn.Name))
}

func (t *txnManager) clearBatch(batch readWriterID) {
	for _, batches := range t.openBatches {
		delete(batches, batch)
	}
}

func (t *txnManager) trackWriteOnBatch(w readWriterID, txn txnID) {
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

func (t *txnManager) closeAll() {
	for _, txn := range t.txnIDMap {
		if txn != nil {
			t.close(txn)
		}
	}
	t.liveTxns = nil
	t.txnIDMap = make(map[txnID]*roachpb.Transaction)
	t.openBatches = make(map[txnID]map[readWriterID]struct{})
}

func (t *txnManager) parse(input txnID) *roachpb.Transaction {
	return t.txnIDMap[input]
}

type tsManager struct {
	rng         *rand.Rand
	tsGenerator *tsGenerator
}

var _ operandManager = &tsManager{}

func (t *tsManager) opener() string {
	return ""
}

func (t *tsManager) count() int {
	// Always return a non-zero count so opener() is never called.
	return int(t.tsGenerator.lastTS.WallTime) + 1
}

func (t *tsManager) closeAll() {
	// No-op.
}

func (t *tsManager) toString(ts hlc.Timestamp) string {
	return fmt.Sprintf("%d", ts.WallTime)
}

func (t *tsManager) parse(input string) hlc.Timestamp {
	var ts hlc.Timestamp
	wallTime, err := strconv.ParseInt(input, 10, 0)
	if err != nil {
		panic(err)
	}
	ts.WallTime = wallTime
	return ts
}

func (t *tsManager) get() string {
	return t.toString(t.tsGenerator.randomPastTimestamp(t.rng))
}

type readWriterID string

type readWriterManager struct {
	rng              *rand.Rand
	m                *metaTestRunner
	liveBatches      []readWriterID
	batchIDMap       map[readWriterID]engine.Batch
	batchGenCounter  uint64
	batchOpenCounter uint64
}

var _ operandManager = &readWriterManager{}

func (w *readWriterManager) get() string {
	// 25% chance of returning the engine, even if there are live batches.
	if len(w.liveBatches) == 0 || w.rng.Float64() < 0.25 {
		return "engine"
	}

	return string(w.liveBatches[w.rng.Intn(len(w.liveBatches))])
}

func (w *readWriterManager) open() engine.ReadWriter {
	batch := w.m.engine.NewBatch()
	w.batchOpenCounter++
	id := readWriterID(fmt.Sprintf("batch%d", w.batchOpenCounter))
	w.batchIDMap[id] = batch
	return batch
}

func (w *readWriterManager) opener() string {
	return "batch_open"
}

// generateOpen is called during generation to generate a batch ID.
func (w *readWriterManager) generateOpen() readWriterID {
	w.batchGenCounter++
	id := readWriterID(fmt.Sprintf("batch%d", w.batchGenCounter))
	w.batchIDMap[id] = nil
	w.liveBatches = append(w.liveBatches, id)

	return id
}

// generateClose is called during generation when an operation that closes a
// readWriter is generated.
func (w *readWriterManager) generateClose(id readWriterID) {
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
	w.m.txnManager.clearBatch(id)
}

func (w *readWriterManager) count() int {
	return len(w.batchIDMap) + 1
}

func (w *readWriterManager) close(op engine.ReadWriter) {
	// No-op if engine.
	if op == w.m.engine {
		return
	}

	batch := op.(engine.Batch)
	for id, batch2 := range w.batchIDMap {
		if batch == batch2 {
			delete(w.batchIDMap, id)
			break
		}
	}
	batch.Close()
}

func (w *readWriterManager) closeAll() {
	for _, batch := range w.batchIDMap {
		if batch != nil {
			batch.Close()
		}
	}
	w.liveBatches = w.liveBatches[:0]
	w.batchIDMap = make(map[readWriterID]engine.Batch)
}

func (w *readWriterManager) parse(input readWriterID) engine.ReadWriter {
	if input == "engine" {
		return w.m.engine
	}

	if batch, ok := w.batchIDMap[input]; ok {
		return batch
	}
	panic(fmt.Sprintf("couldn't find readwriter with id %s", input))
}

type iteratorID string
type iteratorInfo struct {
	id          iteratorID
	lowerBound  roachpb.Key
	iter        engine.Iterator
	isBatchIter bool
}

type iteratorManager struct {
	rng             *rand.Rand
	readerToIter    map[readWriterID][]iteratorID
	iterInfo        map[iteratorID]iteratorInfo
	liveIters       []iteratorID
	iterOpenCounter uint64
	iterGenCounter  uint64
}

var _ operandManager = &iteratorManager{}

func (i *iteratorManager) get() string {
	if len(i.liveIters) == 0 {
		panic("no open iterators")
	}

	return string(i.liveIters[i.rng.Intn(len(i.liveIters))])
}

// generateOpen is called during generation to generate an iterator ID for the
// specified readWriter.
func (i *iteratorManager) generateOpen(rwID readWriterID) iteratorID {
	i.iterGenCounter++
	id := iteratorID(fmt.Sprintf("iter%d", i.iterGenCounter))
	i.iterInfo[id] = iteratorInfo{
		id:          id,
		lowerBound:  nil,
		iter:        nil,
		isBatchIter: rwID != "engine",
	}
	i.readerToIter[rwID] = append(i.readerToIter[rwID], id)
	i.liveIters = append(i.liveIters, id)
	return id
}

// generateClose is called during generation when an operation that closes an
// iterator is generated.
func (i *iteratorManager) generateClose(id iteratorID) {
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

func (i *iteratorManager) getInfo(id iteratorID) iteratorInfo {
	return i.iterInfo[id]
}

// open is called during execution to open a new iterator.
func (i *iteratorManager) open(rwID readWriterID, rw engine.ReadWriter, options engine.IterOptions) engine.Iterator {
	i.iterOpenCounter++
	iter := rw.NewIterator(options)
	id := iteratorID(fmt.Sprintf("iter%d", i.iterOpenCounter))
	i.iterInfo[id] = iteratorInfo{
		id:          id,
		lowerBound:  options.LowerBound,
		iter:        iter,
		isBatchIter: rwID != "engine",
	}
	return iter
}

func (i *iteratorManager) opener() string {
	return "iterator_open"
}

func (i *iteratorManager) count() int {
	return len(i.iterInfo)
}

// close is called during execution to close an iterator.
func (i *iteratorManager) close(iter engine.Iterator) {
	id := i.toString(iter)
	delete(i.iterInfo, id)
	iter.Close()
}

func (i *iteratorManager) closeAll() {
	for _, iterInfo := range i.iterInfo {
		if iterInfo.iter != nil {
			iterInfo.iter.Close()
		}
	}
	i.liveIters = nil
	i.iterInfo = make(map[iteratorID]iteratorInfo)
	i.readerToIter = make(map[readWriterID][]iteratorID)
}

func (i *iteratorManager) toString(iter engine.Iterator) iteratorID {
	for id, info := range i.iterInfo {
		if info.iter == iter {
			return id
		}
	}
	panic("iterator not found")
}

func (i *iteratorManager) parse(id iteratorID) engine.Iterator {
	if info, ok := i.iterInfo[id]; ok {
		return info.iter
	}
	return nil
}
