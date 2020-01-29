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
	OPERAND_TRANSACTION operandType = iota
	OPERAND_READWRITER
	OPERAND_MVCC_KEY
	OPERAND_PAST_TS
	OPERAND_VALUE
	OPERAND_TEST_RUNNER
	OPERAND_ITERATOR
)

const (
	maxValueSize = 16
)

// operandManager represents an object to manage  instances of a type of
// object that can be passed as an "operand" to an operation. For simplicity,
// we create operandManagers for each type of argument, even primitive ones like
// MVCCKeys and values. All state about open objects (iterators, transactions,
// writers, etc) should be stored in an operandManager.
type operandManager interface {
	get() operand
	opener() string
	count() int
	close(operand)
	closeAll()
	toString(operand) string
	parse(string) operand
}

type operand interface{}

func generateBytes(rng *rand.Rand, min int, max int) []byte {
	// For better readability, stick to lowercase alphabet characters.
	iterations := min + int(float64(max-min)*rng.Float64())
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

func (k *keyManager) close(operand) {
	// No-op.
}

func (k *keyManager) toString(key operand) string {
	mvccKey := key.(engine.MVCCKey)
	return fmt.Sprintf("%s/%d", mvccKey.Key, mvccKey.Timestamp.WallTime)
}

func (k *keyManager) get() operand {
	// 15% chance of returning a new key even if some exist.
	if len(k.liveKeys) == 0 || k.rng.Float64() < 0.15 {
		return k.open()
	}

	return k.liveKeys[int(k.rng.Float64()*float64(len(k.liveKeys)))]
}

func (k *keyManager) closeAll() {
	// No-op.
}

func (k *keyManager) parse(input string) operand {
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

func (v *valueManager) open() []byte {
	return v.get().([]byte)
}

func (v *valueManager) get() operand {
	return generateBytes(v.rng, 4, maxValueSize)
}

func (v *valueManager) close(operand) {
	// No-op.
}

func (v *valueManager) closeAll() {
	// No-op.
}

func (v *valueManager) toString(value operand) string {
	return fmt.Sprintf("%s", value.([]byte))
}

func (v *valueManager) parse(input string) operand {
	var value = make([]byte, 0, maxValueSize)
	_, err := fmt.Sscanf(input, "%s", &value)
	if err != nil {
		panic(err)
	}
	return value
}

type txnManager struct {
	rng             *rand.Rand
	testRunner      *metaTestRunner
	tsGenerator     *tsGenerator
	liveTxns        []*roachpb.Transaction
	txnIdMap        map[string]*roachpb.Transaction
	inFlightBatches map[*roachpb.Transaction][]engine.Batch
	txnCounter      uint64
}

var _ operandManager = &txnManager{}

func (t *txnManager) opener() string {
	return "txn_open"
}

func (t *txnManager) count() int {
	return len(t.liveTxns)
}

func (t *txnManager) get() operand {
	if len(t.liveTxns) == 0 {
		panic("no open txns")
	}
	return t.liveTxns[int(t.rng.Float64()*float64(len(t.liveTxns)))]
}

func (t *txnManager) open() *roachpb.Transaction {
	t.txnCounter++
	ts := t.tsGenerator.generate()
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             uuid.FromUint128(uint128.FromInts(0, t.txnCounter)),
			Key:            roachpb.KeyMin,
			WriteTimestamp: ts,
			Sequence:       0,
		},
		Name:                    fmt.Sprintf("t%d", t.txnCounter),
		DeprecatedOrigTimestamp: ts,
		ReadTimestamp:           ts,
		Status:                  roachpb.PENDING,
	}
	t.liveTxns = append(t.liveTxns, txn)
	t.txnIdMap[txn.Name] = txn

	return txn
}

func (t *txnManager) close(op operand) {
	txn := op.(*roachpb.Transaction)
	for _, span := range txn.IntentSpans {
		intent := roachpb.MakeIntent(txn, span)
		intent.Status = roachpb.COMMITTED
		_, err := engine.MVCCResolveWriteIntent(context.TODO(), t.testRunner.engine, nil, intent)
		if err != nil {
			panic(err)
		}
	}

	delete(t.txnIdMap, txn.Name)
	delete(t.inFlightBatches, txn)
	idx := len(t.liveTxns)
	for i := range t.liveTxns {
		if t.liveTxns[i] == txn {
			idx = i
			break
		}
	}
	t.liveTxns[idx] = t.liveTxns[len(t.liveTxns)-1]
	t.liveTxns = t.liveTxns[:len(t.liveTxns)-1]
}

func (t *txnManager) clearBatch(batch engine.Batch) {
	for txn, batches := range t.inFlightBatches {
		found := false
		savedLen := len(batches)
		for i := 0; i < savedLen; i++ {
			batch2 := batches[i]
			if batch == batch2 {
				batches[i] = batches[len(batches) - 1]
				batches = batches[:len(batches) - 1]
				savedLen--
				i--
				found = true
				// Don't break; this batch could appear multiple times.
			}
		}
		if found {
			t.inFlightBatches[txn] = batches
		}
	}
}

func (t *txnManager) closeAll() {
	for _, txn := range t.liveTxns {
		t.close(txn)
	}
}

func (t *txnManager) toString(op operand) string {
	txn := op.(*roachpb.Transaction)
	return txn.Name
}

func (t *txnManager) parse(input string) operand {
	return t.txnIdMap[input]
}

// TODO(itsbilal): Use this in an inconsistent version of MVCCScan.
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

func (t *tsManager) close(operand) {
	// No-op.
}

func (t *tsManager) closeAll() {
	// No-op.
}

func (t *tsManager) toString(op operand) string {
	ts := op.(hlc.Timestamp)
	return fmt.Sprintf("%d", ts.WallTime)
}

func (t *tsManager) parse(input string) operand {
	var ts hlc.Timestamp
	wallTime, err := strconv.ParseInt(input, 10, 0)
	if err != nil {
		panic(err)
	}
	ts.WallTime = wallTime
	return ts
}

func (t *tsManager) get() operand {
	return t.tsGenerator.randomPastTimestamp(t.rng)
}

type testRunnerManager struct {
	t *metaTestRunner
}

var _ operandManager = &testRunnerManager{}

func (t *testRunnerManager) opener() string {
	return ""
}

func (t *testRunnerManager) count() int {
	return 1
}

func (t *testRunnerManager) close(operand) {
	// No-op.
}

func (t *testRunnerManager) closeAll() {
	// No-op.
}

func (t *testRunnerManager) toString(operand) string {
	return "t"
}

func (t *testRunnerManager) parse(string) operand {
	return t.t
}

func (t *testRunnerManager) get() operand {
	return t.t
}

type readWriterManager struct {
	rng          *rand.Rand
	eng          engine.Engine
	liveBatches  []engine.Batch
	batchToIdMap map[engine.Batch]int
	batchCounter int
}

var _ operandManager = &readWriterManager{}

func (w *readWriterManager) get() operand {
	// 25% chance of returning the engine, even if there are live batches.
	if len(w.liveBatches) == 0 || w.rng.Float64() < 0.25 {
		return w.eng
	}

	return w.liveBatches[int(w.rng.Float64()*float64(len(w.liveBatches)))]
}

func (w *readWriterManager) open() engine.ReadWriter {
	batch := w.eng.NewBatch()
	w.batchCounter++
	w.liveBatches = append(w.liveBatches, batch)
	w.batchToIdMap[batch] = w.batchCounter
	return batch
}

func (w *readWriterManager) opener() string {
	return "batch_open"
}

func (w *readWriterManager) count() int {
	return len(w.liveBatches) + 1
}

func (w *readWriterManager) close(op operand) {
	// No-op if engine.
	if op == w.eng {
		return
	}

	batch := op.(engine.Batch)
	for i, batch2 := range w.liveBatches {
		if batch2 == batch {
			w.liveBatches[i] = w.liveBatches[len(w.liveBatches)-1]
			w.liveBatches = w.liveBatches[:len(w.liveBatches)-1]
			break
		}
	}
	delete(w.batchToIdMap, batch)
	batch.Close()
}

func (w *readWriterManager) closeAll() {
	for _, batch := range w.liveBatches {
		batch.Close()
	}
	w.liveBatches = w.liveBatches[:0]
	w.batchToIdMap = make(map[engine.Batch]int)
}

func (w *readWriterManager) toString(op operand) string {
	if w.eng == op {
		return "engine"
	}
	return fmt.Sprintf("batch%d", w.batchToIdMap[op.(engine.Batch)])
}

func (w *readWriterManager) parse(input string) operand {
	if input == "engine" {
		return w.eng
	}

	var id int
	_, err := fmt.Sscanf(input, "batch%d", &id)
	if err != nil {
		panic(err)
	}

	for batch, id2 := range w.batchToIdMap {
		if id == id2 {
			return batch
		}
	}
	panic(fmt.Sprintf("couldn't find readwriter with id %d", id))
}

type iteratorManager struct {
	rng          *rand.Rand
	testRunner   *metaTestRunner
	readerToIter map[engine.Reader][]engine.Iterator
	iterToId     map[engine.Iterator]uint64
	liveIters    []engine.Iterator
	iterCounter  uint64
}

var _ operandManager = &iteratorManager{}

func (i *iteratorManager) get() operand {
	if len(i.liveIters) == 0 {
		panic("no open iterators")
	}

	return i.liveIters[int(i.rng.Float64()*float64(len(i.liveIters)))]
}

func (i *iteratorManager) open(rw engine.ReadWriter, options engine.IterOptions) engine.Iterator {
	i.iterCounter++
	iter := rw.NewIterator(options)
	i.readerToIter[rw] = append(i.readerToIter[rw], iter)
	i.iterToId[iter] = i.iterCounter
	i.liveIters = append(i.liveIters, iter)
	return iter
}

func (i *iteratorManager) opener() string {
	return "iterator_open"
}

func (i *iteratorManager) count() int {
	return len(i.iterToId)
}

func (i *iteratorManager) close(op operand) {
	iter := op.(engine.Iterator)
	iter.Close()

	delete(i.iterToId, iter)
	// Clear iter from liveIters
	for j, iter2 := range i.liveIters {
		if iter == iter2 {
			i.liveIters[j] = i.liveIters[len(i.liveIters)-1]
			i.liveIters = i.liveIters[:len(i.liveIters)-1]
			break
		}
	}
	// Clear iter from readerToIter
	for reader, iters := range i.readerToIter {
		for j, iter2 := range iters {
			if iter == iter2 {
				// Delete iters[j]
				iters[j] = iters[len(iters)-1]
				i.readerToIter[reader] = iters[:len(iters)-1]

				return
			}
		}
	}
}

func (i *iteratorManager) closeAll() {
	for iter, _ := range i.iterToId {
		iter.Close()
	}
}

func (i *iteratorManager) toString(op operand) string {
	return fmt.Sprintf("iter%d", i.iterToId[op.(engine.Iterator)])
}

func (i *iteratorManager) parse(input string) operand {
	var id uint64
	_, err := fmt.Sscanf(input, "iter%d", &id)
	if err != nil {
		panic(err)
	}

	for iter, id2 := range i.iterToId {
		if id == id2 {
			return iter
		}
	}
	return nil
}
