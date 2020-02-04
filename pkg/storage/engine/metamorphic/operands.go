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
	operandTestRunner
	operandIterator
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
	// get retrieves an instance of this operand. Depending on operand type (eg.
	// keys), it could also generate and return a new type of an instance.
	get() operand
	// opener returns the name of an operation (defined in operations.go) that
	// always creates a new instance of this object. Called by the operation
	// generator when an operation requires one instance of this operand to exist,
	// and count() == 0.
	opener() string
	// count returns the number of live objects being managed by this manager. If
	// 0, the opener() operation can be called when necessary.
	count() int
	// close closes the specified operand and cleans up references to it
	// internally.
	close(operand)
	// closeAll closes all managed operands. Used when the test exits, or when a
	// restart operation executes.
	closeAll()
	// toString converts the specified operand to a parsable string. This string
	// will be printed in error messages and output files.
	toString(operand) string
	// parse converts the specified string into an operand instance. Should be the
	// exact inverse of toString().
	parse(string) operand
}

type operand interface{}

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

	return k.liveKeys[k.rng.Intn(len(k.liveKeys))]
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
	rng         *rand.Rand
	testRunner  *metaTestRunner
	tsGenerator *tsGenerator
	liveTxns    []*roachpb.Transaction
	txnIDMap    map[string]*roachpb.Transaction
	openBatches map[*roachpb.Transaction]map[engine.Batch]struct{}
	txnCounter  uint64
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
	return t.liveTxns[t.rng.Intn(len(t.liveTxns))]
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
	t.txnIDMap[txn.Name] = txn

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

	delete(t.txnIDMap, txn.Name)
	delete(t.openBatches, txn)
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
	for _, batches := range t.openBatches {
		delete(batches, batch)
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
	batchToIDMap map[engine.Batch]int
	batchCounter int
}

var _ operandManager = &readWriterManager{}

func (w *readWriterManager) get() operand {
	// 25% chance of returning the engine, even if there are live batches.
	if len(w.liveBatches) == 0 || w.rng.Float64() < 0.25 {
		return w.eng
	}

	return w.liveBatches[w.rng.Intn(len(w.liveBatches))]
}

func (w *readWriterManager) open() engine.ReadWriter {
	batch := w.eng.NewBatch()
	w.batchCounter++
	w.liveBatches = append(w.liveBatches, batch)
	w.batchToIDMap[batch] = w.batchCounter
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
	delete(w.batchToIDMap, batch)
	batch.Close()
}

func (w *readWriterManager) closeAll() {
	for _, batch := range w.liveBatches {
		batch.Close()
	}
	w.liveBatches = w.liveBatches[:0]
	w.batchToIDMap = make(map[engine.Batch]int)
}

func (w *readWriterManager) toString(op operand) string {
	if w.eng == op {
		return "engine"
	}
	return fmt.Sprintf("batch%d", w.batchToIDMap[op.(engine.Batch)])
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

	for batch, id2 := range w.batchToIDMap {
		if id == id2 {
			return batch
		}
	}
	panic(fmt.Sprintf("couldn't find readwriter with id %d", id))
}

type iteratorInfo struct {
	id         uint64
	lowerBound roachpb.Key
}

type iteratorManager struct {
	rng          *rand.Rand
	readerToIter map[engine.Reader][]engine.Iterator
	iterToInfo   map[engine.Iterator]iteratorInfo
	liveIters    []engine.Iterator
	iterCounter  uint64
}

var _ operandManager = &iteratorManager{}

func (i *iteratorManager) get() operand {
	if len(i.liveIters) == 0 {
		panic("no open iterators")
	}

	return i.liveIters[i.rng.Intn(len(i.liveIters))]
}

func (i *iteratorManager) open(rw engine.ReadWriter, options engine.IterOptions) engine.Iterator {
	i.iterCounter++
	iter := rw.NewIterator(options)
	i.readerToIter[rw] = append(i.readerToIter[rw], iter)
	i.iterToInfo[iter] = iteratorInfo{
		id:         i.iterCounter,
		lowerBound: options.LowerBound,
	}
	i.liveIters = append(i.liveIters, iter)
	return iter
}

func (i *iteratorManager) opener() string {
	return "iterator_open"
}

func (i *iteratorManager) count() int {
	return len(i.iterToInfo)
}

func (i *iteratorManager) close(op operand) {
	iter := op.(engine.Iterator)
	iter.Close()

	delete(i.iterToInfo, iter)
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
	for iter := range i.iterToInfo {
		iter.Close()
	}
}

func (i *iteratorManager) toString(op operand) string {
	return fmt.Sprintf("iter%d", i.iterToInfo[op.(engine.Iterator)].id)
}

func (i *iteratorManager) parse(input string) operand {
	var id uint64
	_, err := fmt.Sscanf(input, "iter%d", &id)
	if err != nil {
		panic(err)
	}

	for iter, info := range i.iterToInfo {
		if id == info.id {
			return iter
		}
	}
	return nil
}
