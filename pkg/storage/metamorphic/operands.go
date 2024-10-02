// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand"
	"sort"
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
	operandUnusedMVCCKey
	operandPastTS
	operandNextTS
	operandValue
	operandIterator
	operandFloat
	operandBool
	operandSavepoint
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
	// or parse() method on the concrete operand generator. Operands generated
	// so far are passed in, in case this generator relies on them to generate
	// new ones.
	get(args []string) string
	// getNew retrieves a new instance of this type of operand. Called when an
	// opener operation (with isOpener = true) needs an ID to store its output.
	getNew(args []string) string
	// opener returns the name of an operation generator (defined in
	// operations.go) that always creates a new instance of this object. Called by
	// the test runner when an operation requires one instance of this
	// operand to exist, and count() == 0.
	opener() string
	// count returns the number of live objects being managed by this generator.
	// If this generator depends on some other previously-generated args, those
	// can be obtained from the args list. If 0, the opener() operation can be
	// called when necessary.
	count(args []string) int
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

func (k *keyGenerator) count(args []string) int {
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

func (k *keyGenerator) get(args []string) string {
	// 15% chance of returning a new key even if some exist.
	if len(k.liveKeys) == 0 || k.rng.Float64() < 0.30 {
		return k.toString(k.open())
	}

	return k.toString(k.liveKeys[k.rng.Intn(len(k.liveKeys))])
}

func (k *keyGenerator) getNew(args []string) string {
	return k.get(args)
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

// txnKeyGenerator generates keys that are currently unused by other writers
// on the same txn.
//
// Requires: the last two args to be (readWriterID, txnID).
type txnKeyGenerator struct {
	txns *txnGenerator
	keys *keyGenerator
}

var _ operandGenerator = &keyGenerator{}

func (k *txnKeyGenerator) opener() string {
	return ""
}

func (k *txnKeyGenerator) count(args []string) int {
	// Always return a non-zero count so opener() is never called.
	return len(k.txns.inUseKeys) + 1
}

func (k *txnKeyGenerator) get(args []string) string {
	writer := readWriterID(args[0])
	txn := txnID(args[1])

	for {
		rawKey := k.keys.get(args)
		key := k.keys.parse(rawKey)

		conflictFound := false
		k.txns.forEachConflict(writer, txn, key.Key, nil, func(span roachpb.Span) bool {
			conflictFound = true
			return false
		})
		if !conflictFound {
			return rawKey
		}
	}
}

func (k *txnKeyGenerator) getNew(args []string) string {
	return k.get(args)
}

func (k *txnKeyGenerator) closeAll() {
	// No-op.
}

func (k *txnKeyGenerator) parse(input string) storage.MVCCKey {
	return k.keys.parse(input)
}

type valueGenerator struct {
	rng *rand.Rand
}

var _ operandGenerator = &valueGenerator{}

func (v *valueGenerator) opener() string {
	return ""
}

func (v *valueGenerator) count(args []string) int {
	return 1
}

func (v *valueGenerator) get(args []string) string {
	return v.toString(generateBytes(v.rng, 4, maxValueSize))
}

func (v *valueGenerator) getNew(args []string) string {
	return v.get(args)
}

func (v *valueGenerator) closeAll() {
	// No-op.
}

func (v *valueGenerator) toString(value []byte) string {
	return string(value)
}

func (v *valueGenerator) parse(input string) []byte {
	return []byte(input)
}

type txnID string

type writtenKeySpan struct {
	key    roachpb.Span
	writer readWriterID
	txn    txnID
}

type txnGenerator struct {
	rng         *rand.Rand
	testRunner  *metaTestRunner
	tsGenerator *tsGenerator
	liveTxns    []txnID
	txnIDMap    map[txnID]*roachpb.Transaction
	// Set of batches with written-to keys for each txn. Does not track writes
	// directly to the engine.
	openBatches map[txnID]map[readWriterID]struct{}
	// Number of open savepoints for each transaction. As savepoints cannot be
	// "closed", this count can only go up.
	openSavepoints map[txnID]int
	// Stores keys written to by each writer/txn during operation generation.
	// Sorted in key order, with no overlaps.
	inUseKeys []writtenKeySpan
	// Counts "generated" transactions - i.e. how many txn_open()s have been
	// inserted so far. Could stay 0 in check mode.
	txnGenCounter uint64
}

var _ operandGenerator = &txnGenerator{}

func (t *txnGenerator) opener() string {
	return "txn_open"
}

func (t *txnGenerator) count(args []string) int {
	return len(t.txnIDMap)
}

func (t *txnGenerator) get(args []string) string {
	if len(t.liveTxns) == 0 {
		panic("no open txns")
	}
	return string(t.liveTxns[t.rng.Intn(len(t.liveTxns))])
}

// getNew returns a transaction ID, and saves this transaction as a "live"
// transaction for generation purposes. Called only during generation, and
// must be matched with a generateClose call.
func (t *txnGenerator) getNew(args []string) string {
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
	delete(t.openSavepoints, id)

	// Delete all keys in inUseKeys where inUseKeys[i].txn == id. j points to the
	// next slot in t.inUseKeys where a key span being retained will go.
	j := 0
	for i := range t.inUseKeys {
		if t.inUseKeys[i].txn != id {
			t.inUseKeys[j] = t.inUseKeys[i]
			j++
		}
	}
	t.inUseKeys = t.inUseKeys[:j]

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

func (t *txnGenerator) forEachConflict(
	w readWriterID, txn txnID, key roachpb.Key, endKey roachpb.Key, fn func(roachpb.Span) bool,
) {
	if endKey == nil {
		endKey = key.Next()
	}

	start := sort.Search(len(t.inUseKeys), func(i int) bool {
		return key.Compare(t.inUseKeys[i].key.EndKey) < 0
	})
	end := sort.Search(len(t.inUseKeys), func(i int) bool {
		return endKey.Compare(t.inUseKeys[i].key.Key) <= 0
	})

	for i := start; i < end; i++ {
		if t.inUseKeys[i].writer != w || t.inUseKeys[i].txn != txn {
			// Conflict found.
			if cont := fn(t.inUseKeys[i].key); !cont {
				return
			}
		}
	}
}

// truncateSpanForConflicts truncates [key, endKey) to a sub-span that does
// not conflict with any in-flight writes. If no such span is found, an empty
// span (i.e. key >= endKey) is returned. Callers are expected to handle that
// case gracefully.
func (t *txnGenerator) truncateSpanForConflicts(
	w readWriterID, txn txnID, key, endKey roachpb.Key,
) roachpb.Span {
	// forEachConflict is guaranteed to iterate over conflicts in key order,
	// with the lowest conflicting key first. Find the first conflict and
	// truncate the span to that range.
	t.forEachConflict(w, txn, key, endKey, func(conflict roachpb.Span) bool {
		if conflict.ContainsKey(key) {
			key = append([]byte(nil), conflict.EndKey...)
			return true
		}
		endKey = conflict.Key
		return false
	})
	result := roachpb.Span{
		Key:    key,
		EndKey: endKey,
	}
	return result
}

func (t *txnGenerator) addWrittenKeySpan(
	w readWriterID, txn txnID, key roachpb.Key, endKey roachpb.Key,
) {
	span := roachpb.Span{Key: key, EndKey: endKey}
	if endKey == nil {
		endKey = key.Next()
		span.EndKey = endKey
	}
	// writtenKeys is sorted in key order, and no two spans are overlapping.
	// However we do _not_ merge perfectly-adjacent spans when adding;
	// as it is legal to have [a,b) and [b,c) belonging to two different writers.
	// start is the earliest span that either contains key, or if there is no such
	// span, is the first span beyond key. end is the earliest span that does not
	// include any keys in [key, endKey).
	start := sort.Search(len(t.inUseKeys), func(i int) bool {
		return key.Compare(t.inUseKeys[i].key.EndKey) < 0
	})
	end := sort.Search(len(t.inUseKeys), func(i int) bool {
		return endKey.Compare(t.inUseKeys[i].key.Key) <= 0
	})
	if start == len(t.inUseKeys) {
		// Append at end.
		t.inUseKeys = append(t.inUseKeys, writtenKeySpan{
			key:    span,
			writer: w,
			txn:    txn,
		})
		return
	}
	if start == end {
		// start == end implies that the span cannot contain start, and by
		// definition it is beyond end. So [key, endKey) does not overlap with an
		// existing span and needs to be placed before start.
		t.inUseKeys = append(t.inUseKeys, writtenKeySpan{})
		copy(t.inUseKeys[start+1:], t.inUseKeys[start:])
		t.inUseKeys[start] = writtenKeySpan{
			key:    span,
			writer: w,
			txn:    txn,
		}
		return
	} else if start > end {
		panic(fmt.Sprintf("written keys not in sorted order: %d > %d", start, end))
	}
	// INVARIANT: start < end. The start span may or may not contain key. And we
	// know end > 0. We will be merging existing spans, and need to compute which
	// spans to merge and what keys to use for the resulting span. The resulting
	// span will go into `span`, and will replace writtenKeys[start:end].
	if t.inUseKeys[start].key.Key.Compare(key) < 0 {
		// The start span contains key. So use the start key of the start span since
		// it will result in the wider span.
		span.Key = t.inUseKeys[start].key.Key
	}
	// Else, span.Key is equal to key which is the wider span. Note that no span
	// overlaps with key so we are not extending this into the span at start-1.
	//
	// The span at end-1 may end at a key less or greater than endKey.
	if t.inUseKeys[end-1].key.EndKey.Compare(endKey) > 0 {
		// Existing span ends at a key greater than endKey, so construct the wider
		// span.
		span.EndKey = t.inUseKeys[end-1].key.EndKey
	}
	// We blindly replace the existing spans in writtenKeys[start:end] with the
	// new one. This is okay as long as any conflicting operation looks at
	// inUseKeys before generating itself; this method is only called once no
	// conflicts are found and the write operation is successfully generated.
	t.inUseKeys[start] = writtenKeySpan{
		key:    span,
		writer: w,
		txn:    txn,
	}
	n := copy(t.inUseKeys[start+1:], t.inUseKeys[end:])
	t.inUseKeys = t.inUseKeys[:start+1+n]
}

func (t *txnGenerator) trackTransactionalWrite(w readWriterID, txn txnID, key, endKey roachpb.Key) {
	if len(endKey) > 0 && key.Compare(endKey) >= 0 {
		// No-op.
		return
	}
	t.addWrittenKeySpan(w, txn, key, endKey)
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
	t.inUseKeys = t.inUseKeys[:0]
	t.openSavepoints = make(map[txnID]int)
}

type savepointGenerator struct {
	rng          *rand.Rand
	txnGenerator *txnGenerator
}

var _ operandGenerator = &savepointGenerator{}

func (s *savepointGenerator) get(args []string) string {
	// Since get is being called as opposed to getNew, there must be a nonzero
	// value at s.txnGenerator.openSavepoints[id].
	id := txnID(args[len(args)-1])
	n := s.rng.Intn(s.txnGenerator.openSavepoints[id])
	return strconv.Itoa(n)
}

func (s *savepointGenerator) getNew(args []string) string {
	id := txnID(args[len(args)-1])
	s.txnGenerator.openSavepoints[id]++
	return strconv.Itoa(s.txnGenerator.openSavepoints[id] - 1)
}

func (s *savepointGenerator) opener() string {
	return "txn_create_savepoint"
}

func (s *savepointGenerator) count(args []string) int {
	id := txnID(args[len(args)-1])
	return s.txnGenerator.openSavepoints[id]
}

func (s *savepointGenerator) closeAll() {
	// No-op.
}

type pastTSGenerator struct {
	rng         *rand.Rand
	tsGenerator *tsGenerator
}

var _ operandGenerator = &pastTSGenerator{}

func (t *pastTSGenerator) opener() string {
	return ""
}

func (t *pastTSGenerator) count(args []string) int {
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

func (t *pastTSGenerator) get(args []string) string {
	return t.toString(t.tsGenerator.randomPastTimestamp(t.rng))
}

func (t *pastTSGenerator) getNew(args []string) string {
	return t.get(args)
}

// Similar to pastTSGenerator, except it always increments the "current" timestamp
// and returns the newest one.
type nextTSGenerator struct {
	pastTSGenerator
}

func (t *nextTSGenerator) get(args []string) string {
	return t.toString(t.tsGenerator.generate())
}

func (t *nextTSGenerator) getNew(args []string) string {
	return t.get(args)
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

func (w *readWriterGenerator) get(args []string) string {
	// 25% chance of returning the engine, even if there are live batches.
	if len(w.liveBatches) == 0 || w.rng.Float64() < 0.25 {
		return "engine"
	}

	return string(w.liveBatches[w.rng.Intn(len(w.liveBatches))])
}

// getNew is called during generation to generate a batch ID.
func (w *readWriterGenerator) getNew(args []string) string {
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

func (w *readWriterGenerator) count(args []string) int {
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

func (i *iteratorGenerator) get(args []string) string {
	if len(i.liveIters) == 0 {
		panic("no open iterators")
	}

	return string(i.liveIters[i.rng.Intn(len(i.liveIters))])
}

func (i *iteratorGenerator) getNew(args []string) string {
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

func (i *iteratorGenerator) count(args []string) int {
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

func (f *floatGenerator) get(args []string) string {
	return fmt.Sprintf("%.4f", f.rng.Float32())
}

func (f *floatGenerator) getNew(args []string) string {
	return f.get(args)
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

func (f *floatGenerator) count(args []string) int {
	return 1
}

func (f *floatGenerator) closeAll() {
	// No-op.
}

type boolGenerator struct {
	rng *rand.Rand
}

func (f *boolGenerator) get(args []string) string {
	return fmt.Sprintf("%t", f.rng.Float32() < 0.5)
}

func (f *boolGenerator) getNew(args []string) string {
	return f.get(args)
}

func (f *boolGenerator) parse(input string) bool {
	var result bool
	if _, err := fmt.Sscanf(input, "%t", &result); err != nil {
		panic(err)
	}
	return result
}

func (f *boolGenerator) opener() string {
	// Not applicable, because count() is always nonzero.
	return ""
}

func (f *boolGenerator) count(args []string) int {
	return 1
}

func (f *boolGenerator) closeAll() {
	// No-op.
}
