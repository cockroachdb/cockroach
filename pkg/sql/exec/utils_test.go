// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/pkg/errors"
)

// tuple represents a row with any-type columns.
type tuple []interface{}

// tuples represents a table of a single type.
type tuples []tuple

// runTests is a helper that automatically runs your tests with varied batch
// sizes and with and without a random selection vector.
// Provide a test function that takes a list of input Operators, which will give
// back the tuples provided in batches.
func runTests(t *testing.T, tups []tuples, test func(t *testing.T, inputs []Operator)) {
	rng, _ := randutil.NewPseudoRand()

	for _, batchSize := range []uint16{1, 2, 3, 16, 1024} {
		for _, useSel := range []bool{false, true} {
			t.Run(fmt.Sprintf("batchSize=%d/sel=%t", batchSize, useSel), func(t *testing.T) {
				inputSources := make([]Operator, len(tups))
				if useSel {
					for i, tup := range tups {
						inputSources[i] = newOpTestSelInput(rng, batchSize, tup)
					}
				} else {
					for i, tup := range tups {
						inputSources[i] = newOpTestInput(batchSize, tup)
					}
				}
				test(t, inputSources)
			})
		}
	}
}

// runTestsWithFixedSel is a helper that (with a given fixed selection vector)
// automatically runs your tests with varied batch sizes. Provide a test
// function that takes a list of input Operators, which will give back the
// tuples provided in batches.
func runTestsWithFixedSel(
	t *testing.T, tups []tuples, sel []uint16, test func(t *testing.T, inputs []Operator),
) {
	for _, batchSize := range []uint16{1, 2, 3, 16, 1024} {
		t.Run(fmt.Sprintf("batchSize=%d/fixedSel", batchSize), func(t *testing.T) {
			inputSources := make([]Operator, len(tups))
			for i, tup := range tups {
				inputSources[i] = newOpFixedSelTestInput(sel, batchSize, tup)
			}
			test(t, inputSources)
		})
	}
}

// opTestInput is an Operator that columnarizes test input in the form of tuples
// of arbitrary Go types. It's meant to be used in Operator unit tests in
// conjunction with opTestOutput like the following:
//
// inputTuples := tuples{
//   {1,2,3.3,true},
//   {5,6,7.0,false},
// }
// tupleSource := newOpTestInput(inputTuples, types.Bool)
// opUnderTest := newFooOp(tupleSource, ...)
// output := newOpTestOutput(opUnderTest, expectedOutputTuples)
// if err := output.Verify(); err != nil {
//     t.Fatal(err)
// }
type opTestInput struct {
	typs []types.T

	batchSize uint16
	tuples    tuples
	batch     coldata.Batch
	useSel    bool
	rng       *rand.Rand
	selection []uint16
}

var _ Operator = &opTestInput{}

// newOpTestInput returns a new opTestInput with the given input tuples. The
// input tuples are translated into types automatically, using simple rules
// (e.g. integers always become Int64).
func newOpTestInput(batchSize uint16, tuples tuples) *opTestInput {
	ret := &opTestInput{
		batchSize: batchSize,
		tuples:    tuples,
	}
	return ret
}

func newOpTestSelInput(rng *rand.Rand, batchSize uint16, tuples tuples) *opTestInput {
	ret := &opTestInput{
		useSel:    true,
		rng:       rng,
		batchSize: batchSize,
		tuples:    tuples,
	}
	return ret
}

func (s *opTestInput) Init() {
	if len(s.tuples) == 0 {
		panic("empty tuple source")
	}

	typs := make([]types.T, len(s.tuples[0]))
	for i := range typs {
		// Default type for test cases is Int64 in case the entire column is null
		// and the type is indeterminate.
		typs[i] = types.Int64
		for _, tup := range s.tuples {
			if tup[i] != nil {
				typs[i] = types.FromGoType(tup[i])
				break
			}
		}
	}

	s.typs = typs
	s.batch = coldata.NewMemBatch(typs)

	s.selection = make([]uint16, coldata.BatchSize)
	for i := range s.selection {
		s.selection[i] = uint16(i)
	}
}

func (s *opTestInput) Next() coldata.Batch {
	if len(s.tuples) == 0 {
		s.batch.SetLength(0)
		return s.batch
	}
	batchSize := s.batchSize
	if len(s.tuples) < int(batchSize) {
		batchSize = uint16(len(s.tuples))
	}
	tups := s.tuples[:batchSize]
	s.tuples = s.tuples[batchSize:]

	tupleLen := len(tups[0])
	for i := range tups {
		if len(tups[i]) != tupleLen {
			panic(fmt.Sprintf("mismatched tuple lens: found %+v expected %d vals",
				tups[i], tupleLen))
		}
	}

	if s.useSel {
		// We have populated s.selection vector with possibly more indices than we
		// have actual tuples for, so some "default" tuples will be introduced but
		// will not be selected due to the length of the batch being equal to the
		// number of actual tuples.
		//
		// To introduce an element of chaos in the testing process we shuffle the
		// selection vector; however, in the real environment we expect that
		// indices in the selection vector to be in ascending order, so we sort
		// only those indices that correspond to the actual tuples. For example,
		// say we have 3 actual tuples, and after shuffling the selection vector
		// is [200, 50, 100, ...], so we sort only those 3 values to get to
		// [50, 100, 200, ...] in order to "scan" the selection vector in
		// sequential order.
		s.rng.Shuffle(len(s.selection), func(i, j int) {
			s.selection[i], s.selection[j] = s.selection[j], s.selection[i]
		})
		sort.Slice(s.selection[:batchSize], func(i, j int) bool {
			return s.selection[i] < s.selection[j]
		})

		s.batch.SetSelection(true)
		copy(s.batch.Selection(), s.selection)
	} else {
		s.batch.SetSelection(false)
	}

	for i := range s.typs {
		vec := s.batch.ColVec(i)
		vec.UnsetNulls()
		// Automatically convert the Go values into exec.Type slice elements using
		// reflection. This is slow, but acceptable for tests.
		col := reflect.ValueOf(vec.Col())
		for j := uint16(0); j < batchSize; j++ {
			outputIdx := s.selection[j]
			if tups[j][i] == nil {
				vec.SetNull(outputIdx)
			} else {
				col.Index(int(outputIdx)).Set(
					reflect.ValueOf(tups[j][i]).Convert(reflect.TypeOf(vec.Col()).Elem()))
			}
		}
	}

	s.batch.SetLength(batchSize)
	return s.batch
}

type opFixedSelTestInput struct {
	typs []types.T

	batchSize uint16
	tuples    tuples
	batch     coldata.Batch
	sel       []uint16
	// idx is the index of the tuple to be emitted next. We need to maintain it
	// in case the provided selection vector or provided tuples (if sel is nil)
	// is longer than requested batch size.
	idx uint16
}

var _ Operator = &opFixedSelTestInput{}

// newOpFixedSelTestInput returns a new opFixedSelTestInput with the given
// input tuples and selection vector. The input tuples are translated into
// types automatically, using simple rules (e.g. integers always become Int64).
func newOpFixedSelTestInput(sel []uint16, batchSize uint16, tuples tuples) *opFixedSelTestInput {
	ret := &opFixedSelTestInput{
		batchSize: batchSize,
		sel:       sel,
		tuples:    tuples,
	}
	return ret
}

func (s *opFixedSelTestInput) Init() {
	if len(s.tuples) == 0 {
		panic("empty tuple source")
	}

	typs := make([]types.T, len(s.tuples[0]))
	for i := range typs {
		// Default type for test cases is Int64 in case the entire column is null
		// and the type is indeterminate.
		typs[i] = types.Int64
		for _, tup := range s.tuples {
			if tup[i] != nil {
				typs[i] = types.FromGoType(tup[i])
				break
			}
		}
	}

	s.typs = typs
	s.batch = coldata.NewMemBatch(typs)
	tupleLen := len(s.tuples[0])
	for _, i := range s.sel {
		if len(s.tuples[i]) != tupleLen {
			panic(fmt.Sprintf("mismatched tuple lens: found %+v expected %d vals",
				s.tuples[i], tupleLen))
		}
	}

	if s.sel != nil {
		s.batch.SetSelection(true)
		// When non-nil selection vector is given, we convert all tuples into the
		// Go values at once, and we'll be copying an appropriate chunk of the
		// selection vector later in Next().
		for i := range s.typs {
			vec := s.batch.ColVec(i)
			vec.UnsetNulls()
			// Automatically convert the Go values into exec.Type slice elements using
			// reflection. This is slow, but acceptable for tests.
			col := reflect.ValueOf(vec.Col())
			for j := 0; j < len(s.tuples); j++ {
				if s.tuples[j][i] == nil {
					vec.SetNull(uint16(j))
				} else {
					col.Index(j).Set(
						reflect.ValueOf(s.tuples[j][i]).Convert(reflect.TypeOf(vec.Col()).Elem()))
				}
			}
		}
	}

}

func (s *opFixedSelTestInput) Next() coldata.Batch {
	var batchSize uint16
	if s.sel == nil {
		batchSize = s.batchSize
		if uint16(len(s.tuples))-s.idx < batchSize {
			batchSize = uint16(len(s.tuples)) - s.idx
		}
		// When nil selection vector is given, we convert only the tuples that fit
		// into the current batch (keeping the s.idx in mind).
		for i := range s.typs {
			vec := s.batch.ColVec(i)
			vec.UnsetNulls()
			// Automatically convert the Go values into exec.Type slice elements using
			// reflection. This is slow, but acceptable for tests.
			col := reflect.ValueOf(vec.Col())
			for j := uint16(0); j < batchSize; j++ {
				if s.tuples[s.idx+j][i] == nil {
					vec.SetNull(j)
				} else {
					col.Index(int(j)).Set(
						reflect.ValueOf(s.tuples[s.idx+j][i]).Convert(reflect.TypeOf(vec.Col()).Elem()))
				}
			}
		}
	} else {
		if s.idx == uint16(len(s.sel)) {
			s.batch.SetLength(0)
			return s.batch
		}
		batchSize = s.batchSize
		if uint16(len(s.sel))-s.idx < batchSize {
			batchSize = uint16(len(s.sel)) - s.idx
		}
		// All tuples have already been converted to the Go values, so we only need
		// to set the right selection vector for s.batch.
		copy(s.batch.Selection(), s.sel[s.idx:s.idx+batchSize])
	}
	s.batch.SetLength(batchSize)
	s.idx += batchSize
	return s.batch
}

// opTestOutput is a test verification struct that ensures its input batches
// match some expected output tuples.
type opTestOutput struct {
	input    Operator
	cols     []int
	expected tuples

	curIdx uint16
	batch  coldata.Batch
}

// newOpTestOutput returns a new opTestOutput, initialized with the given input
// to verify on the given column indices that the output is exactly equal to
// the expected tuples.
func newOpTestOutput(input Operator, cols []int, expected tuples) *opTestOutput {
	input.Init()

	return &opTestOutput{
		input:    input,
		cols:     cols,
		expected: expected,
	}
}

func (r *opTestOutput) next() tuple {
	if r.batch == nil || r.curIdx >= r.batch.Length() {
		// Get a fresh batch.
		r.batch = r.input.Next()
		if r.batch.Length() == 0 {
			return nil
		}
		r.curIdx = 0
	}
	ret := make(tuple, len(r.cols))
	out := reflect.ValueOf(ret)
	curIdx := r.curIdx
	if sel := r.batch.Selection(); sel != nil {
		curIdx = sel[curIdx]
	}
	for outIdx, colIdx := range r.cols {
		vec := r.batch.ColVec(colIdx)
		if vec.NullAt(curIdx) {
			ret[outIdx] = nil
		} else {
			col := reflect.ValueOf(vec.Col())
			out.Index(outIdx).Set(col.Index(int(curIdx)))
		}
	}
	r.curIdx++
	return ret
}

// Verify ensures that the input to this opTestOutput produced the same results
// and in the same order as the ones expected in the opTestOutput's expected
// tuples, using a slow, reflection-based comparison method, returning an error
// if the input isn't equal to the expected.
func (r *opTestOutput) Verify() error {
	var actual tuples
	for {
		tup := r.next()
		if tup == nil {
			break
		}
		actual = append(actual, tup)
	}
	return assertTuplesOrderedEqual(r.expected, actual)
}

// VerifyAnyOrder ensures that the input to this opTestOutput produced the same
// results but in any order (meaning set comparison behavior is used) as the
// ones expected in the opTestOutput's expected tuples, using a slow,
// reflection-based comparison method, returning an error if the input isn't
// equal to the expected.
func (r *opTestOutput) VerifyAnyOrder() error {
	var actual tuples
	for {
		tup := r.next()
		if tup == nil {
			break
		}
		actual = append(actual, tup)
	}
	return assertTuplesSetsEqual(r.expected, actual)
}

// tupleEquals checks that two tuples are equal, using a slow,
// reflection-based method to do the comparison. Reflection is used so that
// values can be compared in a type-agnostic way.
func tupleEquals(expected tuple, actual tuple) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := 0; i < len(actual); i++ {
		if expected[i] == nil || actual[i] == nil {
			if expected[i] != nil || actual[i] != nil {
				return false
			}
		} else {
			if !reflect.DeepEqual(reflect.ValueOf(actual[i]).Convert(reflect.TypeOf(expected[i])).Interface(), expected[i]) {
				return false
			}
		}
	}
	return true
}

// assertTuplesSetsEqual asserts that two sets of tuples are equal.
func assertTuplesSetsEqual(expected tuples, actual tuples) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	actualTupleUsed := make([]bool, len(actual))
	for _, te := range expected {
		matched := false
		for j, ta := range actual {
			if !actualTupleUsed[j] {
				if tupleEquals(te, ta) {
					actualTupleUsed[j] = true
					matched = true
					break
				}
			}
		}
		if !matched {
			return errors.Errorf("expected %+v, actual %+v\n", expected, actual)
		}
	}
	return nil
}

// assertTuplesOrderedEqual asserts that two permutations of tuples are equal
// in order.
func assertTuplesOrderedEqual(expected tuples, actual tuples) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	for i := range expected {
		if !tupleEquals(expected[i], actual[i]) {
			return errors.Errorf("expected %+v, actual %+v\n", expected, actual)
		}
	}
	return nil
}

// repeatableBatchSource is an Operator that returns the same batch forever.
type repeatableBatchSource struct {
	internalBatch coldata.Batch
	batchLen      uint16
	// sel specifies the desired selection vector for the batch.
	sel []uint16

	batchesToReturn int
	batchesReturned int
}

var _ Operator = &repeatableBatchSource{}

// newRepeatableBatchSource returns a new Operator initialized to return its
// input batch forever (including the selection vector if batch comes with it).
func newRepeatableBatchSource(batch coldata.Batch) *repeatableBatchSource {
	src := &repeatableBatchSource{
		internalBatch: batch,
		batchLen:      batch.Length(),
	}
	if batch.Selection() != nil {
		src.sel = make([]uint16, batch.Length())
		copy(src.sel, batch.Selection())
	}
	return src
}

func (s *repeatableBatchSource) Next() coldata.Batch {
	s.internalBatch.SetSelection(s.sel != nil)
	s.batchesReturned++
	if s.batchesToReturn != 0 && s.batchesReturned > s.batchesToReturn {
		s.internalBatch.SetLength(0)
	} else {
		s.internalBatch.SetLength(s.batchLen)
	}
	if s.sel != nil {
		// Since selection vectors are mutable, to make sure that we return the
		// batch with the given selection vector, we need to reset
		// s.internalBatch.Selection() to s.sel on every iteration.
		copy(s.internalBatch.Selection(), s.sel)
	}
	return s.internalBatch
}

func (s *repeatableBatchSource) Init() {}

func (s *repeatableBatchSource) resetBatchesToReturn(b int) {
	s.batchesToReturn = b
	s.batchesReturned = 0
}

// finiteBatchSource is an Operator that returns the same batch a specified
// number of times.
type finiteBatchSource struct {
	repeatableBatch *repeatableBatchSource

	usableCount int
}

var _ Operator = &finiteBatchSource{}

var emptyBatch = coldata.NewMemBatch([]types.T{})

// newFiniteBatchSource returns a new Operator initialized to return its input
// batch a specified number of times.
func newFiniteBatchSource(batch coldata.Batch, usableCount int) *finiteBatchSource {
	return &finiteBatchSource{
		repeatableBatch: newRepeatableBatchSource(batch),
		usableCount:     usableCount,
	}
}

func (f *finiteBatchSource) Init() {
	f.repeatableBatch.Init()
}

func (f *finiteBatchSource) Next() coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		return f.repeatableBatch.Next()
	}
	return emptyBatch
}

// randomLengthBatchSource is an Operator that forever returns the same batch at
// a different length each time.
type randomLengthBatchSource struct {
	internalBatch coldata.Batch
	rng           *rand.Rand
}

var _ Operator = &randomLengthBatchSource{}

// newRandomLengthBatchSource returns a new Operator initialized to return a
// batch of random length between [1, col.BatchSize) forever.
func newRandomLengthBatchSource(batch coldata.Batch) *randomLengthBatchSource {
	return &randomLengthBatchSource{
		internalBatch: batch,
	}
}

func (r *randomLengthBatchSource) Init() {
	r.rng, _ = randutil.NewPseudoRand()
}

func (r *randomLengthBatchSource) Next() coldata.Batch {
	r.internalBatch.SetLength(uint16(randutil.RandIntInRange(r.rng, 1, int(coldata.BatchSize))))
	return r.internalBatch
}

// finiteChunksSource is an Operator that returns a batch specified number of
// times. The first matchLen columns of the batch are incremented every time
// (except for the first) the batch is returned to emulate source that is
// already ordered on matchLen columns.
type finiteChunksSource struct {
	repeatableBatch *repeatableBatchSource

	usableCount int
	matchLen    int
	adjustment  []int64
}

var _ Operator = &finiteBatchSource{}

func newFiniteChunksSource(batch coldata.Batch, usableCount int, matchLen int) *finiteChunksSource {
	return &finiteChunksSource{
		repeatableBatch: newRepeatableBatchSource(batch),
		usableCount:     usableCount,
		matchLen:        matchLen,
	}
}

func (f *finiteChunksSource) Init() {
	f.repeatableBatch.Init()
	f.adjustment = make([]int64, f.matchLen)
}

func (f *finiteChunksSource) Next() coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		batch := f.repeatableBatch.Next()
		if f.adjustment[0] == 0 {
			// We need to calculate the difference between the first and the last
			// tuples in batch in first matchLen columns so that in the following
			// calls to Next() the batch is adjusted such that tuples in consecutive
			// batches are ordered on the first matchLen columns.
			for col := 0; col < f.matchLen; col++ {
				firstValue := batch.ColVec(col).Int64()[0]
				lastValue := batch.ColVec(col).Int64()[batch.Length()-1]
				f.adjustment[col] = lastValue - firstValue
			}
		} else {
			for i := 0; i < f.matchLen; i++ {
				int64Vec := batch.ColVec(i).Int64()
				for j := range int64Vec {
					int64Vec[j] += f.adjustment[i]
				}
			}
		}
		return batch
	}
	return coldata.NewMemBatch([]types.T{})
}

func TestOpTestInputOutput(t *testing.T) {
	inputs := []tuples{
		{
			{1, 2, 100},
			{1, 3, -3},
			{0, 4, 5},
			{1, 5, 0},
		},
	}
	runTests(t, inputs, func(t *testing.T, sources []Operator) {
		out := newOpTestOutput(sources[0], []int{0, 1, 2}, inputs[0])

		if err := out.Verify(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRepeatableBatchSource(t *testing.T) {
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	batchLen := uint16(10)
	batch.SetLength(batchLen)
	input := newRepeatableBatchSource(batch)

	b := input.Next()
	b.SetLength(0)
	b.SetSelection(true)

	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected repeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() != nil {
		t.Fatalf("expected repeatableBatchSource to reset selection vector, found %+v", b.Selection())
	}
}

func TestRepeatableBatchSourceWithFixedSel(t *testing.T) {
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	rng, _ := randutil.NewPseudoRand()
	sel := randomSel(rng, 10 /* batchSize */, 0 /* probOfOmitting */)
	batchLen := uint16(len(sel))
	batch.SetLength(batchLen)
	batch.SetSelection(true)
	copy(batch.Selection(), sel)
	input := newRepeatableBatchSource(batch)
	b := input.Next()

	b.SetLength(0)
	b.SetSelection(false)
	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected repeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected repeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := uint16(0); i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected repeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}

	newSel := randomSel(rng, 10 /* batchSize */, 0.2 /* probOfOmitting */)
	newBatchLen := uint16(len(sel))
	b.SetLength(newBatchLen)
	b.SetSelection(true)
	copy(b.Selection(), newSel)
	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected repeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected repeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := uint16(0); i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected repeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}
}

// chunkingBatchSource is a batch source that takes unlimited-size columns and
// chunks them into BatchSize-sized chunks when Nexted.
type chunkingBatchSource struct {
	typs []types.T
	cols []coldata.Vec
	len  uint64

	curIdx uint64
	batch  coldata.Batch
}

// newChunkingBatchSource returns a new chunkingBatchSource with the given
// column types, columns, and length.
func newChunkingBatchSource(typs []types.T, cols []coldata.Vec, len uint64) *chunkingBatchSource {
	return &chunkingBatchSource{
		typs: typs,
		cols: cols,
		len:  len,
	}
}

func (c *chunkingBatchSource) Init() {
	c.batch = coldata.NewMemBatch(c.typs)
	for i := range c.cols {
		c.batch.ColVec(i).SetCol(c.cols[i].Col())
	}
}

func (c *chunkingBatchSource) Next() coldata.Batch {
	if c.curIdx >= c.len {
		c.batch.SetLength(0)
	}
	lastIdx := c.curIdx + coldata.BatchSize
	if lastIdx > c.len {
		lastIdx = c.len
	}
	for i, vec := range c.batch.ColVecs() {
		vec.SetCol(c.cols[i].Slice(c.typs[i], c.curIdx, lastIdx).Col())
	}
	c.batch.SetLength(uint16(lastIdx - c.curIdx))
	c.curIdx = lastIdx
	return c.batch
}

// batchBuffer exposes a buffer of coldata.Batches through an Operator
// interface. If there are no batches to return, Next will panic.
type batchBuffer struct {
	buffer []coldata.Batch
}

var _ Operator = &batchBuffer{}

func newBatchBuffer() *batchBuffer {
	return &batchBuffer{
		buffer: make([]coldata.Batch, 0, 2),
	}
}

func (b *batchBuffer) add(batch coldata.Batch) {
	b.buffer = append(b.buffer, batch)
}

func (b *batchBuffer) Init() {}

func (b *batchBuffer) Next() coldata.Batch {
	batch := b.buffer[0]
	b.buffer = b.buffer[1:]
	return batch
}
