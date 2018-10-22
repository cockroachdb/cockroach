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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

// tuple represents a row with any-type columns.
type tuple []interface{}

// tuples represents a table of a single type.
type tuples []tuple

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
	typs      []types.T
	extraCols []types.T
	tuples    tuples
	batch     ColBatch
}

var _ Operator = &opTestInput{}

// newOpTestInput returns a new opTestInput, with the given input tuples and
// the given extra column types. The input tuples are translated into types
// automatically, using simple rules (e.g. integers always become Int64).
// The extraCols slice represents any extra columns that the batch must have
// for output or temp columns for operators in the test.
func newOpTestInput(tuples tuples, extraCols ...types.T) *opTestInput {
	ret := &opTestInput{
		tuples:    tuples,
		extraCols: extraCols,
	}
	ret.Init()
	return ret
}

func (s *opTestInput) Init() {
	if len(s.tuples) == 0 {
		panic("empty tuple source")
	}
	tuple := s.tuples[0]
	typs := make([]types.T, len(tuple))
	for i := range tuple {
		typs[i] = types.FromGoType(tuple[i])
	}
	s.typs = typs
	s.batch = NewMemBatch(append(typs, s.extraCols...))
}

func (s *opTestInput) Next() ColBatch {
	if len(s.tuples) == 0 {
		s.batch.SetLength(0)
		return s.batch
	}
	batchSize := uint16(ColBatchSize)
	if len(s.tuples) < int(batchSize) {
		batchSize = uint16(len(s.tuples))
	}
	tuples := s.tuples[:batchSize]
	s.tuples = s.tuples[batchSize:]

	tupleLen := len(tuples[0])
	for i := range tuples {
		if len(tuples[i]) != tupleLen {
			panic(fmt.Sprintf("mismatched tuple len: found %d expected %d",
				len(tuples[i]), tupleLen))
		}
	}
	for i, typ := range s.typs {
		vec := s.batch.ColVec(i)
		switch typ {
		case types.Bool, types.Bytes:
			panic("unhandled")
		default:
			// Automatically convert the Go values into exec.Type slice elements using
			// reflection. This is slow, but acceptable for tests.
			col := reflect.ValueOf(vec.Col())
			for j := uint16(0); j < batchSize; j++ {
				col.Index(int(j)).Set(
					reflect.ValueOf(tuples[j][i]).Convert(reflect.TypeOf(vec.Col()).Elem()))
			}
		}
	}

	s.batch.SetLength(batchSize)
	s.batch.SetSelection(false)
	return s.batch
}

// opTestOutput is a test verification struct that ensures its input batches
// match some expected output tuples.
type opTestOutput struct {
	input    Operator
	cols     []int
	expected tuples

	curIdx uint16
	batch  ColBatch
}

// newOpTestOutput returns a new opTestOutput, initialized with the given input
// to verify on the given column indices that the output is exactly equal to
// the expected tuples.
func newOpTestOutput(input Operator, cols []int, expected tuples) *opTestOutput {
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
		switch vec.Col().(type) {
		case Bytes:
			panic("unhandled")
		default:
			col := reflect.ValueOf(vec.Col())
			out.Index(outIdx).Set(col.Index(int(curIdx)))
		}
	}
	r.curIdx++
	return ret
}

// Verify ensures that the input to this opTestOutput produced the same results
// as the ones expected in the opTestOutput's expected tuples, using a slow,
// reflection-based comparison method, returning an error if the input isn't
// equal to the expected.
func (r *opTestOutput) Verify() error {
	var actual tuples
	for {
		tuple := r.next()
		if tuple == nil {
			break
		}
		actual = append(actual, tuple)
	}
	return assertTuplesEquals(r.expected, actual)
}

// assertTupleEquals asserts that two tuples are equal, using a slow,
// reflection-based method to do the assertion. Reflection is used so that
// values can be compared in a type-agnostic way.
func assertTupleEquals(expected tuple, actual tuple) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	for i := 0; i < len(actual); i++ {
		if reflect.ValueOf(actual[i]).Convert(reflect.TypeOf(expected[i])).Interface() != expected[i] {
			return errors.Errorf("expected %+v, actual %+v", expected, actual)
		}
	}
	return nil
}

// assertTuplesEquals asserts that two sets of tuples are equal.
func assertTuplesEquals(expected tuples, actual tuples) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	for i, t := range expected {
		if err := assertTupleEquals(t, actual[i]); err != nil {
			return errors.Wrapf(err, "expected %+v, actual %+v:\n", expected, actual)
		}
	}
	return nil
}

// repeatableBatchSource is an Operator that returns the same batch forever.
type repeatableBatchSource struct {
	internalBatch ColBatch
}

var _ Operator = &repeatableBatchSource{}

// newRepeatableBatchSource returns a new Operator initialized to return
// its input batch forever.
func newRepeatableBatchSource(batch ColBatch) *repeatableBatchSource {
	return &repeatableBatchSource{
		internalBatch: batch,
	}
}

func (s *repeatableBatchSource) Next() ColBatch {
	s.internalBatch.SetSelection(false)
	s.internalBatch.SetLength(ColBatchSize)
	return s.internalBatch
}

func (s *repeatableBatchSource) Init() {}
