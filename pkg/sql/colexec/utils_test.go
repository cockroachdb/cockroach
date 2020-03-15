// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/pkg/errors"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tuple represents a row with any-type columns.
type tuple []interface{}

func (t tuple) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range t {
		if i != 0 {
			sb.WriteString(", ")
		}
		if d, ok := t[i].(apd.Decimal); ok {
			sb.WriteString(d.String())
		} else {
			sb.WriteString(fmt.Sprintf("%v", t[i]))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (t tuple) less(other tuple) bool {
	for i := range t {
		// If either side is nil, we short circuit the comparison. For nil, we
		// define: nil < {any_none_nil}
		if t[i] == nil && other[i] == nil {
			continue
		} else if t[i] == nil && other[i] != nil {
			return true
		} else if t[i] != nil && other[i] == nil {
			return false
		}

		lhsVal := reflect.ValueOf(t[i])
		rhsVal := reflect.ValueOf(other[i])

		// apd.Decimal are not comparable, so we check that first.
		if lhsVal.Type().Name() == "Decimal" && lhsVal.CanInterface() {
			lhsDecimal := lhsVal.Interface().(apd.Decimal)
			rhsDecimal := rhsVal.Interface().(apd.Decimal)
			cmp := (&lhsDecimal).CmpTotal(&rhsDecimal)
			if cmp == 0 {
				continue
			} else if cmp == -1 {
				return true
			} else {
				return false
			}
		}

		// coltypes.Bytes is represented as []uint8.
		if lhsVal.Type().String() == "[]uint8" {
			lhsStr := string(lhsVal.Interface().([]uint8))
			rhsStr := string(rhsVal.Interface().([]uint8))
			if lhsStr == rhsStr {
				continue
			} else if lhsStr < rhsStr {
				return true
			} else {
				return false
			}
		}

		// No need to compare these two elements when they are the same.
		if t[i] == other[i] {
			continue
		}

		switch typ := lhsVal.Type().Name(); typ {
		case "int", "int16", "int32", "int64":
			return lhsVal.Int() < rhsVal.Int()
		case "uint", "uint16", "uint32", "uint64":
			return lhsVal.Uint() < rhsVal.Uint()
		case "float", "float64":
			return lhsVal.Float() < rhsVal.Float()
		case "bool":
			return lhsVal.Bool() == false && rhsVal.Bool() == true
		case "string":
			return lhsVal.String() < rhsVal.String()
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("Unhandled comparison type: %s", typ))
		}
	}
	return false
}

// tuples represents a table with any-type columns.
type tuples []tuple

// sort returns a copy of sorted tuples.
func (t tuples) sort() tuples {
	b := make(tuples, len(t))
	for i := range b {
		b[i] = make(tuple, len(t[i]))
		copy(b[i], t[i])
	}
	sort.SliceStable(b, func(i, j int) bool {
		lhs := b[i]
		rhs := b[j]
		return lhs.less(rhs)
	})
	return b
}

type verifierType int

const (
	// orderedVerifier compares the input and output tuples, returning an error
	// if they're not identical.
	orderedVerifier verifierType = iota
	// unorderedVerifier compares the input and output tuples as sets, returning
	// an error if they aren't equal by set comparison (irrespective of order).
	unorderedVerifier
)

type verifierFn func(output *opTestOutput) error

// maybeHasNulls is a helper function that returns whether any of the columns in b
// (maybe) have nulls.
func maybeHasNulls(b coldata.Batch) bool {
	if b.Length() == 0 {
		return false
	}
	for i := 0; i < b.Width(); i++ {
		if b.ColVec(i).MaybeHasNulls() {
			return true
		}
	}
	return false
}

type testRunner func(*testing.T, []tuples, [][]coltypes.T, tuples, interface{}, func([]Operator) (Operator, error))

// variableOutputBatchSizeInitializer is implemented by operators that can be
// initialized with variable output size batches. This allows runTests to
// increase test coverage of these operators.
type variableOutputBatchSizeInitializer interface {
	initWithOutputBatchSize(int)
}

// runTests is a helper that automatically runs your tests with varied batch
// sizes and with and without a random selection vector.
// tups is the sets of input tuples.
// expected is the set of output tuples.
// constructor is a function that takes a list of input Operators and returns
// the operator to test, or an error.
func runTests(
	t *testing.T,
	tups []tuples,
	expected tuples,
	verifier interface{},
	constructor func(inputs []Operator) (Operator, error),
) {
	runTestsWithTyps(t, tups, nil /* typs */, expected, verifier, constructor)
}

// runTestsWithTyps is the same as runTests with an ability to specify the
// types of the input tuples.
// - typs is the type schema of the input tuples. Note that this is a multi-
//   dimensional slice which allows for specifying different schemas for each
//   of the inputs.
func runTestsWithTyps(
	t *testing.T,
	tups []tuples,
	typs [][]coltypes.T,
	expected tuples,
	verifier interface{},
	constructor func(inputs []Operator) (Operator, error),
) {
	runTestsWithoutAllNullsInjection(t, tups, typs, expected, verifier, constructor)

	t.Run("allNullsInjection", func(t *testing.T) {
		// This test replaces all values in the input tuples with nulls and ensures
		// that the output is different from the "original" output (i.e. from the
		// one that is returned without nulls injection).
		onlyNullsInTheInput := true
	OUTER:
		for _, tup := range tups {
			for i := 0; i < len(tup); i++ {
				for j := 0; j < len(tup[i]); j++ {
					if tup[i][j] != nil {
						onlyNullsInTheInput = false
						break OUTER
					}
				}
			}
		}
		opConstructor := func(injectAllNulls bool) Operator {
			inputSources := make([]Operator, len(tups))
			var inputTypes []coltypes.T
			for i, tup := range tups {
				if typs != nil {
					inputTypes = typs[i]
				}
				input := newOpTestInput(1 /* batchSize */, tup, inputTypes)
				input.injectAllNulls = injectAllNulls
				inputSources[i] = input
			}
			op, err := constructor(inputSources)
			if err != nil {
				t.Fatal(err)
			}
			op.Init()
			return op
		}
		ctx := context.Background()
		originalOp := opConstructor(false /* injectAllNulls */)
		opWithNulls := opConstructor(true /* injectAllNulls */)
		foundDifference := false
		for {
			originalBatch := originalOp.Next(ctx)
			batchWithNulls := opWithNulls.Next(ctx)
			if originalBatch.Length() != batchWithNulls.Length() {
				foundDifference = true
				break
			}
			if originalBatch.Length() == 0 {
				break
			}
			var originalTuples, tuplesWithNulls tuples
			for i := 0; i < originalBatch.Length(); i++ {
				// We checked that the batches have the same length.
				originalTuples = append(originalTuples, getTupleFromBatch(originalBatch, i))
				tuplesWithNulls = append(tuplesWithNulls, getTupleFromBatch(batchWithNulls, i))
			}
			if err := assertTuplesSetsEqual(originalTuples, tuplesWithNulls); err != nil {
				// err is non-nil which means that the batches are different.
				foundDifference = true
				break
			}
		}
		if onlyNullsInTheInput {
			require.False(t, foundDifference, "since there were only "+
				"nulls in the input tuples, we expect for all nulls injection to not "+
				"change the output")
		} else {
			require.True(t, foundDifference, "since there were "+
				"non-nulls in the input tuples, we expect for all nulls injection to "+
				"change the output")
		}
		if c, ok := originalOp.(closer); ok {
			require.NoError(t, c.Close(ctx))
		}
		if c, ok := opWithNulls.(closer); ok {
			require.NoError(t, c.Close(ctx))
		}
	})
}

// runTestsWithoutAllNullsInjection is the same as runTests, but it skips the
// all nulls injection test. Use this only when the all nulls injection should
// not change the output of the operator under testing.
// NOTE: please leave a justification why you're using this variant of
// runTests.
func runTestsWithoutAllNullsInjection(
	t *testing.T,
	tups []tuples,
	typs [][]coltypes.T,
	expected tuples,
	verifier interface{},
	constructor func(inputs []Operator) (Operator, error),
) {
	skipVerifySelAndNullsResets := true
	var verifyFn verifierFn
	switch v := verifier.(type) {
	case verifierType:
		switch v {
		case orderedVerifier:
			verifyFn = (*opTestOutput).Verify
			// Note that this test makes sense only if we expect tuples to be
			// returned in the same order (otherwise the second batch's selection
			// vector or nulls info can be different and that is totally valid).
			skipVerifySelAndNullsResets = false
		case unorderedVerifier:
			verifyFn = (*opTestOutput).VerifyAnyOrder
		default:
			execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected verifierType %d", v))
		}
	case verifierFn:
		verifyFn = v
	}
	runTestsWithFn(t, tups, typs, func(t *testing.T, inputs []Operator) {
		op, err := constructor(inputs)
		if err != nil {
			t.Fatal(err)
		}
		out := newOpTestOutput(op, expected)
		if err := verifyFn(out); err != nil {
			t.Fatal(err)
		}
	})

	if !skipVerifySelAndNullsResets {
		t.Run("verifySelAndNullResets", func(t *testing.T) {
			// This test ensures that operators that "own their own batches", such as
			// any operator that has to reshape its output, are not affected by
			// downstream modification of batches.
			// We run the main loop twice: once to determine what the operator would
			// output on its second Next call (we need the first call to Next to get a
			// reference to a batch to modify), and a second time to modify the batch
			// and verify that this does not change the operator output.
			// NOTE: this test makes sense only if the operator returns two non-zero
			// length batches (if not, we short-circuit the test since the operator
			// doesn't have to restore anything on a zero-length batch).
			var (
				secondBatchHasSelection, secondBatchHasNulls bool
				inputTypes                                   []coltypes.T
			)
			for round := 0; round < 2; round++ {
				inputSources := make([]Operator, len(tups))
				for i, tup := range tups {
					if typs != nil {
						inputTypes = typs[i]
					}
					inputSources[i] = newOpTestInput(1 /* batchSize */, tup, inputTypes)
				}
				op, err := constructor(inputSources)
				if err != nil {
					t.Fatal(err)
				}
				if vbsiOp, ok := op.(variableOutputBatchSizeInitializer); ok {
					// initialize the operator with a very small output batch size to
					// increase the likelihood that multiple batches will be output.
					vbsiOp.initWithOutputBatchSize(1)
				} else {
					op.Init()
				}
				ctx := context.Background()
				b := op.Next(ctx)
				if b.Length() == 0 {
					return
				}
				if round == 1 {
					if secondBatchHasSelection {
						b.SetSelection(false)
					} else {
						b.SetSelection(true)
					}
					if secondBatchHasNulls {
						// ResetInternalBatch will throw away the null information.
						b.ResetInternalBatch()
					} else {
						for i := 0; i < b.Width(); i++ {
							b.ColVec(i).Nulls().SetNulls()
						}
					}
				}
				b = op.Next(ctx)
				if b.Length() == 0 {
					return
				}
				if round == 0 {
					secondBatchHasSelection = b.Selection() != nil
					secondBatchHasNulls = maybeHasNulls(b)
				}
				if round == 1 {
					if secondBatchHasSelection {
						assert.NotNil(t, b.Selection())
					} else {
						assert.Nil(t, b.Selection())
					}
					if secondBatchHasNulls {
						assert.True(t, maybeHasNulls(b))
					} else {
						assert.False(t, maybeHasNulls(b))
					}
				}
				if c, ok := op.(closer); ok {
					// Some operators need an explicit Close if not drained completely of
					// input.
					assert.NoError(t, c.Close(ctx))
				}
			}
		})
	}

	t.Run("randomNullsInjection", func(t *testing.T) {
		// This test randomly injects nulls in the input tuples and ensures that
		// the operator doesn't panic.
		inputSources := make([]Operator, len(tups))
		var inputTypes []coltypes.T
		for i, tup := range tups {
			if typs != nil {
				inputTypes = typs[i]
			}
			input := newOpTestInput(1 /* batchSize */, tup, inputTypes)
			input.injectRandomNulls = true
			inputSources[i] = input
		}
		op, err := constructor(inputSources)
		if err != nil {
			t.Fatal(err)
		}
		op.Init()
		ctx := context.Background()
		for b := op.Next(ctx); b.Length() > 0; b = op.Next(ctx) {
		}
	})
}

// runTestsWithFn is like runTests, but the input function is responsible for
// performing any required tests. Please note that runTestsWithFn is a worse
// testing facility than runTests, because it can't get a handle on the operator
// under test and therefore can't perform as many extra checks. You should
// always prefer using runTests over runTestsWithFn.
// - tups is the sets of input tuples.
// - typs is the type schema of the input tuples. Note that this is a multi-
//   dimensional slice which allows for specifying different schemas for each
//   of the inputs. This can also be left nil in which case the types will be
//   determined at the runtime looking at the first input tuple, and if the
//   determination doesn't succeed for a value of the tuple (likely because
//   it's a nil), then that column will be assumed by default of type Int64.
// - test is a function that takes a list of input Operators and performs
//   testing with t.
func runTestsWithFn(
	t *testing.T, tups []tuples, typs [][]coltypes.T, test func(t *testing.T, inputs []Operator),
) {
	// Run tests over batchSizes of 1, (sometimes) a batch size that is small but
	// greater than 1, and a full coldata.BatchSize().
	batchSizes := make([]int, 0, 3)
	batchSizes = append(batchSizes, 1)
	smallButGreaterThanOne := int(math.Trunc(.002 * float64(coldata.BatchSize())))
	if smallButGreaterThanOne > 1 {
		batchSizes = append(batchSizes, smallButGreaterThanOne)
	}
	batchSizes = append(batchSizes, coldata.BatchSize())

	for _, batchSize := range batchSizes {
		for _, useSel := range []bool{false, true} {
			t.Run(fmt.Sprintf("batchSize=%d/sel=%t", batchSize, useSel), func(t *testing.T) {
				inputSources := make([]Operator, len(tups))
				var inputTypes []coltypes.T
				if useSel {
					for i, tup := range tups {
						if typs != nil {
							inputTypes = typs[i]
						}
						rng, _ := randutil.NewPseudoRand()
						inputSources[i] = newOpTestSelInput(rng, batchSize, tup, inputTypes)
					}
				} else {
					for i, tup := range tups {
						if typs != nil {
							inputTypes = typs[i]
						}
						inputSources[i] = newOpTestInput(batchSize, tup, inputTypes)
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
	t *testing.T, tups []tuples, sel []int, test func(t *testing.T, inputs []Operator),
) {
	for _, batchSize := range []int{1, 2, 3, 16, 1024} {
		t.Run(fmt.Sprintf("batchSize=%d/fixedSel", batchSize), func(t *testing.T) {
			inputSources := make([]Operator, len(tups))
			for i, tup := range tups {
				inputSources[i] = newOpFixedSelTestInput(sel, batchSize, tup)
			}
			test(t, inputSources)
		})
	}
}

// setColVal is a test helper function to set the given value at the equivalent
// col[idx]. This function is slow due to reflection.
func setColVal(vec coldata.Vec, idx int, val interface{}) {
	if vec.Type() == coltypes.Bytes {
		var (
			bytesVal []byte
			ok       bool
		)
		bytesVal, ok = val.([]byte)
		if !ok {
			bytesVal = []byte(val.(string))
		}
		vec.Bytes().Set(idx, bytesVal)
		return
	}
	reflect.ValueOf(vec.Col()).Index(idx).Set(reflect.ValueOf(val).Convert(reflect.TypeOf(vec.Col()).Elem()))
}

// opTestInput is an Operator that columnarizes test input in the form of tuples
// of arbitrary Go coltypes. It's meant to be used in Operator unit tests in
// conjunction with opTestOutput like the following:
//
// inputTuples := tuples{
//   {1,2,3.3,true},
//   {5,6,7.0,false},
// }
// tupleSource := newOpTestInput(inputTuples, coltypes.Bool)
// opUnderTest := newFooOp(tupleSource, ...)
// output := newOpTestOutput(opUnderTest, expectedOutputTuples)
// if err := output.Verify(); err != nil {
//     t.Fatal(err)
// }
type opTestInput struct {
	ZeroInputNode

	typs []coltypes.T

	batchSize int
	tuples    tuples
	batch     coldata.Batch
	useSel    bool
	rng       *rand.Rand
	selection []int

	// injectAllNulls determines whether opTestInput will replace all values in
	// the input tuples with nulls.
	injectAllNulls bool

	// injectRandomNulls determines whether opTestInput will randomly replace
	// each value in the input tuples with a null.
	injectRandomNulls bool
}

var _ Operator = &opTestInput{}

// newOpTestInput returns a new opTestInput with the given input tuples and the
// given type schema. If typs is nil, the input tuples are translated into
// types automatically, using simple rules (e.g. integers always become Int64).
func newOpTestInput(batchSize int, tuples tuples, typs []coltypes.T) *opTestInput {
	ret := &opTestInput{
		batchSize: batchSize,
		tuples:    tuples,
		typs:      typs,
	}
	return ret
}

func newOpTestSelInput(
	rng *rand.Rand, batchSize int, tuples tuples, typs []coltypes.T,
) *opTestInput {
	ret := &opTestInput{
		useSel:    true,
		rng:       rng,
		batchSize: batchSize,
		tuples:    tuples,
		typs:      typs,
	}
	return ret
}

func (s *opTestInput) Init() {
	if s.typs == nil {
		if len(s.tuples) == 0 {
			execerror.VectorizedInternalPanic("empty tuple source with no specified types")
		}

		// The type schema was not provided, so we need to determine it based on
		// the input tuple.
		s.typs = make([]coltypes.T, len(s.tuples[0]))
		for i := range s.typs {
			// Default type for test cases is Int64 in case the entire column is null
			// and the type is indeterminate.
			s.typs[i] = coltypes.Int64
			for _, tup := range s.tuples {
				if tup[i] != nil {
					s.typs[i] = coltypes.FromGoType(tup[i])
					break
				}
			}
		}
	}
	s.batch = testAllocator.NewMemBatch(s.typs)

	s.selection = make([]int, coldata.BatchSize())
	for i := range s.selection {
		s.selection[i] = i
	}
}

func (s *opTestInput) Next(context.Context) coldata.Batch {
	s.batch.ResetInternalBatch()
	if len(s.tuples) == 0 {
		return coldata.ZeroBatch
	}
	batchSize := s.batchSize
	if len(s.tuples) < batchSize {
		batchSize = len(s.tuples)
	}
	tups := s.tuples[:batchSize]
	s.tuples = s.tuples[batchSize:]

	tupleLen := len(tups[0])
	for i := range tups {
		if len(tups[i]) != tupleLen {
			execerror.VectorizedInternalPanic(fmt.Sprintf("mismatched tuple lens: found %+v expected %d vals",
				tups[i], tupleLen))
		}
	}

	if s.useSel {
		for i := range s.selection {
			s.selection[i] = i
		}
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
		// Any unused elements in the selection vector are set to a value larger
		// than the max batch size, so the test will panic if this part of the slice
		// is accidentally accessed.
		for i := range s.selection[batchSize:] {
			s.selection[batchSize+i] = coldata.BatchSize() + 1
		}

		s.batch.SetSelection(true)
		copy(s.batch.Selection(), s.selection)
	}

	// Reset nulls for all columns in this batch.
	for _, colVec := range s.batch.ColVecs() {
		if colVec.Type() != coltypes.Unhandled {
			colVec.Nulls().UnsetNulls()
		}
	}

	rng := rand.New(rand.NewSource(123))

	for i := range s.typs {
		vec := s.batch.ColVec(i)
		typ := vec.Type()
		// Automatically convert the Go values into exec.Type slice elements using
		// reflection. This is slow, but acceptable for tests.
		col := reflect.ValueOf(vec.Col())
		for j := 0; j < batchSize; j++ {
			// If useSel is false, then the selection vector will contain
			// [0, ..., batchSize] in ascending order.
			outputIdx := s.selection[j]
			injectRandomNull := s.injectRandomNulls && rng.Float64() < 0.5
			if tups[j][i] == nil || s.injectAllNulls || injectRandomNull {
				vec.Nulls().SetNull(outputIdx)
				if rng.Float64() < 0.5 {
					// With 50% probability we set garbage data in the value to make sure
					// that it doesn't affect the computation when the value is actually
					// NULL. For the other 50% of cases we leave the data unset which
					// exercises other scenarios (like division by zero when the value is
					// actually NULL).
					if typ == coltypes.Decimal {
						d := apd.Decimal{}
						_, err := d.SetFloat64(rng.Float64())
						if err != nil {
							execerror.VectorizedInternalPanic(fmt.Sprintf("%v", err))
						}
						col.Index(outputIdx).Set(reflect.ValueOf(d))
					} else if typ == coltypes.Bytes {
						newBytes := make([]byte, rng.Intn(16)+1)
						rng.Read(newBytes)
						setColVal(vec, outputIdx, newBytes)
					} else if val, ok := quick.Value(reflect.TypeOf(vec.Col()).Elem(), rng); ok {
						setColVal(vec, outputIdx, val.Interface())
					} else {
						execerror.VectorizedInternalPanic(fmt.Sprintf("could not generate a random value of type %T\n.", vec.Type()))
					}
				}
			} else {
				setColVal(vec, outputIdx, tups[j][i])
			}
		}
	}

	s.batch.SetLength(batchSize)
	return s.batch
}

type opFixedSelTestInput struct {
	ZeroInputNode

	typs []coltypes.T

	batchSize int
	tuples    tuples
	batch     coldata.Batch
	sel       []int
	// idx is the index of the tuple to be emitted next. We need to maintain it
	// in case the provided selection vector or provided tuples (if sel is nil)
	// is longer than requested batch size.
	idx int
}

var _ Operator = &opFixedSelTestInput{}

// newOpFixedSelTestInput returns a new opFixedSelTestInput with the given
// input tuples and selection vector. The input tuples are translated into
// types automatically, using simple rules (e.g. integers always become Int64).
func newOpFixedSelTestInput(sel []int, batchSize int, tuples tuples) *opFixedSelTestInput {
	ret := &opFixedSelTestInput{
		batchSize: batchSize,
		sel:       sel,
		tuples:    tuples,
	}
	return ret
}

func (s *opFixedSelTestInput) Init() {
	if len(s.tuples) == 0 {
		execerror.VectorizedInternalPanic("empty tuple source")
	}

	typs := make([]coltypes.T, len(s.tuples[0]))
	for i := range typs {
		// Default type for test cases is Int64 in case the entire column is null
		// and the type is indeterminate.
		typs[i] = coltypes.Int64
		for _, tup := range s.tuples {
			if tup[i] != nil {
				typs[i] = coltypes.FromGoType(tup[i])
				break
			}
		}
	}

	s.typs = typs
	s.batch = testAllocator.NewMemBatch(s.typs)
	tupleLen := len(s.tuples[0])
	for _, i := range s.sel {
		if len(s.tuples[i]) != tupleLen {
			execerror.VectorizedInternalPanic(fmt.Sprintf("mismatched tuple lens: found %+v expected %d vals",
				s.tuples[i], tupleLen))
		}
	}

	// Reset nulls for all columns in this batch.
	for i := 0; i < s.batch.Width(); i++ {
		s.batch.ColVec(i).Nulls().UnsetNulls()
	}

	if s.sel != nil {
		s.batch.SetSelection(true)
		// When non-nil selection vector is given, we convert all tuples into the
		// Go values at once, and we'll be copying an appropriate chunk of the
		// selection vector later in Next().
		for i := range s.typs {
			vec := s.batch.ColVec(i)
			// Automatically convert the Go values into exec.Type slice elements using
			// reflection. This is slow, but acceptable for tests.
			for j := 0; j < len(s.tuples); j++ {
				if s.tuples[j][i] == nil {
					vec.Nulls().SetNull(j)
				} else {
					setColVal(vec, j, s.tuples[j][i])
				}
			}
		}
	}

}

func (s *opFixedSelTestInput) Next(context.Context) coldata.Batch {
	var batchSize int
	if s.sel == nil {
		batchSize = s.batchSize
		if len(s.tuples)-s.idx < batchSize {
			batchSize = len(s.tuples) - s.idx
		}
		// When nil selection vector is given, we convert only the tuples that fit
		// into the current batch (keeping the s.idx in mind).
		for i := range s.typs {
			vec := s.batch.ColVec(i)
			vec.Nulls().UnsetNulls()
			for j := 0; j < batchSize; j++ {
				if s.tuples[s.idx+j][i] == nil {
					vec.Nulls().SetNull(j)
				} else {
					// Automatically convert the Go values into exec.Type slice elements using
					// reflection. This is slow, but acceptable for tests.
					setColVal(vec, j, s.tuples[s.idx+j][i])
				}
			}
		}
	} else {
		if s.idx == len(s.sel) {
			return coldata.ZeroBatch
		}
		batchSize = s.batchSize
		if len(s.sel)-s.idx < batchSize {
			batchSize = len(s.sel) - s.idx
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
	OneInputNode
	expected tuples

	curIdx int
	batch  coldata.Batch
}

// newOpTestOutput returns a new opTestOutput, initialized with the given input
// to verify that the output is exactly equal to the expected tuples.
func newOpTestOutput(input Operator, expected tuples) *opTestOutput {
	input.Init()

	return &opTestOutput{
		OneInputNode: NewOneInputNode(input),
		expected:     expected,
	}
}

// getTupleFromBatch is a helper function that extracts a tuple at index
// tupleIdx from batch.
func getTupleFromBatch(batch coldata.Batch, tupleIdx int) tuple {
	ret := make(tuple, batch.Width())
	out := reflect.ValueOf(ret)
	if sel := batch.Selection(); sel != nil {
		tupleIdx = sel[tupleIdx]
	}
	for colIdx := range ret {
		vec := batch.ColVec(colIdx)
		if vec.Nulls().NullAt(tupleIdx) {
			ret[colIdx] = nil
		} else {
			var val reflect.Value
			if colBytes, ok := vec.Col().(*coldata.Bytes); ok {
				val = reflect.ValueOf(append([]byte(nil), colBytes.Get(tupleIdx)...))
			} else if vec.Type() == coltypes.Decimal {
				colDec := vec.Decimal()
				var newDec apd.Decimal
				newDec.Set(&colDec[tupleIdx])
				val = reflect.ValueOf(newDec)
			} else {
				val = reflect.ValueOf(vec.Col()).Index(tupleIdx)
			}
			out.Index(colIdx).Set(val)
		}
	}
	return ret
}

func (r *opTestOutput) next(ctx context.Context) tuple {
	if r.batch == nil || r.curIdx >= r.batch.Length() {
		// Get a fresh batch.
		r.batch = r.input.Next(ctx)
		if r.batch.Length() == 0 {
			return nil
		}
		r.curIdx = 0
	}
	ret := getTupleFromBatch(r.batch, r.curIdx)
	r.curIdx++
	return ret
}

// Verify ensures that the input to this opTestOutput produced the same results
// and in the same order as the ones expected in the opTestOutput's expected
// tuples, using a slow, reflection-based comparison method, returning an error
// if the input isn't equal to the expected.
func (r *opTestOutput) Verify() error {
	ctx := context.Background()
	var actual tuples
	for {
		tup := r.next(ctx)
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
	ctx := context.Background()
	var actual tuples
	for {
		tup := r.next(ctx)
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
			// Special case for NaN, since it does not equal itself.
			if f1, ok := expected[i].(float64); ok {
				if f2, ok := actual[i].(float64); ok {
					if math.IsNaN(f1) && math.IsNaN(f2) {
						continue
					} else if !math.IsNaN(f1) && !math.IsNaN(f2) && math.Abs(f1-f2) < 1e-6 {
						continue
					}
				}
			}
			if !reflect.DeepEqual(
				reflect.ValueOf(actual[i]).Convert(reflect.TypeOf(expected[i])).Interface(),
				expected[i],
			) || !reflect.DeepEqual(
				reflect.ValueOf(expected[i]).Convert(reflect.TypeOf(actual[i])).Interface(),
				actual[i],
			) {
				return false
			}
		}
	}
	return true
}

func makeError(expected tuples, actual tuples) error {
	var expStr, actStr strings.Builder
	for i := range expected {
		expStr.WriteString(fmt.Sprintf("%d: %s\n", i, expected[i].String()))
	}
	for i := range actual {
		actStr.WriteString(fmt.Sprintf("%d: %s\n", i, actual[i].String()))
	}

	diff := difflib.UnifiedDiff{
		A:       difflib.SplitLines(expStr.String()),
		B:       difflib.SplitLines(actStr.String()),
		Context: 100,
	}
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return errors.Errorf("expected didn't match actual, failed to make diff %s", err)
	}
	return errors.Errorf("expected didn't match actual. diff:\n%s", text)
}

// assertTuplesSetsEqual asserts that two sets of tuples are equal.
func assertTuplesSetsEqual(expected tuples, actual tuples) error {
	if len(expected) != len(actual) {
		return makeError(expected, actual)
	}
	actual = actual.sort()
	expected = expected.sort()
	return assertTuplesOrderedEqual(expected, actual)
}

// assertTuplesOrderedEqual asserts that two permutations of tuples are equal
// in order.
func assertTuplesOrderedEqual(expected tuples, actual tuples) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	for i := range expected {
		if !tupleEquals(expected[i], actual[i]) {
			return makeError(expected, actual)
		}
	}
	return nil
}

// finiteBatchSource is an Operator that returns the same batch a specified
// number of times.
type finiteBatchSource struct {
	ZeroInputNode

	repeatableBatch *RepeatableBatchSource

	usableCount int
}

var _ Operator = &finiteBatchSource{}

// newFiniteBatchSource returns a new Operator initialized to return its input
// batch a specified number of times.
func newFiniteBatchSource(batch coldata.Batch, usableCount int) *finiteBatchSource {
	return &finiteBatchSource{
		repeatableBatch: NewRepeatableBatchSource(testAllocator, batch),
		usableCount:     usableCount,
	}
}

func (f *finiteBatchSource) Init() {
	f.repeatableBatch.Init()
}

func (f *finiteBatchSource) Next(ctx context.Context) coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		return f.repeatableBatch.Next(ctx)
	}
	return coldata.ZeroBatch
}

func (f *finiteBatchSource) reset(usableCount int) {
	f.usableCount = usableCount
}

// finiteChunksSource is an Operator that returns a batch specified number of
// times. The first matchLen columns of the batch are incremented every time
// (except for the first) the batch is returned to emulate source that is
// already ordered on matchLen columns.
type finiteChunksSource struct {
	ZeroInputNode
	repeatableBatch *RepeatableBatchSource

	usableCount int
	matchLen    int
	adjustment  []int64
}

var _ Operator = &finiteChunksSource{}

func newFiniteChunksSource(batch coldata.Batch, usableCount int, matchLen int) *finiteChunksSource {
	return &finiteChunksSource{
		repeatableBatch: NewRepeatableBatchSource(testAllocator, batch),
		usableCount:     usableCount,
		matchLen:        matchLen,
	}
}

func (f *finiteChunksSource) Init() {
	f.repeatableBatch.Init()
	f.adjustment = make([]int64, f.matchLen)
}

func (f *finiteChunksSource) Next(ctx context.Context) coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		batch := f.repeatableBatch.Next(ctx)
		if f.matchLen > 0 && f.adjustment[0] == 0 {
			// We need to calculate the difference between the first and the last
			// tuples in batch in first matchLen columns so that in the following
			// calls to Next() the batch is adjusted such that tuples in consecutive
			// batches are ordered on the first matchLen columns.
			for col := 0; col < f.matchLen; col++ {
				firstValue := batch.ColVec(col).Int64()[0]
				lastValue := batch.ColVec(col).Int64()[batch.Length()-1]
				f.adjustment[col] = lastValue - firstValue + 1
			}
		} else {
			for i := 0; i < f.matchLen; i++ {
				int64Vec := batch.ColVec(i).Int64()
				for j := range int64Vec {
					int64Vec[j] += f.adjustment[i]
				}
				// We need to update the adjustments because RepeatableBatchSource
				// returns the original batch that it was instantiated with, and we
				// want to have constantly non-decreasing vectors.
				firstValue := batch.ColVec(i).Int64()[0]
				lastValue := batch.ColVec(i).Int64()[batch.Length()-1]
				f.adjustment[i] += lastValue - firstValue + 1
			}
		}
		return batch
	}
	return coldata.ZeroBatch
}

func TestOpTestInputOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	inputs := []tuples{
		{
			{1, 2, 100},
			{1, 3, -3},
			{0, 4, 5},
			{1, 5, 0},
		},
	}
	runTestsWithFn(t, inputs, nil /* typs */, func(t *testing.T, sources []Operator) {
		out := newOpTestOutput(sources[0], inputs[0])

		if err := out.Verify(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRepeatableBatchSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	batchLen := 10
	if coldata.BatchSize() < batchLen {
		batchLen = coldata.BatchSize()
	}
	batch.SetLength(batchLen)
	input := NewRepeatableBatchSource(testAllocator, batch)

	b := input.Next(context.Background())
	b.SetLength(0)
	b.SetSelection(true)

	b = input.Next(context.Background())
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() != nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, found %+v", b.Selection())
	}
}

func TestRepeatableBatchSourceWithFixedSel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	rng, _ := randutil.NewPseudoRand()
	batchSize := 10
	if batchSize > coldata.BatchSize() {
		batchSize = coldata.BatchSize()
	}
	sel := randomSel(rng, batchSize, 0 /* probOfOmitting */)
	batchLen := len(sel)
	batch.SetLength(batchLen)
	batch.SetSelection(true)
	copy(batch.Selection(), sel)
	input := NewRepeatableBatchSource(testAllocator, batch)
	b := input.Next(context.Background())

	b.SetLength(0)
	b.SetSelection(false)
	b = input.Next(context.Background())
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := 0; i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}

	newSel := randomSel(rng, 10 /* batchSize */, 0.2 /* probOfOmitting */)
	newBatchLen := len(sel)
	b.SetLength(newBatchLen)
	b.SetSelection(true)
	copy(b.Selection(), newSel)
	b = input.Next(context.Background())
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := 0; i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}
}

// chunkingBatchSource is a batch source that takes unlimited-size columns and
// chunks them into BatchSize()-sized chunks when Nexted.
type chunkingBatchSource struct {
	ZeroInputNode
	typs []coltypes.T
	cols []coldata.Vec
	len  int

	curIdx int
	batch  coldata.Batch
}

var _ Operator = &chunkingBatchSource{}

// newChunkingBatchSource returns a new chunkingBatchSource with the given
// column types, columns, and length.
func newChunkingBatchSource(typs []coltypes.T, cols []coldata.Vec, len int) *chunkingBatchSource {
	return &chunkingBatchSource{
		typs: typs,
		cols: cols,
		len:  len,
	}
}

func (c *chunkingBatchSource) Init() {
	c.batch = testAllocator.NewMemBatch(c.typs)
	for i := range c.cols {
		c.batch.ColVec(i).SetCol(c.cols[i].Col())
		c.batch.ColVec(i).SetNulls(c.cols[i].Nulls())
	}
}

func (c *chunkingBatchSource) Next(context.Context) coldata.Batch {
	if c.curIdx >= c.len {
		return coldata.ZeroBatch
	}
	// Explicitly set to false since this could be modified by the downstream
	// operators. This is sufficient because both the vectors and the nulls are
	// explicitly set below. ResetInternalBatch cannot be used here because we're
	// operating on Windows into the vectors.
	c.batch.SetSelection(false)
	lastIdx := c.curIdx + coldata.BatchSize()
	if lastIdx > c.len {
		lastIdx = c.len
	}
	for i, vec := range c.batch.ColVecs() {
		vec.SetCol(c.cols[i].Window(c.typs[i], c.curIdx, lastIdx).Col())
		nullsSlice := c.cols[i].Nulls().Slice(c.curIdx, lastIdx)
		vec.SetNulls(&nullsSlice)
	}
	c.batch.SetLength(lastIdx - c.curIdx)
	c.curIdx = lastIdx
	return c.batch
}

func (c *chunkingBatchSource) reset() {
	c.curIdx = 0
}

// joinTestCase is a helper struct shared by the hash and merge join unit
// tests. Not all fields have to be filled in, but init() method *must* be
// called.
type joinTestCase struct {
	description           string
	joinType              sqlbase.JoinType
	leftTuples            []tuple
	leftTypes             []coltypes.T
	leftOutCols           []uint32
	leftEqCols            []uint32
	leftDirections        []execinfrapb.Ordering_Column_Direction
	rightTuples           []tuple
	rightTypes            []coltypes.T
	rightOutCols          []uint32
	rightEqCols           []uint32
	rightDirections       []execinfrapb.Ordering_Column_Direction
	leftEqColsAreKey      bool
	rightEqColsAreKey     bool
	expected              []tuple
	outputBatchSize       int
	skipAllNullsInjection bool
	onExpr                execinfrapb.Expression
}

func (tc *joinTestCase) init() {
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = coldata.BatchSize()
	}

	if len(tc.leftDirections) == 0 {
		tc.leftDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.leftTypes))
		for i := range tc.leftDirections {
			tc.leftDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}

	if len(tc.rightDirections) == 0 {
		tc.rightDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.rightTypes))
		for i := range tc.rightDirections {
			tc.rightDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}
}

type sortTestCase struct {
	description string
	tuples      tuples
	expected    tuples
	logTypes    []types.T
	ordCols     []execinfrapb.Ordering_Column
	matchLen    int
	k           uint16
}
