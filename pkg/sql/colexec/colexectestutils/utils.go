// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectestutils

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
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tuple represents a row with any-type columns.
type Tuple []interface{}

func (t Tuple) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range t {
		if i != 0 {
			sb.WriteString(", ")
		}
		if d, ok := t[i].(apd.Decimal); ok {
			sb.WriteString(d.String())
		} else if d, ok := t[i].(*apd.Decimal); ok {
			sb.WriteString(d.String())
		} else if d, ok := t[i].([]byte); ok {
			sb.WriteString(string(d))
		} else if d, ok := t[i].(json.JSON); ok {
			sb.WriteString(d.String())
		} else {
			sb.WriteString(fmt.Sprintf("%v", t[i]))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (t Tuple) less(other Tuple, evalCtx *tree.EvalContext, tupleFromOtherSet Tuple) bool {
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
		// Check whether we have datum-backed values.
		if d1, ok := t[i].(tree.Datum); ok {
			d2 := other[i].(tree.Datum)
			cmp := d1.Compare(evalCtx, d2)
			if cmp == 0 {
				continue
			}
			return cmp < 0
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

		// json.JSON are not comparable.
		if lhsVal.Type().Implements(reflect.TypeOf((*json.JSON)(nil)).Elem()) && lhsVal.CanInterface() {
			lhsJSON := lhsVal.Interface().(json.JSON)
			rhsJSON := rhsVal.Interface().(json.JSON)
			cmp, err := lhsJSON.Compare(rhsJSON)
			if err != nil {
				colexecerror.InternalError(errors.NewAssertionErrorWithWrappedErrf(err, "failed json compare"))
			}
			if cmp == 0 {
				continue
			} else if cmp == -1 {
				return true
			} else {
				return false
			}
		}

		// types.Bytes is represented as []uint8.
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

		if lhsVal.Type().Name() == "Duration" {
			lhsDuration := lhsVal.Interface().(duration.Duration)
			rhsDuration := rhsVal.Interface().(duration.Duration)
			cmp := lhsDuration.Compare(rhsDuration)
			if cmp == 0 {
				continue
			} else if cmp == -1 {
				return true
			} else {
				return false
			}
		}

		if lhsVal.Type().Name() == "Time" {
			lhsTime := lhsVal.Interface().(time.Time)
			rhsTime := rhsVal.Interface().(time.Time)
			if lhsTime.Equal(rhsTime) {
				continue
			} else if lhsTime.Before(rhsTime) {
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
			return !lhsVal.Bool() && rhsVal.Bool()
		case "string":
			lString, rString := lhsVal.String(), rhsVal.String()
			if tupleFromOtherSet != nil && len(tupleFromOtherSet) > i {
				if d, ok := tupleFromOtherSet[i].(tree.Datum); ok {
					// The tuple from the other set has a datum value, so we
					// will convert the string to datum. See the comment on
					// tuples.sort for more details.
					d1 := stringToDatum(lString, d.ResolvedType(), evalCtx)
					d2 := stringToDatum(rString, d.ResolvedType(), evalCtx)
					cmp := d1.Compare(evalCtx, d2)
					if cmp == 0 {
						continue
					}
					return cmp < 0
				}
			}
			return lString < rString
		default:
			colexecerror.InternalError(errors.AssertionFailedf("Unhandled comparison type: %s", typ))
		}
	}
	return false
}

func (t Tuple) clone() Tuple {
	b := make(Tuple, len(t))
	for i := range b {
		b[i] = t[i]
	}

	return b
}

// Tuples represents a table with any-type columns.
type Tuples []Tuple

// Clone returns a deep copy of t.
func (t Tuples) Clone() Tuples {
	b := make(Tuples, len(t))
	for i := range b {
		b[i] = t[i].clone()
	}
	return b
}

func (t Tuples) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range t {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(t[i].String())
	}
	sb.WriteString("]")
	return sb.String()
}

// sort returns a copy of sorted tuples. tupleFromOtherSet is any tuple that
// comes from other tuples and is used to determine the desired types.
//
// Currently, this function is only used in order to speed up the comparison of
// the expected tuple set with the actual one, and it is possible that we have
// tree.Datum in the latter but strings in the former. In order to use the same
// ordering when sorting the strings, we need to peek into the actual tuple to
// determine whether we want to convert the string to datum before comparison.
func (t Tuples) sort(evalCtx *tree.EvalContext, tupleFromOtherSet Tuple) Tuples {
	b := make(Tuples, len(t))
	for i := range b {
		b[i] = make(Tuple, len(t[i]))
		copy(b[i], t[i])
	}
	sort.SliceStable(b, func(i, j int) bool {
		lhs := b[i]
		rhs := b[j]
		return lhs.less(rhs, evalCtx, tupleFromOtherSet)
	})
	return b
}

// VerifierType determines how the expected and the actual tuples should be
// compared.
type VerifierType int

const (
	// OrderedVerifier compares the input and output tuples, returning an error
	// if they're not identical.
	OrderedVerifier VerifierType = iota
	// UnorderedVerifier compares the input and output tuples as sets, returning
	// an error if they aren't equal by set comparison (irrespective of order).
	UnorderedVerifier
)

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

// TestRunner is the signature of RunTestsWithTyps that can be used to
// substitute it with RunTestsWithoutAllNullsInjection when applicable.
type TestRunner func(*testing.T, *colmem.Allocator, []Tuples, [][]*types.T, Tuples, VerifierType, func([]colexecop.Operator) (colexecop.Operator, error))

// RunTests is a helper that automatically runs your tests with varied batch
// sizes and with and without a random selection vector.
// tups is the sets of input tuples.
// expected is the set of output tuples.
// constructor is a function that takes a list of input Operators and returns
// the operator to test, or an error.
func RunTests(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	expected Tuples,
	verifier VerifierType,
	constructor func(inputs []colexecop.Operator) (colexecop.Operator, error),
) {
	RunTestsWithTyps(t, allocator, tups, nil /* typs */, expected, verifier, constructor)
}

// RunTestsWithTyps is the same as RunTests with an ability to specify the
// types of the input tuples.
// - typs is the type schema of the input tuples. Note that this is a multi-
//   dimensional slice which allows for specifying different schemas for each
//   of the inputs.
func RunTestsWithTyps(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	typs [][]*types.T,
	expected Tuples,
	verifier VerifierType,
	constructor func(inputs []colexecop.Operator) (colexecop.Operator, error),
) {
	RunTestsWithoutAllNullsInjection(t, allocator, tups, typs, expected, verifier, constructor)

	{
		ctx := context.Background()
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(ctx)
		log.Info(ctx, "allNullsInjection")
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
		opConstructor := func(injectAllNulls bool) colexecop.Operator {
			inputSources := make([]colexecop.Operator, len(tups))
			var inputTypes []*types.T
			for i, tup := range tups {
				if typs != nil {
					inputTypes = typs[i]
				}
				input := NewOpTestInput(allocator, 1 /* batchSize */, tup, inputTypes).(*opTestInput)
				input.injectAllNulls = injectAllNulls
				inputSources[i] = input
			}
			op, err := constructor(inputSources)
			if err != nil {
				t.Fatal(err)
			}
			op.Init(ctx)
			return op
		}
		originalOp := opConstructor(false /* injectAllNulls */)
		opWithNulls := opConstructor(true /* injectAllNulls */)
		foundDifference := false
		for {
			originalBatch := originalOp.Next()
			batchWithNulls := opWithNulls.Next()
			if originalBatch.Length() != batchWithNulls.Length() {
				foundDifference = true
				break
			}
			if originalBatch.Length() == 0 {
				break
			}
			var originalTuples, tuplesWithNulls Tuples
			for i := 0; i < originalBatch.Length(); i++ {
				// We checked that the batches have the same length.
				originalTuples = append(originalTuples, GetTupleFromBatch(originalBatch, i))
				tuplesWithNulls = append(tuplesWithNulls, GetTupleFromBatch(batchWithNulls, i))
			}
			if err := AssertTuplesSetsEqual(originalTuples, tuplesWithNulls, evalCtx); err != nil {
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
		closeIfCloser(t, originalOp)
		closeIfCloser(t, opWithNulls)
	}
}

// closeIfCloser is a testing utility function that checks whether op is a
// colexecop.Closer and closes it if so.
//
// RunTests harness needs to do that once it is done with op. In non-test
// setting, the closing happens at the end of the query execution.
func closeIfCloser(t *testing.T, op colexecop.Operator) {
	if c, ok := op.(colexecop.Closer); ok {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// isOperatorChainResettable traverses the whole operator tree rooted at op and
// returns true if all nodes are resetters.
func isOperatorChainResettable(op execinfra.OpNode) bool {
	if _, resettable := op.(colexecop.ResettableOperator); !resettable {
		return false
	}
	for i := 0; i < op.ChildCount(true /* verbose */); i++ {
		if !isOperatorChainResettable(op.Child(i, true /* verbose */)) {
			return false
		}
	}
	return true
}

// RunTestsWithoutAllNullsInjection is the same as RunTests, but it skips the
// all nulls injection test. Use this only when the all nulls injection should
// not change the output of the operator under testing.
// NOTE: please leave a justification why you're using this variant of
// RunTests.
func RunTestsWithoutAllNullsInjection(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	typs [][]*types.T,
	expected Tuples,
	verifier VerifierType,
	constructor func(inputs []colexecop.Operator) (colexecop.Operator, error),
) {
	RunTestsWithoutAllNullsInjectionWithErrorHandler(
		t, allocator, tups, typs, expected, verifier, constructor, func(err error) { t.Fatal(err) },
	)
}

// RunTestsWithoutAllNullsInjectionWithErrorHandler is the same as
// RunTestsWithoutAllNullsInjection but takes in an additional argument function
// that handles any errors encountered during the test run (e.g. if the panic is
// expected to occur during the execution, it will be caught, and the error
// handler could verify that the expected error was, in fact, encountered).
func RunTestsWithoutAllNullsInjectionWithErrorHandler(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	typs [][]*types.T,
	expected Tuples,
	verifier VerifierType,
	constructor func(inputs []colexecop.Operator) (colexecop.Operator, error),
	errorHandler func(error),
) {
	ctx := context.Background()
	verifyFn := (*OpTestOutput).VerifyAnyOrder
	skipVerifySelAndNullsResets := true
	if verifier == OrderedVerifier {
		verifyFn = (*OpTestOutput).Verify
		// Note that this test makes sense only if we expect tuples to be
		// returned in the same order (otherwise the second batch's selection
		// vector or nulls info can be different and that is totally valid).
		skipVerifySelAndNullsResets = false
	}
	RunTestsWithFn(t, allocator, tups, typs, func(t *testing.T, inputs []colexecop.Operator) {
		op, err := constructor(inputs)
		if err != nil {
			t.Fatal(err)
		}
		out := NewOpTestOutput(op, expected)
		if err := verifyFn(out); err != nil {
			errorHandler(err)
		}
		if isOperatorChainResettable(op) {
			log.Info(ctx, "reusing after reset")
			out.Reset(ctx)
			if err := verifyFn(out); err != nil {
				errorHandler(err)
			}
		}
		closeIfCloser(t, op)
	})

	if !skipVerifySelAndNullsResets {
		log.Info(ctx, "verifySelAndNullResets")
		// This test ensures that operators that "own their own batches", such as
		// any operator that has to reshape its output, are not affected by
		// downstream modification of batches.
		// We run the main loop twice: once to determine what the operator would
		// output on its second Next call (we need the first call to Next to get a
		// reference to a batch to modify), and a second time to modify the batch
		// and verify that this does not change the operator output.
		var (
			secondBatchHasSelection, secondBatchHasNulls bool
			inputTypes                                   []*types.T
		)
		for round := 0; round < 2; round++ {
			inputSources := make([]colexecop.Operator, len(tups))
			for i, tup := range tups {
				if typs != nil {
					inputTypes = typs[i]
				}
				inputSources[i] = NewOpTestInput(allocator, 1 /* batchSize */, tup, inputTypes)
			}
			op, err := constructor(inputSources)
			if err != nil {
				t.Fatal(err)
			}
			// We might short-circuit, so defer the closing of the operator.
			defer closeIfCloser(t, op)
			op.Init(ctx)
			// NOTE: this test makes sense only if the operator returns two
			// non-zero length batches (if not, we short-circuit the test since
			// the operator doesn't have to restore anything on a zero-length
			// batch).
			lessThanTwoBatches := true
			if err = colexecerror.CatchVectorizedRuntimeError(func() {
				b := op.Next()
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
				b = op.Next()
				if b.Length() == 0 {
					return
				}
				lessThanTwoBatches = false
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
			}); err != nil {
				errorHandler(err)
			}
			if lessThanTwoBatches {
				return
			}
		}
	}

	{
		log.Info(ctx, "randomNullsInjection")
		// This test randomly injects nulls in the input tuples and ensures that
		// the operator doesn't panic.
		inputSources := make([]colexecop.Operator, len(tups))
		var inputTypes []*types.T
		for i, tup := range tups {
			if typs != nil {
				inputTypes = typs[i]
			}
			input := NewOpTestInput(allocator, 1 /* batchSize */, tup, inputTypes).(*opTestInput)
			input.injectRandomNulls = true
			inputSources[i] = input
		}
		op, err := constructor(inputSources)
		if err != nil {
			t.Fatal(err)
		}
		if err = colexecerror.CatchVectorizedRuntimeError(func() {
			op.Init(ctx)
			for b := op.Next(); b.Length() > 0; b = op.Next() {
			}
		}); err != nil {
			errorHandler(err)
		}
		closeIfCloser(t, op)
	}
}

// RunTestsWithFn is like RunTests, but the input function is responsible for
// performing any required tests. Please note that RunTestsWithFn is a worse
// testing facility than RunTests, because it can't get a handle on the operator
// under test and therefore can't perform as many extra checks. You should
// always prefer using RunTests over RunTestsWithFn.
// - tups is the sets of input tuples.
// - typs is the type schema of the input tuples. Note that this is a multi-
//   dimensional slice which allows for specifying different schemas for each
//   of the inputs. This can also be left nil in which case the types will be
//   determined at the runtime looking at the first input tuple, and if the
//   determination doesn't succeed for a value of the tuple (likely because
//   it's a nil), then that column will be assumed by default of type Int64.
// - test is a function that takes a list of input Operators and performs
//   testing with t.
func RunTestsWithFn(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	typs [][]*types.T,
	test func(t *testing.T, inputs []colexecop.Operator),
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
			log.Infof(context.Background(), "batchSize=%d/sel=%t", batchSize, useSel)
			inputSources := make([]colexecop.Operator, len(tups))
			var inputTypes []*types.T
			if useSel {
				for i, tup := range tups {
					if typs != nil {
						inputTypes = typs[i]
					}
					rng, _ := randutil.NewPseudoRand()
					inputSources[i] = newOpTestSelInput(allocator, rng, batchSize, tup, inputTypes)
				}
			} else {
				for i, tup := range tups {
					if typs != nil {
						inputTypes = typs[i]
					}
					inputSources[i] = NewOpTestInput(allocator, batchSize, tup, inputTypes)
				}
			}
			test(t, inputSources)
		}
	}
}

// RunTestsWithFixedSel is a helper that (with a given fixed selection vector)
// automatically runs your tests with varied batch sizes. Provide a test
// function that takes a list of input Operators, which will give back the
// tuples provided in batches.
func RunTestsWithFixedSel(
	t *testing.T,
	allocator *colmem.Allocator,
	tups []Tuples,
	typs []*types.T,
	sel []int,
	test func(t *testing.T, inputs []colexecop.Operator),
) {
	for _, batchSize := range []int{1, 2, 3, 16, 1024} {
		log.Infof(context.Background(), "batchSize=%d/fixedSel", batchSize)
		inputSources := make([]colexecop.Operator, len(tups))
		for i, tup := range tups {
			inputSources[i] = NewOpFixedSelTestInput(allocator, sel, batchSize, tup, typs)
		}
		test(t, inputSources)
	}
}

func stringToDatum(val string, typ *types.T, evalCtx *tree.EvalContext) tree.Datum {
	expr, err := parser.ParseExpr(val)
	if err != nil {
		colexecerror.InternalError(err)
	}
	semaCtx := tree.MakeSemaContext()
	typedExpr, err := tree.TypeCheck(context.Background(), expr, &semaCtx, typ)
	if err != nil {
		colexecerror.InternalError(err)
	}
	d, err := typedExpr.Eval(evalCtx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	return d
}

// setColVal is a test helper function to set the given value at the equivalent
// col[idx]. This function is slow due to reflection.
func setColVal(vec coldata.Vec, idx int, val interface{}, evalCtx *tree.EvalContext) {
	switch vec.CanonicalTypeFamily() {
	case types.BytesFamily:
		var (
			bytesVal []byte
			ok       bool
		)
		bytesVal, ok = val.([]byte)
		if !ok {
			bytesVal = []byte(val.(string))
		}
		vec.Bytes().Set(idx, bytesVal)
	case types.DecimalFamily:
		// setColVal is used in multiple places, therefore val can be either a float
		// or apd.Decimal.
		if decimalVal, ok := val.(apd.Decimal); ok {
			vec.Decimal()[idx].Set(&decimalVal)
		} else {
			floatVal := val.(float64)
			decimalVal, _, err := apd.NewFromString(fmt.Sprintf("%f", floatVal))
			if err != nil {
				colexecerror.InternalError(
					errors.AssertionFailedf("unable to set decimal %f: %v", floatVal, err))
			}
			// .Set is used here instead of assignment to ensure the pointer address
			// of the underlying storage for apd.Decimal remains the same. This can
			// cause the code that does not properly use execgen package to fail.
			vec.Decimal()[idx].Set(decimalVal)
		}
	case types.JsonFamily:
		var j json.JSON
		if j2, ok := val.(json.JSON); ok {
			j = j2
		} else {
			s := val.(string)
			var err error
			j, err = json.ParseJSON(s)
			if err != nil {
				colexecerror.InternalError(
					errors.AssertionFailedf("unable to set json %s: %v", s, err))
			}
		}
		vec.JSON().Set(idx, j)
	case typeconv.DatumVecCanonicalTypeFamily:
		switch v := val.(type) {
		case *coldataext.Datum:
			vec.Datum().Set(idx, v)
		case tree.Datum:
			vec.Datum().Set(idx, v)
		case string:
			vec.Datum().Set(idx, stringToDatum(v, vec.Type(), evalCtx))
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected type %T of datum-backed value: %v", v, v))
		}
	default:
		reflect.ValueOf(vec.Col()).Index(idx).Set(reflect.ValueOf(val).Convert(reflect.TypeOf(vec.Col()).Elem()))
	}
}

// extrapolateTypesFromTuples determines the type schema based on the input
// tuples.
func extrapolateTypesFromTuples(tups Tuples) []*types.T {
	typs := make([]*types.T, len(tups[0]))
	for i := range typs {
		// Default type for test cases is Int64 in case the entire column is
		// null and the type is indeterminate.
		typs[i] = types.Int
		for _, tup := range tups {
			if tup[i] != nil {
				typs[i] = typeconv.UnsafeFromGoType(tup[i])
				break
			}
		}
	}
	return typs
}

// opTestInput is an Operator that columnarizes test input in the form of
// tuples of arbitrary Go types. It's meant to be used in Operator unit tests
// in conjunction with OpTestOutput like the following:
//
// inputTuples := tuples{
//   {1,2,3.3,true},
//   {5,6,7.0,false},
// }
// tupleSource := NewOpTestInput(inputTuples, types.Bool)
// opUnderTest := newFooOp(tupleSource, ...)
// output := NewOpTestOutput(opUnderTest, expectedOutputTuples)
// if err := output.Verify(); err != nil {
//     t.Fatal(err)
// }
type opTestInput struct {
	colexecop.ZeroInputNode

	allocator *colmem.Allocator

	typs []*types.T

	batchSize int
	tuples    Tuples
	// initialTuples are tuples passed in into the constructor, and we keep the
	// reference to them in order to be able to reset the operator.
	initialTuples Tuples
	batch         coldata.Batch
	useSel        bool
	rng           *rand.Rand
	selection     []int
	evalCtx       *tree.EvalContext

	// injectAllNulls determines whether opTestInput will replace all values in
	// the input tuples with nulls.
	injectAllNulls bool

	// injectRandomNulls determines whether opTestInput will randomly replace
	// each value in the input tuples with a null.
	injectRandomNulls bool
}

var _ colexecop.ResettableOperator = &opTestInput{}

// NewOpTestInput returns a new opTestInput with the given input tuples and the
// given type schema. If typs is nil, the input tuples are translated into
// types automatically, using simple rules (e.g. integers always become Int64).
func NewOpTestInput(
	allocator *colmem.Allocator, batchSize int, tuples Tuples, typs []*types.T,
) colexecop.Operator {
	ret := &opTestInput{
		allocator:     allocator,
		batchSize:     batchSize,
		tuples:        tuples,
		initialTuples: tuples,
		typs:          typs,
		evalCtx:       tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
	return ret
}

func newOpTestSelInput(
	allocator *colmem.Allocator, rng *rand.Rand, batchSize int, tuples Tuples, typs []*types.T,
) *opTestInput {
	ret := &opTestInput{
		allocator:     allocator,
		useSel:        true,
		rng:           rng,
		batchSize:     batchSize,
		tuples:        tuples,
		initialTuples: tuples,
		typs:          typs,
		evalCtx:       tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
	return ret
}

func (s *opTestInput) Init(context.Context) {
	if s.typs == nil {
		if len(s.tuples) == 0 {
			colexecerror.InternalError(errors.AssertionFailedf("empty tuple source with no specified types"))
		}
		s.typs = extrapolateTypesFromTuples(s.tuples)
	}
	s.batch = s.allocator.NewMemBatchWithMaxCapacity(s.typs)

	s.selection = make([]int, coldata.BatchSize())
	for i := range s.selection {
		s.selection[i] = i
	}
}

func (s *opTestInput) Next() coldata.Batch {
	if len(s.tuples) == 0 {
		return coldata.ZeroBatch
	}
	s.batch.ResetInternalBatch()
	batchSize := s.batchSize
	if len(s.tuples) < batchSize {
		batchSize = len(s.tuples)
	}
	tups := s.tuples[:batchSize]
	s.tuples = s.tuples[batchSize:]

	tupleLen := len(tups[0])
	for i := range tups {
		if len(tups[i]) != tupleLen {
			colexecerror.InternalError(errors.AssertionFailedf("mismatched tuple lens: found %+v expected %d vals",
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
		if colVec.CanonicalTypeFamily() != types.UnknownFamily {
			colVec.Nulls().UnsetNulls()
		}
	}

	rng, _ := randutil.NewPseudoRand()

	for i := range s.typs {
		vec := s.batch.ColVec(i)
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
					canonicalTypeFamily := vec.CanonicalTypeFamily()
					switch canonicalTypeFamily {
					case types.DecimalFamily:
						d := apd.Decimal{}
						_, err := d.SetFloat64(rng.Float64())
						if err != nil {
							colexecerror.InternalError(errors.AssertionFailedf("%v", err))
						}
						col.Index(outputIdx).Set(reflect.ValueOf(d))
					case types.BytesFamily:
						newBytes := make([]byte, rng.Intn(16)+1)
						rng.Read(newBytes)
						setColVal(vec, outputIdx, newBytes, s.evalCtx)
					case types.IntervalFamily:
						setColVal(vec, outputIdx, duration.MakeDuration(rng.Int63(), rng.Int63(), rng.Int63()), s.evalCtx)
					case types.JsonFamily:
						j, err := json.Random(20, rng)
						if err != nil {
							colexecerror.InternalError(errors.AssertionFailedf("%v", err))
						}
						setColVal(vec, outputIdx, j, s.evalCtx)
					case typeconv.DatumVecCanonicalTypeFamily:
						switch vec.Type().Family() {
						case types.CollatedStringFamily:
							collatedStringType := types.MakeCollatedString(types.String, *randgen.RandCollationLocale(rng))
							randomBytes := make([]byte, rng.Intn(16)+1)
							rng.Read(randomBytes)
							d, err := tree.NewDCollatedString(string(randomBytes), collatedStringType.Locale(), &tree.CollationEnvironment{})
							if err != nil {
								colexecerror.InternalError(err)
							}
							setColVal(vec, outputIdx, d, s.evalCtx)
						case types.TimeTZFamily:
							setColVal(vec, outputIdx, tree.NewDTimeTZFromOffset(timeofday.FromInt(rng.Int63()), rng.Int31()), s.evalCtx)
						case types.TupleFamily:
							setColVal(vec, outputIdx, stringToDatum("(NULL)", vec.Type(), s.evalCtx), s.evalCtx)
						default:
							colexecerror.InternalError(errors.AssertionFailedf("unexpected datum-backed type: %s", vec.Type()))
						}
					default:
						if val, ok := quick.Value(reflect.TypeOf(vec.Col()).Elem(), rng); ok {
							setColVal(vec, outputIdx, val.Interface(), s.evalCtx)
						} else {
							colexecerror.InternalError(errors.AssertionFailedf("could not generate a random value of type %s", vec.Type()))
						}
					}
				}
			} else {
				setColVal(vec, outputIdx, tups[j][i], s.evalCtx)
			}
		}
	}

	s.batch.SetLength(batchSize)
	return s.batch
}

func (s *opTestInput) Reset(context.Context) {
	s.tuples = s.initialTuples
}

type opFixedSelTestInput struct {
	colexecop.ZeroInputNode

	allocator *colmem.Allocator

	typs []*types.T

	batchSize int
	tuples    Tuples
	batch     coldata.Batch
	sel       []int
	evalCtx   *tree.EvalContext
	// idx is the index of the tuple to be emitted next. We need to maintain it
	// in case the provided selection vector or provided tuples (if sel is nil)
	// is longer than requested batch size.
	idx int
}

var _ colexecop.ResettableOperator = &opFixedSelTestInput{}

// NewOpFixedSelTestInput returns a new opFixedSelTestInput with the given
// input tuples and selection vector. The input tuples are translated into
// types automatically, using simple rules (e.g. integers always become Int64).
func NewOpFixedSelTestInput(
	allocator *colmem.Allocator, sel []int, batchSize int, tuples Tuples, typs []*types.T,
) colexecop.Operator {
	ret := &opFixedSelTestInput{
		allocator: allocator,
		batchSize: batchSize,
		sel:       sel,
		tuples:    tuples,
		typs:      typs,
		evalCtx:   tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
	return ret
}

func (s *opFixedSelTestInput) Init(context.Context) {
	if s.typs == nil {
		if len(s.tuples) == 0 {
			colexecerror.InternalError(errors.AssertionFailedf("empty tuple source with no specified types"))
		}
		s.typs = extrapolateTypesFromTuples(s.tuples)
	}

	s.batch = s.allocator.NewMemBatchWithMaxCapacity(s.typs)
	tupleLen := len(s.tuples[0])
	for _, i := range s.sel {
		if len(s.tuples[i]) != tupleLen {
			colexecerror.InternalError(errors.AssertionFailedf("mismatched tuple lens: found %+v expected %d vals",
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
					setColVal(vec, j, s.tuples[j][i], s.evalCtx)
				}
			}
		}
	}

}

func (s *opFixedSelTestInput) Next() coldata.Batch {
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
					setColVal(vec, j, s.tuples[s.idx+j][i], s.evalCtx)
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

func (s *opFixedSelTestInput) Reset(context.Context) {
	s.idx = 0
}

// OpTestOutput is a test verification struct that ensures its input batches
// match some expected output tuples.
type OpTestOutput struct {
	colexecop.OneInputNode
	expected Tuples
	evalCtx  *tree.EvalContext

	curIdx int
	batch  coldata.Batch
}

// NewOpTestOutput returns a new OpTestOutput, initialized with the given input
// to verify that the output is exactly equal to the expected tuples.
func NewOpTestOutput(input colexecop.Operator, expected Tuples) *OpTestOutput {
	input.Init(context.Background())

	return &OpTestOutput{
		OneInputNode: colexecop.NewOneInputNode(input),
		expected:     expected,
		evalCtx:      tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
}

// GetTupleFromBatch is a helper function that extracts a tuple at index
// tupleIdx from batch.
func GetTupleFromBatch(batch coldata.Batch, tupleIdx int) Tuple {
	ret := make(Tuple, batch.Width())
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
			family := vec.CanonicalTypeFamily()
			if colBytes, ok := vec.Col().(*coldata.Bytes); ok {
				val = reflect.ValueOf(append([]byte(nil), colBytes.Get(tupleIdx)...))
			} else if family == types.DecimalFamily {
				colDec := vec.Decimal()
				var newDec apd.Decimal
				newDec.Set(&colDec[tupleIdx])
				val = reflect.ValueOf(newDec)
			} else if family == types.JsonFamily {
				colJSON := vec.JSON()
				newJSON := colJSON.Get(tupleIdx)
				b, err := json.EncodeJSON(nil, newJSON)
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				_, j, err := json.DecodeJSON(b)
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				val = reflect.ValueOf(j)
			} else if family == typeconv.DatumVecCanonicalTypeFamily {
				val = reflect.ValueOf(vec.Datum().Get(tupleIdx).(*coldataext.Datum).Datum)
			} else {
				val = reflect.ValueOf(vec.Col()).Index(tupleIdx)
			}
			out.Index(colIdx).Set(val)
		}
	}
	return ret
}

func (r *OpTestOutput) next() Tuple {
	if r.batch == nil || r.curIdx >= r.batch.Length() {
		// Get a fresh batch.
		r.batch = r.Input.Next()
		if r.batch.Length() == 0 {
			return nil
		}
		r.curIdx = 0
	}
	ret := GetTupleFromBatch(r.batch, r.curIdx)
	r.curIdx++
	return ret
}

// Reset implements the Resetter interface.
func (r *OpTestOutput) Reset(ctx context.Context) {
	if r, ok := r.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	r.curIdx = 0
	r.batch = nil
}

// Verify ensures that the input to this OpTestOutput produced the same results
// and in the same order as the ones expected in the OpTestOutput's expected
// tuples, using a slow, reflection-based comparison method, returning an error
// if the input isn't equal to the expected.
func (r *OpTestOutput) Verify() error {
	var actual Tuples
	for {
		var tup Tuple
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			tup = r.next()
		}); err != nil {
			return err
		}
		if tup == nil {
			break
		}
		actual = append(actual, tup)
	}
	return assertTuplesOrderedEqual(r.expected, actual, r.evalCtx)
}

// VerifyAnyOrder ensures that the input to this OpTestOutput produced the same
// results but in any order (meaning set comparison behavior is used) as the
// ones expected in the OpTestOutput's expected tuples, using a slow,
// reflection-based comparison method, returning an error if the input isn't
// equal to the expected.
func (r *OpTestOutput) VerifyAnyOrder() error {
	var actual Tuples
	for {
		var tup Tuple
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			tup = r.next()
		}); err != nil {
			return err
		}
		if tup == nil {
			break
		}
		actual = append(actual, tup)
	}
	return AssertTuplesSetsEqual(r.expected, actual, r.evalCtx)
}

// tupleEquals checks that two tuples are equal, using a slow,
// reflection-based method to do the comparison. Reflection is used so that
// values can be compared in a type-agnostic way.
func tupleEquals(expected Tuple, actual Tuple, evalCtx *tree.EvalContext) bool {
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
			// Special case for decimals.
			if d1, ok := actual[i].(apd.Decimal); ok {
				if f2, ok := expected[i].(float64); ok {
					d2, _, err := apd.NewFromString(fmt.Sprintf("%f", f2))
					if err == nil && d1.Cmp(d2) == 0 {
						continue
					} else {
						return false
					}
				}
			}
			// Special case for JSON.
			if j1, ok := actual[i].(json.JSON); ok {
				var j2 json.JSON
				switch t := expected[i].(type) {
				case string:
					var err error
					j2, err = json.ParseJSON(t)
					if err != nil {
						colexecerror.ExpectedError(err)
					}
				case json.JSON:
					j2 = t
				}
				cmp, err := j1.Compare(j2)
				if err != nil {
					colexecerror.ExpectedError(err)
				}
				if cmp == 0 {
					continue
				} else {
					return false
				}
			}
			// Special case for datum-backed types.
			if d1, ok := actual[i].(tree.Datum); ok {
				if d, ok := d1.(*coldataext.Datum); ok {
					d1 = d.Datum
				}
				var d2 tree.Datum
				switch d := expected[i].(type) {
				case *coldataext.Datum:
					d2 = d.Datum
				case tree.Datum:
					d2 = d
				case string:
					d2 = stringToDatum(d, d1.ResolvedType(), evalCtx)
				default:
					return false
				}
				if d1.Compare(evalCtx, d2) == 0 {
					continue
				}
				return false
			}
			// Default case.
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

func makeError(expected Tuples, actual Tuples) error {
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

// AssertTuplesSetsEqual asserts that two sets of tuples are equal.
func AssertTuplesSetsEqual(expected Tuples, actual Tuples, evalCtx *tree.EvalContext) error {
	if len(expected) != len(actual) {
		return makeError(expected, actual)
	}
	var tupleFromOtherSet Tuple
	if len(expected) > 0 {
		tupleFromOtherSet = expected[0]
	}
	actual = actual.sort(evalCtx, tupleFromOtherSet)
	if len(actual) > 0 {
		tupleFromOtherSet = actual[0]
	}
	expected = expected.sort(evalCtx, tupleFromOtherSet)
	return assertTuplesOrderedEqual(expected, actual, evalCtx)
}

// assertTuplesOrderedEqual asserts that two permutations of tuples are equal
// in order.
func assertTuplesOrderedEqual(expected Tuples, actual Tuples, evalCtx *tree.EvalContext) error {
	if len(expected) != len(actual) {
		return errors.Errorf("expected %+v, actual %+v", expected, actual)
	}
	for i := range expected {
		if !tupleEquals(expected[i], actual[i], evalCtx) {
			return makeError(expected, actual)
		}
	}
	return nil
}

// FiniteBatchSource is an Operator that returns the same batch a specified
// number of times.
type FiniteBatchSource struct {
	colexecop.ZeroInputNode

	repeatableBatch *colexecop.RepeatableBatchSource

	usableCount int
}

var _ colexecop.Operator = &FiniteBatchSource{}

// NewFiniteBatchSource returns a new Operator initialized to return its input
// batch a specified number of times.
func NewFiniteBatchSource(
	allocator *colmem.Allocator, batch coldata.Batch, typs []*types.T, usableCount int,
) *FiniteBatchSource {
	return &FiniteBatchSource{
		repeatableBatch: colexecop.NewRepeatableBatchSource(allocator, batch, typs),
		usableCount:     usableCount,
	}
}

// Init implements the Operator interface.
func (f *FiniteBatchSource) Init(ctx context.Context) {
	f.repeatableBatch.Init(ctx)
}

// Next implements the Operator interface.
func (f *FiniteBatchSource) Next() coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		return f.repeatableBatch.Next()
	}
	return coldata.ZeroBatch
}

// Reset resets FiniteBatchSource to return the same batch usableCount number of
// times.
func (f *FiniteBatchSource) Reset(usableCount int) {
	f.usableCount = usableCount
}

// finiteChunksSource is an Operator that returns a batch specified number of
// times. The first matchLen columns of the batch are incremented every time
// (except for the first) the batch is returned to emulate source that is
// already ordered on matchLen columns.
type finiteChunksSource struct {
	colexecop.ZeroInputNode
	repeatableBatch *colexecop.RepeatableBatchSource

	usableCount int
	matchLen    int
	adjustment  []int64
}

var _ colexecop.Operator = &finiteChunksSource{}

// NewFiniteChunksSource returns a new finiteChunksSource.
func NewFiniteChunksSource(
	allocator *colmem.Allocator, batch coldata.Batch, typs []*types.T, usableCount int, matchLen int,
) colexecop.Operator {
	return &finiteChunksSource{
		repeatableBatch: colexecop.NewRepeatableBatchSource(allocator, batch, typs),
		usableCount:     usableCount,
		matchLen:        matchLen,
	}
}

func (f *finiteChunksSource) Init(ctx context.Context) {
	f.repeatableBatch.Init(ctx)
	f.adjustment = make([]int64, f.matchLen)
}

func (f *finiteChunksSource) Next() coldata.Batch {
	if f.usableCount > 0 {
		f.usableCount--
		batch := f.repeatableBatch.Next()
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

// chunkingBatchSource is a batch source that takes unlimited-size columns and
// chunks them into BatchSize()-sized chunks when Nexted.
type chunkingBatchSource struct {
	colexecop.ZeroInputNode
	allocator *colmem.Allocator
	typs      []*types.T
	cols      []coldata.Vec
	len       int

	curIdx int
	batch  coldata.Batch
}

var _ colexecop.ResettableOperator = &chunkingBatchSource{}

// NewChunkingBatchSource returns a new chunkingBatchSource with the given
// column types, columns, and length.
func NewChunkingBatchSource(
	allocator *colmem.Allocator, typs []*types.T, cols []coldata.Vec, len int,
) colexecop.ResettableOperator {
	return &chunkingBatchSource{
		allocator: allocator,
		typs:      typs,
		cols:      cols,
		len:       len,
	}
}

func (c *chunkingBatchSource) Init(context.Context) {
	c.batch = c.allocator.NewMemBatchWithMaxCapacity(c.typs)
	for i := range c.cols {
		c.batch.ColVec(i).SetCol(c.cols[i].Col())
		c.batch.ColVec(i).SetNulls(c.cols[i].Nulls())
	}
}

func (c *chunkingBatchSource) Next() coldata.Batch {
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
	for i := range c.typs {
		// Note that new vectors could be appended to the batch, but we are not
		// responsible for updating those, so we iterate only up to len(c.typs)
		// as per out initialization.
		c.batch.ColVec(i).SetCol(c.cols[i].Window(c.curIdx, lastIdx).Col())
		nullsSlice := c.cols[i].Nulls().Slice(c.curIdx, lastIdx)
		c.batch.ColVec(i).SetNulls(&nullsSlice)
	}
	c.batch.SetLength(lastIdx - c.curIdx)
	c.curIdx = lastIdx
	return c.batch
}

func (c *chunkingBatchSource) Reset(context.Context) {
	c.curIdx = 0
}

// MinBatchSize is the minimum acceptable size of batches for tests in colexec*
// packages.
const MinBatchSize = 3

// GenerateBatchSize generates somewhat random value to set coldata.BatchSize()
// to.
func GenerateBatchSize() int {
	randomizeBatchSize := envutil.EnvOrDefaultBool("COCKROACH_RANDOMIZE_BATCH_SIZE", true)
	if randomizeBatchSize {
		rng, _ := randutil.NewPseudoRand()
		// sizesToChooseFrom specifies some predetermined and one random sizes
		// that we will choose from. Such distribution is chosen due to the
		// fact that most of our unit tests don't have a lot of data, so in
		// order to exercise the multi-batch behavior we favor really small
		// batch sizes. On the other hand, we also want to occasionally
		// exercise that we handle batch sizes larger than default one
		// correctly.
		var sizesToChooseFrom = []int{
			MinBatchSize,
			MinBatchSize + 1,
			MinBatchSize + 2,
			coldata.BatchSize(),
			MinBatchSize + rng.Intn(coldata.MaxBatchSize-MinBatchSize),
		}
		return sizesToChooseFrom[rng.Intn(len(sizesToChooseFrom))]
	}
	return coldata.BatchSize()
}

// CallbackMetadataSource is a utility struct that implements the
// colexecop.MetadataSource interface by calling a provided callback.
type CallbackMetadataSource struct {
	DrainMetaCb func() []execinfrapb.ProducerMetadata
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s CallbackMetadataSource) DrainMeta() []execinfrapb.ProducerMetadata {
	return s.DrainMetaCb()
}
