// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"reflect"
	"runtime"
	"testing"
)

var testOperator Operator = 1111

func TestScalarOperatorTransmitsNulls(t *testing.T) {
	scalarOperators := [30]Operator{BitandOp, BitorOp, BitxorOp, PlusOp, MinusOp, MultOp, DivOp, FloorDivOp,
		ModOp, PowOp, EqOp, NeOp, LtOp, GtOp, LeOp, GeOp, LikeOp, NotLikeOp, ILikeOp,
		NotILikeOp, SimilarToOp, NotSimilarToOp, RegMatchOp, NotRegMatchOp, RegIMatchOp,
		NotRegIMatchOp, ConstOp, BBoxCoversOp, BBoxIntersectsOp, testOperator}
	for _, op := range scalarOperators {
		b := ScalarOperatorTransmitsNulls(op)
		if b == false && op != testOperator {
			t.Errorf("testOperator should not has passed")
		}
		if op == testOperator && b == true {
			t.Error("testOperator was supposed to return false")
		}
	}
	t.Log("Test completd successfully")

}

func TestAggregateIsNeverNull(t *testing.T) {
	scalarOperators := [4]Operator{CountOp, CountRowsOp, RegressionCountOp, testOperator}
	for _, op := range scalarOperators {
		b := AggregateIsNeverNull(op)
		if b == false && op != testOperator {
			t.Errorf("testOperator should not has passed")
		}
		if op == testOperator && b == true {
			t.Error("testOperator was supposed to return false")
		}
	}
	t.Log("Test completd successfully")

}

func TestBoolOperatorRequiresNotNullArgs(t *testing.T) {
	scalarOperators := [19]Operator{EqOp, LtOp, LeOp, GtOp, GeOp, NeOp,
		LikeOp, NotLikeOp, ILikeOp, NotILikeOp, SimilarToOp, NotSimilarToOp,
		RegMatchOp, NotRegMatchOp, RegIMatchOp, NotRegIMatchOp, BBoxCoversOp,
		BBoxIntersectsOp, testOperator}
	for _, op := range scalarOperators {
		b := BoolOperatorRequiresNotNullArgs(op)
		if b == false && op != testOperator {
			t.Errorf("%v should have passed", op)
		}
		if op == testOperator && b == true {
			t.Error("testOperator was supposed to return false")
		}
	}
	t.Log("Test completd successfully")

}

func checkIfPainWasRaised(op Operator, panicBool *bool, invokedFunction func(op Operator) bool) bool {
	defer func() {
		if r := recover(); r != nil {
			*panicBool = true
		}
	}()
	return invokedFunction(op)
}

func TestAggregateIgnoresDuplicates(t *testing.T) {
	panicBool := false
	checkIfPainWasRaised(testOperator, &panicBool, AggregateIgnoresDuplicates)
	if !panicBool {
		t.Errorf("Painc should have been raised for %v", testOperator)
	}

	scalarOperators := [12]Operator{AnyNotNullAggOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConstAggOp, ConstNotNullAggOp, FirstAggOp, MaxOp, MinOp, STExtentOp, STUnionOp}

	for _, op := range scalarOperators {
		panicBool := false
		if !checkIfPainWasRaised(op, &panicBool, AggregateIgnoresDuplicates) {
			t.Errorf("%v should have passed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

	scalarOperators1 := [34]Operator{ArrayAggOp, AvgOp, ConcatAggOp, CountOp, CorrOp, CountRowsOp, SumIntOp,
		SumOp, SqrDiffOp, VarianceOp, StdDevOp, XorAggOp, JsonAggOp, JsonbAggOp,
		StringAggOp, PercentileDiscOp, PercentileContOp, StdDevPopOp, STMakeLineOp,
		VarPopOp, JsonObjectAggOp, JsonbObjectAggOp, STCollectOp, CovarPopOp,
		CovarSampOp, RegressionAvgXOp, RegressionAvgYOp, RegressionInterceptOp,
		RegressionR2Op, RegressionSlopeOp, RegressionSXXOp, RegressionSXYOp,
		RegressionSYYOp, RegressionCountOp}

	for _, op := range scalarOperators1 {
		panicBool := false
		if checkIfPainWasRaised(op, &panicBool, AggregateIgnoresDuplicates) {
			t.Errorf("%v should have failed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

}

func TestAggregateIsNullOnEmpty(t *testing.T) {
	panicBool := false
	checkIfPainWasRaised(testOperator, &panicBool, AggregateIsNullOnEmpty)
	if !panicBool {
		t.Errorf("Painc should have been raised for %v", testOperator)
	}

	scalarOperators := [43]Operator{AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp,
		BitOrAggOp, BoolAndOp, BoolOrOp, ConcatAggOp, ConstAggOp,
		ConstNotNullAggOp, CorrOp, FirstAggOp, JsonAggOp, JsonbAggOp,
		MaxOp, MinOp, SqrDiffOp, StdDevOp, STMakeLineOp, StringAggOp, SumOp, SumIntOp,
		VarianceOp, XorAggOp, PercentileDiscOp, PercentileContOp,
		JsonObjectAggOp, JsonbObjectAggOp, StdDevPopOp, STCollectOp, STExtentOp, STUnionOp,
		VarPopOp, CovarPopOp, CovarSampOp, RegressionAvgXOp, RegressionAvgYOp,
		RegressionInterceptOp, RegressionR2Op, RegressionSlopeOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp}

	for _, op := range scalarOperators {
		panicBool := false
		if !checkIfPainWasRaised(op, &panicBool, AggregateIsNullOnEmpty) {
			t.Errorf("%v should have passed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

	scalarOperators1 := [3]Operator{CountOp, CountRowsOp, RegressionCountOp}

	for _, op := range scalarOperators1 {
		panicBool := false
		if checkIfPainWasRaised(op, &panicBool, AggregateIsNullOnEmpty) {
			t.Errorf("%v should have failed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

}

func TestAggregateIsNeverNullOnNonNullInput(t *testing.T) {
	panicBool := false
	checkIfPainWasRaised(testOperator, &panicBool, AggregateIsNeverNullOnNonNullInput)
	if !panicBool {
		t.Errorf("Painc should have been raised for %v", testOperator)
	}

	scalarOperators := [39]Operator{AnyNotNullAggOp, ArrayAggOp, AvgOp, BitAndAggOp,
		BitOrAggOp, BoolAndOp, BoolOrOp, ConcatAggOp, ConstAggOp,
		ConstNotNullAggOp, CountOp, CountRowsOp, FirstAggOp,
		JsonAggOp, JsonbAggOp, MaxOp, MinOp, SqrDiffOp, STMakeLineOp,
		StringAggOp, SumOp, SumIntOp, XorAggOp, PercentileDiscOp, PercentileContOp,
		JsonObjectAggOp, JsonbObjectAggOp, StdDevPopOp, STCollectOp, STExtentOp, STUnionOp,
		VarPopOp, CovarPopOp, RegressionAvgXOp, RegressionAvgYOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp, RegressionCountOp}

	for _, op := range scalarOperators {
		panicBool := false
		if !checkIfPainWasRaised(op, &panicBool, AggregateIsNeverNullOnNonNullInput) {
			t.Errorf("%v should have passed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

	scalarOperators1 := [7]Operator{VarianceOp, StdDevOp, CorrOp, CovarSampOp, RegressionInterceptOp,
		RegressionR2Op, RegressionSlopeOp}

	for _, op := range scalarOperators1 {
		panicBool := false
		if checkIfPainWasRaised(op, &panicBool, AggregateIsNeverNullOnNonNullInput) {
			t.Errorf("%v should have failed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

}

func TestAggregateIgnoresNulls(t *testing.T) {
	panicBool := false
	checkIfPainWasRaised(testOperator, &panicBool, AggregateIgnoresNulls)
	if !panicBool {
		t.Errorf("Painc should have been raised for %v", testOperator)
	}

	scalarOperators := [37]Operator{AnyNotNullAggOp, AvgOp, BitAndAggOp, BitOrAggOp, BoolAndOp, BoolOrOp,
		ConstNotNullAggOp, CorrOp, CountOp, MaxOp, MinOp, SqrDiffOp, StdDevOp,
		StringAggOp, SumOp, SumIntOp, VarianceOp, XorAggOp, PercentileDiscOp,
		PercentileContOp, STMakeLineOp, STCollectOp, STExtentOp, STUnionOp, StdDevPopOp,
		VarPopOp, CovarPopOp, CovarSampOp, RegressionAvgXOp, RegressionAvgYOp,
		RegressionInterceptOp, RegressionR2Op, RegressionSlopeOp, RegressionSXXOp,
		RegressionSXYOp, RegressionSYYOp, RegressionCountOp}

	for _, op := range scalarOperators {
		panicBool := false
		if !checkIfPainWasRaised(op, &panicBool, AggregateIgnoresNulls) {
			t.Errorf("%v should have passed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

	scalarOperators1 := [9]Operator{ArrayAggOp, ConcatAggOp, ConstAggOp, CountRowsOp, FirstAggOp, JsonAggOp,
		JsonbAggOp, JsonObjectAggOp, JsonbObjectAggOp}

	for _, op := range scalarOperators1 {
		panicBool := false
		if checkIfPainWasRaised(op, &panicBool, AggregateIgnoresNulls) {
			t.Errorf("%v should have failed", op)
		}
		if panicBool {
			t.Error("No panic should have been raised")
		}
	}

}

// TestAggregateProperties verifies that the various helper functions for
// various properties of aggregations handle all aggregation operators.
func TestAggregateProperties(t *testing.T) {
	check := func(fn func()) bool {
		ok := true
		func() {
			defer func() {
				if x := recover(); x != nil {
					ok = false
				}
			}()
			fn()
		}()
		return ok
	}

	for _, op := range AggregateOperators {
		funcs := []func(Operator) bool{
			AggregateIgnoresDuplicates,
			AggregateIgnoresNulls,
			AggregateIsNeverNull,
			AggregateIsNeverNullOnNonNullInput,
			AggregateIsNullOnEmpty,
		}

		for _, fn := range funcs {
			if !check(func() { fn(op) }) {
				fnName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
				t.Errorf("%s not handled by %s", op, fnName)
			}
		}

		for _, op2 := range AggregateOperators {
			if !check(func() { AggregatesCanMerge(op, op2) }) {
				t.Errorf("%s,%s not handled by AggregatesCanMerge", op, op2)
				break
			}
		}
	}
}
