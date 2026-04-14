// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// TestOperatorCamelCase verifies that CamelCase names for all operators contain
// no whitespace and none of the punctuation characters forbidden in plangram
// production names.
func TestOperatorCamelCase(t *testing.T) {
	const forbidden = "\"'\\:=()|; \t\n\r"
	for op := Operator(1); op < NumOperators; op++ {
		name := op.CamelCase()
		if strings.ContainsAny(name, forbidden) {
			t.Errorf("CamelCase(%d) = %q contains forbidden character", op, name)
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
