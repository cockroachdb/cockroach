// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"math"
	"testing"
)

func TestSelectivity(t *testing.T) {
	test := func(actual, expected Selectivity) {
		t.Helper()
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	testFloat := func(actual, expected float64) {
		t.Helper()
		if actual != expected {
			t.Errorf("expected: %f, actual: %f", expected, actual)
		}
	}

	testMultiply := func(actual, other, expected Selectivity) {
		t.Helper()
		actual.Multiply(other)
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	testAdd := func(actual, other, expected Selectivity) {
		t.Helper()
		actual.Add(other)
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	testDivide := func(actual, other, expected Selectivity) {
		t.Helper()
		actual.Divide(other)
		if actual != expected {
			t.Errorf("expected: %s, actual: %s", expected, actual)
		}
	}

	s := func(sel float64) Selectivity {
		return MakeSelectivity(sel)
	}
	inf := math.MaxFloat64

	// MakeSelectivityFromFraction variations.
	test(MakeSelectivityFromFraction(1, 2), s(0.5))
	test(MakeSelectivityFromFraction(1, 1), OneSelectivity)
	test(MakeSelectivityFromFraction(1.5, 1), OneSelectivity)
	test(MakeSelectivityFromFraction(1, 0), OneSelectivity)
	test(MakeSelectivityFromFraction(0, 0), OneSelectivity)

	// MinSelectivity variations.
	test(MinSelectivity(s(0.4), s(0.5)), s(0.4))
	test(MinSelectivity(s(0.5), s(0.4)), s(0.4))
	test(MinSelectivity(ZeroSelectivity, OneSelectivity), ZeroSelectivity)
	test(MinSelectivity(ZeroSelectivity, s(epsilon)), ZeroSelectivity)
	test(MinSelectivity(s(0), s(epsilon)), s(epsilon))

	// selectivityInRange variations.
	testFloat(selectivityInRange(epsilon), epsilon)
	testFloat(selectivityInRange(1), 1)
	testFloat(selectivityInRange(0.5), 0.5)
	testFloat(selectivityInRange(1.3), 1)
	testFloat(selectivityInRange(0), epsilon)
	testFloat(selectivityInRange(1.3), 1)
	testFloat(selectivityInRange(inf), 1)

	// Multiply variations.
	testMultiply(s(0), s(0), s(epsilon*epsilon))
	testMultiply(s(0.5), s(0.4), s(0.5*0.4))
	testMultiply(s(2), OneSelectivity, OneSelectivity)
	testMultiply(s(inf), s(inf), OneSelectivity)
	testMultiply(s(0), s(0.4), s(epsilon*0.4))
	testMultiply(ZeroSelectivity, s(0.5), s(epsilon))

	// Add variations.
	testAdd(s(0), s(0), s(epsilon+epsilon))
	testAdd(s(0.5), s(0.4), s(0.5+0.4))
	testAdd(s(0.5), s(0.6), OneSelectivity)
	testAdd(s(inf), s(inf), OneSelectivity)
	testAdd(OneSelectivity, OneSelectivity, OneSelectivity)
	testAdd(ZeroSelectivity, ZeroSelectivity, s(epsilon))

	// Divide variations.
	testDivide(s(0.4), s(0.5), s(0.8))
	testDivide(s(0.5), OneSelectivity, s(0.5))
	testDivide(OneSelectivity, s(0.5), OneSelectivity)
	testDivide(OneSelectivity, OneSelectivity, OneSelectivity)
}
