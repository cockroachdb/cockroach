// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

const epsilon = 1e-10

func TestSelectivity(t *testing.T) {
	test := func(card, expected props.Selectivity) {
		t.Helper()
		if card != expected {
			t.Errorf("expected: %s, actual: %s", expected, card)
		}
	}

	testFloat := func(card, expected float64) {
		t.Helper()
		if card != expected {
			t.Errorf("expected: %f, actual: %f", expected, card)
		}
	}

	testMultiply := func(card, other, expected props.Selectivity) {
		t.Helper()
		card.Multiply(other)
		if card != expected {
			t.Errorf("expected: %s, actual: %s", expected, card)
		}
	}

	testAdd := func(card, other, expected props.Selectivity) {
		t.Helper()
		card.Add(other)
		if card != expected {
			t.Errorf("expected: %s, actual: %s", expected, card)
		}
	}

	testDivide := func(card, other, expected props.Selectivity) {
		t.Helper()
		card.Divide(other)
		if card != expected {
			t.Errorf("expected: %s, actual: %s", expected, card)
		}
	}

	s := func(sel float64) props.Selectivity {
		return props.MakeSelectivity(sel)
	}
	inf := math.MaxFloat64

	// MinSelectivity variations.
	test(props.MinSelectivity(s(0.4), s(0.5)), s(0.4))
	test(props.MinSelectivity(s(0.5), s(0.4)), s(0.4))
	test(props.MinSelectivity(props.ZeroSelectivity, props.OneSelectivity), props.ZeroSelectivity)
	test(props.MinSelectivity(props.ZeroSelectivity, s(epsilon)), props.ZeroSelectivity)
	test(props.MinSelectivity(s(0), s(epsilon)), s(epsilon))

	// SelectivityInRange variations.
	testFloat(props.SelectivityInRange(epsilon), epsilon)
	testFloat(props.SelectivityInRange(1), 1)
	testFloat(props.SelectivityInRange(0.5), 0.5)
	testFloat(props.SelectivityInRange(1.3), 1)
	testFloat(props.SelectivityInRange(0), epsilon)
	testFloat(props.SelectivityInRange(1.3), 1)
	testFloat(props.SelectivityInRange(inf), 1)

	// Multiply variations.
	testMultiply(s(0), s(0), s(epsilon * epsilon))
	testMultiply(s(0.5), s(0.4), s(0.5 * 0.4))
	testMultiply(s(2), props.OneSelectivity, props.OneSelectivity)
	testMultiply(s(inf), s(inf), props.OneSelectivity)
	testMultiply(s(0), s(0.4), s(epsilon * 0.4))
  testMultiply(props.ZeroSelectivity, s(0.5), s(epsilon))

	// Add variations.
	testAdd(s(0), s(0), s(epsilon + epsilon))
	testAdd(s(0.5), s(0.4), s(0.5 + 0.4))
	testAdd(s(0.5), s(0.6), props.OneSelectivity)
	testAdd(s(inf), s(inf), props.OneSelectivity)
	testAdd(props.OneSelectivity, props.OneSelectivity, props.OneSelectivity)
	testAdd(props.ZeroSelectivity, props.ZeroSelectivity, s(epsilon))

	// Divide variations.
	testDivide(s(0.4), s(0.5), s(0.8))
	testDivide(s(0.5), props.OneSelectivity, s(0.5))
	testDivide(props.OneSelectivity, s(0.5), props.OneSelectivity)
	testDivide(props.OneSelectivity, props.OneSelectivity, props.OneSelectivity)
}
