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

package props_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func TestCardinality(t *testing.T) {
	test := func(card, expected props.Cardinality) {
		t.Helper()
		if card != expected {
			t.Errorf("expected: %s, actual: %s", expected, card)
		}
	}

	c := func(min, max uint32) props.Cardinality {
		return props.Cardinality{Min: min, Max: max}
	}
	inf := uint32(math.MaxUint32)

	// AsLowAs variations.
	test(c(0, 10).AsLowAs(0), c(0, 10))
	test(c(1, 10).AsLowAs(0), c(0, 10))
	test(c(5, 10).AsLowAs(1), c(1, 10))
	test(c(1, 10).AsLowAs(5), c(1, 10))
	test(c(1, 10).AsLowAs(20), c(1, 10))
	test(props.AnyCardinality.AsLowAs(1), c(0, inf))

	// Limit variations.
	test(c(0, 10).Limit(5), c(0, 5))
	test(c(1, 10).Limit(10), c(1, 10))
	test(c(5, 10).Limit(1), c(1, 1))
	test(props.AnyCardinality.Limit(1), c(0, 1))

	// AtLeast variations.
	test(c(0, 10).AtLeast(c(1, 1)), c(1, 10))
	test(c(1, 10).AtLeast(c(5, 15)), c(5, 15))
	test(c(5, 10).AtLeast(c(1, 2)), c(5, 10))
	test(c(5, 10).AtLeast(c(1, 8)), c(5, 10))
	test(c(5, 10).AtLeast(c(7, 8)), c(7, 10))
	test(c(5, 10).AtLeast(c(1, 15)), c(5, 15))
	test(c(5, 10).AtLeast(c(7, 15)), c(7, 15))
	test(props.AnyCardinality.AtLeast(c(1, 10)), c(1, inf))
	test(props.AnyCardinality.AtLeast(c(inf, inf)), c(inf, inf))

	// Add variations.
	test(c(0, 10).Add(c(5, 5)), c(5, 15))
	test(c(0, 10).Add(c(20, 30)), c(20, 40))
	test(c(1, 10).Add(props.AnyCardinality), c(1, inf))
	test(c(inf, inf).Add(props.AnyCardinality), c(inf, inf))

	// Product variations.
	test(c(0, 10).Product(c(5, 5)), c(0, 50))
	test(c(1, 10).Product(c(2, 2)), c(2, 20))
	test(c(1, 10).Product(props.AnyCardinality), c(0, inf))
	test(c(inf, inf).Product(props.OneCardinality), c(inf, inf))
	test(c(inf, inf).Product(c(inf, inf)), c(inf, inf))

	// Skip variations.
	test(c(0, 0).Skip(1), c(0, 0))
	test(c(0, 10).Skip(5), c(0, 5))
	test(c(5, 10).Skip(5), c(0, 5))
	test(props.AnyCardinality.Skip(5), c(0, inf))
	test(c(inf, inf).Skip(5), c(inf-5, inf))
}
