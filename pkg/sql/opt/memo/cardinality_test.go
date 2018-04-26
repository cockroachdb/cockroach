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

package memo

import (
	"math"
	"testing"
)

func TestCardinality(t *testing.T) {
	test := func(card Cardinality, expected string) {
		t.Helper()
		if card.String() != expected {
			t.Errorf("expected: %s, actual: %s", expected, card.String())
		}
	}

	maxCard := Cardinality{Min: math.MaxUint32, Max: math.MaxUint32}

	// Filter variations.
	test(Cardinality{Min: 0, Max: 10}.AsLowAs(0), "[0 - 10]")
	test(Cardinality{Min: 1, Max: 10}.AsLowAs(0), "[0 - 10]")
	test(Cardinality{Min: 5, Max: 10}.AsLowAs(1), "[1 - 10]")
	test(Cardinality{Min: 1, Max: 10}.AsLowAs(5), "[1 - 10]")
	test(Cardinality{Min: 1, Max: 10}.AsLowAs(20), "[1 - 10]")
	test(AnyCardinality.AsLowAs(1), "[0 - ]")

	// AtLeast variations.
	test(Cardinality{Min: 0, Max: 10}.AtLeast(1), "[1 - 10]")
	test(Cardinality{Min: 1, Max: 10}.AtLeast(5), "[5 - 10]")
	test(Cardinality{Min: 5, Max: 10}.AtLeast(15), "[15 - 15]")
	test(Cardinality{Min: 5, Max: 10}.AtLeast(1), "[5 - 10]")
	test(AnyCardinality.AtLeast(1), "[1 - ]")
	test(AnyCardinality.AtLeast(math.MaxUint32), "[4294967295 - ]")

	// AtMost variations.
	test(Cardinality{Min: 0, Max: 10}.AtMost(5), "[0 - 5]")
	test(Cardinality{Min: 1, Max: 10}.AtMost(10), "[1 - 10]")
	test(Cardinality{Min: 5, Max: 10}.AtMost(1), "[1 - 1]")
	test(AnyCardinality.AtMost(1), "[0 - 1]")

	// Add variations.
	test(Cardinality{Min: 0, Max: 10}.Add(Cardinality{Min: 5, Max: 5}), "[5 - 15]")
	test(Cardinality{Min: 0, Max: 10}.Add(Cardinality{Min: 20, Max: 30}), "[20 - 40]")
	test(Cardinality{Min: 1, Max: 10}.Add(AnyCardinality), "[1 - ]")
	test(maxCard.Add(AnyCardinality), "[4294967295 - ]")

	// Product variations.
	test(Cardinality{Min: 0, Max: 10}.Product(Cardinality{Min: 5, Max: 5}), "[0 - 50]")
	test(Cardinality{Min: 1, Max: 10}.Product(Cardinality{Min: 2, Max: 2}), "[2 - 20]")
	test(Cardinality{Min: 1, Max: 10}.Product(AnyCardinality), "[0 - ]")
	test(maxCard.Product(OneCardinality), "[4294967295 - ]")
	test(maxCard.Product(maxCard), "[4294967295 - ]")

	// Skip variations.
	test(Cardinality{Min: 0, Max: 0}.Skip(1), "[0 - 0]")
	test(Cardinality{Min: 0, Max: 10}.Skip(5), "[0 - 5]")
	test(Cardinality{Min: 5, Max: 10}.Skip(5), "[0 - 5]")
	test(AnyCardinality.Skip(5), "[0 - ]")
	test(maxCard.Skip(5), "[4294967290 - ]")
}
