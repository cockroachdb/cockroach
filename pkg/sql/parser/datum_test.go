// Copyright 2016 The Cockroach Authors.
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
//
// Author: David Eisenstat (eisen@cockroachlabs.com)

package parser

import (
	"math"
	"testing"
)

func TestDFloatCompare(t *testing.T) {
	values := []Datum{DNull}
	for _, x := range []float64{math.NaN(), math.Inf(-1), -1, 0, 1, math.Inf(1)} {
		values = append(values, NewDFloat(DFloat(x)))
	}
	for i, x := range values {
		for j, y := range values {
			expected := 0
			if i < j {
				expected = -1
			} else if i > j {
				expected = 1
			}
			got := x.Compare(y)
			if got != expected {
				t.Errorf("comparing DFloats %s and %s: expected %d, got %d", x, y, expected, got)
			}
		}
	}
}
