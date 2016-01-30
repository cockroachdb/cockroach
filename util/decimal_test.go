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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package util

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/util/randutil"

	"gopkg.in/inf.v0"
)

var floatDecimalEqualities = map[float64]*inf.Dec{
	-987650000: inf.NewDec(-98765, -4),
	-123.2:     inf.NewDec(-1232, 1),
	-1:         inf.NewDec(-1, 0),
	-.00000121: inf.NewDec(-121, 8),
	0:          inf.NewDec(0, 0),
	.00000121:  inf.NewDec(121, 8),
	1:          inf.NewDec(1, 0),
	123.2:      inf.NewDec(1232, 1),
	987650000:  inf.NewDec(98765, -4),
}

func TestNewDecFromFloat(t *testing.T) {
	for tf, td := range floatDecimalEqualities {
		if dec := NewDecFromFloat(tf); dec.Cmp(td) != 0 {
			t.Errorf("NewDecFromFloat(%f) expected to give %s, but got %s", tf, td, dec)
		}
	}
}

func TestFloat64FromDec(t *testing.T) {
	for tf, td := range floatDecimalEqualities {
		f, err := Float64FromDec(td)
		if err != nil {
			t.Errorf("Float64FromDec(%s) expected to give %f, but returned error: %v", td, tf, err)
		}
		if f != tf {
			t.Errorf("Float64FromDec(%s) expected to give %f, but got %f", td, tf, f)
		}
	}
}

