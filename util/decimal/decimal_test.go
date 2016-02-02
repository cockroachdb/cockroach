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

package decimal

import (
	"math"
	"testing"

	"gopkg.in/inf.v0"

	_ "github.com/cockroachdb/cockroach/util/log" // for flags
	"github.com/cockroachdb/cockroach/util/randutil"
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

		var dec inf.Dec
		if SetFromFloat(&dec, tf); dec.Cmp(td) != 0 {
			t.Errorf("SetFromFloat(%f) expected to set decimal to %s, but got %s", tf, td, dec)
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

func TestDecimalMod(t *testing.T) {
	tests := []struct {
		x, y, z string
	}{
		{"3", "2", "1"},
		{"3451204593", "2454495034", "996709559"},
		{"24544.95034", ".3451204593", "0.3283950433"},
		{".1", ".1", "0"},
		{"0", "1.001", "0"},
		{"-7.5", "2", "-1.5"},
		{"7.5", "-2", "1.5"},
		{"-7.5", "-2", "-1.5"},
	}
	for i, tc := range tests {
		x, y, exp := new(inf.Dec), new(inf.Dec), new(inf.Dec)
		x.SetString(tc.x)
		y.SetString(tc.y)
		exp.SetString(tc.z)

		// Test return value.
		z := Mod(nil, x, y)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test provided decimal mutation.
		z.SetString("0.0")
		Mod(z, x, y)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test dividend mutation.
		Mod(x, x, y)
		if exp.Cmp(x) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, x)
		}
		x.SetString(tc.x)

		// Test divisor mutation.
		Mod(y, x, y)
		if exp.Cmp(y) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, y)
		}
		y.SetString(tc.y)

		// Test dividend and divisor mutation, if possible.
		if tc.x == tc.y {
			Mod(x, x, x)
			if exp.Cmp(x) != 0 {
				t.Errorf("%d: expected %s, got %s", i, exp, x)
			}
			x.SetString(tc.x)
		}
	}
}

func BenchmarkDecimalMod(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	populate := func(vals []*inf.Dec) []*inf.Dec {
		for i := range vals {
			f := 0.0
			for f == 0 {
				f = rng.Float64()
			}
			vals[i] = NewDecFromFloat(f)
		}
		return vals
	}

	dividends := populate(make([]*inf.Dec, 10000))
	divisors := populate(make([]*inf.Dec, 10000))

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Mod(z, dividends[i%len(dividends)], divisors[i%len(divisors)])
	}
}

func TestDecimalSqrt(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"0", "0"},
		{".12345678987654321122763812", "0.3513641841117891"},
		{"4", "2"},
		{"9", "3"},
		{"100", "10"},
		{"2454495034", "49542.8605754653613946"},
		{"24544.95034", "156.6682812186308502"},
		{"1234567898765432112.2763812", "1111111110.0000000055243715"},
	}
	for i, tc := range tests {
		x, exp := new(inf.Dec), new(inf.Dec)
		x.SetString(tc.input)
		exp.SetString(tc.expected)

		// Test return value.
		z := Sqrt(nil, x, 16)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test provided decimal mutation.
		z.SetString("0.0")
		Sqrt(z, x, 16)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test same argument mutation.
		Sqrt(x, x, 16)
		if exp.Cmp(x) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, x)
		}
	}
}

func BenchmarkDecimalSqrt(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 10000)
	for i := range vals {
		vals[i] = NewDecFromFloat(math.Abs(rng.Float64()))
	}

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sqrt(z, vals[i%len(vals)], 16)
	}
}
