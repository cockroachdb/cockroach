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

type decimalOneArgTestCase struct {
	input    string
	expected string
}

type decimalTwoArgsTestCase struct {
	input1   string
	input2   string
	expected string
}

func testDecimalSingleArgFunc(t *testing.T, f func(*inf.Dec, *inf.Dec, inf.Scale) *inf.Dec, s inf.Scale, tests []decimalOneArgTestCase) {
	for i, tc := range tests {
		x, exp := new(inf.Dec), new(inf.Dec)
		x.SetString(tc.input)
		exp.SetString(tc.expected)

		// Test allocated return value.
		z := f(nil, x, s)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test provided decimal mutation.
		z.SetString("0.0")
		f(z, x, s)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test same arg mutation.
		f(x, x, s)
		if exp.Cmp(x) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, x)
		}
		x.SetString(tc.input)
	}
}

func testDecimalDoubleArgFunc(t *testing.T, f func(*inf.Dec, *inf.Dec, *inf.Dec, inf.Scale) *inf.Dec, s inf.Scale, tests []decimalTwoArgsTestCase) {
	for i, tc := range tests {
		x, y, exp := new(inf.Dec), new(inf.Dec), new(inf.Dec)
		x.SetString(tc.input1)
		y.SetString(tc.input2)
		exp.SetString(tc.expected)

		// Test allocated return value.
		z := f(nil, x, y, s)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test provided decimal mutation.
		z.SetString("0.0")
		f(z, x, y, s)
		if exp.Cmp(z) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, z)
		}

		// Test first arg mutation.
		f(x, x, y, s)
		if exp.Cmp(x) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, x)
		}
		x.SetString(tc.input1)

		// Test second arg mutation.
		f(y, x, y, s)
		if exp.Cmp(y) != 0 {
			t.Errorf("%d: expected %s, got %s", i, exp, y)
		}
		y.SetString(tc.input2)

		// Test both arg mutation, if possible.
		if tc.input1 == tc.input2 {
			f(x, x, x, s)
			if exp.Cmp(x) != 0 {
				t.Errorf("%d: expected %s, got %s", i, exp, x)
			}
			x.SetString(tc.input1)
		}
	}
}

func TestDecimalMod(t *testing.T) {
	tests := []decimalTwoArgsTestCase{
		{"3", "2", "1"},
		{"3451204593", "2454495034", "996709559"},
		{"24544.95034", ".3451204593", "0.3283950433"},
		{".1", ".1", "0"},
		{"0", "1.001", "0"},
		{"-7.5", "2", "-1.5"},
		{"7.5", "-2", "1.5"},
		{"-7.5", "-2", "-1.5"},
	}
	modWithScale := func(z, x, y *inf.Dec, s inf.Scale) *inf.Dec {
		return Mod(z, x, y)
	}
	testDecimalDoubleArgFunc(t, modWithScale, 0, tests)
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
	tests := []decimalOneArgTestCase{
		{"0", "0"},
		{".12345678987654321122763812", "0.3513641841117891"},
		{"4", "2"},
		{"9", "3"},
		{"100", "10"},
		{"2454495034", "49542.8605754653613946"},
		{"24544.95034", "156.6682812186308502"},
		{"1234567898765432112.2763812", "1111111110.0000000055243715"},
	}
	testDecimalSingleArgFunc(t, Sqrt, 16, tests)
}

func TestDecimalSqrtDoubleScale(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{"0", "0"},
		{".12345678987654321122763812", "0.35136418411178907639479458498081"},
		{"4", "2"},
		{"9", "3"},
		{"100", "10"},
		{"2454495034", "49542.86057546536139455430949116585673"},
		{"24544.95034", "156.66828121863085021083671472749063"},
		{"1234567898765432112.2763812", "1111111110.00000000552437154552437153179097"},
	}
	testDecimalSingleArgFunc(t, Sqrt, 32, tests)
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

func TestDecimalCbrt(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{"-567", "-8.2767725291433620"},
		{"-1", "-1.0"},
		{"-0.001", "-0.1"},
		{".00000001", "0.0021544346900319"},
		{".001234567898217312", "0.1072765982021206"},
		{".001", "0.1"},
		{".123", "0.4973189833268590"},
		{"0", "0"},
		{"1", "1"},
		{"2", "1.2599210498948732"},
		{"1000", "10.0"},
		{"1234567898765432112.2763812", "1072765.9821799668569064"},
	}
	testDecimalSingleArgFunc(t, Cbrt, 16, tests)
}

func TestDecimalCbrtDoubleScale(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{"-567", "-8.27677252914336200839737332507556"},
		{"-1", "-1.0"},
		{"-0.001", "-0.1"},
		{".00000001", "0.00215443469003188372175929356652"},
		{".001234567898217312", "0.10727659820212056117037629887220"},
		{".001", "0.1"},
		{".123", "0.49731898332685904156500833828550"},
		{"0", "0"},
		{"1", "1"},
		{"2", "1.25992104989487316476721060727823"},
		{"1000", "10.0"},
		{"1234567898765432112.2763812", "1072765.98217996685690644770246374397146"},
	}
	testDecimalSingleArgFunc(t, Cbrt, 32, tests)
}

func BenchmarkDecimalCbrt(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 10000)
	for i := range vals {
		vals[i] = NewDecFromFloat(rng.Float64())
	}

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Cbrt(z, vals[i%len(vals)], 16)
	}
}

func TestDecimalLog(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{".001234567898217312", "-6.6970342501104617"},
		{".5", "-0.6931471805599453"},
		{"1", "0"},
		{"2", "0.6931471805599453"},
		{"1234.56789", "7.1184763011977896"},
		{"1234567898765432112.2763812", "41.6572527032084749"},
	}
	testDecimalSingleArgFunc(t, Log, 16, tests)
}

func TestDecimalLogDoubleScale(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{".001234567898217312", "-6.69703425011046173258548487981855"},
		{".5", "-0.69314718055994530941723212145818"},
		{"1", "0"},
		{"2", "0.69314718055994530941723212145818"},
		{"1234.56789", "7.11847630119778961310397607454138"},
		{"1234567898765432112.2763812", "41.65725270320847492372271693721825"},
	}
	testDecimalSingleArgFunc(t, Log, 32, tests)
}

func TestDecimalLog10(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{".001234567898217312", "-2.9084850199400556"},
		{".001", "-3"},
		{".123", "-0.9100948885606021"},
		{"1", "0"},
		{"123", "2.0899051114393979"},
		{"1000", "3"},
		{"1234567898765432112.2763812", "18.0915149802527613"},
	}
	testDecimalSingleArgFunc(t, Log10, 16, tests)
}

func TestDecimalLog10DoubleScale(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{".001234567898217312", "-2.90848501994005559707805612700747"},
		{".001", "-3"},
		{".123", "-0.91009488856060206819556024677670"},
		{"1", "0"},
		{"123", "2.08990511143939793180443975322329"},
		{"1000", "3"},
		{"1234567898765432112.2763812", "18.09151498025276129089765759457130"},
	}
	testDecimalSingleArgFunc(t, Log10, 32, tests)
}

func TestDecimalLogN(t *testing.T) {
	tests := []decimalTwoArgsTestCase{
		{".001234567898217312", strE, "-6.6970342501104617"},
		{".001234567898217312", "10", "-2.9084850199400556"},
		{".001", "10", "-3"},
		{".123", "10", "-0.9100948885606021"},
		{"1", "10", "0"},
		{"123", "10", "2.0899051114393979"},
		{"1000", "10", "3"},
		{"1234567898765432112.2763812", strE, "41.6572527032084749"},
		{"1234567898765432112.2763812", "10", "18.0915149802527613"},
	}
	testDecimalDoubleArgFunc(t, LogN, 16, tests)
}

func TestDecimalLogNDoubleScale(t *testing.T) {
	tests := []decimalTwoArgsTestCase{
		{".001234567898217312", strE, "-6.69703425011046173258548487981855"},
		{".001234567898217312", "10", "-2.90848501994005559707805612700747"},
		{".001", "10", "-3"},
		{".123", "10", "-0.91009488856060206819556024677670"},
		{"1", "10", "0"},
		{"123", "10", "2.08990511143939793180443975322330"},
		{"1000", "10", "3"},
		{"1234567898765432112.2763812", strE, "41.65725270320847492372271693721825"},
		{"1234567898765432112.2763812", "10", "18.09151498025276129089765759457130"},
	}
	testDecimalDoubleArgFunc(t, LogN, 32, tests)
}

func BenchmarkDecimalLog(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 10000)
	for i := range vals {
		vals[i] = NewDecFromFloat(math.Abs(rng.Float64()))
	}

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Log(z, vals[i%len(vals)], 16)
	}
}

func TestDecimalExp(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{"2.1", "8.1661699125676501"},
		{"1", "2.7182818284590452"},

		{"2", "7.3890560989306502"},
		{"0.0001", "1.0001000050001667"},

		{"-7.1", "0.0008251049232659"},
		{"-0.7", "0.4965853037914095"},
		{"0.8", "2.2255409284924676"},

		{"-6.6970342501104617", "0.0012345678982173"},
		{"-0.6931471805599453", ".5"},
		{"0.6931471805599453", "2"},
		{"7.1184763011977896", "1234.5678899999999838"},

		{"41.6572527032084749", "1234567898765432082.9890763978113354"},
		{"312.345", "4463853675713824294922499817029570039071067102402155066185430427302882199695129254111120181064791178979160372068542599780002019509758173.2401488061929997"},
	}
	testDecimalSingleArgFunc(t, Exp, 16, tests)
}

func TestDecimalExpDoubleScale(t *testing.T) {
	tests := []decimalOneArgTestCase{
		{"2.1", "8.16616991256765007344972741047863"},
		{"1", "2.71828182845904523536028747135266"},

		{"2", "7.38905609893065022723042746057501"},
		{"0.0001", "1.00010000500016667083341666805558"},

		{"-7.1", "0.00082510492326590427014622545675"},
		{"-0.7", "0.49658530379140951470480009339753"},
		{"0.8", "2.22554092849246760457953753139508"},

		{"-6.6970342501104617", "0.00123456789821731204022899358047"},
		{"-0.6931471805599453", "0.50000000000000000470861606072909"},
		{"0.6931471805599453", "1.99999999999999998116553575708365"},
		{"7.1184763011977896", "1234.56788999999998382225190704296197"},

		{"41.6572527032084749", "1234567898765432082.98907639781133543894457806069743"},
		{"312.345", "4463853675713824294922499817029570039071067102402155066185430427302882199695129254111120181064791178979160372068542599780002019509758173.24014880619299965312338408024449"},
	}
	testDecimalSingleArgFunc(t, Exp, 32, tests)
}

func BenchmarkDecimalExp(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]*inf.Dec, 100)
	for i := range vals {
		vals[i] = NewDecFromFloat(math.Abs(rng.Float64()))
		vals[i].Add(vals[i], inf.NewDec(int64(randutil.RandIntInRange(rng, 0, 100)), 0))
	}

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Exp(z, vals[i%len(vals)], 16)
	}
}

func TestDecimalPow(t *testing.T) {
	tests := []decimalTwoArgsTestCase{
		{"2", "0", "1"},
		{"8.14", "1", "8.14"},
		{"-3", "2", "9"},
		{"2", "3", "8"},
		{"4", "0.5", "2"},
		{"2", "-3", "0.125"},
		{"3.14", "9.604", "59225.9915180848144580"},
		{"4.042131231", "86.9627324951673", "56558611276325345873179603915517177973179624550320948.7364709633024969"},
		{"12.56558611276325345873179603915517177973179624550320948", "1", "12.5655861127632535"},
		{"9223372036854775807123.1", "2", "85070591730234615849667701979706147052698553.61"},
		{"-9223372036854775807123.1", "2", "85070591730234615849667701979706147052698553.61"},
		{"9223372036854775807123.1", "3", "784637716923335095255678472236230098075796571287653754351907705219.391"},
		{"-9223372036854775807123.1", "3", "-784637716923335095255678472236230098075796571287653754351907705219.391"},
	}
	testDecimalDoubleArgFunc(t, Pow, 16, tests)
}

func TestDecimalPowDoubleScale(t *testing.T) {
	tests := []decimalTwoArgsTestCase{
		{"2", "0", "1"},
		{"8.14", "1", "8.14"},
		{"-3", "2", "9"},
		{"2", "3", "8"},
		{"4", "0.5", "2"},
		{"2", "-3", "0.125"},
		{"3.14", "9.604", "59225.99151808481445796912159493126569"},
		{"4.042131231", "86.9627324951673", "56558611276325345873179603915517177973179624550320948.73647096330249691821726648938363"},
		{"12.56558611276325345873179603915517177973179624550320948", "1", "12.56558611276325345873179603915517"},
		{"9223372036854775807123.1", "2", "85070591730234615849667701979706147052698553.61"},
		{"-9223372036854775807123.1", "2", "85070591730234615849667701979706147052698553.61"},
		{"9223372036854775807123.1", "3", "784637716923335095255678472236230098075796571287653754351907705219.391"},
		{"-9223372036854775807123.1", "3", "-784637716923335095255678472236230098075796571287653754351907705219.391"},
	}
	testDecimalDoubleArgFunc(t, Pow, 32, tests)
}

func BenchmarkDecimalPow(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	xs := make([]*inf.Dec, 100)
	ys := make([]*inf.Dec, 100)

	for i := range ys {
		ys[i] = NewDecFromFloat(math.Abs(rng.Float64()))
		ys[i].Add(ys[i], inf.NewDec(int64(randutil.RandIntInRange(rng, 0, 10)), 0))

		xs[i] = NewDecFromFloat(math.Abs(rng.Float64()))
		xs[i].Add(xs[i], inf.NewDec(int64(randutil.RandIntInRange(rng, 0, 10)), 0))
	}

	z := new(inf.Dec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Pow(z, xs[i%len(ys)], ys[i%len(ys)], 16)
	}
}
