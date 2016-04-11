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
	"fmt"
	"math"
	"strconv"
	"strings"

	"gopkg.in/inf.v0"
)

var e = smallExp(inf.NewDec(1, 0), decimalOne, 1000)

// NewDecFromFloat allocates and returns a new Dec set to the given
// float64 value. The function will panic if the float is NaN or ±Inf.
func NewDecFromFloat(f float64) *inf.Dec {
	return SetFromFloat(new(inf.Dec), f)
}

// SetFromFloat sets z to the given float64 value and returns z. The
// function will panic if the float is NaN or ±Inf.
func SetFromFloat(z *inf.Dec, f float64) *inf.Dec {
	switch {
	case math.IsInf(f, 0):
		panic("cannot create a decimal from an infinte float")
	case math.IsNaN(f):
		panic("cannot create a decimal from an NaN float")
	}

	s := strconv.FormatFloat(f, 'e', -1, 64)

	// Determine the decimal's exponent.
	var e10 int64
	e := strings.IndexByte(s, 'e')
	for i := e + 2; i < len(s); i++ {
		e10 = e10*10 + int64(s[i]-'0')
	}
	switch s[e+1] {
	case '-':
		e10 = -e10
	case '+':
	default:
		panic(fmt.Sprintf("malformed float: %v -> %s", f, s))
	}
	e10++

	// Determine the decimal's mantissa.
	var mant int64
	i := 0
	neg := false
	if s[0] == '-' {
		i++
		neg = true
	}
	for ; i < e; i++ {
		if s[i] == '.' {
			continue
		}
		mant = mant*10 + int64(s[i]-'0')
		e10--
	}
	if neg {
		mant = -mant
	}

	return z.SetUnscaled(mant).SetScale(inf.Scale(-e10))
}

// Float64FromDec converts a decimal to a float64 value, returning
// the value and any error that occurred. This converson exposes a
// possible loss of information.
func Float64FromDec(dec *inf.Dec) (float64, error) {
	return strconv.ParseFloat(dec.String(), 64)
}

// Mod performs the modulo arithmatic x % y and stores the
// result in z, which is also the return value. It is valid for z
// to be nil, in which case it will be allocated internally.
// Mod will panic if the y is zero.
//
// The modulo calculation is implemented using the algorithm:
//     x % y = x - (y * ⌊x / y⌋).
func Mod(z, x, y *inf.Dec) *inf.Dec {
	switch z {
	case nil:
		z = new(inf.Dec)
	case x:
		x = new(inf.Dec)
		x.Set(z)
		if z == y {
			y = x
		}
	case y:
		y = new(inf.Dec)
		y.Set(z)
	}
	z.QuoRound(x, y, 0, inf.RoundDown)
	return z.Sub(x, z.Mul(z, y))
}

// Sqrt calculates the square root of x to the specified scale
// and stores the result in z, which is also the return value.
// The function will panic if x is a negative number.
//
// The square root calculation is implemented using Newton's Method.
// We start with an initial estimate for sqrt(d), and then iterate:
//     x_{n+1} = 1/2 * ( x_n + (d / x_n) ).
func Sqrt(z, x *inf.Dec, s inf.Scale) *inf.Dec {
	switch z {
	case nil:
		z = new(inf.Dec)
	case x:
		x = new(inf.Dec)
		x.Set(z)
	}

	// Validate the sign of x.
	switch x.Sign() {
	case -1:
		panic(fmt.Sprintf("square root of negative number: %s", x))
	case 0:
		return z.SetUnscaled(0).SetScale(0)
	}

	// Use half as the initial estimate.
	z.Mul(x, decimalHalf)

	// Iterate.
	tmp := new(inf.Dec)
	for loop := newLoop("sqrt", z, s, 1); ; {
		tmp.QuoRound(x, z, s+2, inf.RoundHalfUp) // t = d / x_n
		tmp.Add(tmp, z)                          // t = x_n + (d / x_n)
		z.Mul(tmp, decimalHalf)                  // x_{n+1} = 0.5 * t
		if loop.done(z) {
			break
		}
	}

	// Round to the desired scale.
	return z.Round(z, s, inf.RoundHalfUp)
}

// Cbrt calculates the cube root of x to the specified scale
// and stores the result in z, which is also the return value.
//
// The cube root calculation is implemented using Newton-Raphson
// method. We start with an initial estimate for cbrt(d), and
// then iterate:
//     x_{n+1} = 1/3 * ( 2 * x_n + (d / x_n / x_n) ).
func Cbrt(z, x *inf.Dec, s inf.Scale) *inf.Dec {
	switch z {
	case nil:
		z = new(inf.Dec)
	case x:
		x = new(inf.Dec)
		x.Set(z)
	}

	// Validate the sign of x.
	switch x.Sign() {
	case -1:
		// Make sure args aren't mutated and return -Cbrt(-x).
		x = new(inf.Dec).Neg(x)
		z = Cbrt(z, x, s)
		return z.Neg(z)
	case 0:
		return z.SetUnscaled(0).SetScale(0)
	}

	z.Set(x)
	exp8 := 0

	// Follow Ken Turkowski paper:
	// https://people.freebsd.org/~lstewart/references/apple_tr_kt32_cuberoot.pdf
	//
	// Computing the cube root of any number is reduced to computing
	// the cube root of a number between 0.125 and 1. After the next loops,
	// x = z * 8^exp8 will hold.
	for z.Cmp(decimalOneEighth) < 0 {
		exp8--
		z.Mul(z, decimalEight)
	}

	for z.Cmp(decimalOne) > 0 {
		exp8++
		z.Mul(z, decimalOneEighth)
	}

	// Use this polynomial to approximate the cube root between 0.125 and 1.
	// z = (-0.46946116 * z + 1.072302) * z + 0.3812513
	// It will serve as an initial estimate, hence the precision of this
	// computation may only impact performance, not correctness.
	z0 := new(inf.Dec).Set(z)
	z.Mul(z, decimalCbrtC1)
	z.Add(z, decimalCbrtC2)
	z.Mul(z, z0)
	z.Add(z, decimalCbrtC3)

	for ; exp8 < 0; exp8++ {
		z.Mul(z, decimalHalf)
	}

	for ; exp8 > 0; exp8-- {
		z.Mul(z, decimalTwo)
	}

	z0.Set(z)

	// Loop until convergence.
	for loop := newLoop("cbrt", x, s, 1); ; {
		// z = (2.0 * z0 +  x / (z0 * z0) ) / 3.0;
		z.Set(z0)
		z.Mul(z, z0)
		z.QuoRound(x, z, s+2, inf.RoundHalfUp)
		z.Add(z, z0)
		z.Add(z, z0)
		z.QuoRound(z, decimalThree, s+2, inf.RoundHalfUp)

		if loop.done(z) {
			break
		}
		z0.Set(z)
	}

	// Round to the desired scale.
	return z.Round(z, s, inf.RoundHalfUp)
}

// LogN computes the log of x with base n to the specified scale and
// stores the result in z, which is also the return value. The function
// will panic if x is a negative number or if n is a negative number.
func LogN(z *inf.Dec, x *inf.Dec, n *inf.Dec, s inf.Scale) *inf.Dec {
	if z == n {
		n = new(inf.Dec).Set(n)
	}
	z = Log(z, x, s+1)
	return z.QuoRound(z, Log(nil, n, s+1), s, inf.RoundHalfUp)
}

// Log10 computes the log of x with base 10 to the specified scale and
// stores the result in z, which is also the return value. The function
// will panic if x is a negative number.
func Log10(z *inf.Dec, x *inf.Dec, s inf.Scale) *inf.Dec {
	z = Log(z, x, s)
	return z.QuoRound(z, decimalLog10, s, inf.RoundHalfUp)
}

// Log computes the natural log of x using the Maclaurin series for
// log(1-x) to the specified scale and stores the result in z, which
// is also the return value. The function will panic if x is a negative
// number.
func Log(z *inf.Dec, x *inf.Dec, s inf.Scale) *inf.Dec {
	// Validate the sign of x.
	if x.Sign() <= 0 {
		panic(fmt.Sprintf("natural log of non-positive value: %s", x))
	}

	// Allocate if needed and make sure args aren't mutated.
	x = new(inf.Dec).Set(x)
	if z == nil {
		z = new(inf.Dec)
	} else {
		z.SetUnscaled(0).SetScale(0)
	}

	// The series wants x < 1, and log 1/x == -log x, so exploit that.
	invert := false
	if x.Cmp(decimalOne) > 0 {
		invert = true
		x.QuoRound(decimalOne, x, s*2, inf.RoundHalfUp)
	}

	// x = mantissa * 2**exp, and 0.5 <= mantissa < 1.
	// So log(x) is log(mantissa)+exp*log(2), and 1-x will be
	// between 0 and 0.5, so the series for 1-x will converge well.
	// (The series converges slowly in general.)
	exp2 := int64(0)
	for x.Cmp(decimalHalf) < 0 {
		x.Mul(x, decimalTwo)
		exp2--
	}
	exp := inf.NewDec(exp2, 0)
	exp.Mul(exp, decimalLog2)
	if invert {
		exp.Neg(exp)
	}

	// y = 1-x (whereupon x = 1-y and we use that in the series).
	y := inf.NewDec(1, 0)
	y.Sub(y, x)

	// The Maclaurin series for log(1-y) == log(x) is: -y - y²/2 - y³/3 ...
	yN := new(inf.Dec).Set(y)
	term := new(inf.Dec)
	n := inf.NewDec(1, 0)

	// Loop over the Maclaurin series given above until convergence.
	for loop := newLoop("log", x, s, 40); ; {
		n.SetUnscaled(int64(loop.i + 1))
		term.QuoRound(yN, n, s+2, inf.RoundHalfUp)
		z.Sub(z, term)
		if loop.done(z) {
			break
		}
		// Advance y**index (multiply by y).
		yN.Mul(yN, y)
	}

	if invert {
		z.Neg(z)
	}
	z.Add(z, exp)

	// Round to the desired scale.
	return z.Round(z, s, inf.RoundHalfUp)
}

// For integers we use exponentiation by squaring.
// See: https://en.wikipedia.org/wiki/Exponentiation_by_squaring
func integerPower(z, x *inf.Dec, y int64, s inf.Scale) *inf.Dec {
	if z == nil {
		z = new(inf.Dec)
	}

	neg := y < 0
	if neg {
		y = -y
	}

	z.Set(decimalOne)
	for y > 0 {
		if y%2 == 1 {
			z = z.Mul(z, x)
		}
		y >>= 1
		x.Mul(x, x)
	}

	if neg {
		z = z.QuoRound(decimalOne, z, s+2, inf.RoundHalfUp)
	}
	return z.Round(z, s, inf.RoundHalfUp)
}

// smallExp computes z * e^x using the Taylor series to the specified scale and
// stores the result in z, which is also the return value. It should be used
// with small x values only.
func smallExp(z, x *inf.Dec, s inf.Scale) *inf.Dec {
	// Allocate if needed and make sure args aren't mutated.
	if z == nil {
		z = new(inf.Dec)
		z.SetUnscaled(1).SetScale(0)
	}
	n := new(inf.Dec)
	tmp := new(inf.Dec).Set(z)
	for loop := newLoop("exp", z, s, 1); !loop.done(z); {
		n.Add(n, decimalOne)
		tmp.Mul(tmp, x)
		tmp.QuoRound(tmp, n, s+2, inf.RoundHalfUp)
		z.Add(z, tmp)
	}
	// Round to the desired scale.
	return z.Round(z, s, inf.RoundHalfUp)
}

// Exp computes (e^n) (where n = a*b with a being an integer and b < 1)
// to the specified scale and stores the result in z, which is also the
// return value.
func Exp(z, n *inf.Dec, s inf.Scale) *inf.Dec {
	s += 2
	nn := new(inf.Dec).Set(n)
	if z == nil {
		z = new(inf.Dec)
		z.SetUnscaled(1).SetScale(0)
	} else {
		z.SetUnscaled(1).SetScale(0)
	}

	// We are computing (e^n) by splitting n into an integer and a float
	// (e.g 3.1 ==> x = 3, y = 0.1), this allows us to write
	// e^n = e^(x+y) = e^x * e^y

	// Split out x (integer(n))
	x := new(inf.Dec).Round(nn, 0, inf.RoundDown)

	// Split out y (n - x) which is < 1
	y := new(inf.Dec).Sub(nn, x)

	// convert x to integer
	integer, ok := x.Unscaled()
	if !ok {
		panic("integer out of range")
	}

	ex := integerPower(z, new(inf.Dec).Set(e), integer, s+2)
	return smallExp(ex, y, s-2)
}

// Pow computes (x^y) as e^(y ln x) to the specified scale and stores
// the result in z, which is also the return value. If y is not an
// integer and x is negative nil is returned.
func Pow(z, x, y *inf.Dec, s inf.Scale) *inf.Dec {
	s = s + 2
	if z == nil {
		z = new(inf.Dec)
		z.SetUnscaled(1).SetScale(0)
	}

	// Check if y is of type int.
	tmp := new(inf.Dec).Abs(y)
	isInt := tmp.Cmp(new(inf.Dec).Round(tmp, 0, inf.RoundDown)) == 0

	neg := x.Sign() < 0

	if !isInt && neg {
		return nil
	}

	// Exponent Precision Explanation (RaduBerinde):
	// Say we compute the Log with a scale of k. That means that the result we get is:
	// ln x +/- 10^-k.
	// This leads to an error of y * 10^-k in the exponent, which leads to a
	// multiplicative error of e^(y*10^-k) in the result.
	// For small values of u, e^u can be approximated by 1 + u, so for large k
	// that error is around 1 + y*10^-k. So the additive error will be x^y * y * 10^-k,
	// and we want this to be less than 10^-s. This approximately means that k has to be
	// s + the number of digits before the decimal point in x^y. Which roughly is
	//
	// s + <the number of digits before decimal point in x> * y.
	//
	// exponent precision = s + <the number of digits before decimal point in x> * y.
	numDigits := float64(x.UnscaledBig().BitLen()) / digitsToBitsRatio
	numDigits -= float64(x.Scale())

	// Round up y which should provide us with a threshold in calculating the new scale.
	yu := float64(new(inf.Dec).Round(y, 0, inf.RoundUp).UnscaledBig().Int64())

	// exponent precision = s + <the number of digits before decimal point in x> * y
	es := s + inf.Scale(int32(numDigits*yu))

	tmp = new(inf.Dec).Abs(x)
	Log(tmp, tmp, es)
	tmp.Mul(tmp, y)
	Exp(tmp, tmp, es)

	if neg && y.Round(y, 0, inf.RoundDown).UnscaledBig().Bit(0) == 1 {
		tmp.Neg(tmp)
	}

	// Round to the desired scale.
	return z.Round(tmp, s-2, inf.RoundHalfUp)
}
