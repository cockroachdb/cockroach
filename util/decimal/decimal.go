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
// the value and any error that occured. This converson exposes a
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
