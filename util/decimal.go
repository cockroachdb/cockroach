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
	"fmt"
	"math"
	"strconv"
	"strings"

	"gopkg.in/inf.v0"
)

// DecimalPrecision defines the minimum precision all inexact decimal
// calculations should attempt to achieve.
const DecimalPrecision = 16

// Read-only constants used for compuation.
var decimalHalf = inf.NewDec(5, 1)

// NewDecFromFloat allocates and returns a new Dec set to the given
// float64 value. The function will panic if the float is NaN or ±Inf.
func NewDecFromFloat(f float64) *inf.Dec {
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

	return inf.NewDec(mant, inf.Scale(-e10))
}

// Float64FromDec converts a decimal to a float64 value, returning
// the value and any error that occured. This converson exposes a
// possible loss of information.
func Float64FromDec(dec *inf.Dec) (float64, error) {
	return strconv.ParseFloat(dec.String(), 64)
}

// DecMod performs the modulo arithmatic x % y and stores the
// result in z, which is also the return value. It is valid for z
// to be nil, in which case it will be allocated internally.
// DecMod will panic if the y is zero.
//
// The modulo calculation is implemented using the algorithm:
//     x % y = x - (y * ⌊x / y⌋).
func DecMod(z, x, y *inf.Dec) *inf.Dec {
	switch z {
	case nil:
		z = new(inf.Dec)
	case x:
		x = new(inf.Dec)
		x.Set(z)
	case y:
		y = new(inf.Dec)
		y.Set(z)
	}
	z.QuoRound(x, y, 0, inf.RoundDown)
	return z.Sub(x, z.Mul(z, y))
}

// DecSqrt calculates the square root of x to the specified scale
// and stores the result in z, which is also the return value.
// The function will panic if x is a negative number.
//
// The square root calculation is implemented using Newton's Method.
// We start with an initial estimate for sqrt(d), and then iterate:
//     x_{n+1} = 1/2 * ( x_n + (d / x_n) ).
func DecSqrt(z, x *inf.Dec, s inf.Scale) *inf.Dec {
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

	// Determine the maximum number of iterations needed.
	scale := len(x.UnscaledBig().String())
	if x.Scale() < 0 {
		scale -= int(x.Scale())
	}
	steps := scale + int(s)

	// Iterate.
	tmp := new(inf.Dec)
	for i := 0; i <= steps; i++ {
		tmp.QuoRound(x, z, s, inf.RoundHalfUp) // t = d / x_n
		tmp.Add(tmp, z)                        // t = x_n + (d / x_n)
		z.Mul(tmp, decimalHalf)                // x_{n+1} = 0.5 * t
	}

	// Round to the desired scale.
	return z.Round(z, s, inf.RoundHalfUp)
}
