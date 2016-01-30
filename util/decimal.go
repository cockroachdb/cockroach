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

