// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package floatcmp

import (
	"math"
	"testing"
)

type (
	floatArgs struct {
		expected float64
		actual   float64
	}
	testStruct struct {
		X, Y float64
		I    int
	}
	structArgs struct {
		expected testStruct
		actual   testStruct
	}
	floatTestCase struct {
		name string
		args floatArgs
		want bool
	}
	structTestCase struct {
		name string
		args structArgs
		want bool
	}
)

var floatTests = []floatTestCase{
	{
		name: "zeros",
		args: floatArgs{expected: 0, actual: 0},
		want: true,
	},
	{
		name: "NaNs",
		args: floatArgs{expected: math.NaN(), actual: math.NaN()},
		want: true,
	},
	{
		name: "zero not close to NaN",
		args: floatArgs{expected: 0, actual: math.NaN()},
		want: false,
	},
	{
		name: "positive infinities",
		args: floatArgs{expected: math.Inf(+1), actual: math.Inf(+1)},
		want: true,
	},
	{
		name: "negative infinities",
		args: floatArgs{expected: math.Inf(-1), actual: math.Inf(-1)},
		want: true,
	},
	{
		name: "ones",
		args: floatArgs{expected: 1, actual: 1},
		want: true,
	},
	{
		name: "signs",
		args: floatArgs{expected: 1, actual: -1},
		want: false,
	},
	{
		name: "different",
		args: floatArgs{expected: 1, actual: 2},
		want: false,
	},
	{
		name: "close to zero",
		args: floatArgs{expected: 0, actual: math.Nextafter(0+CloseMargin, math.Inf(-1))},
		want: true,
	},
	{
		name: "not close to zero",
		args: floatArgs{expected: 0, actual: math.Nextafter(0+CloseMargin, math.Inf(+1))},
		want: false,
	},
	{
		name: "close to CloseFraction",
		args: floatArgs{expected: CloseFraction, actual: math.Nextafter(CloseFraction+CloseMargin, math.Inf(-1))},
		want: true,
	},
	{
		name: "not close to CloseFraction",
		args: floatArgs{expected: CloseFraction, actual: math.Nextafter(CloseFraction+CloseMargin, math.Inf(+1))},
		want: false,
	},
	{
		name: "close to one",
		args: floatArgs{expected: 1, actual: math.Nextafter(1+1*CloseFraction, math.Inf(-1))},
		want: true,
	},
	{
		name: "not close to one",
		args: floatArgs{expected: 1, actual: math.Nextafter(1+1*CloseFraction, math.Inf(+1))},
		want: false,
	},
}

func toStructTests(floatTestCases []floatTestCase) []structTestCase {
	structTestCases := make([]structTestCase, 0, len(floatTestCases))
	for _, ft := range floatTestCases {
		structTestCases = append(structTestCases,
			structTestCase{
				name: ft.name + " struct",
				args: structArgs{expected: testStruct{X: ft.args.expected, Y: ft.args.expected, I: 0}, actual: testStruct{X: ft.args.actual, Y: ft.args.actual, I: 0}},
				want: ft.want,
			})
	}
	return structTestCases
}

func TestEqualClose(t *testing.T) {
	for _, tt := range floatTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualApprox(tt.args.expected, tt.args.actual, CloseFraction, CloseMargin); got != tt.want {
				t.Errorf("Close(%.16e, %.16e) = %v, want %v", tt.args.expected, tt.args.actual, got, tt.want)
			}
		})
	}
	for _, tt := range toStructTests(floatTests) {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualApprox(tt.args.expected, tt.args.actual, CloseFraction, CloseMargin); got != tt.want {
				t.Errorf("Close(%.v, %.v) = %v, want %v", tt.args.expected, tt.args.actual, got, tt.want)
			}
		})
	}
}
