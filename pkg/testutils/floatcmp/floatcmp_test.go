// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package floatcmp

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// EqualApprox takes an interface, allowing it to compare equality of both
// primitive data types and structs. We want to test both cases.
type (
	// floatArgs holds the expected and actual values of floating point equality tests.
	floatArgs struct {
		expected float64
		actual   float64
	}

	// testStruct structs are the values compared in struct equality tests.
	testStruct struct {
		X, Y float64
		I    int
	}

	// structArgs holds the expected and actual values of struct equality tests.
	structArgs struct {
		expected testStruct
		actual   testStruct
	}

	// floatTestCase represents a test case for floating point values.
	floatTestCase struct {
		name string
		args floatArgs
		want bool
	}

	// structTestCase represents a test case for struct values.
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

// toStructTests transforms an array of floatTestCases into an array of structTestCases by
// copying the expected and actual values of each floatTestCase into corresponding values
// in each structTestCase.
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

// TestFloatsMatch is a unit test for FloatsMatch() and FloatsMatchApprox()
// functions.
func TestFloatsMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		f1, f2 string
		match  bool
	}{
		{f1: "NaN", f2: "+Inf", match: false},
		{f1: "+Inf", f2: "+Inf", match: true},
		{f1: "NaN", f2: "NaN", match: true},
		{f1: "+Inf", f2: "-Inf", match: false},
		{f1: "-0.0", f2: "0.0", match: true},
		{f1: "0.0", f2: "NaN", match: false},
		{f1: "123.45", f2: "12.345", match: false},
		{f1: "0.1234567890123456", f2: "0.1234567890123455", match: true},
		{f1: "0.1234567890123456", f2: "0.1234567890123457", match: true},
		{f1: "-0.1234567890123456", f2: "0.1234567890123456", match: false},
		{f1: "-0.1234567890123456", f2: "-0.1234567890123455", match: true},
		{f1: "0.142857142857143", f2: "0.14285714285714285", match: true},
		{f1: "NULL", f2: "0.14285714285714285", match: false},
		{f1: "NULL", f2: "NULL", match: true},
	} {
		match, err := FloatsMatch(tc.f1, tc.f2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("FloatsMatch: wrong result on %v", tc)
		}

		match, err = FloatsMatchApprox(tc.f1, tc.f2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("FloatsMatchApprox: wrong result on %v", tc)
		}
	}
}

// TestFloatArraysMatch is a unit test for FloatArraysMatch() and
// FloatArraysMatchApprox() functions.
//
// Note that since these functions use FloatsMatch and FloatsMatchApprox
// internally, some edge cases like NaN and infinities aren't tested here.
func TestFloatArraysMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		arr1, arr2 string
		match      bool
	}{
		{arr1: "NULL", arr2: "NULL", match: true},
		{arr1: "NULL", arr2: "{NULL}", match: false},
		{arr1: "{NULL}", arr2: "NULL", match: false},
		{arr1: "{NULL}", arr2: "{NULL}", match: true},
		{arr1: "NULL", arr2: "{0}", match: false},
		{arr1: "{0}", arr2: "NULL", match: false},
		{arr1: "{NULL,NULL}", arr2: "{NULL,NULL}", match: true},
		{arr1: "{NULL,NULL,NULL}", arr2: "{NULL,NULL}", match: false},
		{arr1: "{-0.0,0.0}", arr2: "{0.0,-0.0}", match: true},
		{arr1: "{0.1,0.2,0.3}", arr2: "{0.1,0.2,0.3}", match: true},
	} {
		match, err := FloatArraysMatch(tc.arr1, tc.arr2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("FloatArraysMatch: wrong result on %v", tc)
		}

		match, err = FloatArraysMatchApprox(tc.arr1, tc.arr2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("FloatArraysMatchApprox: wrong result on %v", tc)
		}
	}
}
