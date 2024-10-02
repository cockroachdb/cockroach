// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package floatcmp provides functions for determining float values to be equal
// if they are within a tolerance. It is designed to be used in tests.
package floatcmp

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	// CloseFraction can be used to set a "close" tolerance for the fraction
	// argument of functions in this package. It should typically be used with
	// the CloseMargin constant for the margin argument. Its value is taken from
	// the close tolerances in go's math package.
	CloseFraction float64 = 1e-14

	// CloseMargin can be used to set a "close" tolerance for the margin
	// argument of functions in this package. It should typically be used with
	// the CloseFraction constant for the fraction argument.
	//
	// It is set to the square of CloseFraction so it is only used when the
	// smaller of the absolute expected and actual values is in the range:
	//
	//   -CloseFraction <= 0 <= CloseFraction
	//
	// CloseMargin is greater than 0 otherwise if either expected or actual were
	// 0 the calculated tolerance from the fraction would be 0.
	CloseMargin = CloseFraction * CloseFraction
)

// EqualApprox reports whether expected and actual are deeply equal with the
// following modifications for float64 and float32 types:
//
// • If both expected and actual are not NaN or infinite, they are equal within
// the larger of the relative fraction or absolute margin calculated from the
// fraction and margin arguments.
//
// • If both expected and actual are NaN, they are equal.
//
// Both fraction and margin must be non-negative.
//
// fraction is used to calculate the tolerance as a relative fraction of the
// smaller of expected and actual:
//
//	tolerance_frac = (fraction * min(|expected|, |actual|))
//
// margin specifies the tolerance as an absolute value:
//
//	tolerance_marg = margin
//
// The tolerance used to determine approximate equality is:
//
//	tolerance = max(tolerance_frac, tolerance_marg)
//
// To use only one of fraction or margin, set the other to 0.
//
// For comparing expected and actual values in tests, typically the fraction
// should be set to the smallest relative fraction to tolerate. The margin
// should be set to a much smaller value so that it is only used when:
//
//	(fraction * min(|expected|, |actual|)) < margin
//
// which allows expected and actual to be approximately equal within margin when
// either is 0.
func EqualApprox(expected interface{}, actual interface{}, fraction float64, margin float64) bool {
	return cmp.Equal(expected, actual, cmpopts.EquateApprox(fraction, margin), cmpopts.EquateNaNs())
}

// FloatsMatchApprox returns whether two floating point represented as
// strings are equal within a tolerance.
func FloatsMatchApprox(expectedString, actualString string) (bool, error) {
	if expectedString == "NULL" || actualString == "NULL" {
		// Default to string matching for NULL, since it can't be parsed as a float.
		return expectedString == actualString, nil
	}
	expected, actual, err := parseTwoFloats(expectedString, actualString)
	if err != nil {
		return false, err
	}
	return EqualApprox(expected, actual, CloseFraction, CloseMargin), nil
}

// splitFloatArray splits the textual representation of the float array of the
// form {1.2,3.4,5.6,...} into its elements (in the textual form).
func splitFloatArray(array string) []string {
	return strings.Split(strings.Trim(array, "{}"), ",")
}

// floatArraysMatchImpl is the common logic for verifying whether two float
// arrays match where per-element verification is done by the provided function.
func floatArraysMatchImpl(
	expected, actual string, matchFn func(string, string) (bool, error),
) (bool, error) {
	if expected == "NULL" || actual == "NULL" {
		// Default to string matching for NULL arrays.
		return expected == actual, nil
	}
	exp, act := splitFloatArray(expected), splitFloatArray(actual)
	if len(exp) != len(act) {
		return false, nil
	}
	for i := range exp {
		match, err := matchFn(exp[i], act[i])
		if !match || err != nil {
			return false, err
		}
	}
	return true, nil
}

// FloatArraysMatchApprox returns whether two floating point arrays represented
// as strings are equal within a tolerance.
func FloatArraysMatchApprox(expectedString, actualString string) (bool, error) {
	return floatArraysMatchImpl(expectedString, actualString, FloatsMatchApprox)
}

// FloatsCmp returns -1 if aString is less than bString, 0 if they are equal,
// and 1 otherwise when aString and bString are parsed as SQL floats. NULL
// sorts before NaN, which sorts before all other numbers.
func FloatsCmp(aString, bString string) (int, error) {
	if aString == "NULL" {
		if bString != "NULL" {
			return -1, nil
		}
		return 0, nil
	} else if bString == "NULL" {
		return 1, nil
	}

	a, b, err := parseTwoFloats(aString, bString)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(a) {
		if !math.IsNaN(b) {
			return -1, nil
		}
		return 0, nil
	} else if math.IsNaN(b) {
		return 1, nil
	}

	if a < b {
		return -1, nil
	} else if a == b {
		return 0, nil
	} else {
		return 1, nil
	}
}

// FloatArraysCmp returns -1 if aString is less than bString, 0 if they are
// equal, and 1 otherwise when aString and bString are parsed as SQL float
// arrays. NULL sorts before NaN, which sorts before all other numbers.
func FloatArraysCmp(aString, bString string) (int, error) {
	if aString == "NULL" {
		if bString != "NULL" {
			return -1, nil
		}
		return 0, nil
	} else if bString == "NULL" {
		return 1, nil
	}
	aFloats, bFloats := splitFloatArray(aString), splitFloatArray(bString)
	// Comparison of arrays is such that we check the "shared" elements first,
	// and only if all of them are equal, then we check the lengths of arrays
	// (see tree.DArray.Compare).
	toCheck := min(len(aFloats), len(bFloats))
	for i := 0; i < toCheck; i++ {
		cmp, err := FloatsCmp(aFloats[i], bFloats[i])
		if cmp != 0 || err != nil {
			return cmp, err
		}
	}
	if len(aFloats) < len(bFloats) {
		return -1, nil
	} else if len(aFloats) > len(bFloats) {
		return 1, nil
	}
	return 0, nil
}

// FloatsMatch returns whether two floating point numbers represented as
// strings have matching 15 significant decimal digits (this is the precision
// that Postgres supports for 'double precision' type).
func FloatsMatch(expectedString, actualString string) (bool, error) {
	if expectedString == "NULL" || actualString == "NULL" {
		// Default to string matching for NULL, since it can't be parsed as a float.
		return expectedString == actualString, nil
	}
	expected, actual, err := parseTwoFloats(expectedString, actualString)
	if err != nil {
		return false, err
	}
	// Check special values - NaN, +Inf, -Inf, 0, -0.
	if math.IsNaN(expected) || math.IsNaN(actual) {
		return math.IsNaN(expected) == math.IsNaN(actual), nil
	}
	if math.IsInf(expected, 0 /* sign */) || math.IsInf(actual, 0 /* sign */) {
		bothNegativeInf := math.IsInf(expected, -1 /* sign */) == math.IsInf(actual, -1 /* sign */)
		bothPositiveInf := math.IsInf(expected, 1 /* sign */) == math.IsInf(actual, 1 /* sign */)
		return bothNegativeInf || bothPositiveInf, nil
	}
	if expected == 0 || actual == 0 {
		return expected == actual, nil
	}
	// Check that the numbers have the same sign.
	if expected*actual < 0 {
		return false, nil
	}
	expected = math.Abs(expected)
	actual = math.Abs(actual)
	// Check that 15 significant digits match. We do so by normalizing the
	// numbers and then checking one digit at a time.
	//
	// normalize converts f to base * 10**power representation where base is in
	// [1.0, 10.0) range.
	normalize := func(f float64) (base float64, power int) {
		for f >= 10 {
			f = f / 10
			power++
		}
		for f < 1 {
			f *= 10
			power--
		}
		return f, power
	}
	var expPower, actPower int
	expected, expPower = normalize(expected)
	actual, actPower = normalize(actual)
	if expPower != actPower {
		return false, nil
	}
	// TODO(yuzefovich): investigate why we can't always guarantee deterministic
	// 15 significant digits and switch back from 14 to 15 digits comparison
	// here. See #56446 for more details.
	for i := 0; i < 14; i++ {
		expDigit := int(expected)
		actDigit := int(actual)
		if expDigit != actDigit {
			return false, nil
		}
		expected -= (expected - float64(expDigit)) * 10
		actual -= (actual - float64(actDigit)) * 10
	}
	return true, nil
}

// FloatArraysMatch returns whether two floating point number arrays represented
// as strings have matching 15 significant decimal digits (this is the precision
// that Postgres supports for 'double precision' type) for each element.
func FloatArraysMatch(expectedString, actualString string) (bool, error) {
	return floatArraysMatchImpl(expectedString, actualString, FloatsMatch)
}

// RoundFloatsInString rounds floats in a given string to the given number of significant figures.
func RoundFloatsInString(s string, significantFigures int) string {
	return string(regexp.MustCompile(`(\d+\.\d+)`).ReplaceAllFunc([]byte(s), func(x []byte) []byte {
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return []byte(err.Error())
		}
		formatSpecifier := "%." + fmt.Sprintf("%dg", significantFigures)
		return []byte(fmt.Sprintf(formatSpecifier, f))
	}))
}

// ParseRoundInStringsDirective parses the directive and returns the number of
// significant figures to round floats to.
func ParseRoundInStringsDirective(directive string) (int, error) {
	kv := strings.Split(directive, "=")
	if len(kv) == 1 {
		// Use 6 significant figures by default.
		return 6, nil
	}
	return strconv.Atoi(kv[1])
}

// parseTwoFloats converts the strings aString and bString to float64 values.
func parseTwoFloats(aString, bString string) (float64, float64, error) {
	a, err := strconv.ParseFloat(aString, 64 /* bitSize */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "when parsing aString")
	}
	b, err := strconv.ParseFloat(bString, 64 /* bitSize */)
	if err != nil {
		return 0, 0, errors.Wrap(err, "when parsing bString")
	}
	return a, b, nil
}
