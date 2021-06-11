// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var castableCanonicalTypeFamilies = make(map[types.Family][]types.Family)

// castOutputTypes contains "fake" output types of a cast operator. It is used
// just to make overloads initialization happy and is a bit of a hack - we
// represent cast overloads as twoArgsOverload (meaning it takes in two
// arguments and returns a third value), whereas it'll actually take in one
// argument (left) and will return the result of a cast to another argument's
// type (right), so the "true" return type is stored in right overload which is
// populated correctly.
var castOutputTypes = make(map[typePair]*types.T)

func registerCastOutputTypes() {
	for _, leftFamily := range supportedCanonicalTypeFamilies {
		for _, leftWidth := range supportedWidthsByCanonicalTypeFamily[leftFamily] {
			for _, rightFamily := range castableCanonicalTypeFamilies[leftFamily] {
				for _, rightWidth := range supportedWidthsByCanonicalTypeFamily[rightFamily] {
					castOutputTypes[typePair{leftFamily, leftWidth, rightFamily, rightWidth}] = types.Bool
				}
			}
		}
	}
}

func populateCastOverloads() {
	registerCastTypeCustomizers()
	registerCastOutputTypes()
	populateTwoArgsOverloads(
		&overloadBase{
			kind: castOverload,
			Name: "cast",
		},
		castOutputTypes,
		func(lawo *lastArgWidthOverload, customizer typeCustomizer) {
			if b, ok := customizer.(castTypeCustomizer); ok {
				lawo.CastFunc = b.getCastFunc()
			}
		}, castTypeCustomizers)
}

// toDecimal returns the templated code that performs the cast to a decimal. It
// first will execute whatever is passed in 'conv' (the main conversion) and
// then will perform the rounding of 'to' variable according to 'toType'.
func toDecimal(conv, to, toType string) string {
	convStr := `
		%[1]s
		if err := tree.LimitDecimalWidth(&%[2]s, int(%[3]s.Precision()), int(%[3]s.Scale())); err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return fmt.Sprintf(convStr, conv, to, toType)
}

func intToDecimal(to, from, _, toType string) string {
	conv := `
		%[1]s.SetInt64(int64(%[2]s))
	`
	return toDecimal(fmt.Sprintf(conv, to, from), to, toType)
}

func intToFloat() func(string, string, string, string) string {
	return func(to, from, _, _ string) string {
		convStr := `
			%[1]s = float64(%[2]s)
			`
		return fmt.Sprintf(convStr, to, from)
	}
}

// getIntToIntCastFunc returns a castFunc between integers of any widths.
func getIntToIntCastFunc(fromWidth, toWidth int32) castFunc {
	if fromWidth == anyWidth {
		fromWidth = 64
	}
	if toWidth == anyWidth {
		toWidth = 64
	}
	return func(to, from, _, _ string) string {
		if fromWidth <= toWidth {
			// If we're not reducing the width, there is no need to perform the
			// integer range check.
			return fmt.Sprintf("%s = int%d(%s)", to, toWidth, from)
		}
		// convStr is a format string expecting five arguments:
		// 1. the result variable name
		// 2. the original value variable name
		// 3. the result width
		// 4. the result width in bytes (not in bits, e.g. 2 for INT2)
		// 5. the result width minus one.
		//
		// We're performing range checks in line with Go's implementation of
		// math.(Max|Min)(16|32) numbers that store the boundaries of the
		// allowed range.
		// NOTE: when updating the code below, make sure to update tree/casts.go
		// as well.
		convStr := `
		shifted := %[2]s >> uint(%[5]d)
		if (%[2]s >= 0 && shifted > 0) || (%[2]s < 0 && shifted < -1) {
			colexecerror.ExpectedError(tree.ErrInt%[4]dOutOfRange)
		}
		%[1]s = int%[3]d(%[2]s)
	`
		return fmt.Sprintf(convStr, to, from, toWidth, toWidth/8, toWidth-1)
	}
}

func floatToInt(intWidth, floatWidth int32) func(string, string, string, string) string {
	return func(to, from, _, _ string) string {
		convStr := `
			if math.IsNaN(float64(%[2]s)) || %[2]s <= float%[4]d(math.MinInt%[3]d) || %[2]s >= float%[4]d(math.MaxInt%[3]d) {
				colexecerror.ExpectedError(tree.ErrIntOutOfRange)
			}
			%[1]s = int%[3]d(%[2]s)
		`
		if intWidth == anyWidth {
			intWidth = 64
		}
		return fmt.Sprintf(convStr, to, from, intWidth, floatWidth)
	}
}

func numToBool(to, from, _, _ string) string {
	convStr := `
		%[1]s = %[2]s != 0
	`
	return fmt.Sprintf(convStr, to, from)
}

func floatToDecimal(to, from, _, toType string) string {
	convStr := `
		if _, err := %[1]s.SetFloat64(float64(%[2]s)); err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return toDecimal(fmt.Sprintf(convStr, to, from), to, toType)
}

func decimalToBool(to, from, _, _ string) string {
	return fmt.Sprintf("%[1]s = %[2]s.Sign() != 0", to, from)
}

func getDecimalToIntCastFunc(toIntWidth int32) castFunc {
	if toIntWidth == anyWidth {
		toIntWidth = 64
	}
	return func(to, from, fromCol, toType string) string {
		// convStr is a format string expecting three arguments:
		// 1. the code snippet that performs an assigment of int64 local
		//    variable named '_i' to the result, possibly performing the bounds
		//    checks
		// 2. the original value variable name
		// 3. the name of the global variable storing the error to be emitted
		//    when the decimal is out of range for the desired int width.
		//
		// NOTE: when updating the code below, make sure to update tree/casts.go
		// as well.
		convStr := `
		{
			tmpDec := &_overloadHelper.TmpDec1
			_, err := tree.DecimalCtx.RoundToIntegralValue(tmpDec, &%[2]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			_i, err := tmpDec.Int64()
			if err != nil {
				colexecerror.ExpectedError(%[3]s)
			}
			%[1]s
		}
	`
		errOutOfRange := "tree.ErrIntOutOfRange"
		if toIntWidth != 64 {
			errOutOfRange = fmt.Sprintf("tree.ErrInt%dOutOfRange", toIntWidth/8)
		}
		return fmt.Sprintf(
			convStr,
			getIntToIntCastFunc(64 /* fromWidth */, toIntWidth)(to, "_i" /* from */, fromCol, toType),
			from,
			errOutOfRange,
		)
	}
}

func decimalToFloat(to, from, _, _ string) string {
	convStr := `
		{
			f, err := %[2]s.Float64()
			if err != nil {
				colexecerror.ExpectedError(tree.ErrFloatOutOfRange)
			}
			%[1]s = f
		}
`
	return fmt.Sprintf(convStr, to, from)
}

func decimalToDecimal(to, from, _, toType string) string {
	return toDecimal(fmt.Sprintf(`%[1]s.Set(&%[2]s)`, to, from), to, toType)
}

func datumToBool(to, from, fromCol, _ string) string {
	convStr := `
		{
			_castedDatum, err := %[2]s.(*coldataext.Datum).Cast(%[3]s, types.Bool)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%[1]s = _castedDatum == tree.DBoolTrue
		}
	`
	return fmt.Sprintf(convStr, to, from, fromCol)
}

func datumToDatum(to, from, fromCol, toType string) string {
	convStr := `
		{
			_castedDatum, err := %[2]s.(*coldataext.Datum).Cast(%[3]s, %[4]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%[1]s = _castedDatum
		}
	`
	return fmt.Sprintf(convStr, to, from, fromCol, toType)
}

// castTypeCustomizer is a type customizer that changes how the templater
// produces cast operator output for a particular type.
type castTypeCustomizer interface {
	getCastFunc() castFunc
}

var castTypeCustomizers = make(map[typePair]typeCustomizer)

func registerCastTypeCustomizer(pair typePair, customizer typeCustomizer) {
	if _, found := castTypeCustomizers[pair]; found {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly cast type customizer already present for %v", pair))
	}
	castTypeCustomizers[pair] = customizer
	alreadyPresent := false
	for _, rightFamily := range castableCanonicalTypeFamilies[pair.leftTypeFamily] {
		if rightFamily == pair.rightTypeFamily {
			alreadyPresent = true
			break
		}
	}
	if !alreadyPresent {
		castableCanonicalTypeFamilies[pair.leftTypeFamily] = append(
			castableCanonicalTypeFamilies[pair.leftTypeFamily], pair.rightTypeFamily,
		)
	}
}

func registerCastTypeCustomizers() {
	// Identity casts.
	//
	// Note that we're using the same "vanilla" type customizers since identity
	// casts are the default behavior of the CastFunc (except for decimals and
	// datum-backed types which are handled separately).
	registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.BoolFamily, anyWidth}, boolCustomizer{})
	// TODO(yuzefovich): add casts between types that have types.BytesFamily as
	// their canonical type family.
	registerCastTypeCustomizer(typePair{types.FloatFamily, anyWidth, types.FloatFamily, anyWidth}, floatCustomizer{})
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerCastTypeCustomizer(typePair{types.IntFamily, intWidth, types.IntFamily, intWidth}, intCustomizer{width: intWidth})
	}
	// TODO(yuzefovich): add casts for Timestamps, Intervals, and datum-backed
	// types.

	// Casts from boolean.
	registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.FloatFamily, anyWidth}, boolCastCustomizer{})
	for _, intWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerCastTypeCustomizer(typePair{types.BoolFamily, anyWidth, types.IntFamily, intWidth}, boolCastCustomizer{})
	}

	// Casts from decimal.
	registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.BoolFamily, anyWidth}, decimalCastCustomizer{toFamily: types.BoolFamily})
	for _, toIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.IntFamily, toIntWidth}, decimalCastCustomizer{toFamily: types.IntFamily, toWidth: toIntWidth})
	}
	registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.FloatFamily, anyWidth}, decimalCastCustomizer{toFamily: types.FloatFamily})
	// Note that we have the decimal "identity" cast here in order to handle
	// possible rounding of the precision.
	registerCastTypeCustomizer(typePair{types.DecimalFamily, anyWidth, types.DecimalFamily, anyWidth}, decimalCastCustomizer{toFamily: types.DecimalFamily})

	// Casts from ints.
	for _, fromIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
		// Casts between ints.
		for _, toIntWidth := range supportedWidthsByCanonicalTypeFamily[types.IntFamily] {
			if fromIntWidth != toIntWidth {
				registerCastTypeCustomizer(typePair{types.IntFamily, fromIntWidth, types.IntFamily, toIntWidth}, intCastCustomizer{fromWidth: fromIntWidth, toFamily: types.IntFamily, toWidth: toIntWidth})
			}
		}
		// Casts to other types.
		for _, toFamily := range []types.Family{types.BoolFamily, types.DecimalFamily, types.FloatFamily} {
			registerCastTypeCustomizer(typePair{types.IntFamily, fromIntWidth, toFamily, anyWidth}, intCastCustomizer{toFamily: toFamily, toWidth: anyWidth})
		}
	}

	// Casts from float.
	for _, toFamily := range []types.Family{types.BoolFamily, types.DecimalFamily, types.IntFamily} {
		for _, toWidth := range supportedWidthsByCanonicalTypeFamily[toFamily] {
			registerCastTypeCustomizer(typePair{types.FloatFamily, anyWidth, toFamily, toWidth}, floatCastCustomizer{toFamily: toFamily, toWidth: toWidth})
		}
	}

	// Casts from datum-backed types.
	registerCastTypeCustomizer(typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, types.BoolFamily, anyWidth}, datumCastCustomizer{toFamily: types.BoolFamily})
	registerCastTypeCustomizer(typePair{typeconv.DatumVecCanonicalTypeFamily, anyWidth, typeconv.DatumVecCanonicalTypeFamily, anyWidth}, datumCastCustomizer{toFamily: typeconv.DatumVecCanonicalTypeFamily})
}

// boolCastCustomizer specifies casts from booleans.
type boolCastCustomizer struct{}

// decimalCastCustomizer specifies casts from decimals.
type decimalCastCustomizer struct {
	toFamily types.Family
	toWidth  int32
}

// floatCastCustomizer specifies casts from floats.
type floatCastCustomizer struct {
	toFamily types.Family
	toWidth  int32
}

// intCastCustomizer specifies casts from ints to other types.
type intCastCustomizer struct {
	fromWidth int32
	toFamily  types.Family
	toWidth   int32
}

// datumCastCustomizer specifies casts from types that are backed by tree.Datum
// to other types.
type datumCastCustomizer struct {
	toFamily types.Family
}

func (boolCastCustomizer) getCastFunc() castFunc {
	return func(to, from, _, _ string) string {
		convStr := `
			%[1]s = 0
			if %[2]s {
				%[1]s = 1
			}
		`
		return fmt.Sprintf(convStr, to, from)
	}
}

func (c decimalCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return decimalToBool
	case types.IntFamily:
		return getDecimalToIntCastFunc(c.toWidth)
	case types.FloatFamily:
		return decimalToFloat
	case types.DecimalFamily:
		return decimalToDecimal
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find a cast from decimal to %s", c.toFamily))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c floatCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return numToBool
	case types.DecimalFamily:
		return floatToDecimal
	case types.IntFamily:
		return floatToInt(c.toWidth, 64)
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find a cast from float to %s with %d width", c.toFamily, c.toWidth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c intCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return numToBool
	case types.DecimalFamily:
		return intToDecimal
	case types.IntFamily:
		return getIntToIntCastFunc(c.fromWidth, c.toWidth)
	case types.FloatFamily:
		return intToFloat()
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find a cast from int to %s with %d width", c.toFamily, c.toWidth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c datumCastCustomizer) getCastFunc() castFunc {
	switch c.toFamily {
	case types.BoolFamily:
		return datumToBool
	case typeconv.DatumVecCanonicalTypeFamily:
		return datumToDatum
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpectedly didn't find a cast from datum-backed type to %s", c.toFamily))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
