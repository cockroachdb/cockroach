// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// castInfos contains supported "from -> to" mappings. This mapping has to be on
// the actual types and not the canonical type families.
//
// The information in this struct must be structured in such a manner that all
// supported casts:
// 1.  from the same type family are contiguous
// 2.  for a fixed "from" type family, all the same "from" widths are contiguous
// 2'. for a fixed "from" type family, anyWidth "from" width is the last one
// 3.  for a fixed "from" type, all the same "to" type families are contiguous
// 4.  for a fixed "from" type and a fixed "to" type family, anyWidth "to" width
//     is the last one.
//
// If this structure is broken, then the generated code will not compile because
// either
// 1. there will be duplicate entries in the switch statements (when
//    "continuity" requirement is broken)
// 2. the 'default' case appears before other in the switch statements (when
//    anyWidth is not the last one).
var castInfos = []supportedCastInfo{
	{types.Bool, types.Float, boolToIntOrFloat},
	{types.Bool, types.Int2, boolToIntOrFloat},
	{types.Bool, types.Int4, boolToIntOrFloat},
	{types.Bool, types.Int, boolToIntOrFloat},

	{types.Decimal, types.Bool, decimalToBool},
	{types.Decimal, types.Int2, getDecimalToIntCastFunc(16)},
	{types.Decimal, types.Int4, getDecimalToIntCastFunc(32)},
	{types.Decimal, types.Int, getDecimalToIntCastFunc(anyWidth)},
	{types.Decimal, types.Float, decimalToFloat},
	{types.Decimal, types.Decimal, decimalToDecimal},

	{types.Int2, types.Int4, getIntToIntCastFunc(16, 32)},
	{types.Int2, types.Int, getIntToIntCastFunc(16, anyWidth)},
	{types.Int2, types.Bool, numToBool},
	{types.Int2, types.Decimal, intToDecimal},
	{types.Int2, types.Float, intToFloat},
	{types.Int4, types.Int2, getIntToIntCastFunc(32, 16)},
	{types.Int4, types.Int, getIntToIntCastFunc(32, anyWidth)},
	{types.Int4, types.Bool, numToBool},
	{types.Int4, types.Decimal, intToDecimal},
	{types.Int4, types.Float, intToFloat},
	{types.Int, types.Int2, getIntToIntCastFunc(anyWidth, 16)},
	{types.Int, types.Int4, getIntToIntCastFunc(anyWidth, 32)},
	{types.Int, types.Bool, numToBool},
	{types.Int, types.Decimal, intToDecimal},
	{types.Int, types.Float, intToFloat},

	{types.Float, types.Bool, numToBool},
	{types.Float, types.Decimal, floatToDecimal},
	{types.Float, types.Int2, floatToInt(16, 64 /* floatWidth */)},
	{types.Float, types.Int4, floatToInt(32, 64 /* floatWidth */)},
	{types.Float, types.Int, floatToInt(anyWidth, 64 /* floatWidth */)},

	// Dates are represented as int64, and we currently mistakenly support dates
	// outside of the range (#40354), so the casts from dates and from ints turn
	// out to be the same.
	// TODO(yuzefovich): add the checks for these casts that dates are finite.
	{types.Date, types.Int2, getIntToIntCastFunc(64 /* fromWidth */, 16 /* toWidth */)},
	{types.Date, types.Int4, getIntToIntCastFunc(64 /* fromWidth */, 32 /* toWidth */)},
	{types.Date, types.Int, getIntToIntCastFunc(64 /* fromWidth */, anyWidth)},
	{types.Date, types.Float, intToFloat},
	{types.Date, types.Decimal, intToDecimal},
}

type supportedCastInfo struct {
	from *types.T
	to   *types.T
	cast castFunc
}

func boolToIntOrFloat(to, from, _, _ string) string {
	convStr := `
			%[1]s = 0
			if %[2]s {
				%[1]s = 1
			}
		`
	return fmt.Sprintf(convStr, to, from)
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

func numToBool(to, from, _, _ string) string {
	convStr := `
		%[1]s = %[2]s != 0
	`
	return fmt.Sprintf(convStr, to, from)
}

func intToDecimal(to, from, _, toType string) string {
	conv := `
		%[1]s.SetInt64(int64(%[2]s))
	`
	return toDecimal(fmt.Sprintf(conv, to, from), to, toType)
}

func intToFloat(to, from, _, _ string) string {
	convStr := `
		%[1]s = float64(%[2]s)
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

// TODO(yuzefovich): add support for casts from datum-backed types. It is
// probably better to handle them separately from all other casts.
var _ = datumToBool
var _ = datumToDatum

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

// The structs below form 4-leveled hierarchy (similar to two-argument
// overloads) and only the bottom level has the access to a castFunc.
//
// The template is expected to have the following structure in order to
// "resolve" the cast overload:
//
//   switch fromType.Family() {
//     // Choose concrete castFromTmplInfo and iterate over Widths field.
//     switch fromType.Width() {
//       // Choose concrete castFromWidthTmplInfo and iterate over To field.
//       switch toType.Family() {
//         // Choose concrete castToTmplInfo and iterate over Widths field.
//         switch toType.Width() {
//           // Finally, you have access to castToWidthTmplInfo which is the
//           // "meat" of the cast overloads.
//           <perform "resolved" actions>
//         }
//       }
//     }
//   }

type castFromTmplInfo struct {
	// TypeFamily contains the type family of the "from" type this struct is
	// handling, with "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths of the "from" type this struct is
	// handling. Note that the entry with 'anyWidth' width must be last in the
	// slice.
	Widths []castFromWidthTmplInfo
}

type castFromWidthTmplInfo struct {
	fromType *types.T
	Width    int32
	// To contains the information about the supported casts from fromType to
	// all other types.
	To []castToTmplInfo
}

type castToTmplInfo struct {
	// TypeFamily contains the type family of the "to" type this struct is
	// handling, with "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths of the "to" type this struct is
	// handling. Note that the entry with 'anyWidth' width must be last in the
	// slice.
	Widths []castToWidthTmplInfo
}

type castToWidthTmplInfo struct {
	toType    *types.T
	Width     int32
	VecMethod string
	GoType    string
	// CastFn is a function that returns a string which performs the cast
	// between fromType (from higher up the hierarchy) and toType.
	CastFn castFunc
}

func (i castFromWidthTmplInfo) VecMethod() string {
	return toVecMethod(typeconv.TypeFamilyToCanonicalTypeFamily(i.fromType.Family()), i.Width)
}

func getTypeName(typ *types.T) string {
	switch typ.Family() {
	case types.IntFamily, types.FloatFamily:
		// TODO(yuzefovich): this is a temporary exception to have smaller diff
		// for the generated code. Remove this in the follow-up commit.
		return toVecMethod(typ.Family(), typ.Width())
	}
	// typ.Name() returns the type name in the lowercase. We want to capitalize
	// the first letter (and all type names start with a letter).
	name := []byte(typ.Name())
	return string(name[0]-32) + string(name[1:])
}

func (i castFromWidthTmplInfo) TypeName() string {
	return getTypeName(i.fromType)
}

func (i castToWidthTmplInfo) TypeName() string {
	return getTypeName(i.toType)
}

func (i castToWidthTmplInfo) Cast(to, from, fromCol, toType string) string {
	return i.CastFn(to, from, fromCol, toType)
}

func (i castToWidthTmplInfo) Sliceable() bool {
	return sliceable(typeconv.TypeFamilyToCanonicalTypeFamily(i.toType.Family()))
}

// Remove unused warnings.
var (
	_ = castFromWidthTmplInfo.VecMethod
	_ = castFromWidthTmplInfo.TypeName
	_ = castToWidthTmplInfo.TypeName
	_ = castToWidthTmplInfo.Cast
	_ = castToWidthTmplInfo.Sliceable
)

// getCastFromTmplInfos populates the 4-leveled hierarchy of structs (mentioned
// above to be used to execute the cast template) from castInfos.
//
// It relies heavily on the structure of castInfos mentioned in the comment to
// it.
func getCastFromTmplInfos() []castFromTmplInfo {
	toTypeFamily := func(typ *types.T) string {
		return "types." + typ.Family().String()
	}
	getWidth := func(typ *types.T) int32 {
		width := int32(anyWidth)
		if typ.Family() == types.IntFamily && typ.Width() < 64 {
			width = typ.Width()
		}
		return width
	}

	var castFromTmplInfos []castFromTmplInfo
	var fromFamilyStartIdx int
	// Single iteration of this loop finds the boundaries of the same "from"
	// type family and processes all casts for this type family.
	for fromFamilyStartIdx < len(castInfos) {
		castInfo := castInfos[fromFamilyStartIdx]
		fromFamilyEndIdx := fromFamilyStartIdx + 1
		for fromFamilyEndIdx < len(castInfos) {
			if castInfo.from.Family() != castInfos[fromFamilyEndIdx].from.Family() {
				break
			}
			fromFamilyEndIdx++
		}

		castFromTmplInfos = append(castFromTmplInfos, castFromTmplInfo{})
		fromFamilyTmplInfo := &castFromTmplInfos[len(castFromTmplInfos)-1]
		fromFamilyTmplInfo.TypeFamily = toTypeFamily(castInfo.from)

		fromWidthStartIdx := fromFamilyStartIdx
		// Single iteration of this loop finds the boundaries of the same "from"
		// width for the fixed "from" type family and processes all casts for
		// "from" type.
		for fromWidthStartIdx < fromFamilyEndIdx {
			castInfo = castInfos[fromWidthStartIdx]
			fromWidthEndIdx := fromWidthStartIdx + 1
			for fromWidthEndIdx < fromFamilyEndIdx {
				if castInfo.from.Width() != castInfos[fromWidthEndIdx].from.Width() {
					break
				}
				fromWidthEndIdx++
			}

			fromFamilyTmplInfo.Widths = append(fromFamilyTmplInfo.Widths, castFromWidthTmplInfo{})
			fromWidthTmplInfo := &fromFamilyTmplInfo.Widths[len(fromFamilyTmplInfo.Widths)-1]
			fromWidthTmplInfo.fromType = castInfo.from
			fromWidthTmplInfo.Width = getWidth(castInfo.from)

			toFamilyStartIdx := fromWidthStartIdx
			// Single iteration of this loop finds the boundaries of the same
			// "to" type family for the fixed "from" type and processes all
			// casts for "to" type family.
			for toFamilyStartIdx < fromWidthEndIdx {
				castInfo = castInfos[toFamilyStartIdx]
				toFamilyEndIdx := toFamilyStartIdx + 1
				for toFamilyEndIdx < fromWidthEndIdx {
					if castInfo.to.Family() != castInfos[toFamilyEndIdx].to.Family() {
						break
					}
					toFamilyEndIdx++
				}

				fromWidthTmplInfo.To = append(fromWidthTmplInfo.To, castToTmplInfo{})
				toFamilyTmplInfo := &fromWidthTmplInfo.To[len(fromWidthTmplInfo.To)-1]
				toFamilyTmplInfo.TypeFamily = toTypeFamily(castInfo.to)

				// We now have fixed "from family", "from width", and "to
				// family", so we can populate the "meat" of the cast tmpl info.
				for castInfoIdx := toFamilyStartIdx; castInfoIdx < toFamilyEndIdx; castInfoIdx++ {
					castInfo = castInfos[castInfoIdx]

					toFamilyTmplInfo.Widths = append(toFamilyTmplInfo.Widths, castToWidthTmplInfo{
						toType:    castInfo.to,
						Width:     getWidth(castInfo.to),
						VecMethod: toVecMethod(typeconv.TypeFamilyToCanonicalTypeFamily(castInfo.to.Family()), getWidth(castInfo.to)),
						GoType:    toPhysicalRepresentation(typeconv.TypeFamilyToCanonicalTypeFamily(castInfo.to.Family()), getWidth(castInfo.to)),
						CastFn:    castInfo.cast,
					})
				}

				// We're done processing the current "to" type family for the
				// given "from" type.
				toFamilyStartIdx = toFamilyEndIdx
			}

			// We're done processing the current width of the "from" type.
			fromWidthStartIdx = fromWidthEndIdx
		}

		// We're done processing the current "from" type family.
		fromFamilyStartIdx = fromFamilyEndIdx
	}

	return castFromTmplInfos
}
