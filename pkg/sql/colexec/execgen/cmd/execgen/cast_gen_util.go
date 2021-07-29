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

// nativeCastInfos contains supported "from -> to" mappings where both types are
// supported natively. This mapping has to be on the actual types and not the
// canonical type families.
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
var nativeCastInfos = []supportedNativeCastInfo{
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

	{types.Bytes, types.Uuid, bytesToUUID},

	{types.String, types.Bool, stringToBool},
	{types.String, types.Bytes, stringToBytes},
	{types.String, types.String, stringToString},
	{types.String, types.Uuid, stringToUUID},

	{types.Jsonb, types.String, jsonToString},
}

type supportedNativeCastInfo struct {
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
	return func(to, from, evalCtx, toType string) string {
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
			getIntToIntCastFunc(64 /* fromWidth */, toIntWidth)(to, "_i" /* from */, evalCtx, toType),
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

func bytesToUUID(to, from, _, _ string) string {
	convStr := `
		_uuid, err := uuid.FromBytes(%[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = _uuid.GetBytes()
	`
	return fmt.Sprintf(convStr, to, from)
}

func stringToBool(to, from, _, _ string) string {
	convStr := `
		var err error
		%[1]s, err = tree.ParseBool(string(%[2]s))
		if err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

func stringToBytes(to, from, _, _ string) string {
	convStr := `
		var err error
		%[1]s, err = lex.DecodeRawBytesToByteArrayAuto(%[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

func stringToString(to, from, _, toType string) string {
	convStr := `
		if %[3]s.Oid() == oid.T_name {
			// For Names we don't perform the truncation, and there is no need
			// to do anything about the Oids since those are stored in the type.
			%[1]s = %[2]s
		} else {
			// bpchar types truncate trailing whitespace.
			if %[3]s.Oid() == oid.T_bpchar {
				%[2]s = bytes.TrimRight(%[2]s, " ")
			}
			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if %[3]s.Width() > 0 {
				// TODO(yuzefovich): figure out whether we can avoid converting
				// to a string.
				%[2]s = []byte(util.TruncateString(string(%[2]s), int(%[3]s.Width())))
			}
			if %[3]s.Oid() == oid.T_char {
				// "char" is supposed to truncate long values.
				// TODO(yuzefovich): figure out whether we can avoid converting
				// to a string.
				%[2]s = []byte(util.TruncateString(string(%[2]s), 1))
			}
			%[1]s = %[2]s
		}
	`
	return fmt.Sprintf(convStr, to, from, toType)
}

func stringToUUID(to, from, _, _ string) string {
	convStr := `
		_uuid, err := uuid.FromString(string(%[2]s))
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = _uuid.GetBytes()
	`
	return fmt.Sprintf(convStr, to, from)
}

func jsonToString(to, from, _, toType string) string {
	convStr := `
		_string := %[2]s.String()
		switch %[3]s.Oid() {
		case oid.T_char:
			// "char" is supposed to truncate long values.
			_string = util.TruncateString(_string, 1)
		case oid.T_bpchar:
			// bpchar types truncate trailing whitespace.
			_string = strings.TrimRight(_string, " ")
		}
		%[1]s = []byte(_string)
	`
	return fmt.Sprintf(convStr, to, from, toType)
}

// getDatumToNativeCastFunc returns a castFunc for casting datum-backed value
// to a value of the specified physical representation (i.e. to natively
// supported type). The returned castFunc assumes that there is a converter
// function named "converter" in scope.
func getDatumToNativeCastFunc(
	nonDatumPhysicalRepresentation string,
) func(string, string, string, string) string {
	return func(to, from, evalCtx, toType string) string {
		convStr := `
		{
			_castedDatum, err := tree.PerformCast(%[3]s, %[2]s.(tree.Datum), %[4]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%[1]s = converter(_castedDatum).(%[5]s)
		}
	`
		return fmt.Sprintf(convStr, to, from, evalCtx, toType, nonDatumPhysicalRepresentation)
	}
}

func datumToDatum(to, from, evalCtx, toType string) string {
	convStr := `
		{
			_castedDatum, err := tree.PerformCast(%[3]s, %[2]s.(tree.Datum), %[4]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%[1]s = _castedDatum
		}
	`
	return fmt.Sprintf(convStr, to, from, evalCtx, toType)
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

type castDatumTmplInfo struct {
	// TypeFamily contains the type family of the "to" type this struct is
	// handling, with "types." prefix.
	TypeFamily string
	// Widths contains all of the type widths of the "to" type this struct is
	// handling. Note that the entry with 'anyWidth' width must be last in the
	// slice.
	Widths []castDatumToWidthTmplInfo
}

type castDatumToWidthTmplInfo struct {
	toType    *types.T
	Width     int32
	VecMethod string
	GoType    string
	// CastFn is a function that returns a string which performs the cast
	// between the datum-backed type and toType.
	CastFn castFunc
}

type castBetweenDatumsTmplInfo struct {
	VecMethod string
	GoType    string
}

func (i castFromWidthTmplInfo) VecMethod() string {
	return toVecMethod(typeconv.TypeFamilyToCanonicalTypeFamily(i.fromType.Family()), i.Width)
}

func getTypeName(typ *types.T) string {
	// typ.Name() returns the type name in the lowercase. We want to capitalize
	// the first letter (and all type names start with a letter).
	name := []byte(typ.Name())
	return string(name[0]-32) + string(name[1:])
}

const datumVecTypeName = "Datum"

func (i castFromWidthTmplInfo) TypeName() string {
	return getTypeName(i.fromType)
}

func (i castFromWidthTmplInfo) Sliceable() bool {
	return sliceable(typeconv.TypeFamilyToCanonicalTypeFamily(i.fromType.Family()))
}

func (i castToWidthTmplInfo) TypeName() string {
	return getTypeName(i.toType)
}

func (i castToWidthTmplInfo) Cast(to, from, evalCtx, toType string) string {
	return i.CastFn(to, from, evalCtx, toType)
}

func (i castToWidthTmplInfo) Sliceable() bool {
	return sliceable(typeconv.TypeFamilyToCanonicalTypeFamily(i.toType.Family()))
}

func (i castDatumTmplInfo) VecMethod() string {
	return toVecMethod(typeconv.DatumVecCanonicalTypeFamily, anyWidth)
}

func (i castDatumTmplInfo) TypeName() string {
	return datumVecTypeName
}

func (i castDatumTmplInfo) Sliceable() bool {
	return false
}

func (i castDatumToWidthTmplInfo) TypeName() string {
	return getTypeName(i.toType)
}

func (i castDatumToWidthTmplInfo) Cast(to, from, evalCtx, toType string) string {
	return i.CastFn(to, from, evalCtx, toType)
}

func (i castDatumToWidthTmplInfo) Sliceable() bool {
	return sliceable(typeconv.TypeFamilyToCanonicalTypeFamily(i.toType.Family()))
}

func (i castBetweenDatumsTmplInfo) TypeName() string {
	return datumVecTypeName
}

func (i castBetweenDatumsTmplInfo) Cast(to, from, evalCtx, toType string) string {
	return datumToDatum(to, from, evalCtx, toType)
}

func (i castBetweenDatumsTmplInfo) Sliceable() bool {
	return false
}

// Remove unused warnings.
var (
	_ = castFromWidthTmplInfo.VecMethod
	_ = castFromWidthTmplInfo.TypeName
	_ = castFromWidthTmplInfo.Sliceable
	_ = castToWidthTmplInfo.TypeName
	_ = castToWidthTmplInfo.Cast
	_ = castToWidthTmplInfo.Sliceable
	_ = castDatumTmplInfo.VecMethod
	_ = castDatumTmplInfo.TypeName
	_ = castDatumTmplInfo.Sliceable
	_ = castDatumToWidthTmplInfo.TypeName
	_ = castDatumToWidthTmplInfo.Cast
	_ = castDatumToWidthTmplInfo.Sliceable
	_ = castBetweenDatumsTmplInfo.TypeName
	_ = castBetweenDatumsTmplInfo.Cast
	_ = castBetweenDatumsTmplInfo.Sliceable
)

type castTmplInfo struct {
	FromNative    []castFromTmplInfo
	FromDatum     []castDatumTmplInfo
	BetweenDatums castBetweenDatumsTmplInfo
}

func getCastFromTmplInfos() castTmplInfo {
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
	var result castTmplInfo

	// Below we populate the 4-leveled hierarchy of structs (mentioned above to
	// be used to execute the cast template) from nativeCastInfos.
	//
	// It relies heavily on the structure of nativeCastInfos mentioned in the
	// comment to it.
	var castFromTmplInfos []castFromTmplInfo
	var fromFamilyStartIdx int
	// Single iteration of this loop finds the boundaries of the same "from"
	// type family and processes all casts for this type family.
	for fromFamilyStartIdx < len(nativeCastInfos) {
		castInfo := nativeCastInfos[fromFamilyStartIdx]
		fromFamilyEndIdx := fromFamilyStartIdx + 1
		for fromFamilyEndIdx < len(nativeCastInfos) {
			if castInfo.from.Family() != nativeCastInfos[fromFamilyEndIdx].from.Family() {
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
			castInfo = nativeCastInfos[fromWidthStartIdx]
			fromWidthEndIdx := fromWidthStartIdx + 1
			for fromWidthEndIdx < fromFamilyEndIdx {
				if castInfo.from.Width() != nativeCastInfos[fromWidthEndIdx].from.Width() {
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
				castInfo = nativeCastInfos[toFamilyStartIdx]
				toFamilyEndIdx := toFamilyStartIdx + 1
				for toFamilyEndIdx < fromWidthEndIdx {
					if castInfo.to.Family() != nativeCastInfos[toFamilyEndIdx].to.Family() {
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
					castInfo = nativeCastInfos[castInfoIdx]

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
	result.FromNative = castFromTmplInfos

	// Now we populate the 2-leveled hierarchy of structs for casts from
	// datum-backed to native types.
	var castDatumTmplInfos []castDatumTmplInfo
	var toFamilyStartIdx int
	// Single iteration of this loop finds the boundaries of the same "to" type
	// family from the datum-backed type and processes all casts for "to" type
	// family.
	nativeTypes := typeconv.TypesSupportedNatively
	for toFamilyStartIdx < len(nativeTypes) {
		toFamilyEndIdx := toFamilyStartIdx + 1
		for toFamilyEndIdx < len(nativeTypes) {
			if nativeTypes[toFamilyStartIdx].Family() != nativeTypes[toFamilyEndIdx].Family() {
				break
			}
			toFamilyEndIdx++
		}

		castDatumTmplInfos = append(castDatumTmplInfos, castDatumTmplInfo{})
		datumTmplInfo := &castDatumTmplInfos[len(castDatumTmplInfos)-1]
		datumTmplInfo.TypeFamily = toTypeFamily(nativeTypes[toFamilyStartIdx])

		// We now have fixed "to family", so we can populate the "meat" of the
		// datum cast tmpl info.
		for toTypeIdx := toFamilyStartIdx; toTypeIdx < toFamilyEndIdx; toTypeIdx++ {
			toType := nativeTypes[toTypeIdx]
			width := getWidth(toType)
			canonicalTypeFamily := typeconv.TypeFamilyToCanonicalTypeFamily(toType.Family())
			physicalRepresentation := toPhysicalRepresentation(canonicalTypeFamily, width)
			datumTmplInfo.Widths = append(datumTmplInfo.Widths, castDatumToWidthTmplInfo{
				toType:    toType,
				Width:     width,
				VecMethod: toVecMethod(canonicalTypeFamily, width),
				GoType:    physicalRepresentation,
				CastFn:    getDatumToNativeCastFunc(physicalRepresentation),
			})
		}

		// We're done processing the current "to" type family for the
		// datum-backed type.
		toFamilyStartIdx = toFamilyEndIdx
	}
	result.FromDatum = castDatumTmplInfos

	// Finally, set up the information for casts between datum-backed types.
	result.BetweenDatums = castBetweenDatumsTmplInfo{
		VecMethod: toVecMethod(typeconv.DatumVecCanonicalTypeFamily, anyWidth),
		GoType:    toPhysicalRepresentation(typeconv.DatumVecCanonicalTypeFamily, anyWidth),
	}

	return result
}
