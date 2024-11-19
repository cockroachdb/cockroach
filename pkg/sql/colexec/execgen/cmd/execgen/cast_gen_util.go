// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
//  3. for a fixed "from" type, all the same "to" type families are contiguous
//  4. for a fixed "from" type and a fixed "to" type family, anyWidth "to" width
//     is the last one.
//
// If this structure is broken, then the generated code will not compile because
// either
//  1. there will be duplicate entries in the switch statements (when
//     "continuity" requirement is broken)
//  2. the 'default' case appears before other in the switch statements (when
//     anyWidth is not the last one).
var nativeCastInfos = []supportedNativeCastInfo{
	{types.Bool, types.Float, boolToIntOrFloat},
	{types.Bool, types.Int2, boolToIntOrFloat},
	{types.Bool, types.Int4, boolToIntOrFloat},
	{types.Bool, types.Int, boolToIntOrFloat},
	{types.Bool, types.String, boolToString},

	{types.Bytes, types.String, bytesToString},
	{types.Bytes, types.Uuid, bytesToUUID},

	{types.Date, types.Decimal, intToDecimal},
	{types.Date, types.Float, intToFloat},
	// Dates are represented as int64, and we currently mistakenly support dates
	// outside of the range (#40354), so the casts from dates and from ints turn
	// out to be the same.
	// TODO(yuzefovich): add the checks for these casts that dates are finite.
	{types.Date, types.Int2, getIntToIntCastFunc(64 /* fromWidth */, 16 /* toWidth */)},
	{types.Date, types.Int4, getIntToIntCastFunc(64 /* fromWidth */, 32 /* toWidth */)},
	{types.Date, types.Int, getIntToIntCastFunc(64 /* fromWidth */, anyWidth)},
	{types.Date, types.String, dateToString},

	{types.Decimal, types.Bool, decimalToBool},
	{types.Decimal, types.Decimal, decimalToDecimal},
	{types.Decimal, types.Float, decimalToFloat},
	{types.Decimal, types.Int2, getDecimalToIntCastFunc(16)},
	{types.Decimal, types.Int4, getDecimalToIntCastFunc(32)},
	{types.Decimal, types.Int, getDecimalToIntCastFunc(anyWidth)},
	{types.Decimal, types.String, decimalToString},

	{types.AnyEnum, types.String, enumToString},

	{types.Float, types.Bool, numToBool},
	{types.Float, types.Decimal, floatToDecimal},
	{types.Float, types.Int2, floatToInt(16, 64 /* floatWidth */)},
	{types.Float, types.Int4, floatToInt(32, 64 /* floatWidth */)},
	{types.Float, types.Int, floatToInt(anyWidth, 64 /* floatWidth */)},
	{types.Float, types.String, floatToString},

	{types.Int2, types.Bool, numToBool},
	{types.Int2, types.Decimal, intToDecimal},
	{types.Int2, types.Float, intToFloat},
	{types.Int2, types.Int4, getIntToIntCastFunc(16, 32)},
	{types.Int2, types.Int, getIntToIntCastFunc(16, anyWidth)},
	{types.Int2, types.String, intToString},
	{types.Int4, types.Bool, numToBool},
	{types.Int4, types.Decimal, intToDecimal},
	{types.Int4, types.Float, intToFloat},
	{types.Int4, types.Int2, getIntToIntCastFunc(32, 16)},
	{types.Int4, types.Int, getIntToIntCastFunc(32, anyWidth)},
	{types.Int4, types.String, intToString},
	{types.Int, types.Bool, numToBool},
	{types.Int, types.Decimal, intToDecimal},
	{types.Int, types.Float, intToFloat},
	{types.Int, types.Int2, getIntToIntCastFunc(anyWidth, 16)},
	{types.Int, types.Int4, getIntToIntCastFunc(anyWidth, 32)},
	{types.Int, types.String, intToString},

	{types.Interval, types.String, intervalToString},

	{types.Jsonb, types.String, jsonToString},

	{types.String, types.Bool, stringToBool},
	{types.String, types.Bytes, stringToBytes},
	{types.String, types.Date, stringToDate},
	{types.String, types.Decimal, stringToDecimal},
	{types.String, types.AnyEnum, stringToEnum},
	{types.String, types.Float, stringToFloat},
	{types.String, types.Int2, getStringToIntCastFunc(16)},
	{types.String, types.Int4, getStringToIntCastFunc(32)},
	{types.String, types.Int, getStringToIntCastFunc(anyWidth)},
	{types.String, types.Interval, stringToInterval},
	{types.String, types.Jsonb, stringToJSON},
	{types.String, types.String, stringToString},
	{types.String, types.Timestamp, getStringToTimestampCastFunc(true /* withoutTimezone */)},
	{types.String, types.TimestampTZ, getStringToTimestampCastFunc(false /* withoutTimezone */)},
	{types.String, types.Uuid, stringToUUID},

	{types.Timestamp, types.String, timestampToString},

	{types.TimestampTZ, types.String, timestampTZToString},

	{types.Uuid, types.String, uuidToString},
}

type supportedNativeCastInfo struct {
	from *types.T
	to   *types.T
	cast castFunc
}

func boolToIntOrFloat(to, from, _, _, _ string) string {
	convStr := `
			%[1]s = 0
			if %[2]s {
				%[1]s = 1
			}
		`
	return fmt.Sprintf(convStr, to, from)
}

func boolToString(to, from, _, toType, _ string) string {
	return toString(fmt.Sprintf("%s = []byte(strconv.FormatBool(%s))", to, from), to, toType)
}

func bytesToString(to, from, evalCtx, toType, _ string) string {
	convStr := `
		_format := %[3]s.SessionData().DataConversionConfig.BytesEncodeFormat
		%[1]s = []byte(lex.EncodeByteArrayToRawBytes(string(%[2]s), _format, false /* skipHexPrefix */))
`
	return toString(fmt.Sprintf(convStr, to, from, evalCtx), to, toType)
}

func bytesToUUID(to, from, _, _, _ string) string {
	convStr := `
		_uuid, err := uuid.FromBytes(%[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = _uuid.GetBytes()
	`
	return fmt.Sprintf(convStr, to, from)
}

func dateToString(to, from, _, toType, buf string) string {
	convStr := `
		_date := pgdate.MakeCompatibleDateFromDisk(%[2]s)
		%[3]s.Reset()
		_date.Format(%[3]s)
		%[1]s = []byte(%[3]s.String())
	`
	return toString(fmt.Sprintf(convStr, to, from, buf), to, toType)
}

func decimalToBool(to, from, _, _, _ string) string {
	return fmt.Sprintf("%[1]s = %[2]s.Sign() != 0", to, from)
}

func decimalToDecimal(to, from, _, toType, _ string) string {
	return toDecimal(fmt.Sprintf(`%[1]s.Set(&%[2]s)`, to, from), to, toType)
}

func decimalToFloat(to, from, _, _, _ string) string {
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

func getDecimalToIntCastFunc(toIntWidth int32) castFunc {
	if toIntWidth == anyWidth {
		toIntWidth = 64
	}
	return func(to, from, evalCtx, toType, buf string) string {
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
			var tmpDec apd.Decimal //gcassert:noescape
			_, err := tree.DecimalCtx.RoundToIntegralValue(&tmpDec, &%[2]s)
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
			getIntToIntCastFunc(64 /* fromWidth */, toIntWidth)(to, "_i" /* from */, evalCtx, toType, buf),
			from,
			errOutOfRange,
		)
	}
}

func decimalToString(to, from, _, toType, _ string) string {
	return toString(fmt.Sprintf("%s = []byte(%s.String())", to, from), to, toType)
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

func numToBool(to, from, _, _, _ string) string {
	convStr := `
		%[1]s = %[2]s != 0
	`
	return fmt.Sprintf(convStr, to, from)
}

func floatToDecimal(to, from, _, toType, _ string) string {
	convStr := `
		if _, err := %[1]s.SetFloat64(float64(%[2]s)); err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return toDecimal(fmt.Sprintf(convStr, to, from), to, toType)
}

func floatToInt(intWidth, floatWidth int32) castFunc {
	return func(to, from, _, _, _ string) string {
		convStr := `
			if math.IsNaN(float64(%[2]s)) || %[2]s <= float%[4]d(math.MinInt%[3]d) || %[2]s >= float%[4]d(math.MaxInt%[3]d) {
				colexecerror.ExpectedError(tree.ErrIntOutOfRange)
			}
			%[1]s = int%[3]d(math.RoundToEven(%[2]s))
		`
		if intWidth == anyWidth {
			intWidth = 64
		}
		return fmt.Sprintf(convStr, to, from, intWidth, floatWidth)
	}
}

func floatToString(to, from, evalCtx, toType, _ string) string {
	convStr := `
		dcc := %[3]s.SessionData().DataConversionConfig
		%[1]s = tree.PgwireFormatFloat(nil /* buf */, %[2]s, dcc, types.Float)
`
	return toString(fmt.Sprintf(convStr, to, from, evalCtx), to, toType)
}

func enumToString(to, from, _, toType, _ string) string {
	conv := `
		_, logical, err := tree.GetEnumComponentsFromPhysicalRep(fromType, %[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = []byte(logical)
`
	return toString(fmt.Sprintf(conv, to, from), to, toType)
}

func intToDecimal(to, from, _, toType, _ string) string {
	conv := `
		%[1]s.SetInt64(int64(%[2]s))
	`
	return toDecimal(fmt.Sprintf(conv, to, from), to, toType)
}

func intToFloat(to, from, _, _, _ string) string {
	convStr := `
		%[1]s = float64(%[2]s)
	`
	return fmt.Sprintf(convStr, to, from)
}

// getIntToIntCastFunc returns a castFunc between integers of any widths.
func getIntToIntCastFunc(fromWidth, toWidth int32) castFunc {
	if fromWidth == anyWidth {
		fromWidth = 64
	}
	if toWidth == anyWidth {
		toWidth = 64
	}
	return func(to, from, _, _, _ string) string {
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

func intToString(to, from, _, toType, _ string) string {
	convStr := `
		if %[3]s.Oid() == oid.T_char {
			// int to "char" casts just return the corresponding ASCII byte.
			if %[2]s > math.MaxInt8 || %[2]s < math.MinInt8 {
				colexecerror.ExpectedError(tree.ErrCharOutOfRange)
			}
			if %[2]s == 0 {
				%[1]s = []byte{}
			} else {
				%[1]s = []byte{byte(%[2]s)}
			}
		} else {
			%[1]s = []byte(strconv.FormatInt(int64(%[2]s), 10))
		}
`
	return toString(fmt.Sprintf(convStr, to, from, toType), to, toType)
}

func intervalToString(to, from, evalCtx, toType, buf string) string {
	convStr := `
		dcc := %[3]s.SessionData().DataConversionConfig
		%[4]s.Reset()
		%[2]s.FormatWithStyle(%[4]s, dcc.IntervalStyle)
		%[1]s = []byte(%[4]s.String())
`
	return toString(fmt.Sprintf(convStr, to, from, evalCtx, buf), to, toType)
}

func jsonToString(to, from, _, toType, _ string) string {
	return toString(fmt.Sprintf("%s = []byte(%s.String())", to, from), to, toType)
}

func stringToBool(to, from, _, _, _ string) string {
	convStr := `
		var err error
		%[1]s, err = tree.ParseBool(string(%[2]s))
		if err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

func stringToBytes(to, from, _, _, _ string) string {
	convStr := `
		var err error
		%[1]s, err = lex.DecodeRawBytesToByteArrayAuto(%[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
	`
	return fmt.Sprintf(convStr, to, from)
}

func stringToDate(to, from, evalCtx, _, _ string) string {
	convStr := `
		_now := %[3]s.GetRelativeParseTime()
		_dateStyle := %[3]s.GetDateStyle()
		_ph := &%[3]s.ParseHelper
		_d, _, err := pgdate.ParseDate(_now, _dateStyle, string(%[2]s), _ph)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = _d.UnixEpochDays()
`
	return fmt.Sprintf(convStr, to, from, evalCtx)
}

func stringToDecimal(to, from, _, toType, _ string) string {
	convStr := `
		_s := strings.TrimSpace(string(%[2]s))
		_, res, err := tree.ExactCtx.SetString(&%[1]s, _s)
		if res != 0 || err != nil {
			colexecerror.ExpectedError(tree.MakeParseError(_s, types.Decimal, err))
		}
		switch %[1]s.Form {
		case apd.NaNSignaling:
			%[1]s.Form = apd.NaN
			%[1]s.Negative = false
		case apd.NaN:
			%[1]s.Negative = false
		case apd.Finite:
			if %[1]s.IsZero() && %[1]s.Negative {
				%[1]s.Negative = false
			}
		}
`
	return toDecimal(fmt.Sprintf(convStr, to, from), to, toType)
}

func stringToEnum(to, from, _, toType, _ string) string {
	// String value is treated as the logical representation of the enum.
	convStr := `
		_logical := string(%[2]s)
		var err error
		%[1]s, _, err = tree.GetEnumComponentsFromLogicalRep(%[3]s, _logical)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
`
	return fmt.Sprintf(convStr, to, from, toType)
}

func stringToFloat(to, from, _, toType, _ string) string {
	convStr := `
		_s := string(%[2]s)
		var _err error
		%[1]s, _err = strconv.ParseFloat(strings.TrimSpace(_s), 64)
		if _err != nil {
			colexecerror.ExpectedError(tree.MakeParseError(_s, %[3]s, _err))
		}
	`
	return fmt.Sprintf(convStr, to, from, toType)
}

func getStringToIntCastFunc(toIntWidth int32) castFunc {
	if toIntWidth == anyWidth {
		toIntWidth = 64
	}
	return func(to, from, evalCtx, toType, buf string) string {
		// convStr is a format string expecting three arguments:
		// 1. the code snippet that performs an assigment of int64 local
		//    variable named '_i' to the result, possibly performing the bounds
		//    checks
		// 2. the original value variable name
		// 3. the name of the global variable storing the type we're casting to.
		convStr := `
		{
			_s := string(%[2]s)
			_i, err := strconv.ParseInt(strings.TrimSpace(_s), 0, 64)
			if err != nil {
				colexecerror.ExpectedError(tree.MakeParseError(_s, %[3]s, err))
			}
			%[1]s
		}
	`
		return fmt.Sprintf(
			convStr,
			getIntToIntCastFunc(64 /* fromWidth */, toIntWidth)(to, "_i" /* from */, evalCtx, toType, buf),
			from,
			toType,
		)
	}
}

func stringToInterval(to, from, evalCtx, toType, _ string) string {
	convStr := `
		_itm, err := %[4]s.IntervalTypeMetadata()
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		_intervalStyle := %[3]s.GetIntervalStyle()
		%[1]s, err = tree.ParseIntervalWithTypeMetadata(_intervalStyle, string(%[2]s), _itm)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
`
	return fmt.Sprintf(convStr, to, from, evalCtx, toType)
}

func stringToJSON(to, from, _, _, _ string) string {
	convStr := `
		var err error
		%[1]s, err = json.ParseJSON(string(%[2]s))
		if err != nil {
			colexecerror.ExpectedError(pgerror.Wrapf(err, pgcode.Syntax, "could not parse JSON"))
		}
`
	return fmt.Sprintf(convStr, to, from)
}

// TODO(yuzefovich): figure out whether we can avoid converting to a string in
// the template below.

// toString returns the templated code that performs the cast to a string. It
// first will execute whatever is passed in 'conv' (the main conversion) and
// then will perform the truncation of 'to' variable according to 'toType'.
func toString(conv, to, toType string) string {
	// The logic here is a combination of the relevant pieces from
	// eval.performCastWithoutPrecisionTruncation as well as from
	// tree.AdjustValueToType.
	convStr := `
		%[1]s
		if %[3]s.Oid() != oid.T_name {
			// bpchar types truncate trailing whitespace.
			if %[3]s.Oid() == oid.T_bpchar {
				%[2]s = bytes.TrimRight(%[2]s, " ")
			}
			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if %[3]s.Width() > 0 {
				%[2]s = []byte(util.TruncateString(string(%[2]s), int(%[3]s.Width())))
			}
			if %[3]s.Oid() == oid.T_char {
				// "char" is supposed to truncate long values.
				%[2]s = []byte(util.TruncateString(string(%[2]s), 1))
			}
		}
		if %[3]s.Width() > 0 && utf8.RuneCountInString(string(%[2]s)) > int(%[3]s.Width()) {
			_typeString := %[3]s.SQLString()
			colexecerror.ExpectedError(
				pgerror.Newf(pgcode.StringDataRightTruncation, "value too long for type " + _typeString,
			))
		}
	`
	return fmt.Sprintf(convStr, conv, to, toType)
}

func stringToString(to, from, _, toType, _ string) string {
	return toString(fmt.Sprintf("%s = %s", to, from), to, toType)
}

// roundTimestamp is a template that takes a timestamp specified by 'from',
// rounds it according to 'precision', and assigns the result to 'to' timestamp.
func roundTimestamp(to, from, precision string) string {
	roundStr := `
		%[1]s = %[2]s.Round(%[3]s)
		if %[1]s.After(tree.MaxSupportedTime) || %[1]s.Before(tree.MinSupportedTime) {
			colexecerror.ExpectedError(tree.NewTimestampExceedsBoundsError(%[1]s))
		}
`
	return fmt.Sprintf(roundStr, to, from, precision)
}

func getStringToTimestampCastFunc(withoutTimezone bool) castFunc {
	return func(to, from, evalCtx, toType, _ string) string {
		var parseTimestampKind string
		if withoutTimezone {
			parseTimestampKind = "WithoutTimezone"
		}
		// TODO(yuzefovich): consider introducing "prologue" sections to the
		// cast template so that we could do some operations (e.g. _roundTo
		// value below) only once per batch.
		convStr := `
		_roundTo := tree.TimeFamilyPrecisionToRoundDuration(%[4]s.Precision())
		_now := %[3]s.GetRelativeParseTime()
		_dateStyle := %[3]s.GetDateStyle()
		_h := %[3]s.GetDateHelper()
		_t, _, err := pgdate.ParseTimestamp%[5]s(_now, _dateStyle, string(%[2]s), _h)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s
`
		roundAndAssign := roundTimestamp(to, "_t", "_roundTo")
		return fmt.Sprintf(convStr, roundAndAssign, from, evalCtx, toType, parseTimestampKind)
	}
}

func stringToUUID(to, from, _, _, _ string) string {
	convStr := `
		_uuid, err := uuid.FromString(string(%[2]s))
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = _uuid.GetBytes()
	`
	return fmt.Sprintf(convStr, to, from)
}

func timestampToString(to, from, _, toType, _ string) string {
	return toString(fmt.Sprintf("%s = tree.PGWireFormatTimestamp(%s, nil, r)", to, from), to, toType)
}

func timestampTZToString(to, from, evalCtx, toType, buf string) string {
	convStr := `
		// Convert to context timezone for correct display.
		_t := %[2]s
		%[4]s
		r = tree.PGWireFormatTimestamp(_t, %[3]s.GetLocation(), r)
`
	roundT := roundTimestamp("_t", "_t", "time.Microsecond")
	return toString(fmt.Sprintf(convStr, to, from, evalCtx, roundT), to, toType)
}

func uuidToString(to, from, _, toType, _ string) string {
	convStr := `
		_uuid, err := uuid.FromBytes(%[2]s)
		if err != nil {
			colexecerror.ExpectedError(err)
		}
		%[1]s = []byte(_uuid.String())
	`
	return toString(fmt.Sprintf(convStr, to, from), to, toType)
}

// getDatumToNativeCastFunc returns a castFunc for casting datum-backed value
// to a value of the specified physical representation (i.e. to natively
// supported type). The returned castFunc assumes that there is a converter
// function named "converter" in scope.
func getDatumToNativeCastFunc(nonDatumPhysicalRepresentation string) castFunc {
	return func(to, from, evalCtx, toType, _ string) string {
		convStr := `
		{
			_castedDatum, err := eval.PerformCast(c.Ctx, %[3]s, %[2]s.(tree.Datum), %[4]s)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			%[1]s = converter(_castedDatum).(%[5]s)
		}
	`
		return fmt.Sprintf(convStr, to, from, evalCtx, toType, nonDatumPhysicalRepresentation)
	}
}

func datumToDatum(to, from, evalCtx, toType, _ string) string {
	convStr := `
		{
			_castedDatum, err := eval.PerformCast(c.Ctx, %[3]s, %[2]s.(tree.Datum), %[4]s)
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
	if typ.Family() == types.EnumFamily {
		// Special case for enums since we're use types.AnyEnum as the
		// representative type.
		return "Enum"
	}
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

func (i castToWidthTmplInfo) Cast(to, from, evalCtx, toType, buf string) string {
	return i.CastFn(to, from, evalCtx, toType, buf)
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

func (i castDatumToWidthTmplInfo) Cast(to, from, evalCtx, toType, buf string) string {
	return i.CastFn(to, from, evalCtx, toType, buf)
}

func (i castDatumToWidthTmplInfo) Sliceable() bool {
	return sliceable(typeconv.TypeFamilyToCanonicalTypeFamily(i.toType.Family()))
}

func (i castBetweenDatumsTmplInfo) TypeName() string {
	return datumVecTypeName
}

func (i castBetweenDatumsTmplInfo) Cast(to, from, evalCtx, toType, buf string) string {
	return datumToDatum(to, from, evalCtx, toType, buf)
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
