// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package pgwire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
)

//go:generate stringer -type=formatCode
type formatCode uint16

const (
	formatText   formatCode = 0
	formatBinary formatCode = 1
)

// pgType contains type metadata used in RowDescription messages.
type pgType struct {
	oid oid.Oid

	// Variable-size types have size=-1.
	// Note that the protocol has both int16 and int32 size fields,
	// so this attribute is an unsized int and should be cast
	// as needed.
	// This field does *not* correspond to the encoded length of a
	// data type, so it's unclear what, if anything, it is used for.
	// To get the right value, "SELECT oid, typlen FROM pg_type"
	// on a postgres server.
	size int
}

//go:generate stringer -type=pgNumericSign
type pgNumericSign uint16

const (
	pgNumericPos pgNumericSign = 0x0000
	pgNumericNeg pgNumericSign = 0x4000
	// pgNumericNan pgNumericSign = 0xC000
)

// The number of decimal digits per int16 Postgres "digit".
const pgDecDigits = 4

type pgNumeric struct {
	ndigits, weight, dscale int16
	sign                    pgNumericSign
}

func pgTypeForParserType(t parser.Type) pgType {
	size := -1
	if s, variable := t.Size(); !variable {
		size = int(s)
	}
	return pgType{
		oid:  t.Oid(),
		size: size,
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(d parser.Datum, sessionLoc *time.Location) {
	if log.V(2) {
		log.Infof(context.TODO(), "pgwire writing TEXT datum of type: %T, %#v", d, d)
	}
	if d == parser.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DBool:
		b.putInt32(1)
		if *v {
			b.writeByte('t')
		} else {
			b.writeByte('f')
		}

	case *parser.DInt:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendInt(b.putbuf[4:4], int64(*v), 10)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DFloat:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], float64(*v), 'f', -1, 64)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DDecimal:
		b.writeLengthPrefixedDatum(v)

	case *parser.DBytes:
		// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
		// Code cribbed from github.com/lib/pq.
		result := make([]byte, 2+hex.EncodedLen(len(*v)))
		result[0] = '\\'
		result[1] = 'x'
		hex.Encode(result[2:], []byte(*v))

		b.putInt32(int32(len(result)))
		b.write(result)

	case *parser.DString:
		b.writeLengthPrefixedString(string(*v))

	case *parser.DCollatedString:
		b.writeLengthPrefixedString(v.Contents)

	case *parser.DDate:
		t := time.Unix(int64(*v)*secondsInDay, 0)
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(t, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DTimestamp:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DTimestampTZ:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, sessionLoc, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DInterval:
		b.writeLengthPrefixedString(v.ValueAsString())

	case *parser.DTuple:
		b.variablePutbuf.WriteString("(")
		for i, d := range v.D {
			if i > 0 {
				b.variablePutbuf.WriteString(",")
			}
			if d == parser.DNull {
				// Emit nothing on NULL.
				continue
			}
			d.Format(&b.variablePutbuf, parser.FmtSimple)
		}
		b.variablePutbuf.WriteString(")")
		b.writeLengthPrefixedVariablePutbuf()

	case *parser.DArray:
		switch d.ResolvedType().Oid() {
		case oid.T_int2vector:
			// int2vectors are serialized as a string of space-separated values.
			for i, d := range v.Array {
				if i > 0 {
					b.variablePutbuf.WriteString(" ")
				}
				d.Format(&b.variablePutbuf, parser.FmtBareStrings)
			}
			b.writeLengthPrefixedVariablePutbuf()
		default:
			// Arrays are serialized as a string of comma-separated values, surrounded
			// by braces.
			b.variablePutbuf.WriteString("{")
			for i, d := range v.Array {
				if i > 0 {
					b.variablePutbuf.WriteString(",")
				}
				d.Format(&b.variablePutbuf, parser.FmtBareStrings)
			}
			b.variablePutbuf.WriteString("}")
			b.writeLengthPrefixedVariablePutbuf()
		}
	case *parser.DOid:
		b.writeLengthPrefixedDatum(v)
	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

func (b *writeBuffer) writeBinaryDatum(d parser.Datum, sessionLoc *time.Location) {
	if log.V(2) {
		log.Infof(context.TODO(), "pgwire writing BINARY datum of type: %T, %#v", d, d)
	}
	if d == parser.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DBool:
		b.putInt32(1)
		if *v {
			b.writeByte(1)
		} else {
			b.writeByte(0)
		}

	case *parser.DInt:
		b.putInt32(8)
		b.putInt64(int64(*v))

	case *parser.DFloat:
		b.putInt32(8)
		b.putInt64(int64(math.Float64bits(float64(*v))))

	case *parser.DDecimal:
		alloc := struct {
			pgNum pgNumeric

			bigI big.Int
		}{
			pgNum: pgNumeric{
				// Since we use 2000 as the exponent limits in parser.DecimalCtx, this
				// conversion should not overflow.
				dscale: int16(-v.Exponent),
			},
		}

		if v.Sign() >= 0 {
			alloc.pgNum.sign = pgNumericPos
		} else {
			alloc.pgNum.sign = pgNumericNeg
		}

		isZero := func(r rune) bool {
			return r == '0'
		}

		// Mostly cribbed from libpqtypes' str2num.
		digits := strings.TrimLeftFunc(alloc.bigI.Abs(&v.Coeff).String(), isZero)
		dweight := len(digits) - int(alloc.pgNum.dscale) - 1
		digits = strings.TrimRightFunc(digits, isZero)

		if dweight >= 0 {
			alloc.pgNum.weight = int16((dweight+1+pgDecDigits-1)/pgDecDigits - 1)
		} else {
			alloc.pgNum.weight = int16(-((-dweight-1)/pgDecDigits + 1))
		}
		offset := (int(alloc.pgNum.weight)+1)*pgDecDigits - (dweight + 1)
		alloc.pgNum.ndigits = int16((len(digits) + offset + pgDecDigits - 1) / pgDecDigits)

		if len(digits) == 0 {
			offset = 0
			alloc.pgNum.ndigits = 0
			alloc.pgNum.weight = 0
		}

		digitIdx := -offset

		nextDigit := func() int16 {
			var ndigit int16
			for nextDigitIdx := digitIdx + pgDecDigits; digitIdx < nextDigitIdx; digitIdx++ {
				ndigit *= 10
				if digitIdx >= 0 && digitIdx < len(digits) {
					ndigit += int16(digits[digitIdx] - '0')
				}
			}
			return ndigit
		}

		b.putInt32(int32(2 * (4 + alloc.pgNum.ndigits)))
		b.putInt16(alloc.pgNum.ndigits)
		b.putInt16(alloc.pgNum.weight)
		b.putInt16(int16(alloc.pgNum.sign))
		b.putInt16(alloc.pgNum.dscale)

		for digitIdx < len(digits) {
			b.putInt16(nextDigit())
		}

	case *parser.DBytes:
		b.putInt32(int32(len(*v)))
		b.write([]byte(*v))

	case *parser.DString:
		b.writeLengthPrefixedString(string(*v))

	case *parser.DCollatedString:
		b.writeLengthPrefixedString(v.Contents)

	case *parser.DTimestamp:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, nil))

	case *parser.DTimestampTZ:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, sessionLoc))

	case *parser.DDate:
		b.putInt32(4)
		b.putInt32(dateToPgBinary(v))

	case *parser.DArray:
		if v.ParamTyp.FamilyEqual(parser.TypeAnyArray) {
			b.setError(errors.New("unsupported binary serialization of multidimensional arrays"))
			return
		}
		subWriter := &writeBuffer{wrapped: b.variablePutbuf}
		// Put the number of dimensions. We currently support 1d arrays only.
		subWriter.putInt32(1)
		hasNulls := 0
		if v.HasNulls {
			hasNulls = 1
		}
		subWriter.putInt32(int32(hasNulls))
		subWriter.putInt32(int32(v.ParamTyp.Oid()))
		subWriter.putInt32(int32(v.Len()))
		subWriter.putInt32(int32(v.Len()))
		for _, elem := range v.Array {
			subWriter.writeBinaryDatum(elem, sessionLoc)
		}
		b.variablePutbuf = subWriter.wrapped
		b.writeLengthPrefixedVariablePutbuf()
	case *parser.DOid:
		b.putInt32(4)
		b.putInt32(int32(v.DInt))
	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

const pgTimeStampFormatNoOffset = "2006-01-02 15:04:05.999999"
const pgTimeStampFormat = pgTimeStampFormatNoOffset + "-07:00"

// formatTs formats t with an optional offset into a format lib/pq understands,
// appending to the provided tmp buffer and reallocating if needed. The function
// will then return the resulting buffer. formatTs is mostly cribbed from
// github.com/lib/pq.
func formatTs(t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}

	if offset != nil {
		b = t.AppendFormat(tmp, pgTimeStampFormat)
	} else {
		b = t.AppendFormat(tmp, pgTimeStampFormatNoOffset)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}

// parseTs parses timestamps in any of the formats that Postgres accepts over
// the wire protocol.
//
// Postgres is lenient in what it accepts as a timestamp, so we must also be
// lenient. As new drivers are used with CockroachDB and formats are found that
// we don't support but Postgres does, add them here. Then create an integration
// test for the driver and add a case to TestParseTs.
func parseTs(str string) (time.Time, error) {
	// See https://github.com/lib/pq/blob/8df6253/encode.go#L480.
	if ts, err := time.Parse("2006-01-02 15:04:05.999999999Z07:00", str); err == nil {
		return ts, nil
	}

	// See https://github.com/cockroachdb/pq/blob/44a6473/encode.go#L470.
	if ts, err := time.Parse(time.RFC3339Nano, str); err == nil {
		return ts, nil
	}

	// pq.ParseTimestamp parses the timestamp format that both Postgres and
	// CockroachDB send in responses, so this allows roundtripping of the encoded
	// timestamps that we send.
	return pq.ParseTimestamp(nil, str)
}

var (
	pgEpochJDate         = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pgEpochJDateFromUnix = int32(pgEpochJDate.Unix() / secondsInDay)
)

// timeToPgBinary calculates the Postgres binary format for a timestamp. The timestamp
// is represented as the number of microseconds between the given time and Jan 1, 2000
// (dubbed the pgEpochJDate), stored within an int64.
func timeToPgBinary(t time.Time, offset *time.Location) int64 {
	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}
	return duration.DiffMicros(t, pgEpochJDate)
}

// pgBinaryToTime takes an int64 and interprets it as the Postgres binary format
// for a timestamp. To create a timestamp from this value, it takes the microseconds
// delta and adds it to pgEpochJDate.
func pgBinaryToTime(i int64) time.Time {
	return duration.AddMicros(pgEpochJDate, i)
}

// dateToPgBinary calculates the Postgres binary format for a date. The date is
// represented as the number of days between the given date and Jan 1, 2000
// (dubbed the pgEpochJDate), stored within an int32.
func dateToPgBinary(d *parser.DDate) int32 {
	return int32(*d) - pgEpochJDateFromUnix
}

// pgBinaryToDate takes an int32 and interprets it as the Postgres binary format
// for a date. To create a date from this value, it takes the day delta and adds
// it to pgEpochJDate.
func pgBinaryToDate(i int32) *parser.DDate {
	daysSinceEpoch := pgEpochJDateFromUnix + i
	return parser.NewDDate(parser.DDate(daysSinceEpoch))
}

// decodeOidDatum decodes bytes with specified Oid and format code into
// a datum.
func decodeOidDatum(id oid.Oid, code formatCode, b []byte) (parser.Datum, error) {
	switch code {
	case formatText:
		switch id {
		case oid.T_bool:
			t, err := strconv.ParseBool(string(b))
			if err != nil {
				return nil, err
			}
			return parser.MakeDBool(parser.DBool(t)), nil
		case oid.T_int2, oid.T_int4, oid.T_int8:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return nil, err
			}
			return parser.NewDInt(parser.DInt(i)), nil
		case oid.T_oid:
			u, err := strconv.ParseUint(string(b), 10, 32)
			if err != nil {
				return nil, err
			}
			return parser.NewDOid(parser.DInt(u)), nil
		case oid.T_float4, oid.T_float8:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return nil, err
			}
			return parser.NewDFloat(parser.DFloat(f)), nil
		case oid.T_numeric:
			d, err := parser.ParseDDecimal(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as decimal", b)
			}
			return d, nil
		case oid.T_bytea:
			// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
			// Code cribbed from github.com/lib/pq.

			// We only support hex encoding.
			if len(b) >= 2 && bytes.Equal(b[:2], []byte("\\x")) {
				b = b[2:] // trim off leading "\\x"
				result := make([]byte, hex.DecodedLen(len(b)))
				if _, err := hex.Decode(result, b); err != nil {
					return nil, err
				}
				return parser.NewDBytes(parser.DBytes(result)), nil
			}
			return nil, errors.Errorf("unsupported bytea encoding: %q", b)
		case oid.T_timestamp:
			ts, err := parseTs(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as timestamp", b)
			}
			return parser.MakeDTimestamp(ts, time.Microsecond), nil
		case oid.T_timestamptz:
			ts, err := parseTs(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as timestamp", b)
			}
			return parser.MakeDTimestampTZ(ts, time.Microsecond), nil
		case oid.T_date:
			ts, err := parseTs(string(b))
			if err != nil {
				res, err := parser.ParseDDate(string(b), time.UTC)
				if err != nil {
					return nil, errors.Errorf("could not parse string %q as date", b)
				}
				return res, nil
			}
			daysSinceEpoch := ts.Unix() / secondsInDay
			return parser.NewDDate(parser.DDate(daysSinceEpoch)), nil
		case oid.T_interval:
			d, err := parser.ParseDInterval(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as interval", b)
			}
			return d, nil
		case oid.T__int2, oid.T__int4, oid.T__int8:
			var arr pq.Int64Array
			if err := (&arr).Scan(b); err != nil {
				return nil, err
			}
			out := parser.NewDArray(parser.TypeInt)
			for _, v := range arr {
				if err := out.Append(parser.NewDInt(parser.DInt(v))); err != nil {
					return nil, err
				}
			}
			return out, nil
		case oid.T__text, oid.T__name:
			var arr pq.StringArray
			if err := (&arr).Scan(b); err != nil {
				return nil, err
			}
			out := parser.NewDArray(parser.TypeString)
			if id == oid.T__name {
				out.ParamTyp = parser.TypeName
			}
			for _, v := range arr {
				var s parser.Datum = parser.NewDString(v)
				if id == oid.T__name {
					s = parser.NewDNameFromDString(s.(*parser.DString))
				}
				if err := out.Append(s); err != nil {
					return nil, err
				}
			}
			return out, nil
		}
	case formatBinary:
		switch id {
		case oid.T_bool:
			if len(b) > 0 {
				switch b[0] {
				case 0:
					return parser.MakeDBool(false), nil
				case 1:
					return parser.MakeDBool(true), nil
				}
			}
			return nil, errors.Errorf("unsupported binary bool: %x", b)
		case oid.T_int2:
			if len(b) < 2 {
				return nil, errors.Errorf("int2 requires 2 bytes for binary format")
			}
			i := int16(binary.BigEndian.Uint16(b))
			return parser.NewDInt(parser.DInt(i)), nil
		case oid.T_int4:
			if len(b) < 4 {
				return nil, errors.Errorf("int4 requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return parser.NewDInt(parser.DInt(i)), nil
		case oid.T_int8:
			if len(b) < 8 {
				return nil, errors.Errorf("int8 requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return parser.NewDInt(parser.DInt(i)), nil
		case oid.T_oid:
			if len(b) < 4 {
				return nil, errors.Errorf("oid requires 4 bytes for binary format")
			}
			u := binary.BigEndian.Uint32(b)
			return parser.NewDOid(parser.DInt(u)), nil
		case oid.T_float4:
			if len(b) < 4 {
				return nil, errors.Errorf("float4 requires 4 bytes for binary format")
			}
			f := math.Float32frombits(binary.BigEndian.Uint32(b))
			return parser.NewDFloat(parser.DFloat(f)), nil
		case oid.T_float8:
			if len(b) < 8 {
				return nil, errors.Errorf("float8 requires 8 bytes for binary format")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(b))
			return parser.NewDFloat(parser.DFloat(f)), nil
		case oid.T_numeric:
			r := bytes.NewReader(b)

			alloc := struct {
				pgNum pgNumeric
				i16   int16

				dd parser.DDecimal
			}{}

			for _, ptr := range []interface{}{
				&alloc.pgNum.ndigits,
				&alloc.pgNum.weight,
				&alloc.pgNum.sign,
				&alloc.pgNum.dscale,
			} {
				if err := binary.Read(r, binary.BigEndian, ptr); err != nil {
					return nil, err
				}
			}

			if alloc.pgNum.ndigits > 0 {
				decDigits := make([]byte, 0, alloc.pgNum.ndigits*pgDecDigits)
				nextDigit := func() error {
					if err := binary.Read(r, binary.BigEndian, &alloc.i16); err != nil {
						return err
					}
					numZeroes := pgDecDigits
					for i16 := alloc.i16; i16 > 0; i16 /= 10 {
						numZeroes--
					}
					for ; numZeroes > 0; numZeroes-- {
						decDigits = append(decDigits, '0')
					}
					return nil
				}

				for i := int16(0); i < alloc.pgNum.ndigits-1; i++ {
					if err := nextDigit(); err != nil {
						return nil, err
					}
					if alloc.i16 > 0 {
						decDigits = strconv.AppendUint(decDigits, uint64(alloc.i16), 10)
					}
				}

				// The last digit may contain padding, which we need to deal with.
				if err := nextDigit(); err != nil {
					return nil, err
				}
				dscale := (alloc.pgNum.ndigits - (alloc.pgNum.weight + 1)) * pgDecDigits
				if overScale := dscale - alloc.pgNum.dscale; overScale > 0 {
					dscale -= overScale
					for i := int16(0); i < overScale; i++ {
						alloc.i16 /= 10
					}
				}
				decDigits = strconv.AppendUint(decDigits, uint64(alloc.i16), 10)
				decString := string(decDigits)
				if _, ok := alloc.dd.Coeff.SetString(decString, 10); !ok {
					return nil, errors.Errorf("could not parse string %q as decimal", decString)
				}
				alloc.dd.SetExponent(-int32(dscale))
			}

			switch alloc.pgNum.sign {
			case pgNumericPos:
			case pgNumericNeg:
				alloc.dd.Neg(&alloc.dd.Decimal)
			default:
				return nil, errors.Errorf("unsupported numeric sign: %d", alloc.pgNum.sign)
			}

			return &alloc.dd, nil
		case oid.T_bytea:
			return parser.NewDBytes(parser.DBytes(b)), nil
		case oid.T_timestamp:
			if len(b) < 8 {
				return nil, errors.Errorf("timestamp requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return parser.MakeDTimestamp(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_timestamptz:
			if len(b) < 8 {
				return nil, errors.Errorf("timestamptz requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return parser.MakeDTimestampTZ(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_date:
			if len(b) < 4 {
				return nil, errors.Errorf("date requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return pgBinaryToDate(i), nil
		case oid.T__int2, oid.T__int4, oid.T__int8, oid.T__text, oid.T__name:
			return decodeBinaryArray(b, code)
		}
	default:
		return nil, errors.Errorf("unsupported format code: %s", code)
	}

	// Types with identical text/binary handling.
	switch id {
	case oid.T_text, oid.T_varchar:
		return parser.NewDString(string(b)), nil
	case oid.T_name:
		return parser.NewDName(string(b)), nil
	default:
		return nil, errors.Errorf("unsupported OID %v with format code %s", id, code)
	}
}

func decodeBinaryArray(b []byte, code formatCode) (parser.Datum, error) {
	hdr := struct {
		Ndims int32
		// Nullflag
		_       int32
		ElemOid int32
		// The next two fields should be arrays of size Ndims. However, since
		// we only support 1-dimensional arrays for now, for convenience we can
		// leave them in this struct as such for `binary.Read` to parse for us.
		DimSize int32
		// Dim lower bound
		_ int32
	}{}
	r := bytes.NewBuffer(b)
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return nil, err
	}
	// Only 1-dimensional arrays are supported for now.
	if hdr.Ndims != 1 {
		return nil, errors.Errorf("unsupported number of array dimensions: %d", hdr.Ndims)
	}

	elemOid := oid.Oid(hdr.ElemOid)
	arr := parser.NewDArray(parser.OidToType[elemOid])
	var vlen int32
	for i := int32(0); i < hdr.DimSize; i++ {
		if err := binary.Read(r, binary.BigEndian, &vlen); err != nil {
			return nil, err
		}
		buf := r.Next(int(vlen))
		elem, err := decodeOidDatum(elemOid, code, buf)
		if err != nil {
			return nil, err
		}
		if err := arr.Append(elem); err != nil {
			return nil, err
		}
	}
	return arr, nil
}
