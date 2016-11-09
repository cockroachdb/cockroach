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
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	"gopkg.in/inf.v0"

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
	// Compare all types that cannot rely on == equality.
	istype := t.FamilyEqual
	switch {
	case istype(parser.TypeTuple):
		return pgType{oid.T_record, -1}
	}
	// Compare all types that can rely on == equality.
	switch t {
	case parser.TypeNull:
		return pgType{oid: oid.T_unknown}
	case parser.TypeBool:
		return pgType{oid.T_bool, 1}
	case parser.TypeBytes:
		return pgType{oid.T_bytea, -1}
	case parser.TypeInt:
		return pgType{oid.T_int8, 8}
	case parser.TypeFloat:
		return pgType{oid.T_float8, 8}
	case parser.TypeDecimal:
		return pgType{oid.T_numeric, -1}
	case parser.TypeString:
		return pgType{oid.T_text, -1}
	case parser.TypeDate:
		return pgType{oid.T_date, 8}
	case parser.TypeTimestamp:
		return pgType{oid.T_timestamp, 8}
	case parser.TypeTimestampTZ:
		return pgType{oid.T_timestamptz, 8}
	case parser.TypeInterval:
		return pgType{oid.T_interval, 8}
	case parser.TypeStringArray:
		return pgType{oid.T__text, -1}
	case parser.TypeIntArray:
		return pgType{oid.T__int8, -1}
	default:
		panic(fmt.Sprintf("unsupported type %s", t))
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
	switch v := d.(type) {
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
		b.writeLengthPrefixedString(v.Dec.String())

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

	case *parser.DDate:
		t := time.Unix(int64(*v)*secondsInDay, 0)
		s := formatTs(t, nil)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DTimestamp:
		s := formatTs(v.Time, nil)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DTimestampTZ:
		s := formatTs(v.Time, sessionLoc)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *parser.DInterval:
		b.writeLengthPrefixedString(v.String())

	case *parser.DTuple:
		var tb bytes.Buffer
		tb.WriteString("(")
		for i, d := range *v {
			if i > 0 {
				tb.WriteString(",")
			}
			if d == parser.DNull {
				// Emit nothing on NULL.
				continue
			}
			tb.WriteString(d.String())
		}
		tb.WriteString(")")
		b.writeLengthPrefixedString(tb.String())

	case *parser.DArray:
		var tb bytes.Buffer
		tb.WriteString("{")
		for i, d := range v.Array {
			if i > 0 {
				tb.WriteString(",")
			}
			tb.WriteString(d.String())
		}
		tb.WriteString("}")
		b.writeLengthPrefixedString(tb.String())

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
	switch v := d.(type) {
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
				dscale: int16(v.Scale()),
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
		digits := strings.TrimLeftFunc(alloc.bigI.Abs(v.UnscaledBig()).String(), isZero)
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

	case *parser.DTimestamp:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, nil))

	case *parser.DTimestampTZ:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, sessionLoc))

	case *parser.DDate:
		b.putInt32(4)
		b.putInt32(dateToPgBinary(v))

	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

const pgTimeStampFormatNoOffset = "2006-01-02 15:04:05.999999"
const pgTimeStampFormat = pgTimeStampFormatNoOffset + "-07:00"

// formatTs formats t into a format lib/pq understands.
// Mostly cribbed from github.com/lib/pq.
func formatTs(t time.Time, offset *time.Location) (b []byte) {
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
		b = []byte(t.Format(pgTimeStampFormat))
	} else {
		b = []byte(t.Format(pgTimeStampFormatNoOffset))
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
	// RFC3339Nano is sent by github.com/lib/pq (go).
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
	var d parser.Datum
	switch id {
	case oid.T_bool:
		switch code {
		case formatText:
			v, err := strconv.ParseBool(string(b))
			if err != nil {
				return d, err
			}
			d = parser.MakeDBool(parser.DBool(v))
		case formatBinary:
			switch b[0] {
			case 0:
				d = parser.MakeDBool(false)
			case 1:
				d = parser.MakeDBool(true)
			default:
				return d, errors.Errorf("unsupported binary bool: %q", b)
			}
		default:
			return d, errors.Errorf("unsupported bool format code: %s", code)
		}
	case oid.T_int2:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		case formatBinary:
			if len(b) < 2 {
				return d, errors.Errorf("int2 requires 2 bytes for binary format")
			}
			i := int16(binary.BigEndian.Uint16(b))
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, errors.Errorf("unsupported int2 format code: %s", code)
		}
	case oid.T_int4:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		case formatBinary:
			if len(b) < 4 {
				return d, errors.Errorf("int4 requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, errors.Errorf("unsupported int4 format code: %s", code)
		}
	case oid.T_int8:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		case formatBinary:
			if len(b) < 8 {
				return d, errors.Errorf("int8 requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, errors.Errorf("unsupported int8 format code: %s", code)
		}
	case oid.T_float4:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.NewDFloat(parser.DFloat(f))
		case formatBinary:
			if len(b) < 4 {
				return d, errors.Errorf("float4 requires 4 bytes for binary format")
			}
			f := math.Float32frombits(binary.BigEndian.Uint32(b))
			d = parser.NewDFloat(parser.DFloat(f))
		default:
			return d, errors.Errorf("unsupported float4 format code: %s", code)
		}
	case oid.T_float8:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.NewDFloat(parser.DFloat(f))
		case formatBinary:
			if len(b) < 8 {
				return d, errors.Errorf("float8 requires 8 bytes for binary format")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(b))
			d = parser.NewDFloat(parser.DFloat(f))
		default:
			return d, errors.Errorf("unsupported float8 format code: %s", code)
		}
	case oid.T_numeric:
		switch code {
		case formatText:
			dd := &parser.DDecimal{}
			if _, ok := dd.SetString(string(b)); !ok {
				return nil, errors.Errorf("could not parse string %q as decimal", b)
			}
			d = dd
		case formatBinary:
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
					return d, err
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
						return d, err
					}
					if alloc.i16 > 0 {
						decDigits = strconv.AppendUint(decDigits, uint64(alloc.i16), 10)
					}
				}

				// The last digit may contain padding, which we need to deal with.
				if err := nextDigit(); err != nil {
					return d, err
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
				if _, ok := alloc.dd.UnscaledBig().SetString(decString, 10); !ok {
					return nil, errors.Errorf("could not parse string %q as decimal", decString)
				}
				alloc.dd.SetScale(inf.Scale(dscale))
			}

			switch alloc.pgNum.sign {
			case pgNumericPos:
			case pgNumericNeg:
				alloc.dd.Neg(&alloc.dd.Dec)
			default:
				return d, errors.Errorf("unsupported numeric sign: %s", alloc.pgNum.sign)
			}

			d = &alloc.dd
		default:
			return d, errors.Errorf("unsupported numeric format code: %s", code)
		}
	case oid.T_text, oid.T_varchar:
		switch code {
		case formatText, formatBinary:
			d = parser.NewDString(string(b))
		default:
			return d, errors.Errorf("unsupported text format code: %s", code)
		}
	case oid.T_bytea:
		switch code {
		case formatText:
			// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
			// Code cribbed from github.com/lib/pq.

			// We only support hex encoding.
			if len(b) >= 2 && bytes.Equal(b[:2], []byte("\\x")) {
				b = b[2:] // trim off leading "\\x"
				result := make([]byte, hex.DecodedLen(len(b)))
				_, err := hex.Decode(result, b)
				if err != nil {
					return d, err
				}
				d = parser.NewDBytes(parser.DBytes(result))
			} else {
				return d, errors.Errorf("unsupported bytea encoding: %q", b)
			}
		case formatBinary:
			d = parser.NewDBytes(parser.DBytes(b))
		default:
			return d, errors.Errorf("unsupported bytea format code: %s", code)
		}
	case oid.T_timestamp:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, errors.Errorf("could not parse string %q as timestamp", b)
			}
			d = parser.MakeDTimestamp(ts, time.Microsecond)
		case formatBinary:
			if len(b) < 8 {
				return d, errors.Errorf("timestamp requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			d = parser.MakeDTimestamp(pgBinaryToTime(i), time.Microsecond)
		default:
			return d, errors.Errorf("unsupported timestamp format code: %s", code)
		}
	case oid.T_timestamptz:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, errors.Errorf("could not parse string %q as timestamp", b)
			}
			d = parser.MakeDTimestampTZ(ts, time.Microsecond)
		case formatBinary:
			if len(b) < 8 {
				return d, errors.Errorf("timestamptz requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			d = parser.MakeDTimestampTZ(pgBinaryToTime(i), time.Microsecond)
		default:
			return d, errors.Errorf("unsupported timestamptz format code: %s", code)
		}
	case oid.T_date:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				res, err := parser.ParseDDate(string(b), time.UTC)
				if err != nil {
					return d, errors.Errorf("could not parse string %q as date", b)
				}
				d = res
			} else {
				daysSinceEpoch := ts.Unix() / secondsInDay
				d = parser.NewDDate(parser.DDate(daysSinceEpoch))
			}
		case formatBinary:
			if len(b) < 4 {
				return d, errors.Errorf("date requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			d = pgBinaryToDate(i)
		default:
			return d, errors.Errorf("unsupported date format code: %s", code)
		}
	case oid.T_interval:
		switch code {
		case formatText:
			d, err := parser.ParseDInterval(string(b))
			if err != nil {
				return d, errors.Errorf("could not parse string %q as interval", b)
			}
			return d, nil
		default:
			return d, errors.Errorf("unsupported interval format code: %s", code)
		}
	default:
		return d, errors.Errorf("unsupported OID: %v", id)
	}
	return d, nil
}
