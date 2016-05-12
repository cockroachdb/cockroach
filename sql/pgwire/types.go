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
	"reflect"
	"strconv"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/pq"
	"github.com/cockroachdb/pq/oid"
)

//go:generate stringer -type=formatCode
type formatCode int16

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

//go:generate stringer -type=pgSign
type pgSign uint16

const (
	pgNumericPos pgSign = 0x0000
	pgNumericNeg pgSign = 0x4000
	// pgNumericNan pgSign = 0xC000
)

const (
	decDigits = 4
)

var (
	decShift = big.NewInt(int64(math.Pow10(decDigits)))
)

type pgNumeric struct {
	ndigits, weight, dscale int16
	sign                    pgSign
}

func typeForDatum(d parser.Datum) pgType {
	if d == parser.DNull {
		return pgType{}
	}
	switch d.(type) {
	case *parser.DBool:
		return pgType{oid.T_bool, 1}

	case *parser.DBytes:
		return pgType{oid.T_bytea, -1}

	case *parser.DInt:
		return pgType{oid.T_int8, 8}

	case *parser.DFloat:
		return pgType{oid.T_float8, 8}

	case *parser.DDecimal:
		return pgType{oid.T_numeric, -1}

	case *parser.DString:
		return pgType{oid.T_text, -1}

	case *parser.DDate:
		return pgType{oid.T_date, 8}

	case *parser.DTimestamp:
		return pgType{oid.T_timestamp, 8}

	case *parser.DTimestampTZ:
		return pgType{oid.T_timestamptz, 8}

	case *parser.DInterval:
		return pgType{oid.T_interval, 8}

	default:
		panic(fmt.Sprintf("unsupported type %T", d))
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(d parser.Datum, sessionLoc *time.Location) {
	if log.V(2) {
		log.Infof("pgwire writing TEXT datum of type: %T, %#v", d, d)
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
		// TODO(tamird): @petermattis sez this allocates. Investigate.
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

	default:
		b.setError(util.Errorf("unsupported type %T", d))
	}
}

func (b *writeBuffer) writeBinaryDatum(d parser.Datum) {
	if log.V(2) {
		log.Infof("pgwire writing BINARY datum of type: %T, %#v", d, d)
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

			bigI              big.Int
			wholeDec, fracDec inf.Dec
		}{}

		if v.Sign() >= 0 {
			alloc.pgNum.sign = pgNumericPos
		} else {
			alloc.pgNum.sign = pgNumericNeg
		}

		alloc.wholeDec.Abs(&v.Dec)
		alloc.wholeDec.Round(&alloc.wholeDec, 0, inf.RoundDown)

		alloc.fracDec.Abs(&v.Dec)
		alloc.fracDec.Sub(&alloc.fracDec, &alloc.wholeDec)

		alloc.pgNum.dscale = int16(alloc.fracDec.Scale())

		for big := alloc.bigI.Set(alloc.wholeDec.UnscaledBig()); big.Sign() > 0; big.Div(big, decShift) {
			alloc.pgNum.ndigits++
			alloc.pgNum.weight++
		}

		if alloc.pgNum.weight > 0 {
			alloc.pgNum.weight--
		}

		for big := alloc.bigI.Set(alloc.fracDec.UnscaledBig()); big.Sign() > 0; big.Div(big, decShift) {
			alloc.pgNum.ndigits++
		}

		b.putInt32(int32(2 * (4 + alloc.pgNum.ndigits)))
		b.putInt16(alloc.pgNum.ndigits)
		b.putInt16(alloc.pgNum.weight)
		b.putInt16(int16(alloc.pgNum.sign))
		b.putInt16(alloc.pgNum.dscale)

		for i := int16(0); i < alloc.pgNum.weight+1; i++ {
			big := alloc.bigI.Set(alloc.wholeDec.UnscaledBig())
			for j := i + 1; j < alloc.pgNum.weight+1; j++ {
				big.Div(big, decShift)
			}
			big.Mod(big, decShift)
			b.putInt16(int16(big.Int64()))
		}

		for i := alloc.pgNum.weight + 1; i < alloc.pgNum.ndigits; i++ {
			// Push decDigits ahead of the decimal point.
			alloc.fracDec.SetScale(alloc.fracDec.Scale() - decDigits)

			// Reuse wholeDesc now that we're done with it.
			dec := alloc.wholeDec.Round(&alloc.fracDec, 0, inf.RoundDown)

			// Send the stuff left of the decimal point.
			b.putInt16(int16(dec.UnscaledBig().Int64()))

			// Remove the stuff past the decimal point.
			alloc.fracDec.Sub(&alloc.fracDec, dec)
		}

	case *parser.DBytes:
		b.putInt32(int32(len(*v)))
		b.write([]byte(*v))

	case *parser.DString:
		b.writeLengthPrefixedString(string(*v))

	default:
		b.setError(util.Errorf("unsupported type %T", d))
	}
}

const pgTimeStampFormat = "2006-01-02 15:04:05.999999999-07:00"
const pgTimeStampFormatNoOffset = "2006-01-02 15:04:05.999999999"

// formatTs formats t into a format cockroachdb/pq understands.
// Mostly cribbed from github.com/cockroachdb/pq.
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
	oidToDatum = map[oid.Oid]parser.Datum{
		oid.T_bool:        parser.TypeBool,
		oid.T_bytea:       parser.TypeBytes,
		oid.T_date:        parser.TypeDate,
		oid.T_float4:      parser.TypeFloat,
		oid.T_float8:      parser.TypeFloat,
		oid.T_int2:        parser.TypeInt,
		oid.T_int4:        parser.TypeInt,
		oid.T_int8:        parser.TypeInt,
		oid.T_interval:    parser.TypeInterval,
		oid.T_numeric:     parser.TypeDecimal,
		oid.T_text:        parser.TypeString,
		oid.T_timestamp:   parser.TypeTimestamp,
		oid.T_timestamptz: parser.TypeTimestamp,
		oid.T_varchar:     parser.TypeString,
	}
	// Using reflection to support unhashable types.
	datumToOid = map[reflect.Type]oid.Oid{
		reflect.TypeOf(parser.TypeBool):      oid.T_bool,
		reflect.TypeOf(parser.TypeBytes):     oid.T_bytea,
		reflect.TypeOf(parser.TypeDate):      oid.T_date,
		reflect.TypeOf(parser.TypeFloat):     oid.T_float8,
		reflect.TypeOf(parser.TypeInt):       oid.T_int8,
		reflect.TypeOf(parser.TypeInterval):  oid.T_interval,
		reflect.TypeOf(parser.TypeDecimal):   oid.T_numeric,
		reflect.TypeOf(parser.TypeString):    oid.T_text,
		reflect.TypeOf(parser.TypeTimestamp): oid.T_timestamptz,
	}
)

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
				return d, util.Errorf("unsupported binary bool: %q", b)
			}
		default:
			return d, util.Errorf("unsupported bool format code: %d", code)
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
			var i int16
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, util.Errorf("unsupported int2 format code: %d", code)
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
			var i int32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, util.Errorf("unsupported int4 format code: %d", code)
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
			var i int64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.NewDInt(parser.DInt(i))
		default:
			return d, util.Errorf("unsupported int8 format code: %d", code)
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
			var f float32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.NewDFloat(parser.DFloat(f))
		default:
			return d, util.Errorf("unsupported float4 format code: %d", code)
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
			var f float64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.NewDFloat(parser.DFloat(f))
		default:
			return d, util.Errorf("unsupported float8 format code: %d", code)
		}
	case oid.T_numeric:
		switch code {
		case formatText:
			dd := &parser.DDecimal{}
			if _, ok := dd.SetString(string(b)); !ok {
				return nil, util.Errorf("could not parse string %q as decimal", b)
			}
			d = dd
		case formatBinary:
			r := bytes.NewReader(b)

			alloc := struct {
				pgNum pgNumeric
				i16   int16
				dec   inf.Dec

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
			for i := int16(0); i < alloc.pgNum.ndigits; i++ {
				if err := binary.Read(r, binary.BigEndian, &alloc.i16); err != nil {
					return d, err
				}
				alloc.dec.SetScale(decDigits * inf.Scale(i-alloc.pgNum.weight))
				if overPrecision := alloc.dec.Scale() - inf.Scale(alloc.pgNum.dscale); overPrecision > 0 {
					alloc.i16 /= int16(math.Pow10(int(overPrecision)))
					alloc.dec.SetScale(alloc.dec.Scale() - overPrecision)
				}
				alloc.dec.SetUnscaled(int64(alloc.i16))
				alloc.dd.Add(&alloc.dd.Dec, &alloc.dec)
			}

			switch alloc.pgNum.sign {
			case pgNumericPos:
			case pgNumericNeg:
				alloc.dd.Neg(&alloc.dd.Dec)
			default:
				return d, util.Errorf("unsupported numeric sign: %d", alloc.pgNum.sign)
			}

			d = &alloc.dd
		default:
			return d, util.Errorf("unsupported numeric format code: %d", code)
		}
	case oid.T_text, oid.T_varchar:
		switch code {
		case formatText, formatBinary:
			d = parser.NewDString(string(b))
		default:
			return d, util.Errorf("unsupported text format code: %d", code)
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
				return d, util.Errorf("unsupported bytea encoding: %q", b)
			}
		case formatBinary:
			d = parser.NewDBytes(parser.DBytes(b))
		default:
			return d, util.Errorf("unsupported bytea format code: %d", code)
		}
	case oid.T_timestamp, oid.T_timestamptz:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, util.Errorf("could not parse string %q as timestamp", b)
			}
			d = &parser.DTimestamp{Time: ts}
		default:
			return d, util.Errorf("unsupported timestamp format code: %d", code)
		}
	case oid.T_date:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, util.Errorf("could not parse string %q as date", b)
			}
			daysSinceEpoch := ts.Unix() / secondsInDay
			d = parser.NewDDate(parser.DDate(daysSinceEpoch))
		default:
			return d, util.Errorf("unsupported date format code: %d", code)
		}
	default:
		return d, util.Errorf("unsupported OID: %v", id)
	}
	return d, nil
}
