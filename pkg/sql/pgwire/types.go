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

package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
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

func pgTypeForParserType(t types.T) pgType {
	size := -1
	if s, variable := tree.DatumTypeSize(t); !variable {
		size = int(s)
	}
	return pgType{
		oid:  t.Oid(),
		size: size,
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(ctx context.Context, d tree.Datum, sessionLoc *time.Location) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing TEXT datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBool:
		b.putInt32(1)
		if *v {
			b.writeByte('t')
		} else {
			b.writeByte('f')
		}

	case *tree.DInt:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendInt(b.putbuf[4:4], int64(*v), 10)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DFloat:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], float64(*v), 'f', -1, 64)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DDecimal:
		b.writeLengthPrefixedDatum(v)

	case *tree.DBytes:
		// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
		// Code cribbed from github.com/lib/pq.
		result := make([]byte, 2+hex.EncodedLen(len(*v)))
		result[0] = '\\'
		result[1] = 'x'
		hex.Encode(result[2:], []byte(*v))

		b.putInt32(int32(len(result)))
		b.write(result)

	case *tree.DUuid:
		b.writeLengthPrefixedString(v.UUID.String())

	case *tree.DIPAddr:
		b.writeLengthPrefixedString(v.IPAddr.String())

	case *tree.DString:
		b.writeLengthPrefixedString(string(*v))

	case *tree.DCollatedString:
		b.writeLengthPrefixedString(v.Contents)

	case *tree.DDate:
		t := timeutil.Unix(int64(*v)*secondsInDay, 0)
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(t, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTime:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTime(timeofday.TimeOfDay(*v), b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTimestamp:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTimestampTZ:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, sessionLoc, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DInterval:
		b.writeLengthPrefixedString(v.ValueAsString())

	case *tree.DJSON:
		b.writeLengthPrefixedString(v.JSON.String())

	case *tree.DTuple:
		b.variablePutbuf.WriteString("(")
		for i, d := range v.D {
			if i > 0 {
				b.variablePutbuf.WriteString(",")
			}
			if d == tree.DNull {
				// Emit nothing on NULL.
				continue
			}
			b.simpleFormatter.FormatNode(d)
		}
		b.variablePutbuf.WriteString(")")
		b.writeLengthPrefixedVariablePutbuf()

	case *tree.DArray:
		// Arrays are serialized as a string of comma-separated values, surrounded
		// by braces.
		begin, sep, end := "{", ",", "}"

		if d.ResolvedType().Oid() == oid.T_int2vector {
			// int2vectors are serialized as a string of space-separated values.
			begin, sep, end = "", " ", ""
		}

		b.variablePutbuf.WriteString(begin)
		for i, d := range v.Array {
			if i > 0 {
				b.variablePutbuf.WriteString(sep)
			}
			// TODO(justin): add a test for nested arrays.
			b.arrayFormatter.FormatNode(d)
		}
		b.variablePutbuf.WriteString(end)
		b.writeLengthPrefixedVariablePutbuf()

	case *tree.DOid:
		b.writeLengthPrefixedDatum(v)

	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

func (b *writeBuffer) writeBinaryDatum(
	ctx context.Context, d tree.Datum, sessionLoc *time.Location,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing BINARY datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBool:
		b.putInt32(1)
		if *v {
			b.writeByte(1)
		} else {
			b.writeByte(0)
		}

	case *tree.DInt:
		b.putInt32(8)
		b.putInt64(int64(*v))

	case *tree.DFloat:
		b.putInt32(8)
		b.putInt64(int64(math.Float64bits(float64(*v))))

	case *tree.DDecimal:
		alloc := struct {
			pgNum pgNumeric

			bigI big.Int
		}{
			pgNum: pgNumeric{
				// Since we use 2000 as the exponent limits in tree.DecimalCtx, this
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

	case *tree.DBytes:
		b.putInt32(int32(len(*v)))
		b.write([]byte(*v))

	case *tree.DUuid:
		b.putInt32(16)
		b.write(v.GetBytes())

	case *tree.DIPAddr:
		// We calculate the Postgres binary format for an IPAddr. For the spec see,
		// https://github.com/postgres/postgres/blob/81c5e46c490e2426db243eada186995da5bb0ba7/src/backend/utils/adt/network.c#L144
		// The pgBinary encoding is as follows:
		//  The int32 length of the following bytes.
		//  The family byte.
		//  The mask size byte.
		//  A 0 byte for is_cidr. It's ignored on the postgres frontend.
		//  The length of our IP bytes.
		//  The IP bytes.
		const pgIPAddrBinaryHeaderSize = 4
		if v.Family == ipaddr.IPv4family {
			b.putInt32(net.IPv4len + pgIPAddrBinaryHeaderSize)
			b.writeByte(pgBinaryIPv4family)
			b.writeByte(v.Mask)
			b.writeByte(0)
			b.writeByte(byte(net.IPv4len))
			err := v.Addr.WriteIPv4Bytes(b)
			if err != nil {
				b.setError(err)
			}
		} else if v.Family == ipaddr.IPv6family {
			b.putInt32(net.IPv6len + pgIPAddrBinaryHeaderSize)
			b.writeByte(pgBinaryIPv6family)
			b.writeByte(v.Mask)
			b.writeByte(0)
			b.writeByte(byte(net.IPv6len))
			err := v.Addr.WriteIPv6Bytes(b)
			if err != nil {
				b.setError(err)
			}
		} else {
			b.setError(errors.Errorf("error encoding inet to pgBinary: %v", v.IPAddr))
		}

	case *tree.DString:
		b.writeLengthPrefixedString(string(*v))

	case *tree.DCollatedString:
		b.writeLengthPrefixedString(v.Contents)

	case *tree.DTimestamp:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, nil))

	case *tree.DTimestampTZ:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, sessionLoc))

	case *tree.DDate:
		b.putInt32(4)
		b.putInt32(dateToPgBinary(v))

	case *tree.DTime:
		b.putInt32(8)
		b.putInt64(int64(*v))

	case *tree.DArray:
		if v.ParamTyp.FamilyEqual(types.AnyArray) {
			b.setError(errors.New("unsupported binary serialization of multidimensional arrays"))
			return
		}
		// TODO(andrei): We shouldn't be allocating a new buffer for every array.
		subWriter := newWriteBuffer(nil /* bytecount */)
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
			subWriter.writeBinaryDatum(ctx, elem, sessionLoc)
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)
	case *tree.DOid:
		b.putInt32(4)
		b.putInt32(int32(v.DInt))
	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

const pgTimeFormat = "15:04:05.999999"
const pgTimeStampFormatNoOffset = "2006-01-02 " + pgTimeFormat
const pgTimeStampFormat = pgTimeStampFormatNoOffset + "-07:00"

// formatTime formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func formatTime(t timeofday.TimeOfDay, tmp []byte) []byte {
	return t.ToTime().AppendFormat(tmp, pgTimeFormat)
}

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
func dateToPgBinary(d *tree.DDate) int32 {
	return int32(*d) - pgEpochJDateFromUnix
}

// pgBinaryToDate takes an int32 and interprets it as the Postgres binary format
// for a date. To create a date from this value, it takes the day delta and adds
// it to pgEpochJDate.
func pgBinaryToDate(i int32) *tree.DDate {
	daysSinceEpoch := pgEpochJDateFromUnix + i
	return tree.NewDDate(tree.DDate(daysSinceEpoch))
}

const (
	// pgBinaryIPv4family is the pgwire constant for IPv4. It is defined as
	// AF_INET.
	pgBinaryIPv4family byte = 2
	// pgBinaryIPv6family is the pgwire constant for IPv4. It is defined as
	// AF_NET + 1.
	pgBinaryIPv6family byte = 3
)

// pgBinaryToIPAddr takes an IPAddr and interprets it as the Postgres binary
// format. See https://github.com/postgres/postgres/blob/81c5e46c490e2426db243eada186995da5bb0ba7/src/backend/utils/adt/network.c#L144
// for the binary spec.
func pgBinaryToIPAddr(b []byte) (ipaddr.IPAddr, error) {
	mask := b[1]
	familyByte := b[0]
	var addr ipaddr.Addr
	var family ipaddr.IPFamily

	if familyByte == pgBinaryIPv4family {
		family = ipaddr.IPv4family
	} else if familyByte == pgBinaryIPv6family {
		family = ipaddr.IPv6family
	} else {
		return ipaddr.IPAddr{}, errors.Errorf("unknown family received: %d", familyByte)
	}

	// Get the IP address bytes. The IP address length is byte 3 but is ignored.
	if family == ipaddr.IPv4family {
		// Add the IPv4-mapped IPv6 prefix of 0xFF.
		var tmp [16]byte
		tmp[10] = 0xff
		tmp[11] = 0xff
		copy(tmp[12:], b[4:])
		addr = ipaddr.Addr(uint128.FromBytes(tmp[:]))
	} else {
		addr = ipaddr.Addr(uint128.FromBytes(b[4:]))
	}

	return ipaddr.IPAddr{
		Family: family,
		Mask:   mask,
		Addr:   addr,
	}, nil
}

// decodeOidDatum decodes bytes with specified Oid and format code into
// a datum.
func decodeOidDatum(id oid.Oid, code pgwirebase.FormatCode, b []byte) (tree.Datum, error) {
	switch code {
	case pgwirebase.FormatText:
		switch id {
		case oid.T_bool:
			t, err := strconv.ParseBool(string(b))
			if err != nil {
				return nil, err
			}
			return tree.MakeDBool(tree.DBool(t)), nil
		case oid.T_int2, oid.T_int4, oid.T_int8:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return nil, err
			}
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_oid:
			u, err := strconv.ParseUint(string(b), 10, 32)
			if err != nil {
				return nil, err
			}
			return tree.NewDOid(tree.DInt(u)), nil
		case oid.T_float4, oid.T_float8:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return nil, err
			}
			return tree.NewDFloat(tree.DFloat(f)), nil
		case oid.T_numeric:
			d, err := tree.ParseDDecimal(string(b))
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
				return tree.NewDBytes(tree.DBytes(result)), nil
			}
			return nil, errors.Errorf("unsupported bytea encoding: %q", b)
		case oid.T_timestamp:
			d, err := tree.ParseDTimestamp(string(b), time.Microsecond)
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as timestamp", b)
			}
			return d, nil
		case oid.T_timestamptz:
			d, err := tree.ParseDTimestampTZ(string(b), time.UTC, time.Microsecond)
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as timestamptz", b)
			}
			return d, nil
		case oid.T_date:
			ts, err := tree.ParseDTimestamp(string(b), time.Microsecond)
			if err != nil {
				res, err := tree.ParseDDate(string(b), time.UTC)
				if err != nil {
					return nil, errors.Errorf("could not parse string %q as date", b)
				}
				return res, nil
			}
			daysSinceEpoch := ts.Unix() / secondsInDay
			return tree.NewDDate(tree.DDate(daysSinceEpoch)), nil
		case oid.T_time:
			d, err := tree.ParseDTime(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as time", b)
			}
			return d, nil
		case oid.T_interval:
			d, err := tree.ParseDInterval(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as interval", b)
			}
			return d, nil
		case oid.T_uuid:
			d, err := tree.ParseDUuidFromString(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as uuid", b)
			}
			return d, nil
		case oid.T_inet:
			d, err := tree.ParseDIPAddrFromINetString(string(b))
			if err != nil {
				return nil, errors.Errorf("could not parse string %q as inet", b)
			}
			return d, nil
		case oid.T__int2, oid.T__int4, oid.T__int8:
			var arr pq.Int64Array
			if err := (&arr).Scan(b); err != nil {
				return nil, err
			}
			out := tree.NewDArray(types.Int)
			for _, v := range arr {
				if err := out.Append(tree.NewDInt(tree.DInt(v))); err != nil {
					return nil, err
				}
			}
			return out, nil
		case oid.T__text, oid.T__name:
			var arr pq.StringArray
			if err := (&arr).Scan(b); err != nil {
				return nil, err
			}
			out := tree.NewDArray(types.String)
			if id == oid.T__name {
				out.ParamTyp = types.Name
			}
			for _, v := range arr {
				var s tree.Datum = tree.NewDString(v)
				if id == oid.T__name {
					s = tree.NewDNameFromDString(s.(*tree.DString))
				}
				if err := out.Append(s); err != nil {
					return nil, err
				}
			}
			return out, nil
		}
		if _, ok := types.ArrayOids[id]; ok {
			// Arrays come in in their string form, so we parse them as such and later
			// convert them to their actual datum form.
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			return tree.NewDString(string(b)), nil
		}
	case pgwirebase.FormatBinary:
		switch id {
		case oid.T_bool:
			if len(b) > 0 {
				switch b[0] {
				case 0:
					return tree.MakeDBool(false), nil
				case 1:
					return tree.MakeDBool(true), nil
				}
			}
			return nil, errors.Errorf("unsupported binary bool: %x", b)
		case oid.T_int2:
			if len(b) < 2 {
				return nil, errors.Errorf("int2 requires 2 bytes for binary format")
			}
			i := int16(binary.BigEndian.Uint16(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_int4:
			if len(b) < 4 {
				return nil, errors.Errorf("int4 requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_int8:
			if len(b) < 8 {
				return nil, errors.Errorf("int8 requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_oid:
			if len(b) < 4 {
				return nil, errors.Errorf("oid requires 4 bytes for binary format")
			}
			u := binary.BigEndian.Uint32(b)
			return tree.NewDOid(tree.DInt(u)), nil
		case oid.T_float4:
			if len(b) < 4 {
				return nil, errors.Errorf("float4 requires 4 bytes for binary format")
			}
			f := math.Float32frombits(binary.BigEndian.Uint32(b))
			return tree.NewDFloat(tree.DFloat(f)), nil
		case oid.T_float8:
			if len(b) < 8 {
				return nil, errors.Errorf("float8 requires 8 bytes for binary format")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(b))
			return tree.NewDFloat(tree.DFloat(f)), nil
		case oid.T_numeric:
			r := bytes.NewReader(b)

			alloc := struct {
				pgNum pgNumeric
				i16   int16

				dd tree.DDecimal
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
			return tree.NewDBytes(tree.DBytes(b)), nil
		case oid.T_timestamp:
			if len(b) < 8 {
				return nil, errors.Errorf("timestamp requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestamp(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_timestamptz:
			if len(b) < 8 {
				return nil, errors.Errorf("timestamptz requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestampTZ(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_date:
			if len(b) < 4 {
				return nil, errors.Errorf("date requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return pgBinaryToDate(i), nil
		case oid.T_time:
			if len(b) < 8 {
				return nil, errors.Errorf("time requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTime(timeofday.TimeOfDay(i)), nil
		case oid.T_uuid:
			u, err := tree.ParseDUuidFromBytes(b)
			if err != nil {
				return nil, err
			}
			return u, nil
		case oid.T_inet:
			ipAddr, err := pgBinaryToIPAddr(b)
			if err != nil {
				return nil, err
			}
			return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
		case oid.T__int2, oid.T__int4, oid.T__int8, oid.T__text, oid.T__name:
			return decodeBinaryArray(b, code)
		}
	default:
		return nil, errors.Errorf("unsupported format code: %s", code)
	}

	// Types with identical text/binary handling.
	switch id {
	case oid.T_text, oid.T_varchar, oid.T_jsonb:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		return tree.NewDString(string(b)), nil
	case oid.T_name:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		return tree.NewDName(string(b)), nil
	default:
		return nil, errors.Errorf("unsupported OID %v with format code %s", id, code)
	}
}

var invalidUTF8Error = pgerror.NewErrorf(pgerror.CodeCharacterNotInRepertoireError, "invalid UTF-8 sequence")

// Values which are going to be converted to strings (STRING and NAME) need to
// be valid UTF-8 for us to accept them.
func validateStringBytes(b []byte) error {
	if !utf8.Valid(b) {
		return invalidUTF8Error
	}
	return nil
}

func decodeBinaryArray(b []byte, code pgwirebase.FormatCode) (tree.Datum, error) {
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
	arr := tree.NewDArray(types.OidToType[elemOid])
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
