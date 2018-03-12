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
	"context"
	"encoding/hex"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

		switch d.ResolvedType().Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			// vectors are serialized as a string of space-separated values.
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
			pgNum pgwirebase.PGNumeric

			bigI big.Int
		}{
			pgNum: pgwirebase.PGNumeric{
				// Since we use 2000 as the exponent limits in tree.DecimalCtx, this
				// conversion should not overflow.
				Dscale: int16(-v.Exponent),
			},
		}

		if v.Sign() >= 0 {
			alloc.pgNum.Sign = pgwirebase.PGNumericPos
		} else {
			alloc.pgNum.Sign = pgwirebase.PGNumericNeg
		}

		isZero := func(r rune) bool {
			return r == '0'
		}

		// Mostly cribbed from libpqtypes' str2num.
		digits := strings.TrimLeftFunc(alloc.bigI.Abs(&v.Coeff).String(), isZero)
		dweight := len(digits) - int(alloc.pgNum.Dscale) - 1
		digits = strings.TrimRightFunc(digits, isZero)

		if dweight >= 0 {
			alloc.pgNum.Weight = int16((dweight+1+pgwirebase.PGDecDigits-1)/pgwirebase.PGDecDigits - 1)
		} else {
			alloc.pgNum.Weight = int16(-((-dweight-1)/pgwirebase.PGDecDigits + 1))
		}
		offset := (int(alloc.pgNum.Weight)+1)*pgwirebase.PGDecDigits - (dweight + 1)
		alloc.pgNum.Ndigits = int16((len(digits) + offset + pgwirebase.PGDecDigits - 1) / pgwirebase.PGDecDigits)

		if len(digits) == 0 {
			offset = 0
			alloc.pgNum.Ndigits = 0
			alloc.pgNum.Weight = 0
		}

		digitIdx := -offset

		nextDigit := func() int16 {
			var ndigit int16
			for nextDigitIdx := digitIdx + pgwirebase.PGDecDigits; digitIdx < nextDigitIdx; digitIdx++ {
				ndigit *= 10
				if digitIdx >= 0 && digitIdx < len(digits) {
					ndigit += int16(digits[digitIdx] - '0')
				}
			}
			return ndigit
		}

		b.putInt32(int32(2 * (4 + alloc.pgNum.Ndigits)))
		b.putInt16(alloc.pgNum.Ndigits)
		b.putInt16(alloc.pgNum.Weight)
		b.putInt16(int16(alloc.pgNum.Sign))
		b.putInt16(alloc.pgNum.Dscale)

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
			b.writeByte(pgwirebase.PGBinaryIPv4family)
			b.writeByte(v.Mask)
			b.writeByte(0)
			b.writeByte(byte(net.IPv4len))
			err := v.Addr.WriteIPv4Bytes(b)
			if err != nil {
				b.setError(err)
			}
		} else if v.Family == ipaddr.IPv6family {
			b.putInt32(net.IPv6len + pgIPAddrBinaryHeaderSize)
			b.writeByte(pgwirebase.PGBinaryIPv6family)
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
		// Lower bound, we only support a lower bound of 1.
		subWriter.putInt32(1)
		for _, elem := range v.Array {
			subWriter.writeBinaryDatum(ctx, elem, sessionLoc)
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)
	case *tree.DJSON:
		s := v.JSON.String()
		b.putInt32(int32(len(s) + 1))
		// Postgres version number, as of writing, `1` is the only valid value.
		b.writeByte(1)
		b.writeString(s)
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

// timeToPgBinary calculates the Postgres binary format for a timestamp. The timestamp
// is represented as the number of microseconds between the given time and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int64.
func timeToPgBinary(t time.Time, offset *time.Location) int64 {
	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}
	return duration.DiffMicros(t, pgwirebase.PGEpochJDate)
}

// dateToPgBinary calculates the Postgres binary format for a date. The date is
// represented as the number of days between the given date and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int32.
func dateToPgBinary(d *tree.DDate) int32 {
	return int32(*d) - pgwirebase.PGEpochJDateFromUnix
}
