// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"encoding/binary"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

func pgTypeForParserType(t *types.T) pgType {
	size := -1
	if s, variable := tree.DatumTypeSize(t); !variable {
		size = int(s)
	}
	return pgType{
		oid:  t.Oid(),
		size: size,
	}
}

func writeTextBool(b *writeBuffer, v bool) {
	b.putInt32(1)
	b.writeByte(tree.PgwireFormatBool(v))
}

func writeTextInt64(b *writeBuffer, v int64) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := strconv.AppendInt(b.putbuf[4:4], v, 10)
	b.putInt32(int32(len(s)))
	b.write(s)
}

func writeTextFloat64(b *writeBuffer, fl float64, conv sessiondatapb.DataConversionConfig) {
	var s []byte
	// PostgreSQL supports 'Inf' as a valid literal for the floating point
	// special value Infinity, therefore handling the special cases for them.
	// (https://github.com/cockroachdb/cockroach/issues/62601)
	if math.IsInf(fl, 1) {
		s = []byte("Infinity")
	} else if math.IsInf(fl, -1) {
		s = []byte("-Infinity")
	} else {
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s = strconv.AppendFloat(b.putbuf[4:4], fl, 'g', conv.GetFloatPrec(), 64)
	}
	b.putInt32(int32(len(s)))
	b.write(s)
}

func writeTextBytes(b *writeBuffer, v string, conv sessiondatapb.DataConversionConfig) {
	result := lex.EncodeByteArrayToRawBytes(v, conv.BytesEncodeFormat, false /* skipHexPrefix */)
	b.putInt32(int32(len(result)))
	b.write([]byte(result))
}

func writeTextUUID(b *writeBuffer, v uuid.UUID) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := b.putbuf[4 : 4+36]
	v.StringBytes(s)
	b.putInt32(int32(len(s)))
	b.write(s)
}

func writeTextString(b *writeBuffer, v string, t *types.T) {
	b.writeLengthPrefixedString(tree.ResolveBlankPaddedChar(v, t))
}

func writeTextTimestamp(b *writeBuffer, v time.Time) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := formatTs(v, nil, b.putbuf[4:4])
	b.putInt32(int32(len(s)))
	b.write(s)
}

func writeTextTimestampTZ(b *writeBuffer, v time.Time, sessionLoc *time.Location) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := formatTs(v, sessionLoc, b.putbuf[4:4])
	b.putInt32(int32(len(s)))
	b.write(s)
}

// writeTextDatum writes d to the buffer. Type t must be specified for types
// that have various width encodings and therefore need padding (chars).
// It is ignored (and can be nil) for types which do not need padding.
func (b *writeBuffer) writeTextDatum(
	ctx context.Context,
	d tree.Datum,
	conv sessiondatapb.DataConversionConfig,
	sessionLoc *time.Location,
	t *types.T,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing TEXT datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	writeTextDatumNotNull(b, d, conv, sessionLoc, t)
}

// writeTextDatumNotNull writes d to the buffer when d is not null. Type t must
// be specified for types that have various width encodings and therefore need
// padding (chars). It is ignored (and can be nil) for types which do not need
// padding.
func writeTextDatumNotNull(
	b *writeBuffer,
	d tree.Datum,
	conv sessiondatapb.DataConversionConfig,
	sessionLoc *time.Location,
	t *types.T,
) {
	b.textFormatter.WithDataConversionConfig(
		conv,
		func() {
			switch v := tree.UnwrapDatum(nil, d).(type) {
			case *tree.DBitArray:
				b.textFormatter.FormatNode(v)
				b.writeFromFmtCtx(b.textFormatter)

			case *tree.DBool:
				writeTextBool(b, bool(*v))

			case *tree.DInt:
				writeTextInt64(b, int64(*v))

			case *tree.DFloat:
				fl := float64(*v)
				writeTextFloat64(b, fl, conv)

			case *tree.DDecimal:
				b.writeLengthPrefixedDatum(v)

			case *tree.DBytes:
				writeTextBytes(b, string(*v), conv)

			case *tree.DUuid:
				writeTextUUID(b, v.UUID)

			case *tree.DIPAddr:
				b.writeLengthPrefixedString(v.IPAddr.String())

			case *tree.DString:
				writeTextString(b, string(*v), t)

			case *tree.DCollatedString:
				b.writeLengthPrefixedString(tree.ResolveBlankPaddedChar(v.Contents, t))

			case *tree.DDate:
				b.textFormatter.FormatNode(v)
				b.writeFromFmtCtx(b.textFormatter)

			case *tree.DTime:
				// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
				s := formatTime(timeofday.TimeOfDay(*v), b.putbuf[4:4])
				b.putInt32(int32(len(s)))
				b.write(s)

			case *tree.DTimeTZ:
				// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
				s := formatTimeTZ(v.TimeTZ, b.putbuf[4:4])
				b.putInt32(int32(len(s)))
				b.write(s)

			case *tree.DBox2D:
				s := v.Repr()
				b.putInt32(int32(len(s)))
				b.write([]byte(s))

			case *tree.DGeography:
				s := v.Geography.EWKBHex()
				b.putInt32(int32(len(s)))
				b.write([]byte(s))

			case *tree.DGeometry:
				s := v.Geometry.EWKBHex()
				b.putInt32(int32(len(s)))
				b.write([]byte(s))

			case *tree.DTimestamp:
				writeTextTimestamp(b, v.Time)

			case *tree.DTimestampTZ:
				writeTextTimestampTZ(b, v.Time, sessionLoc)

			case *tree.DInterval:
				b.textFormatter.FormatNode(v)
				b.writeFromFmtCtx(b.textFormatter)

			case *tree.DJSON:
				b.writeLengthPrefixedString(v.JSON.String())

			case *tree.DTuple:
				b.textFormatter.FormatNode(v)
				b.writeFromFmtCtx(b.textFormatter)

			case *tree.DArray:
				// Arrays have custom formatting depending on their OID.
				b.textFormatter.FormatNode(d)
				b.writeFromFmtCtx(b.textFormatter)

			case *tree.DOid:
				b.writeLengthPrefixedDatum(v)

			case *tree.DEnum:
				// Enums are serialized with their logical representation.
				b.writeLengthPrefixedString(v.LogicalRep)

			default:
				b.setError(errors.Errorf("unsupported type %T", d))
			}
		})
}

// getInt64 returns an int64 from vectors of Int family.
func getInt64(vec coldata.Vec, idx int) int64 {
	switch vec.Type().Width() {
	case 16:
		return int64(vec.Int16().Get(idx))
	case 32:
		return int64(vec.Int32().Get(idx))
	default:
		return vec.Int64().Get(idx)
	}
}

// writeTextColumnarElement is the same as writeTextDatum where the datum is
// represented in a columnar element (at position idx in vec).
func (b *writeBuffer) writeTextColumnarElement(
	ctx context.Context,
	vec coldata.Vec,
	idx int,
	conv sessiondatapb.DataConversionConfig,
	sessionLoc *time.Location,
) {
	b.textFormatter.WithDataConversionConfig(
		conv,
		func() {
			if log.V(2) {
				log.Infof(ctx, "pgwire writing TEXT columnar element of type: %s", vec.Type())
			}
			if vec.MaybeHasNulls() && vec.Nulls().NullAt(idx) {
				// NULL is encoded as -1; all other values have a length prefix.
				b.putInt32(-1)
				return
			}
			switch vec.Type().Family() {
			case types.BoolFamily:
				writeTextBool(b, vec.Bool().Get(idx))

			case types.IntFamily:
				writeTextInt64(b, getInt64(vec, idx))

			case types.FloatFamily:
				writeTextFloat64(b, vec.Float64().Get(idx), conv)

			case types.DecimalFamily:
				d := vec.Decimal().Get(idx)
				// The logic here is the simplification of tree.DDecimal.Format given
				// that we use tree.FmtSimple.
				b.writeLengthPrefixedString(d.String())

			case types.BytesFamily:
				writeTextBytes(b, string(vec.Bytes().Get(idx)), conv)

			case types.UuidFamily:
				id, err := uuid.FromBytes(vec.Bytes().Get(idx))
				if err != nil {
					panic(errors.Wrap(err, "unexpectedly couldn't retrieve UUID object"))
				}
				writeTextUUID(b, id)

			case types.StringFamily:
				writeTextString(b, string(vec.Bytes().Get(idx)), vec.Type())

			case types.DateFamily:
				tree.FormatDate(pgdate.MakeCompatibleDateFromDisk(vec.Int64().Get(idx)), b.textFormatter)
				b.writeFromFmtCtx(b.textFormatter)

			case types.TimestampFamily:
				writeTextTimestamp(b, vec.Timestamp().Get(idx))

			case types.TimestampTZFamily:
				writeTextTimestampTZ(b, vec.Timestamp().Get(idx), sessionLoc)

			case types.IntervalFamily:
				tree.FormatDuration(vec.Interval().Get(idx), b.textFormatter)
				b.writeFromFmtCtx(b.textFormatter)

			case types.JsonFamily:
				b.writeLengthPrefixedString(vec.JSON().Get(idx).String())

			default:
				// All other types are represented via the datum-backed vector.
				writeTextDatumNotNull(b, vec.Datum().Get(idx).(*coldataext.Datum).Datum, conv, sessionLoc, vec.Type())
			}
		},
	)
}

func writeBinaryBool(b *writeBuffer, v bool) {
	b.putInt32(1)
	if v {
		b.writeByte(1)
	} else {
		b.writeByte(0)
	}
}

func writeBinaryInt(b *writeBuffer, v int64, t *types.T) {
	switch t.Oid() {
	case oid.T_int2:
		b.putInt32(2)
		b.putInt16(int16(v))
	case oid.T_int4:
		b.putInt32(4)
		b.putInt32(int32(v))
	case oid.T_int8:
		b.putInt32(8)
		b.putInt64(v)
	default:
		b.setError(errors.Errorf("unsupported int oid: %v", t.Oid()))
	}
}

func writeBinaryFloat(b *writeBuffer, v float64, t *types.T) {
	switch t.Oid() {
	case oid.T_float4:
		b.putInt32(4)
		b.putInt32(int32(math.Float32bits(float32(v))))
	case oid.T_float8:
		b.putInt32(8)
		b.putInt64(int64(math.Float64bits(v)))
	default:
		b.setError(errors.Errorf("unsupported float oid: %v", t.Oid()))
	}
}

func writeBinaryDecimal(b *writeBuffer, v *apd.Decimal) {
	if v.Form != apd.Finite {
		b.putInt32(8)
		// 0 digits.
		b.putInt32(0)
		// https://github.com/postgres/postgres/blob/ffa4cbd623dd69f9fa99e5e92426928a5782cf1a/src/backend/utils/adt/numeric.c#L169
		b.write([]byte{0xc0, 0, 0, 0})

		if v.Form == apd.Infinite {
			// TODO(mjibson): #32489
			// The above encoding is not correct for Infinity, but since that encoding
			// doesn't exist in postgres, it's unclear what to do. For now use the NaN
			// encoding and count it to see if anyone even needs this.
			telemetry.Inc(sqltelemetry.BinaryDecimalInfinityCounter)
		}

		return
	}

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

	// The dscale is defined as number of digits (in base 10) visible
	// after the decimal separator, so it can't be negative.
	if alloc.pgNum.Dscale < 0 {
		alloc.pgNum.Dscale = 0
	}

	b.putInt32(int32(2 * (4 + alloc.pgNum.Ndigits)))
	b.putInt16(alloc.pgNum.Ndigits)
	b.putInt16(alloc.pgNum.Weight)
	b.putInt16(int16(alloc.pgNum.Sign))
	b.putInt16(alloc.pgNum.Dscale)

	for digitIdx < len(digits) {
		b.putInt16(nextDigit())
	}
}

func writeBinaryBytes(b *writeBuffer, v []byte) {
	b.putInt32(int32(len(v)))
	b.write(v)
}

func writeBinaryString(b *writeBuffer, v string, t *types.T) {
	b.writeLengthPrefixedString(tree.ResolveBlankPaddedChar(v, t))
}

func writeBinaryTimestamp(b *writeBuffer, v time.Time) {
	b.putInt32(8)
	b.putInt64(timeToPgBinary(v, nil))
}

func writeBinaryTimestampTZ(b *writeBuffer, v time.Time, sessionLoc *time.Location) {
	b.putInt32(8)
	b.putInt64(timeToPgBinary(v, sessionLoc))
}

func writeBinaryDate(b *writeBuffer, v pgdate.Date) {
	b.putInt32(4)
	b.putInt32(v.PGEpochDays())
}

func writeBinaryInterval(b *writeBuffer, v duration.Duration) {
	b.putInt32(16)
	b.putInt64(v.Nanos() / int64(time.Microsecond/time.Nanosecond))
	b.putInt32(int32(v.Days))
	b.putInt32(int32(v.Months))
}

func writeBinaryJSON(b *writeBuffer, v json.JSON) {
	s := v.String()
	b.putInt32(int32(len(s) + 1))
	// Postgres version number, as of writing, `1` is the only valid value.
	b.writeByte(1)
	b.writeString(s)
}

// writeBinaryDatum writes d to the buffer. Type t must be specified for types
// that have various width encodings (floats, ints, chars). It is ignored
// (and can be nil) for types with a 1:1 datum:type mapping.
func (b *writeBuffer) writeBinaryDatum(
	ctx context.Context, d tree.Datum, sessionLoc *time.Location, t *types.T,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing BINARY datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	writeBinaryDatumNotNull(ctx, b, d, sessionLoc, t)
}

// writeBinaryDatumNotNull writes d to the buffer when d is not null. Type t
// must be specified for types that have various width encodings (floats, ints,
// chars). It is ignored (and can be nil) for types with a 1:1 datum:type
// mapping.
func writeBinaryDatumNotNull(
	ctx context.Context, b *writeBuffer, d tree.Datum, sessionLoc *time.Location, t *types.T,
) {
	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBitArray:
		words, lastBitsUsed := v.EncodingParts()
		if len(words) == 0 {
			b.putInt32(4)
		} else {
			// Encode the length of the output bytes. It is computed here so we don't
			// have to keep a buffer.
			// 4: the int32 of the bitLen.
			// 8*(len(words)-1): number of 8-byte words except the last one since it's
			//   partial.
			// (lastBitsUsed+7)/8: number of bytes that will be written in the last
			//   partial word. The /8 rounds down, such that the +7 will cause 1-or-more
			//   bits to use a byte, but 0 will not.
			b.putInt32(4 + int32(8*(len(words)-1)) + int32((lastBitsUsed+7)/8))
		}
		bitLen := v.BitLen()
		b.putInt32(int32(bitLen))
		var byteBuf [8]byte
		for i := 0; i < len(words)-1; i++ {
			w := words[i]
			binary.BigEndian.PutUint64(byteBuf[:], w)
			b.write(byteBuf[:])
		}
		if len(words) > 0 {
			w := words[len(words)-1]
			for i := uint(0); i < uint(lastBitsUsed); i += 8 {
				c := byte(w >> (56 - i))
				b.writeByte(c)
			}
		}

	case *tree.DBool:
		writeBinaryBool(b, bool(*v))

	case *tree.DInt:
		writeBinaryInt(b, int64(*v), t)

	case *tree.DFloat:
		writeBinaryFloat(b, float64(*v), t)

	case *tree.DDecimal:
		writeBinaryDecimal(b, &v.Decimal)

	case *tree.DBytes:
		writeBinaryBytes(b, []byte(*v))

	case *tree.DUuid:
		writeBinaryBytes(b, v.GetBytes())

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

	case *tree.DEnum:
		b.writeLengthPrefixedString(v.LogicalRep)

	case *tree.DString:
		writeBinaryString(b, string(*v), t)

	case *tree.DCollatedString:
		b.writeLengthPrefixedString(tree.ResolveBlankPaddedChar(v.Contents, t))

	case *tree.DTimestamp:
		writeBinaryTimestamp(b, v.Time)

	case *tree.DTimestampTZ:
		writeBinaryTimestampTZ(b, v.Time, sessionLoc)

	case *tree.DDate:
		writeBinaryDate(b, v.Date)

	case *tree.DTime:
		b.putInt32(8)
		b.putInt64(int64(*v))

	case *tree.DTimeTZ:
		b.putInt32(12)
		b.putInt64(int64(v.TimeOfDay))
		b.putInt32(v.OffsetSecs)

	case *tree.DInterval:
		writeBinaryInterval(b, v.Duration)

	case *tree.DTuple:
		// TODO(andrei): We shouldn't be allocating a new buffer for every array.
		subWriter := newWriteBuffer(nil /* bytecount */)
		// Put the number of datums.
		subWriter.putInt32(int32(len(v.D)))
		tupleTypes := t.TupleContents()
		for i, elem := range v.D {
			oid := tupleTypes[i].Oid()
			subWriter.putInt32(int32(oid))
			subWriter.writeBinaryDatum(ctx, elem, sessionLoc, tupleTypes[i])
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)

	case *tree.DBox2D:
		b.putInt32(32)
		b.putInt64(int64(math.Float64bits(v.LoX)))
		b.putInt64(int64(math.Float64bits(v.HiX)))
		b.putInt64(int64(math.Float64bits(v.LoY)))
		b.putInt64(int64(math.Float64bits(v.HiY)))

	case *tree.DGeography:
		b.putInt32(int32(len(v.EWKB())))
		b.write(v.EWKB())

	case *tree.DGeometry:
		b.putInt32(int32(len(v.EWKB())))
		b.write(v.EWKB())

	case *tree.DArray:
		if v.ParamTyp.Family() == types.ArrayFamily {
			b.setError(unimplemented.NewWithIssueDetail(32552,
				"binenc", "unsupported binary serialization of multidimensional arrays"))
			return
		}
		// TODO(andrei): We shouldn't be allocating a new buffer for every array.
		subWriter := newWriteBuffer(nil /* bytecount */)
		// Put the number of dimensions. We currently support 1d arrays only.
		var ndims int32 = 1
		if v.Len() == 0 {
			ndims = 0
		}
		subWriter.putInt32(ndims)
		hasNulls := 0
		if v.HasNulls {
			hasNulls = 1
		}
		oid := v.ParamTyp.Oid()
		subWriter.putInt32(int32(hasNulls))
		subWriter.putInt32(int32(oid))
		if v.Len() > 0 {
			subWriter.putInt32(int32(v.Len()))
			// Lower bound, we only support a lower bound of 1.
			subWriter.putInt32(1)
			for _, elem := range v.Array {
				subWriter.writeBinaryDatum(ctx, elem, sessionLoc, v.ParamTyp)
			}
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)

	case *tree.DJSON:
		writeBinaryJSON(b, v.JSON)

	case *tree.DOid:
		b.putInt32(4)
		b.putInt32(int32(v.DInt))
	default:
		b.setError(errors.AssertionFailedf("unsupported type %T", d))
	}
}

// writeBinaryColumnarElement is the same as writeBinaryDatum where the datum is
// represented in a columnar element (at position idx in vec).
func (b *writeBuffer) writeBinaryColumnarElement(
	ctx context.Context, vec coldata.Vec, idx int, sessionLoc *time.Location,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing BINARY columnar element of type: %s", vec.Type())
	}
	if vec.MaybeHasNulls() && vec.Nulls().NullAt(idx) {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch vec.Type().Family() {
	case types.BoolFamily:
		writeBinaryBool(b, vec.Bool().Get(idx))

	case types.IntFamily:
		writeBinaryInt(b, getInt64(vec, idx), vec.Type())

	case types.FloatFamily:
		writeBinaryFloat(b, vec.Float64().Get(idx), vec.Type())

	case types.DecimalFamily:
		v := vec.Decimal().Get(idx)
		writeBinaryDecimal(b, &v)

	case types.BytesFamily:
		writeBinaryBytes(b, vec.Bytes().Get(idx))

	case types.UuidFamily:
		writeBinaryBytes(b, vec.Bytes().Get(idx))

	case types.StringFamily:
		writeBinaryString(b, string(vec.Bytes().Get(idx)), vec.Type())

	case types.TimestampFamily:
		writeBinaryTimestamp(b, vec.Timestamp().Get(idx))

	case types.TimestampTZFamily:
		writeBinaryTimestampTZ(b, vec.Timestamp().Get(idx), sessionLoc)

	case types.DateFamily:
		writeBinaryDate(b, pgdate.MakeCompatibleDateFromDisk(vec.Int64().Get(idx)))

	case types.IntervalFamily:
		writeBinaryInterval(b, vec.Interval().Get(idx))

	case types.JsonFamily:
		writeBinaryJSON(b, vec.JSON().Get(idx))

	default:
		// All other types are represented via the datum-backed vector.
		writeBinaryDatumNotNull(ctx, b, vec.Datum().Get(idx).(*coldataext.Datum).Datum, sessionLoc, vec.Type())
	}
}

const (
	pgTimeFormat              = "15:04:05.999999"
	pgTimeTZFormat            = pgTimeFormat + "-07"
	pgDateFormat              = "2006-01-02"
	pgTimeStampFormatNoOffset = pgDateFormat + " " + pgTimeFormat
	pgTimeStampFormat         = pgTimeStampFormatNoOffset + "-07"
	pgTime2400Format          = "24:00:00"
)

// formatTime formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func formatTime(t timeofday.TimeOfDay, tmp []byte) []byte {
	// time.Time's AppendFormat does not recognize 2400, so special case it accordingly.
	if t == timeofday.Time2400 {
		return []byte(pgTime2400Format)
	}
	return t.ToTime().AppendFormat(tmp, pgTimeFormat)
}

// formatTimeTZ formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func formatTimeTZ(t timetz.TimeTZ, tmp []byte) []byte {
	format := pgTimeTZFormat
	if t.OffsetSecs%60 != 0 {
		format += ":00:00"
	} else if t.OffsetSecs%3600 != 0 {
		format += ":00"
	}
	ret := t.ToTime().AppendFormat(tmp, format)
	// time.Time's AppendFormat does not recognize 2400, so special case it accordingly.
	if t.TimeOfDay == timeofday.Time2400 {
		// It instead reads 00:00:00. Replace that text.
		var newRet []byte
		newRet = append(newRet, pgTime2400Format...)
		newRet = append(newRet, ret[len(pgTime2400Format):]...)
		ret = newRet
	}
	return ret
}

func formatTs(t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	var format string
	if offset != nil {
		format = pgTimeStampFormat
		if _, offsetSeconds := t.In(offset).Zone(); offsetSeconds%60 != 0 {
			format += ":00:00"
		} else if offsetSeconds%3600 != 0 {
			format += ":00"
		}
	} else {
		format = pgTimeStampFormatNoOffset
	}
	return formatTsWithFormat(format, t, offset, tmp)
}

// formatTsWithFormat formats t with an optional offset into a format
// lib/pq understands, appending to the provided tmp buffer and
// reallocating if needed. The function will then return the resulting
// buffer. formatTsWithFormat is mostly cribbed from github.com/lib/pq.
func formatTsWithFormat(format string, t time.Time, offset *time.Location, tmp []byte) (b []byte) {
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

	b = t.AppendFormat(tmp, format)
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
