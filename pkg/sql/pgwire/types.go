// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
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
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
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
	size := tree.PGWireTypeSize(t)
	tOid := t.Oid()
	if tOid == oid.T_text && t.Width() > 0 {
		tOid = oid.T_varchar
	}
	return pgType{
		oid:  tOid,
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

func writeTextFloat(
	b *writeBuffer, fl float64, conv sessiondatapb.DataConversionConfig, typ *types.T,
) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := tree.PgwireFormatFloat(b.putbuf[4:4], fl, conv, typ)
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

func writeTextString(b *writeBuffer, s string, t *types.T) {
	pad := bpcharPadding(len(s), t)
	b.putInt32(int32(len(s) + pad))
	b.writeString(s)

	// apply padding (in the form of blanks spaces) to the right of the write buffer
	for pad > 0 {
		n := min(pad, len(spaces))
		b.write(spaces[:n])
		pad -= n
	}
}

func writeTextTimestamp(b *writeBuffer, v time.Time) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := tree.PGWireFormatTimestamp(v, nil, b.putbuf[4:4])
	b.putInt32(int32(len(s)))
	b.write(s)
}

func writeTextTimestampTZ(b *writeBuffer, v time.Time, sessionLoc *time.Location) {
	// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
	s := tree.PGWireFormatTimestamp(v, sessionLoc, b.putbuf[4:4])
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

	oldDCC := b.textFormatter.SetDataConversionConfig(conv)
	oldLoc := b.textFormatter.SetLocation(sessionLoc)
	defer func() {
		b.textFormatter.SetDataConversionConfig(oldDCC)
		b.textFormatter.SetLocation(oldLoc)
	}()
	switch v := tree.UnwrapDOidWrapper(d).(type) {
	case *tree.DBitArray:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DBool:
		writeTextBool(b, bool(*v))

	case *tree.DInt:
		writeTextInt64(b, int64(*v))

	case *tree.DFloat:
		fl := float64(*v)
		writeTextFloat(b, fl, conv, t)

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
		writeTextString(b, v.Contents, t)

	case *tree.DDate:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DTime:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := tree.PGWireFormatTime(timeofday.TimeOfDay(*v), b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTimeTZ:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := tree.PGWireFormatTimeTZ(v.TimeTZ, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DVoid:
		b.putInt32(0)

	case *tree.DPGLSN:
		s := v.LSN.String()
		b.putInt32(int32(len(s)))
		b.write([]byte(s))

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

	case *tree.DJsonpath:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DTSQuery:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DTSVector:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DTuple:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DPGVector:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DArray:
		// Arrays have custom formatting depending on their OID.
		b.textFormatter.FormatNode(d)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DOid:
		// OIDs have a special case for the "unknown" (zero) oid.
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DEnum:
		// Enums are serialized with their logical representation.
		b.writeLengthPrefixedString(v.LogicalRep)

	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

// getInt64 returns an int64 from vectors of Int family.
func getInt64(vecs *coldata.TypedVecs, vecIdx, rowIdx int, typ *types.T) int64 {
	colIdx := vecs.ColsMap[vecIdx]
	switch typ.Width() {
	case 16:
		return int64(vecs.Int16Cols[colIdx].Get(rowIdx))
	case 32:
		return int64(vecs.Int32Cols[colIdx].Get(rowIdx))
	default:
		return vecs.Int64Cols[colIdx].Get(rowIdx)
	}
}

// writeTextColumnarElement is the same as writeTextDatum where the datum is
// represented in a columnar element (at position rowIdx in the vector at
// position vecIdx in vecs).
func (b *writeBuffer) writeTextColumnarElement(
	ctx context.Context,
	vecs *coldata.TypedVecs,
	vecIdx int,
	rowIdx int,
	conv sessiondatapb.DataConversionConfig,
	sessionLoc *time.Location,
) {
	oldDCC := b.textFormatter.SetDataConversionConfig(conv)
	oldLoc := b.textFormatter.SetLocation(sessionLoc)
	defer func() {
		b.textFormatter.SetDataConversionConfig(oldDCC)
		b.textFormatter.SetLocation(oldLoc)
	}()
	typ := vecs.Vecs[vecIdx].Type()
	if log.V(2) {
		log.Infof(ctx, "pgwire writing TEXT columnar element of type: %s", typ)
	}
	if vecs.Nulls[vecIdx].MaybeHasNulls() && vecs.Nulls[vecIdx].NullAt(rowIdx) {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	colIdx := vecs.ColsMap[vecIdx]
	switch typ.Family() {
	case types.BoolFamily:
		writeTextBool(b, vecs.BoolCols[colIdx].Get(rowIdx))

	case types.IntFamily:
		writeTextInt64(b, getInt64(vecs, vecIdx, rowIdx, typ))

	case types.FloatFamily:
		writeTextFloat(b, vecs.Float64Cols[colIdx].Get(rowIdx), conv, typ)

	case types.DecimalFamily:
		d := vecs.DecimalCols[colIdx].Get(rowIdx)
		// The logic here is the simplification of tree.DDecimal.Format given
		// that we use tree.FmtSimple.
		b.writeLengthPrefixedString(d.String())

	case types.BytesFamily:
		writeTextBytes(
			b, unsafeBytesToString(vecs.BytesCols[colIdx].Get(rowIdx)), conv,
		)

	case types.UuidFamily:
		id, err := uuid.FromBytes(vecs.BytesCols[colIdx].Get(rowIdx))
		if err != nil {
			panic(errors.Wrap(err, "unexpectedly couldn't retrieve UUID object"))
		}
		writeTextUUID(b, id)

	case types.StringFamily:
		writeTextString(
			b, unsafeBytesToString(vecs.BytesCols[colIdx].Get(rowIdx)), typ,
		)

	case types.DateFamily:
		tree.FormatDate(pgdate.MakeCompatibleDateFromDisk(vecs.Int64Cols[colIdx].Get(rowIdx)), b.textFormatter)
		b.writeFromFmtCtx(b.textFormatter)

	case types.TimestampFamily:
		writeTextTimestamp(b, vecs.TimestampCols[colIdx].Get(rowIdx))

	case types.TimestampTZFamily:
		writeTextTimestampTZ(b, vecs.TimestampCols[colIdx].Get(rowIdx), sessionLoc)

	case types.IntervalFamily:
		tree.FormatDuration(vecs.IntervalCols[colIdx].Get(rowIdx), b.textFormatter)
		b.writeFromFmtCtx(b.textFormatter)

	case types.JsonFamily:
		b.writeLengthPrefixedString(vecs.JSONCols[colIdx].Get(rowIdx).String())

	case types.EnumFamily:
		// Enums are serialized with their logical representation.
		_, logical, err := tree.GetEnumComponentsFromPhysicalRep(typ, vecs.BytesCols[colIdx].Get(rowIdx))
		if err != nil {
			b.setError(err)
		} else {
			b.writeLengthPrefixedString(logical)
		}

	default:
		// All other types are represented via the datum-backed vector.
		writeTextDatumNotNull(b, vecs.DatumCols[colIdx].Get(rowIdx).(tree.Datum), conv, sessionLoc, typ)
	}
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
		if v.Form == apd.Infinite {
			// The 0x20 in the DScale byte does not actually seem to be part of the
			// spec, but that's what PostgreSQL outputs.
			if v.Negative {
				// https://github.com/postgres/postgres/blob/a57d312a7706321d850faa048a562a0c0c01b835/src/backend/utils/adt/numeric.c#L200
				b.write([]byte{0xf0, 0, 0, 0x20})
			} else {
				// https://github.com/postgres/postgres/blob/a57d312a7706321d850faa048a562a0c0c01b835/src/backend/utils/adt/numeric.c#L201
				b.write([]byte{0xd0, 0, 0, 0x20})
			}
			// Official Infinity support for DECIMAL was added in CockroachDB 22.1,
			// so let's keep tracking usage.
			telemetry.Inc(sqltelemetry.BinaryDecimalInfinityCounter)
		} else {
			// https://github.com/postgres/postgres/blob/ffa4cbd623dd69f9fa99e5e92426928a5782cf1a/src/backend/utils/adt/numeric.c#L169
			b.write([]byte{0xc0, 0, 0, 0})
		}
		return
	}

	alloc := struct {
		pgNum pgwirebase.PGNumeric

		bigI apd.BigInt
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

// spaces is used for padding CHAR(N) datums.
var spaces = bytes.Repeat([]byte{' '}, system.CacheLineSize)

func writeBinaryBytes(b *writeBuffer, v []byte, t *types.T) {
	if t.Oid() == oid.T_char && len(v) == 0 {
		// Match Postgres and always explicitly include a null byte if we have
		// an empty string for the "char" type in the binary format.
		v = []byte{0}
	}

	pad := bpcharPadding(len(v), t)
	b.putInt32(int32(len(v) + pad))
	b.write(v)

	// apply padding (in the form of blanks spaces) to the right of the write buffer
	for pad > 0 {
		n := min(pad, len(spaces))
		b.write(spaces[:n])
		pad -= n
	}
}

// bpcharPadding returns the number of spaces to pad when writing a BPCHAR datum of length n.
func bpcharPadding(n int, t *types.T) int {
	if t.Oid() == oid.T_bpchar && n < int(t.Width()) {
		return int(t.Width()) - n
	}
	return 0
}

func writeBinaryString(b *writeBuffer, s string, t *types.T) {
	if t.Oid() == oid.T_char && s == "" {
		// Match Postgres and always explicitly include a null byte if we have
		// an empty string for the "char" type in the binary format.
		s = string([]byte{0})
	}

	writeTextString(b, s, t)
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

func writeBinaryJSON(b *writeBuffer, v json.JSON, t *types.T) {
	s := v.String()
	if t.Oid() == oid.T_jsonb {
		b.putInt32(int32(len(s) + 1))
		// Postgres version number, as of writing, `1` is the only valid value.
		b.writeByte(1)
	} else {
		b.putInt32(int32(len(s)))
	}
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
	switch v := tree.UnwrapDOidWrapper(d).(type) {
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
		writeBinaryBytes(b, []byte(*v), t)

	case *tree.DUuid:
		writeBinaryBytes(b, v.GetBytes(), t)

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
		writeTextString(b, v.Contents, t)

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
		initialLen := b.Len()

		// Reserve bytes for writing length later.
		b.putInt32(int32(0))

		// Put the number of datums.
		b.putInt32(int32(len(v.D)))
		tupleTypes := t.TupleContents()
		for i, elem := range v.D {
			// Untyped tuples don't know the types of the tuple contents, so fallback
			// to using the datum's type.
			elemTyp := elem.ResolvedType()
			if i < len(tupleTypes) && tupleTypes[i].Family() != types.AnyFamily {
				elemTyp = tupleTypes[i]
			}
			b.putInt32(int32(elemTyp.Oid()))
			b.writeBinaryDatum(ctx, elem, sessionLoc, elemTyp)
		}

		lengthToWrite := b.Len() - (initialLen + 4)
		b.putInt32AtIndex(initialLen /* index to write at */, int32(lengthToWrite))

	case *tree.DVoid:
		b.putInt32(0)

	case *tree.DPGLSN:
		b.putInt32(8)
		b.putInt64(int64(v.LSN))

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

	case *tree.DTSQuery:
		initialLen := b.Len()
		// Reserve bytes for writing length later.
		b.putInt32(int32(0))
		ret := tsearch.EncodeTSQueryPGBinary(nil, v.TSQuery)
		b.write(ret)
		lengthToWrite := b.Len() - (initialLen + 4)
		b.putInt32AtIndex(initialLen /* index to write at */, int32(lengthToWrite))

	case *tree.DTSVector:
		initialLen := b.Len()
		// Reserve bytes for writing length later.
		b.putInt32(int32(0))
		ret, err := tsearch.EncodeTSVectorPGBinary(nil, v.TSVector)
		if err != nil {
			b.setError(err)
			return
		}
		b.write(ret)
		lengthToWrite := b.Len() - (initialLen + 4)
		b.putInt32AtIndex(initialLen /* index to write at */, int32(lengthToWrite))

	case *tree.DPGVector:
		// 2 bytes for dimensions, 2 bytes for unused, and 4 bytes for each
		// float4.
		b.putInt32(int32(4 + 4*len(v.T)))
		b.putInt16(int16(len(v.T)))
		b.putInt16(int16(0)) // vec->unused - "reserved for future use, always zero"
		for _, f := range v.T {
			b.putInt32(int32(math.Float32bits(f)))
		}

	case *tree.DArray:
		if v.ParamTyp.Family() == types.ArrayFamily {
			b.setError(unimplemented.NewWithIssueDetail(32552,
				"binenc", "unsupported binary serialization of multidimensional arrays"))
			return
		}

		initialLen := b.Len()

		// Reserve bytes for writing length later.
		b.putInt32(int32(0))

		// Put the number of dimensions. We currently support 1d arrays only.
		var ndims int32 = 1
		if v.Len() == 0 {
			ndims = 0
		}
		b.putInt32(ndims)
		hasNulls := 0
		if v.HasNulls {
			hasNulls = 1
		}
		oid := v.ParamTyp.Oid()
		b.putInt32(int32(hasNulls))
		b.putInt32(int32(oid))
		if v.Len() > 0 {
			b.putInt32(int32(v.Len()))
			// Lower bound, we only support a lower bound of 1.
			b.putInt32(1)
			for _, elem := range v.Array {
				b.writeBinaryDatum(ctx, elem, sessionLoc, v.ParamTyp)
			}
		}

		lengthToWrite := b.Len() - (initialLen + 4)
		b.putInt32AtIndex(initialLen /* index to write at */, int32(lengthToWrite))

	case *tree.DJSON:
		writeBinaryJSON(b, v.JSON, t)

	case *tree.DJsonpath:
		// Version number prefix, as of writing, `1` is the only valid value.
		s := v.String()
		b.putInt32(int32(len(s) + 1))
		b.writeByte(1)
		b.writeString(s)

	case *tree.DOid:
		b.putInt32(4)
		b.putInt32(int32(v.Oid))
	default:
		b.setError(errors.AssertionFailedf("unsupported type %T", d))
	}
}

// writeBinaryColumnarElement is the same as writeBinaryDatum where the datum is
// represented in a columnar element (at position rowIdx in the vector at
// position vecIdx in vecs).
func (b *writeBuffer) writeBinaryColumnarElement(
	ctx context.Context, vecs *coldata.TypedVecs, vecIdx int, rowIdx int, sessionLoc *time.Location,
) {
	typ := vecs.Vecs[vecIdx].Type()
	if log.V(2) {
		log.Infof(ctx, "pgwire writing BINARY columnar element of type: %s", typ)
	}
	if vecs.Nulls[vecIdx].MaybeHasNulls() && vecs.Nulls[vecIdx].NullAt(rowIdx) {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	colIdx := vecs.ColsMap[vecIdx]
	switch typ.Family() {
	case types.BoolFamily:
		writeBinaryBool(b, vecs.BoolCols[colIdx].Get(rowIdx))

	case types.IntFamily:
		writeBinaryInt(b, getInt64(vecs, vecIdx, rowIdx, typ), typ)

	case types.FloatFamily:
		writeBinaryFloat(b, vecs.Float64Cols[colIdx].Get(rowIdx), typ)

	case types.DecimalFamily:
		v := vecs.DecimalCols[colIdx].Get(rowIdx)
		writeBinaryDecimal(b, &v)

	case types.BytesFamily:
		writeBinaryBytes(b, vecs.BytesCols[colIdx].Get(rowIdx), typ)

	case types.UuidFamily:
		writeBinaryBytes(b, vecs.BytesCols[colIdx].Get(rowIdx), typ)

	case types.StringFamily:
		writeBinaryBytes(b, vecs.BytesCols[colIdx].Get(rowIdx), typ)

	case types.TimestampFamily:
		writeBinaryTimestamp(b, vecs.TimestampCols[colIdx].Get(rowIdx))

	case types.TimestampTZFamily:
		writeBinaryTimestampTZ(b, vecs.TimestampCols[colIdx].Get(rowIdx), sessionLoc)

	case types.DateFamily:
		writeBinaryDate(b, pgdate.MakeCompatibleDateFromDisk(vecs.Int64Cols[colIdx].Get(rowIdx)))

	case types.IntervalFamily:
		writeBinaryInterval(b, vecs.IntervalCols[colIdx].Get(rowIdx))

	case types.JsonFamily:
		writeBinaryJSON(b, vecs.JSONCols[colIdx].Get(rowIdx), typ)

	case types.EnumFamily:
		_, logical, err := tree.GetEnumComponentsFromPhysicalRep(typ, vecs.BytesCols[colIdx].Get(rowIdx))
		if err != nil {
			b.setError(err)
		} else {
			b.writeLengthPrefixedString(logical)
		}

	default:
		// All other types are represented via the datum-backed vector.
		writeBinaryDatumNotNull(ctx, b, vecs.DatumCols[colIdx].Get(rowIdx).(tree.Datum), sessionLoc, typ)
	}
}

// timeToPgBinary calculates the Postgres binary format for a timestamp. The timestamp
// is represented as the number of microseconds between the given time and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int64.
func timeToPgBinary(t time.Time, offset *time.Location) int64 {
	if t == pgdate.TimeInfinity {
		// Postgres uses math.MaxInt64 microseconds as the infinity value.
		// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
		return math.MaxInt64
	}
	if t == pgdate.TimeNegativeInfinity {
		// Postgres uses math.MinInt64 microseconds as the negative infinity value.
		// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
		return math.MinInt64
	}

	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}
	return duration.DiffMicros(t, pgwirebase.PGEpochJDate)
}

// unsafeBytesToString constructs a string from a byte slice. It is
// critical that the byte slice not be modified.
func unsafeBytesToString(data []byte) string {
	return *(*string)(unsafe.Pointer(&data))
}
