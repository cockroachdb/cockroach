// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwirebase

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/pgtype"
	"github.com/lib/pq/oid"
)

const maxMessageSize = 1 << 24

// FormatCode represents a pgwire data format.
//
//go:generate stringer -type=FormatCode
type FormatCode uint16

const (
	// FormatText is the default, text format.
	FormatText FormatCode = 0
	// FormatBinary is an alternative, binary, encoding.
	FormatBinary FormatCode = 1
)

var _ BufferedReader = &bufio.Reader{}
var _ BufferedReader = &bytes.Buffer{}

// BufferedReader extended io.Reader with some convenience methods.
type BufferedReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

// ReadBuffer provides a convenient way to read pgwire protocol messages.
type ReadBuffer struct {
	Msg []byte
	tmp [4]byte
}

// reset sets b.Msg to exactly size, attempting to use spare capacity
// at the end of the existing slice when possible and allocating a new
// slice when necessary.
func (b *ReadBuffer) reset(size int) {
	if b.Msg != nil {
		b.Msg = b.Msg[len(b.Msg):]
	}

	if cap(b.Msg) >= size {
		b.Msg = b.Msg[:size]
		return
	}

	allocSize := size
	if allocSize < 4096 {
		allocSize = 4096
	}
	b.Msg = make([]byte, size, allocSize)
}

// ReadUntypedMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; readTypedMsg is used at all
// other times. This returns the number of bytes read and an error, if there
// was one. The number of bytes returned can be non-zero even with an error
// (e.g. if data was read but didn't validate) so that we can more accurately
// measure network traffic.
func (b *ReadBuffer) ReadUntypedMsg(rd io.Reader) (int, error) {
	nread, err := io.ReadFull(rd, b.tmp[:])
	if err != nil {
		return nread, err
	}
	size := int(binary.BigEndian.Uint32(b.tmp[:]))
	// size includes itself.
	size -= 4
	if size > maxMessageSize || size < 0 {
		return nread, NewProtocolViolationErrorf("message size %d out of bounds (0..%d)",
			size, maxMessageSize)
	}

	b.reset(size)
	n, err := io.ReadFull(rd, b.Msg)
	return nread + n, err
}

// ReadTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (b *ReadBuffer) ReadTypedMsg(rd BufferedReader) (ClientMessageType, int, error) {
	typ, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	n, err := b.ReadUntypedMsg(rd)
	return ClientMessageType(typ), n, err
}

// GetString reads a null-terminated string.
func (b *ReadBuffer) GetString() (string, error) {
	pos := bytes.IndexByte(b.Msg, 0)
	if pos == -1 {
		return "", NewProtocolViolationErrorf("NUL terminator not found")
	}
	// Note: this is a conversion from a byte slice to a string which avoids
	// allocation and copying. It is safe because we never reuse the bytes in our
	// read buffer. It is effectively the same as: "s := string(b.Msg[:pos])"
	s := b.Msg[:pos]
	b.Msg = b.Msg[pos+1:]
	return *((*string)(unsafe.Pointer(&s))), nil
}

// GetPrepareType returns the buffer's contents as a PrepareType.
func (b *ReadBuffer) GetPrepareType() (PrepareType, error) {
	v, err := b.GetBytes(1)
	if err != nil {
		return 0, err
	}
	return PrepareType(v[0]), nil
}

// GetBytes returns the buffer's contents as a []byte.
func (b *ReadBuffer) GetBytes(n int) ([]byte, error) {
	if len(b.Msg) < n {
		return nil, NewProtocolViolationErrorf("insufficient data: %d", len(b.Msg))
	}
	v := b.Msg[:n]
	b.Msg = b.Msg[n:]
	return v, nil
}

// GetUint16 returns the buffer's contents as a uint16.
func (b *ReadBuffer) GetUint16() (uint16, error) {
	if len(b.Msg) < 2 {
		return 0, NewProtocolViolationErrorf("insufficient data: %d", len(b.Msg))
	}
	v := binary.BigEndian.Uint16(b.Msg[:2])
	b.Msg = b.Msg[2:]
	return v, nil
}

// GetUint32 returns the buffer's contents as a uint32.
func (b *ReadBuffer) GetUint32() (uint32, error) {
	if len(b.Msg) < 4 {
		return 0, NewProtocolViolationErrorf("insufficient data: %d", len(b.Msg))
	}
	v := binary.BigEndian.Uint32(b.Msg[:4])
	b.Msg = b.Msg[4:]
	return v, nil
}

// NewUnrecognizedMsgTypeErr creates an error for an unrecognized pgwire
// message.
func NewUnrecognizedMsgTypeErr(typ ClientMessageType) error {
	return NewProtocolViolationErrorf("unrecognized client message type %v", typ)
}

// NewProtocolViolationErrorf creates a pgwire ProtocolViolationError.
func NewProtocolViolationErrorf(format string, args ...interface{}) error {
	return pgerror.Newf(pgcode.ProtocolViolation, format, args...)
}

// NewInvalidBinaryRepresentationErrorf creates a pgwire InvalidBinaryRepresentation.
func NewInvalidBinaryRepresentationErrorf(format string, args ...interface{}) error {
	return pgerror.Newf(pgcode.InvalidBinaryRepresentation, format, args...)
}

// validateArrayDimensions takes the number of dimensions and elements and
// returns an error if we don't support that combination.
func validateArrayDimensions(nDimensions int, nElements int) error {
	switch nDimensions {
	case 1:
		break
	case 0:
		// 0-dimensional array means 0-length array: validate that.
		if nElements == 0 {
			break
		}
		fallthrough
	default:
		return unimplemented.NewWithIssuef(32552,
			"%d-dimension arrays not supported; only 1-dimension", nDimensions)
	}
	return nil
}

// DecodeOidDatum decodes bytes with specified Oid and format code into
// a datum. If the ParseTimeContext is nil, reasonable defaults
// will be applied.
func DecodeOidDatum(
	ctx tree.ParseTimeContext, id oid.Oid, code FormatCode, b []byte,
) (tree.Datum, error) {
	switch code {
	case FormatText:
		switch id {
		case oid.T_bool:
			t, err := strconv.ParseBool(string(b))
			if err != nil {
				return nil, err
			}
			return tree.MakeDBool(tree.DBool(t)), nil
		case oid.T_bit, oid.T_varbit:
			t, err := tree.ParseDBitArray(string(b))
			if err != nil {
				return nil, err
			}
			return t, nil
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
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as decimal", b)
			}
			return d, nil
		case oid.T_bytea:
			res, err := lex.DecodeRawBytesToByteArrayAuto(b)
			if err != nil {
				return nil, err
			}
			return tree.NewDBytes(tree.DBytes(res)), nil
		case oid.T_timestamp:
			d, err := tree.ParseDTimestamp(ctx, string(b), time.Microsecond)
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as timestamp", b)
			}
			return d, nil
		case oid.T_timestamptz:
			d, err := tree.ParseDTimestampTZ(ctx, string(b), time.Microsecond)
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as timestamptz", b)
			}
			return d, nil
		case oid.T_date:
			d, err := tree.ParseDDate(ctx, string(b))
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as date", b)
			}
			return d, nil
		case oid.T_time:
			d, err := tree.ParseDTime(nil, string(b))
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as time", b)
			}
			return d, nil

		case oid.T_interval:
			d, err := tree.ParseDInterval(string(b))
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as interval", b)
			}
			return d, nil
		case oid.T_uuid:
			d, err := tree.ParseDUuidFromString(string(b))
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as uuid", b)
			}
			return d, nil
		case oid.T_inet:
			d, err := tree.ParseDIPAddrFromINetString(string(b))
			if err != nil {
				return nil, pgerror.Newf(pgcode.Syntax,
					"could not parse string %q as inet", b)
			}
			return d, nil
		case oid.T__int2, oid.T__int4, oid.T__int8:
			var arr pgtype.Int8Array
			if err := arr.DecodeText(nil, b); err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax,
					"could not parse string %q as int array", b)
			}
			if arr.Status != pgtype.Present {
				return tree.DNull, nil
			}
			if err := validateArrayDimensions(len(arr.Dimensions), len(arr.Elements)); err != nil {
				return nil, err
			}
			out := tree.NewDArray(types.Int)
			var d tree.Datum
			for _, v := range arr.Elements {
				if v.Status != pgtype.Present {
					d = tree.DNull
				} else {
					d = tree.NewDInt(tree.DInt(v.Int))
				}
				if err := out.Append(d); err != nil {
					return nil, err
				}
			}
			return out, nil
		case oid.T__text, oid.T__name:
			var arr pgtype.TextArray
			if err := arr.DecodeText(nil, b); err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax,
					"could not parse string %q as text array", b)
			}
			if arr.Status != pgtype.Present {
				return tree.DNull, nil
			}
			if err := validateArrayDimensions(len(arr.Dimensions), len(arr.Elements)); err != nil {
				return nil, err
			}
			out := tree.NewDArray(types.String)
			if id == oid.T__name {
				out.ParamTyp = types.Name
			}
			var d tree.Datum
			for _, v := range arr.Elements {
				if v.Status != pgtype.Present {
					d = tree.DNull
				} else {
					d = tree.NewDString(v.String)
					if id == oid.T__name {
						d = tree.NewDNameFromDString(d.(*tree.DString))
					}
				}
				if err := out.Append(d); err != nil {
					return nil, err
				}
			}
			return out, nil
		case oid.T_jsonb:
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			return tree.ParseDJSON(string(b))
		}
		if _, ok := types.ArrayOids[id]; ok {
			// Arrays come in in their string form, so we parse them as such and later
			// convert them to their actual datum form.
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			return tree.NewDString(string(b)), nil
		}
	case FormatBinary:
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
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported binary bool: %x", b)
		case oid.T_int2:
			if len(b) < 2 {
				return nil, pgerror.Newf(pgcode.Syntax, "int2 requires 2 bytes for binary format")
			}
			i := int16(binary.BigEndian.Uint16(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_int4:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "int4 requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_int8:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "int8 requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.NewDInt(tree.DInt(i)), nil
		case oid.T_oid:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "oid requires 4 bytes for binary format")
			}
			u := binary.BigEndian.Uint32(b)
			return tree.NewDOid(tree.DInt(u)), nil
		case oid.T_float4:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "float4 requires 4 bytes for binary format")
			}
			f := math.Float32frombits(binary.BigEndian.Uint32(b))
			return tree.NewDFloat(tree.DFloat(f)), nil
		case oid.T_float8:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "float8 requires 8 bytes for binary format")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(b))
			return tree.NewDFloat(tree.DFloat(f)), nil
		case oid.T_numeric:
			r := bytes.NewReader(b)

			alloc := struct {
				pgNum PGNumeric
				i16   int16

				dd tree.DDecimal
			}{}

			for _, ptr := range []interface{}{
				&alloc.pgNum.Ndigits,
				&alloc.pgNum.Weight,
				&alloc.pgNum.Sign,
				&alloc.pgNum.Dscale,
			} {
				if err := binary.Read(r, binary.BigEndian, ptr); err != nil {
					return nil, err
				}
			}

			if alloc.pgNum.Ndigits > 0 {
				decDigits := make([]byte, 0, int(alloc.pgNum.Ndigits)*PGDecDigits)
				nextDigit := func() error {
					if err := binary.Read(r, binary.BigEndian, &alloc.i16); err != nil {
						return err
					}
					numZeroes := PGDecDigits
					for i16 := alloc.i16; i16 > 0; i16 /= 10 {
						numZeroes--
					}
					for ; numZeroes > 0; numZeroes-- {
						decDigits = append(decDigits, '0')
					}
					return nil
				}

				for i := int16(0); i < alloc.pgNum.Ndigits-1; i++ {
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
				Dscale := (alloc.pgNum.Ndigits - (alloc.pgNum.Weight + 1)) * PGDecDigits
				if overScale := Dscale - alloc.pgNum.Dscale; overScale > 0 {
					Dscale -= overScale
					for i := int16(0); i < overScale; i++ {
						alloc.i16 /= 10
					}
				}
				if alloc.i16 > 0 {
					decDigits = strconv.AppendUint(decDigits, uint64(alloc.i16), 10)
				}
				decString := string(decDigits)
				if _, ok := alloc.dd.Coeff.SetString(decString, 10); !ok {
					return nil, pgerror.Newf(pgcode.Syntax, "could not parse string %q as decimal", decString)
				}
				alloc.dd.Exponent = -int32(Dscale)
			}

			switch alloc.pgNum.Sign {
			case PGNumericPos:
			case PGNumericNeg:
				alloc.dd.Neg(&alloc.dd.Decimal)
			case 0xc000:
				// https://github.com/postgres/postgres/blob/ffa4cbd623dd69f9fa99e5e92426928a5782cf1a/src/backend/utils/adt/numeric.c#L169
				return tree.ParseDDecimal("NaN")
			default:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported numeric sign: %d", alloc.pgNum.Sign)
			}

			return &alloc.dd, nil
		case oid.T_bytea:
			return tree.NewDBytes(tree.DBytes(b)), nil
		case oid.T_timestamp:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "timestamp requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestamp(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_timestamptz:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "timestamptz requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestampTZ(pgBinaryToTime(i), time.Microsecond), nil
		case oid.T_date:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "date requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return pgBinaryToDate(i)
		case oid.T_time:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "time requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTime(timeofday.TimeOfDay(i)), nil
		case oid.T_interval:
			if len(b) < 16 {
				return nil, pgerror.Newf(pgcode.Syntax, "interval requires 16 bytes for binary format")
			}
			nanos := (int64(binary.BigEndian.Uint64(b)) / int64(time.Nanosecond)) * int64(time.Microsecond)
			days := int32(binary.BigEndian.Uint32(b[8:]))
			months := int32(binary.BigEndian.Uint32(b[12:]))

			duration := duration.MakeDuration(nanos, int64(days), int64(months))
			return &tree.DInterval{Duration: duration}, nil
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
		case oid.T_jsonb:
			if len(b) < 1 {
				return nil, NewProtocolViolationErrorf("no data to decode")
			}
			if b[0] != 1 {
				return nil, NewProtocolViolationErrorf("expected JSONB version 1")
			}
			// Skip over the version number.
			b = b[1:]
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			return tree.ParseDJSON(string(b))
		case oid.T_varbit, oid.T_bit:
			if len(b) < 4 {
				return nil, NewProtocolViolationErrorf("insufficient data: %d", len(b))
			}
			bitlen := binary.BigEndian.Uint32(b)
			b = b[4:]
			lastBitsUsed := uint64(bitlen % 64)
			if bitlen != 0 && lastBitsUsed == 0 {
				lastBitsUsed = 64
			}
			if len(b)*8 < int(bitlen) {
				return nil, pgerror.Newf(pgcode.Syntax, "unexpected varbit bitlen %d (b: %d)", bitlen, len(b))
			}
			words := make([]uint64, (len(b)+7)/8)
			// We need two loops here. The first loop does full 8-byte decoding. The
			// last word is not guaranteed to be a full 8 bytes, and so the second loop
			// does manual per-byte decoding.
			for i := 0; i < len(words)-1; i++ {
				words[i] = binary.BigEndian.Uint64(b)
				b = b[8:]
			}
			if len(words) > 0 {
				var w uint64
				i := uint(0)
				for ; i < uint(lastBitsUsed); i += 8 {
					if len(b) == 0 {
						return nil, NewInvalidBinaryRepresentationErrorf("incorrect binary data")
					}
					w = (w << 8) | uint64(b[0])
					b = b[1:]
				}
				words[len(words)-1] = w << (64 - i)
			}
			ba, err := bitarray.FromEncodingParts(words, lastBitsUsed)
			return &tree.DBitArray{BitArray: ba}, err
		default:
			if _, ok := types.ArrayOids[id]; ok {
				innerOid := types.OidToType[id].ArrayContents().Oid()
				return decodeBinaryArray(ctx, innerOid, b, code)
			}
		}
	default:
		return nil, errors.AssertionFailedf(
			"unexpected format code: %d", errors.Safe(code))
	}

	// Types with identical text/binary handling.
	switch id {
	case oid.T_text, oid.T_varchar, oid.T_bpchar:
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
		return nil, errors.AssertionFailedf(
			"unsupported OID %v with format code %s", errors.Safe(id), errors.Safe(code))
	}
}

// Values which are going to be converted to strings (STRING and NAME) need to
// be valid UTF-8 for us to accept them.
func validateStringBytes(b []byte) error {
	if !utf8.Valid(b) {
		return invalidUTF8Error
	}
	return nil
}

//PGNumericSign indicates the sign of a numeric.
//go:generate stringer -type=PGNumericSign
type PGNumericSign uint16

const (
	// PGNumericPos represents the + sign.
	PGNumericPos PGNumericSign = 0x0000
	// PGNumericNeg represents the - sign.
	PGNumericNeg PGNumericSign = 0x4000
	// PGNumericNan PGNumericSign = 0xC000
)

// PGDecDigits represents the number of decimal digits per int16 Postgres "digit".
const PGDecDigits = 4

// PGNumeric represents a numeric.
type PGNumeric struct {
	Ndigits, Weight, Dscale int16
	Sign                    PGNumericSign
}

// pgBinaryToTime takes an int64 and interprets it as the Postgres binary format
// for a timestamp. To create a timestamp from this value, it takes the microseconds
// delta and adds it to PGEpochJDate.
func pgBinaryToTime(i int64) time.Time {
	return duration.AddMicros(PGEpochJDate, i)
}

// pgBinaryToDate takes an int32 and interprets it as the Postgres binary format
// for a date. To create a date from this value, it takes the day delta and adds
// it to PGEpochJDate.
func pgBinaryToDate(i int32) (*tree.DDate, error) {
	d, err := pgdate.MakeDateFromPGEpoch(i)
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

// pgBinaryToIPAddr takes an IPAddr and interprets it as the Postgres binary
// format. See https://github.com/postgres/postgres/blob/81c5e46c490e2426db243eada186995da5bb0ba7/src/backend/utils/adt/network.c#L144
// for the binary spec.
func pgBinaryToIPAddr(b []byte) (ipaddr.IPAddr, error) {
	if len(b) < 4 {
		return ipaddr.IPAddr{}, NewProtocolViolationErrorf("insufficient data: %d", len(b))
	}

	mask := b[1]
	familyByte := b[0]
	var addr ipaddr.Addr
	var family ipaddr.IPFamily
	b = b[4:]

	if familyByte == PGBinaryIPv4family {
		family = ipaddr.IPv4family
	} else if familyByte == PGBinaryIPv6family {
		family = ipaddr.IPv6family
	} else {
		return ipaddr.IPAddr{}, NewInvalidBinaryRepresentationErrorf("unknown family received: %d", familyByte)
	}

	// Get the IP address bytes. The IP address length is byte 3 but is ignored.
	if family == ipaddr.IPv4family {
		if len(b) != 4 {
			return ipaddr.IPAddr{}, NewInvalidBinaryRepresentationErrorf("unexpected data: %d", len(b))
		}
		// Add the IPv4-mapped IPv6 prefix of 0xFF.
		var tmp [16]byte
		tmp[10] = 0xff
		tmp[11] = 0xff
		copy(tmp[12:], b)
		addr = ipaddr.Addr(uint128.FromBytes(tmp[:]))
	} else {
		if len(b) != 16 {
			return ipaddr.IPAddr{}, NewInvalidBinaryRepresentationErrorf("unexpected data: %d", len(b))
		}
		addr = ipaddr.Addr(uint128.FromBytes(b))
	}

	return ipaddr.IPAddr{
		Family: family,
		Mask:   mask,
		Addr:   addr,
	}, nil
}

func decodeBinaryArray(
	ctx tree.ParseTimeContext, elemOid oid.Oid, b []byte, code FormatCode,
) (tree.Datum, error) {
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
	if err := validateArrayDimensions(int(hdr.Ndims), int(hdr.DimSize)); err != nil {
		return nil, err
	}

	if elemOid != oid.Oid(hdr.ElemOid) {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch, "wrong element type")
	}
	arr := tree.NewDArray(types.OidToType[elemOid])
	var vlen int32
	for i := int32(0); i < hdr.DimSize; i++ {
		if err := binary.Read(r, binary.BigEndian, &vlen); err != nil {
			return nil, err
		}
		if vlen < 0 {
			if err := arr.Append(tree.DNull); err != nil {
				return nil, err
			}
			continue
		}
		buf := r.Next(int(vlen))
		elem, err := DecodeOidDatum(ctx, elemOid, code, buf)
		if err != nil {
			return nil, err
		}
		if err := arr.Append(elem); err != nil {
			return nil, err
		}
	}
	return arr, nil
}

var invalidUTF8Error = pgerror.Newf(pgcode.CharacterNotInRepertoire, "invalid UTF-8 sequence")

var (
	// PGEpochJDate represents the pg epoch.
	PGEpochJDate = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
)

const (
	// PGBinaryIPv4family is the pgwire constant for IPv4. It is defined as
	// AF_INET.
	PGBinaryIPv4family byte = 2
	// PGBinaryIPv6family is the pgwire constant for IPv4. It is defined as
	// AF_NET + 1.
	PGBinaryIPv6family byte = 3
)
