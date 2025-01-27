// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwirebase

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/lib/pq/oid"
)

const (
	defaultMaxReadBufferMessageSize = 1 << 24
	minReadBufferMessageSize        = 1 << 14
)

const readBufferMaxMessageSizeClusterSettingName = "sql.conn.max_read_buffer_message_size"

// ReadBufferMaxMessageSizeClusterSetting is the cluster setting for configuring
// ReadBuffer default message sizes.
var ReadBufferMaxMessageSizeClusterSetting = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	readBufferMaxMessageSizeClusterSettingName,
	"maximum buffer size to allow for ingesting sql statements. Connections must be restarted for this to take effect.",
	defaultMaxReadBufferMessageSize,
	settings.ByteSizeWithMinimum(minReadBufferMessageSize),
)

// FormatCode represents a pgwire data format.
//
//go:generate stringer -type=FormatCode
type FormatCode uint16

var _ redact.SafeValue = FormatCode(0)

// SafeValue implements the redact.SafeValue interface.
func (i FormatCode) SafeValue() {}

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
	Msg            []byte
	tmp            [4]byte
	maxMessageSize int
}

// ReadBufferOption is an optional argument to use with ReadBuffer.
type ReadBufferOption func(*ReadBuffer)

// ReadBufferOptionWithClusterSettings utilizes the cluster settings for setting
// various defaults in the ReadBuffer.
func ReadBufferOptionWithClusterSettings(sv *settings.Values) ReadBufferOption {
	return func(b *ReadBuffer) {
		if sv != nil {
			b.maxMessageSize = int(ReadBufferMaxMessageSizeClusterSetting.Get(sv))
		}
	}
}

// MakeReadBuffer returns a new ReaderBuffer with the given size.
func MakeReadBuffer(opts ...ReadBufferOption) ReadBuffer {
	buf := ReadBuffer{
		maxMessageSize: defaultMaxReadBufferMessageSize,
	}
	for _, opt := range opts {
		opt(&buf)
	}
	return buf
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
//
// If the error is related to consuming a buffer that is larger than the
// maxMessageSize, the remaining bytes will be read but discarded.
func (b *ReadBuffer) ReadUntypedMsg(rd io.Reader) (int, error) {
	nread, err := io.ReadFull(rd, b.tmp[:])
	if err != nil {
		return nread, err
	}
	size := int(binary.BigEndian.Uint32(b.tmp[:]))
	// size includes itself.
	size -= 4
	if size > b.maxMessageSize || size < 0 {
		err := errors.WithHintf(
			NewProtocolViolationErrorf(
				"message size %s bigger than maximum allowed message size %s",
				humanize.IBytes(uint64(size)),
				humanize.IBytes(uint64(b.maxMessageSize)),
			),
			"the maximum message size can be configured using the %s cluster setting",
			readBufferMaxMessageSizeClusterSettingName,
		)
		if size > 0 {
			err = withMessageTooBigError(err, size)
		}
		return nread, err
	}

	b.reset(size)
	n, err := io.ReadFull(rd, b.Msg)
	return nread + n, err
}

// SlurpBytes will consume n bytes from the read buffer, using the existing
// buffer to ingest the message.
func (b *ReadBuffer) SlurpBytes(rd io.Reader, n int) (int, error) {
	var nRead int
	if b.maxMessageSize > 0 {
		sizeRemaining := n
		for sizeRemaining > 0 {
			toRead := sizeRemaining
			if b.maxMessageSize < sizeRemaining {
				toRead = b.maxMessageSize
			}
			b.reset(toRead)
			readBatch, err := io.ReadFull(rd, b.Msg)
			nRead += readBatch
			sizeRemaining -= readBatch
			if err != nil {
				return nRead, err
			}
		}
	}
	return nRead, nil
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

// GetUnsafeString reads a null-terminated string as a reference.
// Note: The underlying buffer will be prevented from GCing, so long lived
// objects should never use this.
func (b *ReadBuffer) GetUnsafeString() (string, error) {
	pos := bytes.IndexByte(b.Msg, 0)
	if pos == -1 {
		return "", NewProtocolViolationErrorf("NUL terminator not found")
	}
	// This conversion is safe because we never reuse the bytes in our read
	// buffer.
	s := encoding.UnsafeConvertBytesToString(b.Msg[:pos])
	b.Msg = b.Msg[pos+1:]
	return s, nil
}

// GetSafeString reads a null-terminated string as a copy of the original data
// out.
func (b *ReadBuffer) GetSafeString() (string, error) {
	s, err := b.GetUnsafeString()
	if err != nil {
		return "", err
	}
	return strings.Clone(s), nil
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

// GetUint64 returns the buffer's contents as a uint64.
func (b *ReadBuffer) GetUint64() (uint64, error) {
	if len(b.Msg) < 8 {
		return 0, NewProtocolViolationErrorf("insufficient data: %d", len(b.Msg))
	}
	v := binary.BigEndian.Uint64(b.Msg[:8])
	b.Msg = b.Msg[8:]
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

// DecodeDatum decodes bytes with specified type and format code into a datum.
// NB: the caller is **not** allowed to mutate b.
func DecodeDatum(
	ctx context.Context,
	evalCtx *eval.Context,
	typ *types.T,
	code FormatCode,
	b []byte,
	da *tree.DatumAlloc,
) (tree.Datum, error) {
	id := typ.Oid()
	// Use a direct string pointing to b where we are sure we aren't retaining
	// this string.
	bs := encoding.UnsafeConvertBytesToString(b)
	switch code {
	case FormatText:
		switch id {
		case oid.T_record:
			d, _, err := tree.ParseDTupleFromString(evalCtx, bs, typ)
			if err != nil {
				return nil, err
			}
			return d, nil
		case oid.T_bool:
			t, err := strconv.ParseBool(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return tree.MakeDBool(tree.DBool(t)), nil
		case oid.T_bit, oid.T_varbit:
			a, err := bitarray.Parse(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDBitArray(tree.DBitArray{BitArray: a}), nil
		case oid.T_int2, oid.T_int4, oid.T_int8:
			i, err := strconv.ParseInt(bs, 10, 64)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDInt(tree.DInt(i)), nil
		case oid.T_pg_lsn:
			return tree.ParseDPGLSN(bs)
		case oid.T_oid,
			oid.T_regoper,
			oid.T_regproc,
			oid.T_regrole,
			oid.T_regclass,
			oid.T_regtype,
			oid.T_regconfig,
			oid.T_regoperator,
			oid.T_regnamespace,
			oid.T_regprocedure,
			oid.T_regdictionary:
			return eval.ParseDOid(ctx, evalCtx, bs, typ)
		case oid.T_float4, oid.T_float8:
			f, err := strconv.ParseFloat(bs, 64)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDFloat(tree.DFloat(f)), nil
		case oidext.T_box2d:
			v, err := geo.ParseCartesianBoundingBox(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDBox2D(tree.DBox2D{CartesianBoundingBox: v}), nil
		case oidext.T_geography:
			v, err := geo.ParseGeography(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDGeography(tree.DGeography{Geography: v}), nil
		case oidext.T_geometry:
			v, err := geo.ParseGeometry(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDGeometry(tree.DGeometry{Geometry: v}), nil
		case oid.T_void:
			return tree.DVoidDatum, nil
		case oid.T_numeric:
			d, err := tree.ParseDDecimal(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return d, nil
		case oid.T_bytea:
			res, err := lex.DecodeRawBytesToByteArrayAuto(b)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDBytes(tree.DBytes(res)), nil
		case oid.T_timestamp:
			d, _, err := tree.ParseDTimestamp(evalCtx, bs, time.Microsecond)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return d, nil
		case oid.T_timestamptz:
			t, _, err := tree.ParseTimestampTZ(evalCtx, bs, time.Microsecond)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDTimestampTZ(tree.DTimestampTZ{Time: t}), nil
		case oid.T_date:
			d, _, err := tree.ParseDDate(evalCtx, bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return d, nil
		case oid.T_time:
			d, _, err := tree.ParseDTime(nil, bs, time.Microsecond)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return d, nil
		case oid.T_timetz:
			d, _, err := tree.ParseDTimeTZ(evalCtx, bs, time.Microsecond)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return d, nil
		case oid.T_interval:
			v, err := tree.ParseIntervalWithTypeMetadata(evalCtx.GetIntervalStyle(), bs, types.DefaultIntervalTypeMetadata)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDInterval(tree.DInterval{Duration: v}), nil
		case oid.T_uuid:
			v, err := uuid.FromString(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDUuid(tree.DUuid{UUID: v}), nil
		case oid.T_inet:
			var ipAddr ipaddr.IPAddr
			err := ipaddr.ParseINet(bs, &ipAddr)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
		case oid.T_jsonb, oid.T_json:
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			v, err := json.ParseJSON(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDJSON(tree.DJSON{JSON: v}), nil
		case oidext.T_jsonpath:
			return da.NewDJsonpath(tree.DJsonpath(bs)), nil
		case oid.T_tsquery:
			ret, err := tsearch.ParseTSQuery(bs)
			if err != nil {
				return nil, err
			}
			return &tree.DTSQuery{TSQuery: ret}, nil
		case oid.T_tsvector:
			ret, err := tsearch.ParseTSVector(bs)
			if err != nil {
				return nil, err
			}
			return &tree.DTSVector{TSVector: ret}, nil
		case oidext.T_pgvector:
			ret, err := vector.ParseVector(bs)
			if err != nil {
				return nil, err
			}
			return &tree.DPGVector{T: ret}, nil
		}
		switch typ.Family() {
		case types.ArrayFamily, types.TupleFamily:
			// Arrays and tuples come in in their string form, so we parse them
			// as such and later convert them to their actual datum form.
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			return da.NewDString(tree.DString(bs)), nil
		}
	case FormatBinary:
		switch id {
		case oid.T_record:
			return decodeBinaryTuple(ctx, evalCtx, b, da)
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
			return da.NewDInt(tree.DInt(i)), nil
		case oid.T_int4:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "int4 requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			return da.NewDInt(tree.DInt(i)), nil
		case oid.T_int8:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "int8 requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return da.NewDInt(tree.DInt(i)), nil
		case oid.T_pg_lsn:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "lsn requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return da.NewDPGLSN(tree.DPGLSN{LSN: lsn.LSN(i)}), nil
		case oid.T_float4:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "float4 requires 4 bytes for binary format")
			}
			f := math.Float32frombits(binary.BigEndian.Uint32(b))
			return da.NewDFloat(tree.DFloat(f)), nil
		case oid.T_float8:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "float8 requires 8 bytes for binary format")
			}
			f := math.Float64frombits(binary.BigEndian.Uint64(b))
			return da.NewDFloat(tree.DFloat(f)), nil
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
				for i := int16(0); i < alloc.pgNum.Ndigits; i++ {
					if err := binary.Read(r, binary.BigEndian, &alloc.i16); err != nil {
						return nil, err
					}
					// Each 16-bit "digit" can represent a 4 digit number.
					// In the case where each digit is not 4 digits, we must append
					// padding to the beginning, i.e.
					// * "1234" stays "1234"
					// * "123" becomes "0123"
					// * "12" becomes "0012"
					// * "1" becomes "0001"
					// * "0" becomes "0000"
					// * "123456" becomes ["0012", "3456"]
					// * "123456.789" becomes ["0012", "3456", "7890"]
					// * "120123.45678" becomes ["0012", "0123", "4567", "8000"]
					numZeroes := PGDecDigits
					for i16 := alloc.i16; i16 > 0; i16 /= 10 {
						numZeroes--
					}
					for ; numZeroes > 0; numZeroes-- {
						decDigits = append(decDigits, '0')
					}
					if alloc.i16 > 0 {
						decDigits = strconv.AppendUint(decDigits, uint64(alloc.i16), 10)
					}
				}

				// In the case of padding zeros at the end, we may have padded too many
				// digits in the loop. This can be determined if the weight (defined as
				// number of 4 digit groups left of the decimal point - 1) + the scale
				// (total number of digits on the RHS of the decimal point) is less
				// than the number of digits given.
				//
				// In Postgres, this is handled by the "remove trailing zeros" in
				// `make_result_opt_error`, as well as `trunc_var`.
				// Any zeroes are implicitly added back in when operating on the decimal
				// value.
				//
				// Examples (with "," in the digit string for clarity):
				// * for "1234", we have digits ["1234", "0"] for scale 0, which would
				// make the digit string "1234,0000". For scale 0, we need to cut it back
				// to "1234".
				// * for "1234.0", we have digits ["1234", "0"] for scale 1, which would
				// make the digit string "1234,0000". For scale 1, we need to cut it back
				// to "1234.0".
				// * for "1234.000000" we have digits ["1234", "0", "0"] with scale 6,
				// which would make the digit string "1234,0000,0000". We need to cut it
				// back to "1234,0000,00" for this to be correct.
				// * for "123456.00000", we have digits ["12", "3456", "0", "0"] with
				// scale 5, which would make digit string "0012,3456,0000,0000". We need
				// to cut it back to "0012,3456,0000,0" for this to be correct.
				// * for "123456.000000000", we may have digits ["12", "3456", "0", "0", "0"]
				// with scale 5, which would make digit string "0012,3456,0000,0000".
				// We need to cut it back to "0012,3456,0000,0" for this to be correct.
				//
				// This is handled by the below code, which truncates the decDigits
				// such that it fits into the desired dscale. To do this:
				// * ndigits [number of digits provided] - (weight+1) gives the number
				// of digits on the RHS of the decimal place value as determined by
				// the given input. Note dscale can be negative, meaning we truncated
				// the leading zeroes at the front, giving a higher exponent (e.g. 0042,0000
				// can omit the trailing 0000, giving dscale of -4, which makes the exponent 4).
				// * if the digits we have in the buffer on the RHS, as calculated above,
				// is larger than our calculated dscale, truncate our buffer to match the
				// desired dscale.
				dscale := (alloc.pgNum.Ndigits - (alloc.pgNum.Weight + 1)) * PGDecDigits
				if overScale := dscale - alloc.pgNum.Dscale; overScale > 0 {
					dscale -= overScale
					decDigits = decDigits[:len(decDigits)-int(overScale)]
				}

				decString := encoding.UnsafeConvertBytesToString(decDigits)
				if _, ok := alloc.dd.Coeff.SetString(decString, 10); !ok {
					return nil, pgerror.Newf(pgcode.Syntax, "could not parse %q as type decimal", decString)
				}
				alloc.dd.Exponent = -int32(dscale)
			}

			switch alloc.pgNum.Sign {
			case PGNumericPos:
			case PGNumericNeg:
				alloc.dd.Neg(&alloc.dd.Decimal)
			case 0xc000:
				// https://github.com/postgres/postgres/blob/ffa4cbd623dd69f9fa99e5e92426928a5782cf1a/src/backend/utils/adt/numeric.c#L169
				return tree.ParseDDecimal("NaN")
			case 0xd000:
				// https://github.com/postgres/postgres/blob/a57d312a7706321d850faa048a562a0c0c01b835/src/backend/utils/adt/numeric.c#L201
				return tree.ParseDDecimal("Inf")

			case 0xf000:
				// https://github.com/postgres/postgres/blob/a57d312a7706321d850faa048a562a0c0c01b835/src/backend/utils/adt/numeric.c#L200
				return tree.ParseDDecimal("-Inf")
			default:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported numeric sign: %d", alloc.pgNum.Sign)
			}

			return &alloc.dd, nil
		case oid.T_bytea:
			return da.NewDBytes(tree.DBytes(bs)), nil
		case oid.T_timestamp:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "timestamp requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestamp(pgBinaryToTime(i), time.Microsecond)
		case oid.T_timestamptz:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "timestamptz requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return tree.MakeDTimestampTZ(pgBinaryToTime(i), time.Microsecond)
		case oid.T_date:
			if len(b) < 4 {
				return nil, pgerror.Newf(pgcode.Syntax, "date requires 4 bytes for binary format")
			}
			i := int32(binary.BigEndian.Uint32(b))
			v, err := pgdate.MakeDateFromPGEpoch(i)
			if err != nil {
				return nil, err
			}
			return da.NewDDate(tree.DDate{Date: v}), nil
		case oid.T_time:
			if len(b) < 8 {
				return nil, pgerror.Newf(pgcode.Syntax, "time requires 8 bytes for binary format")
			}
			i := int64(binary.BigEndian.Uint64(b))
			return da.NewDTime(tree.DTime(i)), nil
		case oid.T_timetz:
			if len(b) < 12 {
				return nil, pgerror.Newf(pgcode.Syntax, "timetz requires 12 bytes for binary format")
			}
			timeOfDayMicros := int64(binary.BigEndian.Uint64(b))
			offsetSecs := int32(binary.BigEndian.Uint32(b[8:]))
			timeTZ := timetz.MakeTimeTZ(timeofday.TimeOfDay(timeOfDayMicros), offsetSecs)
			return da.NewDTimeTZ(tree.DTimeTZ{TimeTZ: timeTZ}), nil
		case oid.T_interval:
			if len(b) < 16 {
				return nil, pgerror.Newf(pgcode.Syntax, "interval requires 16 bytes for binary format")
			}
			nanos := (int64(binary.BigEndian.Uint64(b)) / int64(time.Nanosecond)) * int64(time.Microsecond)
			days := int32(binary.BigEndian.Uint32(b[8:]))
			months := int32(binary.BigEndian.Uint32(b[12:]))

			duration := duration.MakeDuration(nanos, int64(days), int64(months))
			return da.NewDInterval(tree.DInterval{Duration: duration}), nil
		case oid.T_uuid:
			v, err := uuid.FromBytes(b)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDUuid(tree.DUuid{UUID: v}), nil
		case oid.T_inet:
			ipAddr, err := pgBinaryToIPAddr(b)
			if err != nil {
				return nil, err
			}
			return da.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
		case oid.T_json:
			if err := validateStringBytes(b); err != nil {
				return nil, err
			}
			v, err := json.ParseJSON(bs)
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDJSON(tree.DJSON{JSON: v}), nil
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
			v, err := json.ParseJSON(encoding.UnsafeConvertBytesToString(b))
			if err != nil {
				return nil, tree.MakeParseError(bs, typ, err)
			}
			return da.NewDJSON(tree.DJSON{JSON: v}), nil
		case oidext.T_jsonpath:
			return da.NewDJsonpath(tree.DJsonpath(bs)), nil
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
			return da.NewDBitArray(tree.DBitArray{BitArray: ba}), err
		case oid.T_tsquery:
			ret, err := tsearch.DecodeTSQueryPGBinary(b)
			if err != nil {
				return nil, err
			}
			return tree.NewDTSQuery(ret), nil
		case oid.T_tsvector:
			ret, err := tsearch.DecodeTSVectorPGBinary(b)
			if err != nil {
				return nil, err
			}
			return tree.NewDTSVector(ret), nil
		case oidext.T_geometry:
			v, err := geo.ParseGeometryFromEWKB(b)
			if err != nil {
				return nil, err
			}
			return da.NewDGeometry(tree.DGeometry{Geometry: v}), nil
		case oidext.T_geography:
			v, err := geo.ParseGeographyFromEWKB(b)
			if err != nil {
				return nil, err
			}
			return da.NewDGeography(tree.DGeography{Geography: v}), nil
		default:
			if typ.Family() == types.ArrayFamily {
				return decodeBinaryArray(ctx, evalCtx, typ.ArrayContents(), b, code, da)
			}
			if typ.Family() == types.TupleFamily {
				return decodeBinaryTuple(ctx, evalCtx, b, da)
			}
			if typ.Family() == types.OidFamily {
				if len(b) < 4 {
					return nil, pgerror.Newf(pgcode.ProtocolViolation, "oid requires 4 bytes for binary format")
				}
				u := binary.BigEndian.Uint32(b)
				oidTyp := types.Oid
				if t, ok := types.OidToType[id]; ok {
					oidTyp = t
				}
				return da.NewDOid(tree.MakeDOid(oid.Oid(u), oidTyp)), nil
			}
		}
	default:
		return nil, errors.AssertionFailedf(
			"unexpected format code: %d", errors.Safe(code))
	}

	// Types with identical text/binary handling.
	switch typ.Family() {
	case types.EnumFamily:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		e, err := tree.MakeDEnumFromLogicalRepresentation(typ, bs)
		if err != nil {
			return nil, err
		}
		return da.NewDEnum(e), nil
	case types.RefCursorFamily:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		return da.NewDRefCursor(tree.DString(bs)), nil
	}
	switch id {
	case oid.T_text, oid.T_varchar, oid.T_unknown:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		return da.NewDString(tree.DString(bs)), nil
	case oid.T_bpchar:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		// Trim the trailing spaces.
		sv := strings.TrimRight(bs, " ")
		return da.NewDString(tree.DString(sv)), nil
	case oid.T_char:
		sv := bs
		// Always truncate to 1 byte, and handle the null byte specially.
		if len(b) >= 1 {
			if b[0] == 0 {
				sv = ""
			} else {
				sv = string(b[:1])
			}
		}
		return da.NewDString(tree.DString(sv)), nil
	case oid.T_name:
		if err := validateStringBytes(b); err != nil {
			return nil, err
		}
		return da.NewDName(tree.DString(bs)), nil
	}

	// Fallthrough case.
	return nil, errors.AssertionFailedf(
		"unsupported OID %v with format code %s", errors.Safe(id), errors.Safe(code))
}

// Values which are going to be converted to strings (STRING and NAME) need to
// be valid UTF-8 for us to accept them.
func validateStringBytes(b []byte) error {
	if !utf8.Valid(b) {
		return invalidUTF8Error
	}
	return nil
}

// PGNumericSign indicates the sign of a numeric.
//
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
	// Postgres uses math.MaxInt64 microseconds as the "infinity" timestamp, see:
	// https://github.com/postgres/postgres/blob/9380e5f129d2a160ecc2444f61bb7cb97fd51fbb/src/include/datatype/timestamp.h#L151
	if i == math.MaxInt64 {
		return pgdate.TimeInfinity
	}
	// Postgres uses math.MinInt64 microseconds as the "-infinity" timestamp, see:
	// https://github.com/postgres/postgres/blob/9380e5f129d2a160ecc2444f61bb7cb97fd51fbb/src/include/datatype/timestamp.h#L150
	if i == math.MinInt64 {
		return pgdate.TimeNegativeInfinity
	}
	return duration.AddMicros(PGEpochJDate, i)
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
	ctx context.Context,
	evalCtx *eval.Context,
	t *types.T,
	b []byte,
	code FormatCode,
	da *tree.DatumAlloc,
) (tree.Datum, error) {
	var hdr struct {
		Ndims int32
		// Nullflag
		_       int32
		ElemOid int32
	}
	var dim struct {
		// The next two fields should be arrays of size Ndims. However, since
		// we only support 1-dimensional arrays for now, for convenience we can
		// leave them in this struct as such for `binary.Read` to parse for us.
		DimSize int32
		// Dim lower bound
		_ int32
	}
	r := bytes.NewBuffer(b)
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return nil, err
	}
	if t.Oid() != oid.Oid(hdr.ElemOid) {
		return nil, pgerror.Newf(pgcode.ProtocolViolation, "wrong element type")
	}
	arr := tree.NewDArray(t)
	if hdr.Ndims == 0 {
		return arr, nil
	}
	if err := binary.Read(r, binary.BigEndian, &dim); err != nil {
		return nil, err
	}
	if err := validateArrayDimensions(int(hdr.Ndims), int(dim.DimSize)); err != nil {
		return nil, err
	}
	var vlen int32
	for i := int32(0); i < dim.DimSize; i++ {
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
		elem, err := DecodeDatum(ctx, evalCtx, t, code, buf, da)
		if err != nil {
			return nil, err
		}
		if err := arr.Append(elem); err != nil {
			return nil, err
		}
	}
	return arr, nil
}

const tupleHeaderSize, oidSize, elementSize = 4, 4, 4

func decodeBinaryTuple(
	ctx context.Context, evalCtx *eval.Context, b []byte, da *tree.DatumAlloc,
) (tree.Datum, error) {

	bufferLength := len(b)
	if bufferLength < tupleHeaderSize {
		return nil, errors.WithDetailf(
			pgerror.Newf(pgcode.Syntax, "tuple requires a %d byte header for binary format", tupleHeaderSize),
			"bufferLength=%d", bufferLength)
	}

	bufferStartIdx := 0
	bufferEndIdx := bufferStartIdx + tupleHeaderSize
	numberOfElements := int32(binary.BigEndian.Uint32(b[bufferStartIdx:bufferEndIdx]))
	if numberOfElements < 0 {
		return nil, errors.WithDetailf(pgerror.New(pgcode.Syntax, "tuple must have non-negative number of elements"),
			"numberOfElements=%d", numberOfElements)
	}
	bufferStartIdx = bufferEndIdx

	typs := make([]*types.T, numberOfElements)
	datums := make(tree.Datums, numberOfElements)

	elementIdx := int32(0)

	// decorateSyntaxError is used to output the current state in error messages.
	decorateSyntaxError := func(err error) error {
		return errors.WithDetailf(
			err,
			"elementIdx=%d bufferLength=%d bufferStartIdx=%d bufferEndIdx=%d",
			elementIdx, bufferLength, bufferStartIdx, bufferEndIdx)
	}

	for elementIdx < numberOfElements {

		bufferEndIdx = bufferStartIdx + oidSize
		if bufferEndIdx < bufferStartIdx {
			return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "integer overflow reading element OID for binary format"))
		}
		if bufferLength < bufferEndIdx {
			return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "insufficient bytes reading element OID for binary format"))
		}

		elementOID := int32(binary.BigEndian.Uint32(b[bufferStartIdx:bufferEndIdx]))
		elementType, ok := types.OidToType[oid.Oid(elementOID)]
		if !ok {
			return nil, decorateSyntaxError(pgerror.Newf(pgcode.Syntax, "element type not found for OID %d", elementOID))
		}
		typs[elementIdx] = elementType
		bufferStartIdx = bufferEndIdx

		bufferEndIdx = bufferStartIdx + elementSize
		if bufferEndIdx < bufferStartIdx {
			return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "integer overflow reading element size for binary format"))
		}
		if bufferLength < bufferEndIdx {
			return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "insufficient bytes reading element size for binary format"))
		}

		bytesToRead := binary.BigEndian.Uint32(b[bufferStartIdx:bufferEndIdx])
		bufferStartIdx = bufferEndIdx
		if int32(bytesToRead) == -1 {
			datums[elementIdx] = tree.DNull
		} else {
			bufferEndIdx = bufferStartIdx + int(bytesToRead)
			if bufferEndIdx < bufferStartIdx {
				return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "integer overflow reading element for binary format"))
			}
			if bufferLength < bufferEndIdx {
				return nil, decorateSyntaxError(pgerror.New(pgcode.Syntax, "insufficient bytes reading element for binary format"))
			}

			colDatum, err := DecodeDatum(ctx, evalCtx, elementType, FormatBinary, b[bufferStartIdx:bufferEndIdx], da)

			if err != nil {
				return nil, err
			}

			bufferStartIdx = bufferEndIdx
			datums[elementIdx] = colDatum
		}
		elementIdx++
	}

	tupleTyps := types.MakeTuple(typs)
	return da.NewDTuple(tree.MakeDTuple(tupleTyps, datums...)), nil

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
