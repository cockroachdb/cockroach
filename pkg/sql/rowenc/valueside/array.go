// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// encodeArray produces the value encoding for an array.
func encodeArray(d *tree.DArray, scratch []byte) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return scratch, err
	}
	scratch = scratch[0:0]
	elementType, err := DatumTypeToArrayElementEncodingType(d.ParamTyp)

	if err != nil {
		return nil, err
	}
	header := arrayHeader{
		hasNulls: d.HasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   elementType,
		length:        uint64(d.Len()),
		// We don't encode the NULL bitmap in this function because we do it in lockstep with the
		// main data.
	}
	scratch, err = encodeArrayHeader(header, scratch)
	if err != nil {
		return nil, err
	}
	nullBitmapStart := len(scratch)
	if d.HasNulls {
		for i := 0; i < numBytesInBitArray(d.Len()); i++ {
			scratch = append(scratch, 0)
		}
	}
	for i, e := range d.Array {
		var err error
		if d.HasNulls && e == tree.DNull {
			setBit(scratch[nullBitmapStart:], i)
		} else {
			scratch, err = encodeArrayElement(scratch, e)
			if err != nil {
				return nil, err
			}
		}
	}
	return scratch, nil
}

// decodeArray decodes the value encoding for an array.
func decodeArray(a *tree.DatumAlloc, arrayType *types.T, b []byte) (tree.Datum, []byte, error) {
	header, b, err := decodeArrayHeader(b)
	if err != nil {
		return nil, b, err
	}
	elementType := arrayType.ArrayContents()
	return decodeArrayWithHeader(header, a, arrayType, elementType, b)
}

func decodeArrayWithHeader(
	header arrayHeader, a *tree.DatumAlloc, arrayType, elementType *types.T, b []byte,
) (tree.Datum, []byte, error) {
	result := tree.DArray{
		Array:    make(tree.Datums, header.length),
		ParamTyp: elementType,
	}
	var err error
	if err = result.MaybeSetCustomOid(arrayType); err != nil {
		return nil, b, err
	}
	var val tree.Datum
	for i := uint64(0); i < header.length; i++ {
		if header.isNull(i) {
			result.Array[i] = tree.DNull
			result.HasNulls = true
		} else {
			result.HasNonNulls = true
			val, b, err = DecodeUntaggedDatum(a, elementType, b)
			if err != nil {
				return nil, b, err
			}
			result.Array[i] = val
		}
	}
	return &result, b, nil
}

// arrayHeader is a parameter passing struct between
// encodeArray/decodeArray and encodeArrayHeader/decodeArrayHeader.
//
// It describes the important properties of an array that are useful
// for an efficient value encoding.
type arrayHeader struct {
	// hasNulls is set if the array contains any NULL values.
	hasNulls bool
	// numDimensions is the number of dimensions in the array.
	numDimensions int
	// elementType is the encoding type of the array elements.
	elementType encoding.Type
	// length is the total number of elements encoded.
	length uint64
	// nullBitmap is a compact representation of which array indexes
	// have NULL values.
	nullBitmap []byte
}

// isNull returns true iff the array element at the given index is
// NULL.
func (h arrayHeader) isNull(i uint64) bool {
	return h.hasNulls && ((h.nullBitmap[i/8]>>(i%8))&1) == 1
}

// setBit sets the bit in the given bitmap at index idx to 1. It's used to
// construct the NULL bitmap within arrays.
func setBit(bitmap []byte, idx int) {
	bitmap[idx/8] = bitmap[idx/8] | (1 << uint(idx%8))
}

// numBytesInBitArray returns the minimum number of bytes necessary to
// store the given number of bits.
func numBytesInBitArray(numBits int) int {
	return (numBits + 7) / 8
}

// makeBitVec carves a bitmap (byte array intended to store bits) for
// the given number of bits out of its first argument. It returns the
// remainder of the first argument after the bitmap has been reserved
// into it.
func makeBitVec(src []byte, length int) (b, bitVec []byte) {
	nullBitmapNumBytes := numBytesInBitArray(length)
	return src[nullBitmapNumBytes:], src[:nullBitmapNumBytes]
}

const hasNullFlag = 1 << 4

// encodeArrayHeader is used by encodeArray to encode the header
// at the beginning of the value encoding.
func encodeArrayHeader(h arrayHeader, buf []byte) ([]byte, error) {
	// The header byte we append here is formatted as follows:
	// * The low 4 bits encode the number of dimensions in the array.
	// * The high 4 bits are flags, with the lowest representing whether the array
	//   contains NULLs, and the rest reserved.
	headerByte := h.numDimensions
	if h.hasNulls {
		headerByte = headerByte | hasNullFlag
	}
	buf = append(buf, byte(headerByte))
	buf = encoding.EncodeValueTag(buf, encoding.NoColumnID, h.elementType)
	buf = encoding.EncodeNonsortingUvarint(buf, h.length)
	return buf, nil
}

// decodeArrayHeader is used by decodeArray to decode the header at
// the beginning of the value encoding.
func decodeArrayHeader(b []byte) (arrayHeader, []byte, error) {
	if len(b) < 2 {
		return arrayHeader{}, b, errors.Errorf("buffer too small")
	}
	hasNulls := b[0]&hasNullFlag != 0
	b = b[1:]
	_, dataOffset, _, encType, err := encoding.DecodeValueTag(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	b = b[dataOffset:]
	b, _, length, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	nullBitmap := []byte(nil)
	if hasNulls {
		b, nullBitmap = makeBitVec(b, int(length))
	}
	return arrayHeader{
		hasNulls: hasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   encType,
		length:        length,
		nullBitmap:    nullBitmap,
	}, b, nil
}

var errNestedArraysNotFullySupported = unimplemented.NewWithIssueDetail(32552, "", "nested arrays are not fully supported")

// DatumTypeToArrayElementEncodingType decides an encoding type to
// place in the array header given a datum type. The element encoding
// type is then used to encode/decode array elements.
func DatumTypeToArrayElementEncodingType(t *types.T) (encoding.Type, error) {
	switch t.Family() {
	case types.IntFamily:
		return encoding.Int, nil
	case types.OidFamily:
		return encoding.Int, nil
	case types.FloatFamily:
		return encoding.Float, nil
	case types.Box2DFamily:
		return encoding.Box2D, nil
	case types.GeometryFamily:
		return encoding.Geo, nil
	case types.GeographyFamily:
		return encoding.Geo, nil
	case types.DecimalFamily:
		return encoding.Decimal, nil
	case types.BytesFamily, types.StringFamily, types.CollatedStringFamily,
		types.EnumFamily, types.RefCursorFamily:
		return encoding.Bytes, nil
	case types.TimestampFamily, types.TimestampTZFamily:
		return encoding.Time, nil
	// Note: types.Date was incorrectly mapped to encoding.Time when arrays were
	// first introduced. If any 1.1 users used date arrays, they would have been
	// persisted with incorrect elementType values.
	case types.DateFamily, types.TimeFamily:
		return encoding.Int, nil
	case types.TimeTZFamily:
		return encoding.TimeTZ, nil
	case types.IntervalFamily:
		return encoding.Duration, nil
	case types.BoolFamily:
		return encoding.True, nil
	case types.BitFamily:
		return encoding.BitArray, nil
	case types.PGLSNFamily:
		return encoding.Int, nil
	case types.UuidFamily:
		return encoding.UUID, nil
	case types.INetFamily:
		return encoding.IPAddr, nil
	case types.JsonFamily:
		return encoding.JSON, nil
	case types.TupleFamily:
		return encoding.Tuple, nil
	case types.ArrayFamily:
		return 0, errNestedArraysNotFullySupported
	default:
		return 0, errors.AssertionFailedf("no known encoding type for %s", t.Family().Name())
	}
}

func init() {
	encoding.PrettyPrintArrayValueEncoded = func(b []byte) (string, error) {
		header, b, err := decodeArrayHeader(b)
		if err != nil {
			return "", err
		}
		elementType, err := encodingTypeToDatumType(header.elementType)
		if err != nil {
			return "", err
		}
		arrayType := types.MakeArray(elementType)
		d, rem, err := decodeArrayWithHeader(header, nil /* a */, arrayType, elementType, b)
		if err != nil {
			return "", err
		}
		if len(rem) != 0 {
			return "", errors.Newf("unexpected remainder after decoding array: %v", rem)
		}
		return d.String(), nil
	}
}

func checkElementType(paramType *types.T, elemType *types.T) error {
	if paramType.Family() != elemType.Family() {
		return errors.Errorf("type of array contents %s doesn't match column type %s",
			paramType, elemType.Family())
	}
	if paramType.Family() == types.CollatedStringFamily {
		if paramType.Locale() != elemType.Locale() {
			return errors.Errorf("locale of collated string array being inserted (%s) doesn't match locale of column type (%s)",
				paramType.Locale(), elemType.Locale())
		}
	}
	return nil
}

// encodeArrayElement appends the encoded form of one array element to
// the target byte buffer.
func encodeArrayElement(b []byte, d tree.Datum) ([]byte, error) {
	switch t := tree.UnwrapDOidWrapper(d).(type) {
	case *tree.DInt:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DString:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DBytes:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DBitArray:
		return encoding.EncodeUntaggedBitArrayValue(b, t.BitArray), nil
	case *tree.DFloat:
		return encoding.EncodeUntaggedFloatValue(b, float64(*t)), nil
	case *tree.DBool:
		return encoding.EncodeBoolValue(b, encoding.NoColumnID, bool(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeUntaggedDecimalValue(b, &t.Decimal), nil
	case *tree.DDate:
		return encoding.EncodeUntaggedIntValue(b, t.UnixEpochDaysWithOrig()), nil
	case *tree.DPGLSN:
		return encoding.EncodeUntaggedIntValue(b, int64(t.LSN)), nil
	case *tree.DBox2D:
		return encoding.EncodeUntaggedBox2DValue(b, t.CartesianBoundingBox.BoundingBox)
	case *tree.DGeography:
		return encoding.EncodeUntaggedGeoValue(b, t.SpatialObjectRef())
	case *tree.DGeometry:
		return encoding.EncodeUntaggedGeoValue(b, t.SpatialObjectRef())
	case *tree.DTime:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DTimeTZ:
		return encoding.EncodeUntaggedTimeTZValue(b, t.TimeTZ), nil
	case *tree.DTimestamp:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeUntaggedDurationValue(b, t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUntaggedUUIDValue(b, t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeUntaggedIPAddrValue(b, t.IPAddr), nil
	case *tree.DOid:
		return encoding.EncodeUntaggedIntValue(b, int64(t.Oid)), nil
	case *tree.DCollatedString:
		return encoding.EncodeUntaggedBytesValue(b, []byte(t.Contents)), nil
	case *tree.DOidWrapper:
		return encodeArrayElement(b, t.Wrapped)
	case *tree.DEnum:
		return encoding.EncodeUntaggedBytesValue(b, t.PhysicalRep), nil
	case *tree.DJSON:
		encoded, err := json.EncodeJSON(nil, t.JSON)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeUntaggedBytesValue(b, encoded), nil
	case *tree.DTuple:
		res, _, err := encodeUntaggedTuple(t, b, nil)
		return res, err
	case *tree.DTSQuery:
		encoded := tsearch.EncodeTSQueryPGBinary(nil, t.TSQuery)
		return encoding.EncodeUntaggedBytesValue(b, encoded), nil
	case *tree.DTSVector:
		encoded, err := tsearch.EncodeTSVector(nil, t.TSVector)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeUntaggedBytesValue(b, encoded), nil
	case *tree.DPGVector:
		encoded, err := vector.Encode(nil, t.T)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeUntaggedBytesValue(b, encoded), nil
	default:
		return nil, errors.Errorf("don't know how to encode %s (%T)", d, d)
	}
}
