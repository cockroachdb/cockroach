// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Encode encodes `val` using value encoding and appends it to `appendTo`,
// returning the new buffer.
//
// This is suitable for generating the value part of individual columns
// in a column family.
//
// The encoded value is guaranteed to round trip and decode exactly to its
// input, but is not lexicographically sortable.
//
// The scratch buffer is optional and is used as a temporary buffer for certain
// datum types (JSON, arrays, tuples).
//
// See also: docs/tech-notes/encoding.md, keyside.Encode().
func Encode(appendTo []byte, colID ColumnIDDelta, val tree.Datum) ([]byte, error) {
	res, _, err := EncodeWithScratch(appendTo, colID, val, nil /* scratch */)
	return res, err
}

// EncodeWithScratch is similar to Encode, but requires a scratch buffer that is
// used as a temporary buffer for certain datum types (JSON, arrays, tuples). It
// may overwrite any existing values in the scratch buffer. If successful it
// returns the encoded bytes and the scratch buffer, which allows reusing a
// scratch buffer that has grown during encoding.
func EncodeWithScratch(
	appendTo []byte, colID ColumnIDDelta, val tree.Datum, scratch []byte,
) (res []byte, newScratch []byte, err error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), scratch, nil
	}
	switch t := tree.UnwrapDOidWrapper(val).(type) {
	case *tree.DBitArray:
		return encoding.EncodeBitArrayValue(appendTo, uint32(colID), t.BitArray), scratch, nil
	case *tree.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), scratch, nil
	case *tree.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), scratch, nil
	case *tree.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), scratch, nil
	case *tree.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Decimal), scratch, nil
	case *tree.DString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), scratch, nil
	case *tree.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), scratch, nil
	case *tree.DEncodedKey:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), scratch, nil
	case *tree.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), t.UnixEpochDaysWithOrig()), scratch, nil
	case *tree.DPGLSN:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.LSN)), scratch, nil
	case *tree.DBox2D:
		res, err = encoding.EncodeBox2DValue(appendTo, uint32(colID), t.CartesianBoundingBox.BoundingBox)
		return res, scratch, err
	case *tree.DGeography:
		res, err = encoding.EncodeGeoValue(appendTo, uint32(colID), t.SpatialObjectRef())
		return res, scratch, err
	case *tree.DGeometry:
		res, err = encoding.EncodeGeoValue(appendTo, uint32(colID), t.SpatialObjectRef())
		return res, scratch, err
	case *tree.DTime:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), scratch, nil
	case *tree.DTimeTZ:
		return encoding.EncodeTimeTZValue(appendTo, uint32(colID), t.TimeTZ), scratch, nil
	case *tree.DTimestamp:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), scratch, nil
	case *tree.DTimestampTZ:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), scratch, nil
	case *tree.DInterval:
		return encoding.EncodeDurationValue(appendTo, uint32(colID), t.Duration), scratch, nil
	case *tree.DUuid:
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), t.UUID), scratch, nil
	case *tree.DIPAddr:
		return encoding.EncodeIPAddrValue(appendTo, uint32(colID), t.IPAddr), scratch, nil
	case *tree.DJSON:
		scratch, err = json.EncodeJSON(scratch[:0], t.JSON)
		if err != nil {
			return nil, nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), scratch), scratch, nil
	case *tree.DTSQuery:
		scratch, err = tsearch.EncodeTSQuery(scratch[:0], t.TSQuery)
		if err != nil {
			return nil, nil, err
		}
		return encoding.EncodeTSQueryValue(appendTo, uint32(colID), scratch), scratch, nil
	case *tree.DTSVector:
		scratch, err = tsearch.EncodeTSVector(scratch[:0], t.TSVector)
		if err != nil {
			return nil, nil, err
		}
		return encoding.EncodeTSVectorValue(appendTo, uint32(colID), scratch), scratch, nil
	case *tree.DPGVector:
		scratch, err = vector.Encode(scratch[:0], t.T)
		if err != nil {
			return nil, nil, err
		}
		return encoding.EncodePGVectorValue(appendTo, uint32(colID), scratch), scratch, nil
	case *tree.DArray:
		scratch, err = encodeArray(t, scratch[:0])
		if err != nil {
			return nil, nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), scratch), scratch, nil
	case *tree.DTuple:
		return encodeTuple(t, appendTo, uint32(colID), scratch[:0])
	case *tree.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeContentBytes()), scratch, nil
	case *tree.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.Oid)), scratch, nil
	case *tree.DEnum:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.PhysicalRep), scratch, nil
	case *tree.DVoid:
		return encoding.EncodeVoidValue(appendTo, uint32(colID)), scratch, nil
	default:
		if buildutil.CrdbTestBuild {
			return nil, nil, errors.AssertionFailedf("unable to encode table value: %T", t)
		}
		return nil, nil, errors.Errorf("unable to encode table value: %T", t)
	}
}

// ColumnIDDelta is the difference between two descpb.ColumnIDs. When multiple
// columns are encoded in a single value, the difference relative to the
// previous column ID is encoded for each column (to minimize space usage).
type ColumnIDDelta uint32

// NoColumnID is a sentinel used when we aren't encoding a specific column ID.
// This is used when we use value encodings not to write KV Values but other
// purposes, for example transferring a value over DistSQL (in the row engine).
const NoColumnID = ColumnIDDelta(encoding.NoColumnID)

// MakeColumnIDDelta creates the ColumnIDDelta for the difference between the
// given columns in the same value. For the first column in the value,
// `previous` should be zero / NoColumnID.
func MakeColumnIDDelta(previous, current descpb.ColumnID) ColumnIDDelta {
	if previous > current {
		panic(errors.AssertionFailedf("cannot write column id %d after %d", current, previous))
	}
	return ColumnIDDelta(current - previous)
}
