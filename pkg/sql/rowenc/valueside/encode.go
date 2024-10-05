// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
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
func Encode(appendTo []byte, colID ColumnIDDelta, val tree.Datum, scratch []byte) ([]byte, error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := tree.UnwrapDOidWrapper(val).(type) {
	case *tree.DBitArray:
		return encoding.EncodeBitArrayValue(appendTo, uint32(colID), t.BitArray), nil
	case *tree.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), nil
	case *tree.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Decimal), nil
	case *tree.DString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), nil
	case *tree.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), nil
	case *tree.DEncodedKey:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeBytes()), nil
	case *tree.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), t.UnixEpochDaysWithOrig()), nil
	case *tree.DPGLSN:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.LSN)), nil
	case *tree.DBox2D:
		return encoding.EncodeBox2DValue(appendTo, uint32(colID), t.CartesianBoundingBox.BoundingBox)
	case *tree.DGeography:
		return encoding.EncodeGeoValue(appendTo, uint32(colID), t.SpatialObjectRef())
	case *tree.DGeometry:
		return encoding.EncodeGeoValue(appendTo, uint32(colID), t.SpatialObjectRef())
	case *tree.DTime:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DTimeTZ:
		return encoding.EncodeTimeTZValue(appendTo, uint32(colID), t.TimeTZ), nil
	case *tree.DTimestamp:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeDurationValue(appendTo, uint32(colID), t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeIPAddrValue(appendTo, uint32(colID), t.IPAddr), nil
	case *tree.DJSON:
		encoded, err := json.EncodeJSON(scratch, t.JSON)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), encoded), nil
	case *tree.DTSQuery:
		encoded, err := tsearch.EncodeTSQuery(scratch, t.TSQuery)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeTSQueryValue(appendTo, uint32(colID), encoded), nil
	case *tree.DTSVector:
		encoded, err := tsearch.EncodeTSVector(scratch, t.TSVector)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeTSVectorValue(appendTo, uint32(colID), encoded), nil
	case *tree.DArray:
		a, err := encodeArray(t, scratch)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), a), nil
	case *tree.DTuple:
		return encodeTuple(t, appendTo, uint32(colID), scratch)
	case *tree.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.UnsafeContentBytes()), nil
	case *tree.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.Oid)), nil
	case *tree.DEnum:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.PhysicalRep), nil
	case *tree.DVoid:
		return encoding.EncodeVoidValue(appendTo, uint32(colID)), nil
	default:
		return nil, errors.Errorf("unable to encode table value: %T", t)
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
