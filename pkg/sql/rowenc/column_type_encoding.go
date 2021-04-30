// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// This file contains facilities to encode values of specific SQL
// types to either index keys or to store in the value part of column
// families.

// EncodeTableKey encodes `val` into `b` and returns the new buffer.
// This is suitable to generate index/lookup keys in KV.
//
// The encoded value is guaranteed to be lexicographically sortable,
// but not guaranteed to be round-trippable during decoding: some
// values like decimals or collated strings have composite encoding
// where part of their value lies in the value part of the key/value
// pair.
//
// See also: docs/tech-notes/encoding.md, EncodeTableValue().
func EncodeTableKey(b []byte, val tree.Datum, dir encoding.Direction) ([]byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, errors.Errorf("invalid direction: %d", dir)
	}

	if val == tree.DNull {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}

	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DBool:
		var x int64
		if *t {
			x = 1
		} else {
			x = 0
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x), nil
		}
		return encoding.EncodeVarintDescending(b, x), nil
	case *tree.DInt:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DFloat:
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, float64(*t)), nil
		}
		return encoding.EncodeFloatDescending(b, float64(*t)), nil
	case *tree.DDecimal:
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, &t.Decimal), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Decimal), nil
	case *tree.DString:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DBytes:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DBox2D:
		if dir == encoding.Ascending {
			return encoding.EncodeBox2DAscending(b, t.CartesianBoundingBox.BoundingBox)
		}
		return encoding.EncodeBox2DDescending(b, t.CartesianBoundingBox.BoundingBox)
	case *tree.DGeography:
		so := t.Geography.SpatialObjectRef()
		if dir == encoding.Ascending {
			return encoding.EncodeGeoAscending(b, t.Geography.SpaceCurveIndex(), so)
		}
		return encoding.EncodeGeoDescending(b, t.Geography.SpaceCurveIndex(), so)
	case *tree.DGeometry:
		so := t.Geometry.SpatialObjectRef()
		spaceCurveIndex, err := t.Geometry.SpaceCurveIndex()
		if err != nil {
			return nil, err
		}
		if dir == encoding.Ascending {
			return encoding.EncodeGeoAscending(b, spaceCurveIndex, so)
		}
		return encoding.EncodeGeoDescending(b, spaceCurveIndex, so)
	case *tree.DDate:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, t.UnixEpochDaysWithOrig()), nil
		}
		return encoding.EncodeVarintDescending(b, t.UnixEpochDaysWithOrig()), nil
	case *tree.DTime:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DTimestamp:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DTimestampTZ:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DTimeTZ:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeTZAscending(b, t.TimeTZ), nil
		}
		return encoding.EncodeTimeTZDescending(b, t.TimeTZ), nil
	case *tree.DInterval:
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, t.Duration)
		}
		return encoding.EncodeDurationDescending(b, t.Duration)
	case *tree.DUuid:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.GetBytes()), nil
		}
		return encoding.EncodeBytesDescending(b, t.GetBytes()), nil
	case *tree.DIPAddr:
		data := t.ToBuffer(nil)
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, data), nil
		}
		return encoding.EncodeBytesDescending(b, data), nil
	case *tree.DTuple:
		for _, datum := range t.D {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *tree.DArray:
		return encodeArrayKey(b, t, dir)
	case *tree.DCollatedString:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.Key), nil
		}
		return encoding.EncodeBytesDescending(b, t.Key), nil
	case *tree.DBitArray:
		if dir == encoding.Ascending {
			return encoding.EncodeBitArrayAscending(b, t.BitArray), nil
		}
		return encoding.EncodeBitArrayDescending(b, t.BitArray), nil
	case *tree.DOid:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(t.DInt)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t.DInt)), nil
	case *tree.DEnum:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.PhysicalRep), nil
		}
		return encoding.EncodeBytesDescending(b, t.PhysicalRep), nil
	case *tree.DJSON:
		return nil, unimplemented.NewWithIssue(35706, "unable to encode JSON as a table key")
	}
	return nil, errors.Errorf("unable to encode table key: %T", val)
}

// SkipTableKey skips a value of type valType in key, returning the remainder
// of the key.
func SkipTableKey(key []byte) ([]byte, error) {
	skipLen, err := encoding.PeekLength(key)
	if err != nil {
		return nil, err
	}
	return key[skipLen:], nil
}

// DecodeTableKey decodes a value encoded by EncodeTableKey.
func DecodeTableKey(
	a *DatumAlloc, valType *types.T, key []byte, dir encoding.Direction,
) (tree.Datum, []byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, nil, errors.Errorf("invalid direction: %d", dir)
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return tree.DNull, key, nil
	}
	var rkey []byte
	var err error

	switch valType.Family() {
	case types.ArrayFamily:
		return decodeArrayKey(a, valType, key, dir)
	case types.BitFamily:
		var r bitarray.BitArray
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBitArrayAscending(key)
		} else {
			rkey, r, err = encoding.DecodeBitArrayDescending(key)
		}
		return a.NewDBitArray(tree.DBitArray{BitArray: r}), rkey, err
	case types.BoolFamily:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		// No need to chunk allocate DBool as MakeDBool returns either
		// tree.DBoolTrue or tree.DBoolFalse.
		return tree.MakeDBool(tree.DBool(i != 0)), rkey, err
	case types.IntFamily:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDInt(tree.DInt(i)), rkey, err
	case types.FloatFamily:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return a.NewDFloat(tree.DFloat(f)), rkey, err
	case types.DecimalFamily:
		var d apd.Decimal
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: d})
		return dd, rkey, err
	case types.StringFamily:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		if valType.Oid() == oid.T_name {
			return a.NewDName(tree.DString(r)), rkey, err
		}
		return a.NewDString(tree.DString(r)), rkey, err
	case types.CollatedStringFamily:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		d, err := tree.NewDCollatedString(r, valType.Locale(), &a.env)
		return d, rkey, err
	case types.JsonFamily:
		// Don't attempt to decode the JSON value. Instead, just return the
		// remaining bytes of the key.
		jsonLen, err := encoding.PeekLength(key)
		if err != nil {
			return nil, nil, err
		}
		return tree.DNull, key[jsonLen:], nil
	case types.BytesFamily:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(tree.DBytes(r)), rkey, err
	case types.Box2DFamily:
		var r geopb.BoundingBox
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBox2DAscending(key)
		} else {
			rkey, r, err = encoding.DecodeBox2DDescending(key)
		}
		return a.NewDBox2D(tree.DBox2D{
			CartesianBoundingBox: geo.CartesianBoundingBox{BoundingBox: r},
		}), rkey, err
	case types.GeographyFamily:
		g := a.NewDGeographyEmpty()
		so := g.Geography.SpatialObjectRef()
		if dir == encoding.Ascending {
			rkey, err = encoding.DecodeGeoAscending(key, so)
		} else {
			rkey, err = encoding.DecodeGeoDescending(key, so)
		}
		a.DoneInitNewDGeo(so)
		return g, rkey, err
	case types.GeometryFamily:
		g := a.NewDGeometryEmpty()
		so := g.Geometry.SpatialObjectRef()
		if dir == encoding.Ascending {
			rkey, err = encoding.DecodeGeoAscending(key, so)
		} else {
			rkey, err = encoding.DecodeGeoDescending(key, so)
		}
		a.DoneInitNewDGeo(so)
		return g, rkey, err
	case types.DateFamily:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDDate(tree.MakeDDate(pgdate.MakeCompatibleDateFromDisk(t))), rkey, err
	case types.TimeFamily:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDTime(tree.DTime(t)), rkey, err
	case types.TimeTZFamily:
		var t timetz.TimeTZ
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeTZAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeTZDescending(key)
		}
		return a.NewDTimeTZ(tree.DTimeTZ{TimeTZ: t}), rkey, err
	case types.TimestampFamily:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: t}), rkey, err
	case types.TimestampTZFamily:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: t}), rkey, err
	case types.IntervalFamily:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), rkey, err
	case types.UuidFamily:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		u, err := uuid.FromBytes(r)
		return a.NewDUuid(tree.DUuid{UUID: u}), rkey, err
	case types.INetFamily:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err := ipAddr.FromBuffer(r)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), rkey, err
	case types.OidFamily:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(i))), rkey, err
	case types.EnumFamily:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		phys, log, err := tree.GetEnumComponentsFromPhysicalRep(valType, r)
		if err != nil {
			return nil, nil, err
		}
		return a.NewDEnum(tree.DEnum{EnumTyp: valType, PhysicalRep: phys, LogicalRep: log}), rkey, nil
	default:
		return nil, nil, errors.Errorf("unable to decode table key: %s", valType)
	}
}

// EncodeTableValue encodes `val` into `appendTo` using DatumEncoding_VALUE
// and returns the new buffer.
//
// This is suitable for generating the value part of individual columns
// in a column family.
//
// The encoded value is guaranteed to round
// trip and decode exactly to its input, but is not guaranteed to be
// lexicographically sortable.
//
// See also: docs/tech-notes/encoding.md, EncodeTableKey().
func EncodeTableValue(
	appendTo []byte, colID descpb.ColumnID, val tree.Datum, scratch []byte,
) ([]byte, error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := tree.UnwrapDatum(nil, val).(type) {
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
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), t.UnixEpochDaysWithOrig()), nil
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
	case *tree.DArray:
		a, err := encodeArray(t, scratch)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), a), nil
	case *tree.DTuple:
		return encodeTuple(t, appendTo, uint32(colID), scratch)
	case *tree.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(t.Contents)), nil
	case *tree.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.DInt)), nil
	case *tree.DEnum:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), t.PhysicalRep), nil
	default:
		return nil, errors.Errorf("unable to encode table value: %T", t)
	}
}

// DecodeTableValue decodes a value encoded by EncodeTableValue.
func DecodeTableValue(a *DatumAlloc, valType *types.T, b []byte) (tree.Datum, []byte, error) {
	_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
	if err != nil {
		return nil, b, err
	}
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		return tree.DNull, b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valType.Family() != types.BoolFamily {
		b = b[dataOffset:]
	}
	return DecodeUntaggedDatum(a, valType, b)
}

// DecodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place).
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
func DecodeUntaggedDatum(a *DatumAlloc, t *types.T, buf []byte) (tree.Datum, []byte, error) {
	switch t.Family() {
	case types.IntFamily:
		b, i, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDInt(tree.DInt(i)), b, nil
	case types.StringFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDString(tree.DString(data)), b, nil
	case types.CollatedStringFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		d, err := tree.NewDCollatedString(string(data), t.Locale(), &a.env)
		return d, b, err
	case types.BitFamily:
		b, data, err := encoding.DecodeUntaggedBitArrayValue(buf)
		return a.NewDBitArray(tree.DBitArray{BitArray: data}), b, err
	case types.BoolFamily:
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		b, data, err := encoding.DecodeBoolValue(buf)
		if err != nil {
			return nil, b, err
		}
		return tree.MakeDBool(tree.DBool(data)), b, nil
	case types.FloatFamily:
		b, data, err := encoding.DecodeUntaggedFloatValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDFloat(tree.DFloat(data)), b, nil
	case types.DecimalFamily:
		b, data, err := encoding.DecodeUntaggedDecimalValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDecimal(tree.DDecimal{Decimal: data}), b, nil
	case types.BytesFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDBytes(tree.DBytes(data)), b, nil
	case types.DateFamily:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDate(tree.MakeDDate(pgdate.MakeCompatibleDateFromDisk(data))), b, nil
	case types.Box2DFamily:
		b, data, err := encoding.DecodeUntaggedBox2DValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDBox2D(tree.DBox2D{
			CartesianBoundingBox: geo.CartesianBoundingBox{BoundingBox: data},
		}), b, nil
	case types.GeographyFamily:
		g := a.NewDGeographyEmpty()
		so := g.Geography.SpatialObjectRef()
		b, err := encoding.DecodeUntaggedGeoValue(buf, so)
		a.DoneInitNewDGeo(so)
		if err != nil {
			return nil, b, err
		}
		return g, b, nil
	case types.GeometryFamily:
		g := a.NewDGeometryEmpty()
		so := g.Geometry.SpatialObjectRef()
		b, err := encoding.DecodeUntaggedGeoValue(buf, so)
		a.DoneInitNewDGeo(so)
		if err != nil {
			return nil, b, err
		}
		return g, b, nil
	case types.TimeFamily:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTime(tree.DTime(data)), b, nil
	case types.TimeTZFamily:
		b, data, err := encoding.DecodeUntaggedTimeTZValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimeTZ(tree.DTimeTZ{TimeTZ: data}), b, nil
	case types.TimestampFamily:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: data}), b, nil
	case types.TimestampTZFamily:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: data}), b, nil
	case types.IntervalFamily:
		b, data, err := encoding.DecodeUntaggedDurationValue(buf)
		return a.NewDInterval(tree.DInterval{Duration: data}), b, err
	case types.UuidFamily:
		b, data, err := encoding.DecodeUntaggedUUIDValue(buf)
		return a.NewDUuid(tree.DUuid{UUID: data}), b, err
	case types.INetFamily:
		b, data, err := encoding.DecodeUntaggedIPAddrValue(buf)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: data}), b, err
	case types.JsonFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		// We copy the byte buffer here, because the JSON decoding is lazy, and we
		// do not want to hang on to the backing byte buffer, which might be an
		// entire KV batch.
		cpy := make([]byte, len(data))
		copy(cpy, data)
		j, err := json.FromEncoding(cpy)
		if err != nil {
			return nil, b, err
		}
		return a.NewDJSON(tree.DJSON{JSON: j}), b, nil
	case types.OidFamily:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		return a.NewDOid(tree.MakeDOid(tree.DInt(data))), b, err
	case types.ArrayFamily:
		return decodeArray(a, t.ArrayContents(), buf)
	case types.TupleFamily:
		return decodeTuple(a, t, buf)
	case types.EnumFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		phys, log, err := tree.GetEnumComponentsFromPhysicalRep(t, data)
		if err != nil {
			return nil, nil, err
		}
		return a.NewDEnum(tree.DEnum{EnumTyp: t, PhysicalRep: phys, LogicalRep: log}), b, nil
	default:
		return nil, buf, errors.Errorf("couldn't decode type %s", t)
	}
}

// MarshalColumnValue produces the value encoding of the given datum,
// constrained by the given column type, into a roachpb.Value.
//
// This is used when when the table format does not use column
// families, such as pre-2.0 tables and some system tables.
//
// If val's type is incompatible with col, or if col's type is not yet
// implemented by this function, an error is returned.
func MarshalColumnValue(col catalog.Column, val tree.Datum) (roachpb.Value, error) {
	return MarshalColumnTypeValue(col.GetName(), col.GetType(), val)
}

// MarshalColumnTypeValue is called by MarshalColumnValue and in tests.
func MarshalColumnTypeValue(
	colName string, colType *types.T, val tree.Datum,
) (roachpb.Value, error) {
	var r roachpb.Value

	if val == tree.DNull {
		return r, nil
	}

	switch colType.Family() {
	case types.BitFamily:
		if v, ok := val.(*tree.DBitArray); ok {
			r.SetBitArray(v.BitArray)
			return r, nil
		}
	case types.BoolFamily:
		if v, ok := val.(*tree.DBool); ok {
			r.SetBool(bool(*v))
			return r, nil
		}
	case types.IntFamily:
		if v, ok := tree.AsDInt(val); ok {
			r.SetInt(int64(v))
			return r, nil
		}
	case types.FloatFamily:
		if v, ok := val.(*tree.DFloat); ok {
			r.SetFloat(float64(*v))
			return r, nil
		}
	case types.DecimalFamily:
		if v, ok := val.(*tree.DDecimal); ok {
			err := r.SetDecimal(&v.Decimal)
			return r, err
		}
	case types.StringFamily:
		if v, ok := tree.AsDString(val); ok {
			r.SetString(string(v))
			return r, nil
		}
	case types.BytesFamily:
		if v, ok := val.(*tree.DBytes); ok {
			r.SetString(string(*v))
			return r, nil
		}
	case types.DateFamily:
		if v, ok := val.(*tree.DDate); ok {
			r.SetInt(v.UnixEpochDaysWithOrig())
			return r, nil
		}
	case types.Box2DFamily:
		if v, ok := val.(*tree.DBox2D); ok {
			r.SetBox2D(v.CartesianBoundingBox.BoundingBox)
			return r, nil
		}
	case types.GeographyFamily:
		if v, ok := val.(*tree.DGeography); ok {
			err := r.SetGeo(v.SpatialObject())
			return r, err
		}
	case types.GeometryFamily:
		if v, ok := val.(*tree.DGeometry); ok {
			err := r.SetGeo(v.SpatialObject())
			return r, err
		}
	case types.TimeFamily:
		if v, ok := val.(*tree.DTime); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case types.TimeTZFamily:
		if v, ok := val.(*tree.DTimeTZ); ok {
			r.SetTimeTZ(v.TimeTZ)
			return r, nil
		}
	case types.TimestampFamily:
		if v, ok := val.(*tree.DTimestamp); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case types.TimestampTZFamily:
		if v, ok := val.(*tree.DTimestampTZ); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case types.IntervalFamily:
		if v, ok := val.(*tree.DInterval); ok {
			err := r.SetDuration(v.Duration)
			return r, err
		}
	case types.UuidFamily:
		if v, ok := val.(*tree.DUuid); ok {
			r.SetBytes(v.GetBytes())
			return r, nil
		}
	case types.INetFamily:
		if v, ok := val.(*tree.DIPAddr); ok {
			data := v.ToBuffer(nil)
			r.SetBytes(data)
			return r, nil
		}
	case types.JsonFamily:
		if v, ok := val.(*tree.DJSON); ok {
			data, err := json.EncodeJSON(nil, v.JSON)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	case types.ArrayFamily:
		if v, ok := val.(*tree.DArray); ok {
			if err := checkElementType(v.ParamTyp, colType.ArrayContents()); err != nil {
				return r, err
			}
			b, err := encodeArray(v, nil)
			if err != nil {
				return r, err
			}
			r.SetBytes(b)
			return r, nil
		}
	case types.CollatedStringFamily:
		if v, ok := val.(*tree.DCollatedString); ok {
			if lex.LocaleNamesAreEqual(v.Locale, colType.Locale()) {
				r.SetString(v.Contents)
				return r, nil
			}
			// We can't fail here with a locale mismatch, this is a sign
			// that the proper validation has not been performed upstream in
			// the mutation planning code.
			return r, errors.AssertionFailedf(
				"locale mismatch %q vs %q for column %q",
				v.Locale, colType.Locale(), tree.ErrNameString(colName))
		}
	case types.OidFamily:
		if v, ok := val.(*tree.DOid); ok {
			r.SetInt(int64(v.DInt))
			return r, nil
		}
	case types.EnumFamily:
		if v, ok := val.(*tree.DEnum); ok {
			r.SetBytes(v.PhysicalRep)
			return r, nil
		}
	default:
		return r, errors.AssertionFailedf("unsupported column type: %s", colType.Family())
	}
	return r, errors.AssertionFailedf("mismatched type %q vs %q for column %q",
		val.ResolvedType(), colType.Family(), tree.ErrNameString(colName))
}

// UnmarshalColumnValue is the counterpart to MarshalColumnValues.
//
// It decodes the value from a roachpb.Value using the type expected
// by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalColumnValue(a *DatumAlloc, typ *types.T, value roachpb.Value) (tree.Datum, error) {
	if value.RawBytes == nil {
		return tree.DNull, nil
	}

	switch typ.Family() {
	case types.BitFamily:
		d, err := value.GetBitArray()
		if err != nil {
			return nil, err
		}
		return a.NewDBitArray(tree.DBitArray{BitArray: d}), nil
	case types.BoolFamily:
		v, err := value.GetBool()
		if err != nil {
			return nil, err
		}
		return tree.MakeDBool(tree.DBool(v)), nil
	case types.IntFamily:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDInt(tree.DInt(v)), nil
	case types.FloatFamily:
		v, err := value.GetFloat()
		if err != nil {
			return nil, err
		}
		return a.NewDFloat(tree.DFloat(v)), nil
	case types.DecimalFamily:
		v, err := value.GetDecimal()
		if err != nil {
			return nil, err
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: v})
		return dd, nil
	case types.StringFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		if typ.Oid() == oid.T_name {
			return a.NewDName(tree.DString(v)), nil
		}
		return a.NewDString(tree.DString(v)), nil
	case types.BytesFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDBytes(tree.DBytes(v)), nil
	case types.DateFamily:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDDate(tree.MakeDDate(pgdate.MakeCompatibleDateFromDisk(v))), nil
	case types.Box2DFamily:
		v, err := value.GetBox2D()
		if err != nil {
			return nil, err
		}
		return a.NewDBox2D(tree.DBox2D{
			CartesianBoundingBox: geo.CartesianBoundingBox{BoundingBox: v},
		}), nil
	case types.GeographyFamily:
		v, err := value.GetGeo()
		if err != nil {
			return nil, err
		}
		return a.NewDGeography(tree.DGeography{Geography: geo.MakeGeographyUnsafe(v)}), nil
	case types.GeometryFamily:
		v, err := value.GetGeo()
		if err != nil {
			return nil, err
		}
		return a.NewDGeometry(tree.DGeometry{Geometry: geo.MakeGeometryUnsafe(v)}), nil
	case types.TimeFamily:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDTime(tree.DTime(v)), nil
	case types.TimeTZFamily:
		v, err := value.GetTimeTZ()
		if err != nil {
			return nil, err
		}
		return a.NewDTimeTZ(tree.DTimeTZ{TimeTZ: v}), nil
	case types.TimestampFamily:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: v}), nil
	case types.TimestampTZFamily:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: v}), nil
	case types.IntervalFamily:
		d, err := value.GetDuration()
		if err != nil {
			return nil, err
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), nil
	case types.CollatedStringFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return tree.NewDCollatedString(string(v), typ.Locale(), &a.env)
	case types.UuidFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		u, err := uuid.FromBytes(v)
		if err != nil {
			return nil, err
		}
		return a.NewDUuid(tree.DUuid{UUID: u}), nil
	case types.INetFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err = ipAddr.FromBuffer(v)
		if err != nil {
			return nil, err
		}
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
	case types.OidFamily:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(v))), nil
	case types.ArrayFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		datum, _, err := decodeArrayNoMarshalColumnValue(a, typ.ArrayContents(), v)
		// TODO(yuzefovich): do we want to create a new object via DatumAlloc?
		return datum, err
	case types.JsonFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		_, jsonDatum, err := json.DecodeJSON(v)
		if err != nil {
			return nil, err
		}
		return tree.NewDJSON(jsonDatum), nil
	case types.EnumFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		phys, log, err := tree.GetEnumComponentsFromPhysicalRep(typ, v)
		if err != nil {
			return nil, err
		}
		return a.NewDEnum(tree.DEnum{EnumTyp: typ, PhysicalRep: phys, LogicalRep: log}), nil
	default:
		return nil, errors.Errorf("unsupported column type: %s", typ.Family())
	}
}

// encodeTuple produces the value encoding for a tuple.
func encodeTuple(t *tree.DTuple, appendTo []byte, colID uint32, scratch []byte) ([]byte, error) {
	appendTo = encoding.EncodeValueTag(appendTo, colID, encoding.Tuple)
	appendTo = encoding.EncodeNonsortingUvarint(appendTo, uint64(len(t.D)))

	var err error
	for _, dd := range t.D {
		appendTo, err = EncodeTableValue(appendTo, descpb.ColumnID(encoding.NoColumnID), dd, scratch)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

// decodeTuple decodes a tuple from its value encoding. It is the
// counterpart of encodeTuple().
func decodeTuple(a *DatumAlloc, tupTyp *types.T, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, nil, err
	}

	result := tree.DTuple{
		D: a.NewDatums(len(tupTyp.TupleContents())),
	}

	var datum tree.Datum
	for i := range tupTyp.TupleContents() {
		datum, b, err = DecodeTableValue(a, tupTyp.TupleContents()[i], b)
		if err != nil {
			return nil, b, err
		}
		result.D[i] = datum
	}
	return a.NewDTuple(result), b, nil
}

// encodeArrayKey generates an ordered key encoding of an array.
// The encoding format for an array [a, b] is as follows:
// [arrayMarker, enc(a), enc(b), terminator].
// The terminator is guaranteed to be less than all encoded values,
// so two arrays with the same prefix but different lengths will sort
// correctly. The key difference is that NULL values need to be encoded
// differently, because the standard NULL encoding conflicts with the
// terminator byte. This NULL value is chosen to be larger than the
// terminator but less than all existing encoded values.
func encodeArrayKey(b []byte, array *tree.DArray, dir encoding.Direction) ([]byte, error) {
	var err error
	b = encoding.EncodeArrayKeyMarker(b, dir)
	for _, elem := range array.Array {
		if elem == tree.DNull {
			b = encoding.EncodeNullWithinArrayKey(b, dir)
		} else {
			b, err = EncodeTableKey(b, elem, dir)
			if err != nil {
				return nil, err
			}
		}
	}
	return encoding.EncodeArrayKeyTerminator(b, dir), nil
}

// decodeArrayKey decodes an array key generated by encodeArrayKey.
func decodeArrayKey(
	a *DatumAlloc, t *types.T, buf []byte, dir encoding.Direction,
) (tree.Datum, []byte, error) {
	var err error
	buf, err = encoding.ValidateAndConsumeArrayKeyMarker(buf, dir)
	if err != nil {
		return nil, nil, err
	}

	result := tree.NewDArray(t.ArrayContents())

	for {
		if len(buf) == 0 {
			return nil, nil, errors.AssertionFailedf("invalid array encoding (unterminated)")
		}
		if encoding.IsArrayKeyDone(buf, dir) {
			buf = buf[1:]
			break
		}
		var d tree.Datum
		if encoding.IsNextByteArrayEncodedNull(buf, dir) {
			d = tree.DNull
			buf = buf[1:]
		} else {
			d, buf, err = DecodeTableKey(a, t.ArrayContents(), buf, dir)
			if err != nil {
				return nil, nil, err
			}
		}
		if err := result.Append(d); err != nil {
			return nil, nil, err
		}
	}
	return result, buf, nil
}

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
func decodeArray(a *DatumAlloc, elementType *types.T, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, b, err
	}
	return decodeArrayNoMarshalColumnValue(a, elementType, b)
}

// decodeArrayNoMarshalColumnValue skips the step where the MarshalColumnValue
// is stripped from the bytes. This is required for single-column family arrays.
func decodeArrayNoMarshalColumnValue(
	a *DatumAlloc, elementType *types.T, b []byte,
) (tree.Datum, []byte, error) {
	header, b, err := decodeArrayHeader(b)
	if err != nil {
		return nil, b, err
	}
	result := tree.DArray{
		Array:    make(tree.Datums, header.length),
		ParamTyp: elementType,
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
	case types.BytesFamily, types.StringFamily, types.CollatedStringFamily, types.EnumFamily:
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
	case types.UuidFamily:
		return encoding.UUID, nil
	case types.INetFamily:
		return encoding.IPAddr, nil
	default:
		return 0, errors.AssertionFailedf("no known encoding type for %s", t)
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
	switch t := tree.UnwrapDatum(nil, d).(type) {
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
		return encoding.EncodeUntaggedIntValue(b, int64(t.DInt)), nil
	case *tree.DCollatedString:
		return encoding.EncodeUntaggedBytesValue(b, []byte(t.Contents)), nil
	case *tree.DOidWrapper:
		return encodeArrayElement(b, t.Wrapped)
	case *tree.DEnum:
		return encoding.EncodeUntaggedBytesValue(b, t.PhysicalRep), nil
	default:
		return nil, errors.Errorf("don't know how to encode %s (%T)", d, d)
	}
}
