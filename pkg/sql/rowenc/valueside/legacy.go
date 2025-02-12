// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// MarshalLegacy produces the value encoding of the given datum (constrained by
// the given column type) into a roachpb.Value, using the legacy version 1
// encoding (see docs/tech-notes/encoding.md).
//
// This encoding is used when when the table format does not use column
// families, such as pre-2.0 tables and some system tables.
//
// If val's type is incompatible with colType, or if colType is not yet
// implemented by this function, an error is returned.
func MarshalLegacy(colType *types.T, val tree.Datum) (roachpb.Value, error) {
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
	case types.PGLSNFamily:
		if v, ok := val.(*tree.DPGLSN); ok {
			r.SetInt(int64(v.LSN))
			return r, nil
		}
	case types.RefCursorFamily:
		if v, ok := tree.AsDString(val); ok {
			r.SetString(string(v))
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
	case types.TSQueryFamily:
		if v, ok := val.(*tree.DTSQuery); ok {
			data := tsearch.EncodeTSQueryPGBinary(nil, v.TSQuery)
			r.SetBytes(data)
			return r, nil
		}
	case types.TSVectorFamily:
		if v, ok := val.(*tree.DTSVector); ok {
			data, err := tsearch.EncodeTSVector(nil, v.TSVector)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	case types.PGVectorFamily:
		if v, ok := val.(*tree.DPGVector); ok {
			data, err := vector.Encode(nil, v.T)
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
	case types.TupleFamily:
		if v, ok := val.(*tree.DTuple); ok {
			b, _, err := encodeUntaggedTuple(v, nil /* appendTo */, nil /* scratch */)
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
				"locale mismatch %q vs %q",
				v.Locale, colType.Locale(),
			)
		}
	case types.OidFamily:
		if v, ok := val.(*tree.DOid); ok {
			r.SetInt(int64(v.Oid))
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
	return r, errors.AssertionFailedf("mismatched type %q vs %q", val.ResolvedType(), colType.Family())
}

// UnmarshalLegacy is the counterpart to MarshalLegacy.
//
// It decodes the value from a roachpb.Value using the type expected
// by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalLegacy(a *tree.DatumAlloc, typ *types.T, value roachpb.Value) (tree.Datum, error) {
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
	case types.PGLSNFamily:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDPGLSN(tree.DPGLSN{LSN: lsn.LSN(v)}), nil
	case types.RefCursorFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDRefCursor(tree.DString(v)), nil
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
		return a.NewDCollatedString(string(v), typ.Locale())
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
		return a.NewDOid(tree.MakeDOid(oid.Oid(v), typ)), nil
	case types.ArrayFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		datum, _, err := decodeArray(a, typ, v)
		// TODO(yuzefovich): do we want to create a new object via tree.DatumAlloc?
		return datum, err
	case types.TupleFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		datum, _, err := decodeTuple(a, typ, v)
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
	case types.TSQueryFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		vec, err := tsearch.DecodeTSQueryPGBinary(v)
		if err != nil {
			return nil, err
		}
		return tree.NewDTSQuery(vec), nil
	case types.TSVectorFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		vec, err := tsearch.DecodeTSVector(v)
		if err != nil {
			return nil, err
		}
		return tree.NewDTSVector(vec), nil
	case types.PGVectorFamily:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		_, vec, err := vector.Decode(v)
		if err != nil {
			return nil, err
		}
		return tree.NewDPGVector(vec), nil
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
