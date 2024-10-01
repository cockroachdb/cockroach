// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parquet

import (
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// decoder is used to store typedDecoders of various types in the same
// schema definition.
type decoder interface{}

type typedDecoder[T parquetDatatypes] interface {
	decoder
	decode(v T) (tree.Datum, error)
}

func decode[T parquetDatatypes](dec decoder, v T) (tree.Datum, error) {
	td, ok := dec.(typedDecoder[T])
	if !ok {
		return nil, errors.AssertionFailedf("expected typedDecoder[%T], but found %T", v, dec)
	}
	return td.decode(v)
}

type boolDecoder struct{}

func (boolDecoder) decode(v bool) (tree.Datum, error) {
	return tree.MakeDBool(tree.DBool(v)), nil
}

type stringDecoder struct{}

func (stringDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.NewDString(string(v)), nil
}

type pglsnDecoder struct{}

func (pglsnDecoder) decode(v int64) (tree.Datum, error) {
	return tree.NewDPGLSN(lsn.LSN(v)), nil
}

type refcursorDecoder struct{}

func (refcursorDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.NewDRefCursor(string(v)), nil
}

type int64Decoder struct{}

func (int64Decoder) decode(v int64) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(v)), nil
}

type int32Decoder struct{}

func (int32Decoder) decode(v int32) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(v)), nil
}

type decimalDecoder struct{}

func (decimalDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.ParseDDecimal(string(v))
}

type timestampDecoder struct{}

func (timestampDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	dtStr := string(v)
	d, dependsOnCtx, err := tree.ParseDTimestamp(nil, dtStr, time.Microsecond)
	if dependsOnCtx {
		return nil, errors.Newf("decoding timestamp %s depends on context", v)
	}
	if err != nil {
		return nil, err
	}
	// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent.
	d.Time = d.Time.UTC()
	return d, nil
}

type timestampTZDecoder struct{}

func (timestampTZDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	dtStr := string(v)
	d, dependsOnCtx, err := tree.ParseDTimestampTZ(nil, dtStr, time.Microsecond)
	if dependsOnCtx {
		return nil, errors.Newf("decoding timestampTZ %s depends on context", v)
	}
	if err != nil {
		return nil, err
	}
	// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent.
	d.Time = d.Time.UTC()
	return d, nil
}

type uUIDDecoder struct{}

func (uUIDDecoder) decode(v parquet.FixedLenByteArray) (tree.Datum, error) {
	uid, err := uuid.FromBytes(v)
	if err != nil {
		return nil, err
	}
	return tree.NewDUuid(tree.DUuid{UUID: uid}), nil
}

type iNetDecoder struct{}

func (iNetDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.ParseDIPAddrFromINetString(string(v))
}

type jsonDecoder struct{}

func (jsonDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.ParseDJSON(string(v))
}

type bitDecoder struct{}

func (bitDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	ba, err := bitarray.Parse(string(v))
	if err != nil {
		return nil, err
	}
	return &tree.DBitArray{BitArray: ba}, err
}

type bytesDecoder struct{}

func (bytesDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.NewDBytes(tree.DBytes(v)), nil
}

type enumDecoder struct{}

func (ed enumDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return &tree.DEnum{
		LogicalRep: string(v),
	}, nil
}

type dateDecoder struct{}

func (dateDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	d, dependCtx, err := tree.ParseDDate(nil, string(v))
	if dependCtx {
		return nil, errors.Newf("decoding date %s depends on context", v)
	}
	return d, err
}

type box2DDecoder struct{}

func (box2DDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	b, err := geo.ParseCartesianBoundingBox(string(v))
	if err != nil {
		return nil, err
	}
	return tree.NewDBox2D(b), nil
}

type geographyDecoder struct{}

func (geographyDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(v))
	if err != nil {
		return nil, err
	}
	return &tree.DGeography{Geography: g}, nil
}

type geometryDecoder struct{}

func (geometryDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(v))
	if err != nil {
		return nil, err
	}
	return &tree.DGeometry{Geometry: g}, nil
}

type intervalDecoder struct{}

func (intervalDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.ParseDInterval(duration.IntervalStyle_ISO_8601, string(v))
}

type timeDecoder struct{}

func (timeDecoder) decode(v int64) (tree.Datum, error) {
	return tree.MakeDTime(timeofday.TimeOfDay(v)), nil
}

type timeTZDecoder struct{}

func (timeTZDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	d, dependsOnCtx, err := tree.ParseDTimeTZ(nil, string(v), time.Microsecond)
	if dependsOnCtx {
		return nil, errors.Newf("parsed timeTZ %s depends on context", v)
	}
	return d, err
}

type float32Decoder struct{}

func (float32Decoder) decode(v float32) (tree.Datum, error) {
	return tree.NewDFloat(tree.DFloat(v)), nil
}

type float64Decoder struct{}

func (float64Decoder) decode(v float64) (tree.Datum, error) {
	return tree.NewDFloat(tree.DFloat(v)), nil
}

type oidDecoder struct{}

func (oidDecoder) decode(v int32) (tree.Datum, error) {
	return tree.NewDOid(oid.Oid(v)), nil
}

type regclassDecoder struct{}

func (regclassDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegClass)
	return &dRegClass, nil
}

type regnamespaceDecoder struct{}

func (regnamespaceDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegNamespace)
	return &dRegClass, nil
}

type regprocDecoder struct{}

func (regprocDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegProc)
	return &dRegClass, nil
}

type regprocedureDecoder struct{}

func (regprocedureDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegProcedure)
	return &dRegClass, nil
}

type regroleDecoder struct{}

func (regroleDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegRole)
	return &dRegClass, nil
}

type regtypeDecoder struct{}

func (regtypeDecoder) decode(v int32) (tree.Datum, error) {
	dRegClass := tree.MakeDOid(oid.Oid(v), types.RegType)
	return &dRegClass, nil
}

type collatedStringDecoder struct{}

func (collatedStringDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return &tree.DCollatedString{Contents: string(v)}, nil
}

// decoderFromFamilyAndType returns the decoder to use based on the type oid and
// family. Note the logical similarity to makeColumn in schema.go. This is
// intentional as each decoder returned by this function corresponds to a
// particular colWriter determined by makeColumn.
// TODO: refactor to remove the code duplication with makeColumn
func decoderFromFamilyAndType(typOid oid.Oid, family types.Family) (decoder, error) {
	switch family {
	case types.BoolFamily:
		return boolDecoder{}, nil
	case types.StringFamily:
		return stringDecoder{}, nil
	case types.IntFamily:
		typ, ok := types.OidToType[typOid]
		if !ok {
			return nil, errors.AssertionFailedf("could not determine type from oid %d", typOid)
		}
		if typ.Oid() == oid.T_int8 {
			return int64Decoder{}, nil
		}
		return int32Decoder{}, nil
	case types.DecimalFamily:
		return decimalDecoder{}, nil
	case types.TimestampFamily:
		return timestampDecoder{}, nil
	case types.TimestampTZFamily:
		return timestampTZDecoder{}, nil
	case types.UuidFamily:
		return uUIDDecoder{}, nil
	case types.INetFamily:
		return iNetDecoder{}, nil
	case types.JsonFamily:
		return jsonDecoder{}, nil
	case types.BitFamily:
		return bitDecoder{}, nil
	case types.BytesFamily:
		return bytesDecoder{}, nil
	case types.EnumFamily:
		return enumDecoder{}, nil
	case types.DateFamily:
		return dateDecoder{}, nil
	case types.Box2DFamily:
		return box2DDecoder{}, nil
	case types.PGLSNFamily:
		return pglsnDecoder{}, nil
	case types.RefCursorFamily:
		return refcursorDecoder{}, nil
	case types.GeographyFamily:
		return geographyDecoder{}, nil
	case types.GeometryFamily:
		return geometryDecoder{}, nil
	case types.IntervalFamily:
		return intervalDecoder{}, nil
	case types.TimeFamily:
		return timeDecoder{}, nil
	case types.TimeTZFamily:
		return timeTZDecoder{}, nil
	case types.FloatFamily:
		typ, ok := types.OidToType[typOid]
		if !ok {
			return nil, errors.AssertionFailedf("could not determine type from oid %d", typOid)
		}
		if typ.Oid() == oid.T_float4 {
			return float32Decoder{}, nil
		}
		return float64Decoder{}, nil
	case types.OidFamily:
		typ, ok := types.OidToType[typOid]
		if !ok {
			return nil, errors.AssertionFailedf("could not determine type from oid %d", typOid)
		}
		switch typ.Oid() {
		case oid.T_regclass:
			return regclassDecoder{}, nil
		case oid.T_regnamespace:
			return regnamespaceDecoder{}, nil
		case oid.T_regproc:
			return regprocDecoder{}, nil
		case oid.T_regprocedure:
			return regprocedureDecoder{}, nil
		case oid.T_regrole:
			return regroleDecoder{}, nil
		case oid.T_regtype:
			return regtypeDecoder{}, nil
		case oid.T_oid:
			return oidDecoder{}, nil
		}
		return nil, errors.AssertionFailedf("could not determine type from oid %d", typOid)
	case types.CollatedStringFamily:
		return collatedStringDecoder{}, nil
	default:
		return nil, errors.AssertionFailedf("could not find decoder for type oid %d and family %d", typOid, family)
	}
}

// Defeat the linter's unused lint errors.
func init() {
	var _, _ = boolDecoder{}.decode(false)
	var _, _ = stringDecoder{}.decode(parquet.ByteArray{})
	var _, _ = int32Decoder{}.decode(0)
	var _, _ = int64Decoder{}.decode(0)
	var _, _ = pglsnDecoder{}.decode(0)
	var _, _ = refcursorDecoder{}.decode(parquet.ByteArray{})
	var _, _ = decimalDecoder{}.decode(parquet.ByteArray{})
	var _, _ = timestampDecoder{}.decode(parquet.ByteArray{})
	var _, _ = timestampTZDecoder{}.decode(parquet.ByteArray{})
	var _, _ = uUIDDecoder{}.decode(parquet.FixedLenByteArray{})
	var _, _ = iNetDecoder{}.decode(parquet.ByteArray{})
	var _, _ = jsonDecoder{}.decode(parquet.ByteArray{})
	var _, _ = bitDecoder{}.decode(parquet.ByteArray{})
	var _, _ = bytesDecoder{}.decode(parquet.ByteArray{})
	var _, _ = enumDecoder{}.decode(parquet.ByteArray{})
	var _, _ = dateDecoder{}.decode(parquet.ByteArray{})
	var _, _ = box2DDecoder{}.decode(parquet.ByteArray{})
	var _, _ = geographyDecoder{}.decode(parquet.ByteArray{})
	var _, _ = geometryDecoder{}.decode(parquet.ByteArray{})
	var _, _ = intervalDecoder{}.decode(parquet.ByteArray{})
	var _, _ = timeDecoder{}.decode(0)
	var _, _ = timeTZDecoder{}.decode(parquet.ByteArray{})
	var _, _ = float64Decoder{}.decode(0.0)
	var _, _ = float32Decoder{}.decode(0.0)
	var _, _ = oidDecoder{}.decode(0)
	var _, _ = regclassDecoder{}.decode(0)
	var _, _ = regnamespaceDecoder{}.decode(0)
	var _, _ = regprocDecoder{}.decode(0)
	var _, _ = regprocedureDecoder{}.decode(0)
	var _, _ = regroleDecoder{}.decode(0)
	var _, _ = regtypeDecoder{}.decode(0)
	var _, _ = collatedStringDecoder{}.decode(parquet.ByteArray{})
}
