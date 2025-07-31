// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Decode decodes a value encoded by Encode.
func Decode(
	a *tree.DatumAlloc, valType *types.T, b []byte,
) (_ tree.Datum, remaining []byte, _ error) {
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
func DecodeUntaggedDatum(
	a *tree.DatumAlloc, t *types.T, buf []byte,
) (_ tree.Datum, remaining []byte, _ error) {
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
		d, err := a.NewDCollatedString(string(data), t.Locale())
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
	case types.EncodedKeyFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDEncodedKey(tree.DEncodedKey(data)), b, nil
	case types.DateFamily:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDate(tree.MakeDDate(pgdate.MakeCompatibleDateFromDisk(data))), b, nil
	case types.PGLSNFamily:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDPGLSN(tree.DPGLSN{LSN: lsn.LSN(data)}), b, nil
	case types.RefCursorFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDRefCursor(tree.DString(data)), b, nil
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
	case types.TSQueryFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		v, err := tsearch.DecodeTSQuery(data)
		if err != nil {
			return nil, b, err
		}
		return tree.NewDTSQuery(v), b, nil
	case types.TSVectorFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		v, err := tsearch.DecodeTSVector(data)
		if err != nil {
			return nil, b, err
		}
		return tree.NewDTSVector(v), b, nil
	case types.PGVectorFamily:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		_, vec, err := vector.Decode(data)
		if err != nil {
			return nil, b, err
		}
		return tree.NewDPGVector(vec), b, nil
	case types.OidFamily:
		// TODO: This possibly should decode to uint32 (with corresponding changes
		// to encoding) to ensure that the value fits in a DOid without any loss of
		// precision. In practice, this may not matter, since everything at
		// execution time uses a uint32 for OIDs. The extra safety may not be worth
		// the loss of variable length encoding.
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		return a.NewDOid(tree.MakeDOid(oid.Oid(data), t)), b, err
	case types.ArrayFamily:
		// Skip the encoded data length.
		b, _, _, err := encoding.DecodeNonsortingUvarint(buf)
		if err != nil {
			return nil, nil, err
		}
		return decodeArray(a, t, b)
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
	case types.VoidFamily:
		return a.NewDVoid(), buf, nil
	default:
		if buildutil.CrdbTestBuild {
			return nil, buf, errors.AssertionFailedf("unable to decode table value %s", t.SQLStringForError())
		}
		return nil, buf, errors.Errorf("unable to decode table value %s", t.SQLStringForError())
	}
}

// Decoder is a helper for decoding rows that contain multiple encoded values.
//
// This helper is intended for non-performance-critical uses (like processing
// rangefeed KVs). The query execution engines have more specialized
// implementations for performance reasons.
type Decoder struct {
	colIdxMap catalog.TableColMap
	types     []*types.T
}

// MakeDecoder creates a Decoder for the given columns.
//
// Once created, the Decoder is immutable.
func MakeDecoder(cols []catalog.Column) Decoder {
	var d Decoder
	d.types = make([]*types.T, len(cols))
	for i, col := range cols {
		d.colIdxMap.Set(col.GetID(), i)
		d.types[i] = col.GetType()
	}
	return d
}

// Decode processes multiple encoded values. Values for the columns used to
// create the decoder are populated in the corresponding positions in the Datums
// slice.
//
// If a given column is not encoded, the datum will be DNull.
//
// Values for any other column IDs are ignored.
//
// Decode can be called concurrently on the same Decoder.
func (d *Decoder) Decode(a *tree.DatumAlloc, bytes []byte) (tree.Datums, error) {
	datums := make(tree.Datums, len(d.types))
	for i := range datums {
		datums[i] = tree.DNull
	}

	var lastColID descpb.ColumnID
	for len(bytes) > 0 {
		_, dataOffset, colIDDelta, typ, err := encoding.DecodeValueTag(bytes)
		if err != nil {
			return nil, err
		}
		colID := lastColID + descpb.ColumnID(colIDDelta)
		lastColID = colID
		idx, ok := d.colIdxMap.Get(colID)
		if !ok {
			// This column wasn't requested, so read its length and skip it.
			l, err := encoding.PeekValueLengthWithOffsetsAndType(bytes, dataOffset, typ)
			if err != nil {
				return nil, err
			}
			bytes = bytes[l:]
			continue
		}
		datums[idx], bytes, err = Decode(a, d.types[idx], bytes)
		if err != nil {
			return nil, err
		}
	}
	return datums, nil
}

// encodingTypeToDatumType picks a datum type based on the encoding type. It is
// a guess, so it could be incorrect (e.g. strings and enums use encoding.Bytes,
// yet we'll unconditionally return types.Bytes).
func encodingTypeToDatumType(t encoding.Type) (*types.T, error) {
	switch t {
	case encoding.Int:
		return types.Int, nil
	case encoding.Float:
		return types.Float, nil
	case encoding.Decimal:
		return types.Decimal, nil
	case encoding.Bytes, encoding.BytesDesc:
		return types.Bytes, nil
	case encoding.Time:
		return types.TimestampTZ, nil
	case encoding.Duration:
		return types.Interval, nil
	case encoding.True, encoding.False:
		return types.Bool, nil
	case encoding.UUID:
		return types.Uuid, nil
	case encoding.IPAddr:
		return types.INet, nil
	case encoding.JSON:
		return types.Jsonb, nil
	case encoding.BitArray, encoding.BitArrayDesc:
		return types.VarBit, nil
	case encoding.TimeTZ:
		return types.TimeTZ, nil
	case encoding.Geo, encoding.GeoDesc:
		return types.Geometry, nil
	case encoding.Box2D:
		return types.Box2D, nil
	default:
		return nil, errors.Newf("no known datum type for encoding type %s", t)
	}
}
