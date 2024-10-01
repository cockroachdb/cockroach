// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyside

import (
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Decode decodes a value encoded by Encode from a key.
func Decode(
	a *tree.DatumAlloc, valType *types.T, key []byte, dir encoding.Direction,
) (_ tree.Datum, remainingKey []byte, _ error) {
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
	case types.PGLSNFamily:
		var i uint64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeUvarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeUvarintDescending(key)
		}
		return a.NewDPGLSN(tree.DPGLSN{LSN: lsn.LSN(i)}), rkey, err
	case types.RefCursorFamily:
		var r string
		if dir == encoding.Ascending {
			// Perform a deep copy so that r would never reference the key's
			// memory which might keep the BatchResponse alive.
			rkey, r, err = encoding.DecodeUnsafeStringAscendingDeepCopy(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDRefCursor(tree.DString(r)), rkey, err
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
			// Perform a deep copy so that r would never reference the key's
			// memory which might keep the BatchResponse alive.
			rkey, r, err = encoding.DecodeUnsafeStringAscendingDeepCopy(key, nil)
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
			// Perform a deep copy so that r would never reference the key's
			// memory which might keep the BatchResponse alive.
			rkey, r, err = encoding.DecodeUnsafeStringAscendingDeepCopy(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		d, err := a.NewDCollatedString(r, valType.Locale())
		return d, rkey, err
	case types.JsonFamily:
		var json json.JSON
		json, rkey, err = decodeJSONKey(key, dir)
		if err != nil {
			return nil, nil, err
		}
		d := a.NewDJSON(tree.DJSON{JSON: json})
		return d, rkey, err
	case types.BytesFamily:
		var r []byte
		if dir == encoding.Ascending {
			// No need to perform the deep copy since converting to string below
			// will do that for us.
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(tree.DBytes(r)), rkey, err
	case types.VoidFamily:
		rkey, err = encoding.DecodeVoidAscendingOrDescending(key)
		return a.NewDVoid(), rkey, err
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
			// No need to perform the deep copy since converting to UUID below
			// will do that for us.
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
			// No need to perform the deep copy since converting to IPAddr below
			// will do that for us.
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
		// TODO: This possibly should use DecodeUint32 (with corresponding changes
		// to encoding) to ensure that the value fits in a DOid without any loss of
		// precision. In practice, this may not matter, since everything at
		// execution time uses a uint32 for OIDs. The extra safety may not be worth
		// the loss of variable length encoding.
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDOid(tree.MakeDOid(oid.Oid(i), valType)), rkey, err
	case types.EnumFamily:
		var r []byte
		if dir == encoding.Ascending {
			// No need to perform the deep copy since we only need r for a brief
			// period of time.
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
	case types.EncodedKeyFamily:
		// We don't actually decode anything; we wrap the raw key bytes into a
		// DEncodedKey.
		len, err := encoding.PeekLength(key)
		if err != nil {
			return nil, nil, err
		}
		rkey := key[len:]
		return a.NewDEncodedKey(tree.DEncodedKey(key[:len])), rkey, nil
	default:
		if buildutil.CrdbTestBuild {
			return nil, nil, errors.AssertionFailedf("unable to decode table key: %s", valType.SQLStringForError())
		}
		return nil, nil, errors.Errorf("unable to decode table key: %s", valType.SQLStringForError())
	}
}

// Skip skips one value of in a key, returning the remainder of the key.
func Skip(key []byte) (remainingKey []byte, _ error) {
	skipLen, err := encoding.PeekLength(key)
	if err != nil {
		return nil, err
	}
	return key[skipLen:], nil
}
