// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// Encode encodes `val` using key encoding and appends it to `b`, returning the
// new buffer. It is suitable to generate index/lookup keys in KV.
//
// The encoded value is guaranteed to be lexicographically sortable, but not
// guaranteed to be roundtrippable during decoding: some values like decimals
// or collated strings have composite encoding where part of their value lies in
// the value part of the key/value pair.
//
// See also: docs/tech-notes/encoding.md, valueside.Encode().
func Encode(b []byte, val tree.Datum, dir encoding.Direction) ([]byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, errors.Errorf("invalid direction: %d", dir)
	}

	if val == tree.DNull {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}

	switch t := tree.UnwrapDOidWrapper(val).(type) {
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
	case *tree.DVoid:
		return encoding.EncodeVoidAscendingOrDescending(b), nil
	case *tree.DPGLSN:
		if dir == encoding.Ascending {
			return encoding.EncodeUvarintAscending(b, uint64(t.LSN)), nil
		}
		return encoding.EncodeUvarintDescending(b, uint64(t.LSN)), nil
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
		// TODO(49975): due to the fact that we're not adding any "tuple
		// marker", this encoding is faulty since it can lead to incorrect
		// decoding: e.g. tuple (NULL, NULL) is encoded as [0, 0], but when
		// decoding it, encoding.PeekLength will return 1 leaving the second
		// zero in the buffer which could later result in corruption of the
		// datum for the next column.
		for _, datum := range t.D {
			var err error
			b, err = Encode(b, datum, dir)
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
			return encoding.EncodeVarintAscending(b, int64(t.Oid)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t.Oid)), nil
	case *tree.DEnum:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.PhysicalRep), nil
		}
		return encoding.EncodeBytesDescending(b, t.PhysicalRep), nil
	case *tree.DEncodedKey:
		// DEncodedKey carries an already encoded key.
		return append(b, []byte(*t)...), nil
	case *tree.DJSON:
		return encodeJSONKey(b, t, dir)
	}
	if buildutil.CrdbTestBuild {
		return nil, errors.AssertionFailedf("unable to encode table key: %T", val)
	}
	return nil, errors.Errorf("unable to encode table key: %T", val)
}
