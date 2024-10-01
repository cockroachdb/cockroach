// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// helper routine to simplify wordy code below, return true if value/row is null
// and we should skip it.
func partialIndexAndNullCheck[T []byte | roachpb.Key](
	kys []T, r, start int, nulls *coldata.Nulls, dir encoding.Direction,
) bool {
	if kys[r] == nil {
		return true
	}
	if nulls.NullAt(r + start) {
		if dir == encoding.Ascending {
			kys[r] = encoding.EncodeNullAscending(kys[r])
		} else {
			kys[r] = encoding.EncodeNullDescending(kys[r])
		}
		return true
	}
	return false
}

// encodeKeys is the columnar version of keyside.Encode.
// Cases taken from decodeTableKeyToCol.
//
// vec=nil indicates that all values are NULL.
func encodeKeys[T []byte | roachpb.Key](
	kys []T, dir encoding.Direction, vec *coldata.Vec, start, end int,
) error {
	count := end - start
	if vec == nil {
		for r := 0; r < count; r++ {
			b := kys[r]
			if b == nil {
				continue
			}
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeNullAscending(b)
			} else {
				kys[r] = encoding.EncodeNullDescending(b)
			}
		}
		return nil
	}
	nulls := vec.Nulls()
	switch typ := vec.Type(); typ.Family() {
	case types.BoolFamily:
		bs := vec.Bool()
		for r := 0; r < count; r++ {
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			var x int64
			b := kys[r]
			if bs[r+start] {
				x = 1
			}
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeVarintAscending(b, x)
			} else {
				kys[r] = encoding.EncodeVarintDescending(b, x)
			}
		}
	case types.IntFamily, types.DateFamily:
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			var i int64
			switch typ.Width() {
			case 16:
				is := vec.Int16()
				i = int64(is[r+start])
			case 32:
				is := vec.Int32()
				i = int64(is[r+start])
			default:
				is := vec.Int64()
				i = is[r+start]
			}
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeVarintAscending(b, i)
			} else {
				kys[r] = encoding.EncodeVarintDescending(b, i)
			}
		}
	case types.FloatFamily:
		fs := vec.Float64()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			f := fs[r+start]
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeFloatAscending(b, f)
			} else {
				kys[r] = encoding.EncodeFloatDescending(b, f)
			}
		}
	case types.DecimalFamily:
		ds := vec.Decimal()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			d := &ds[r+start]
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeDecimalAscending(b, d)
			} else {
				kys[r] = encoding.EncodeDecimalDescending(b, d)
			}
		}
	case types.BytesFamily, types.StringFamily, types.UuidFamily, types.EnumFamily:
		ss := vec.Bytes()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			s := ss.Get(r + start)
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeStringAscending(b, encoding.UnsafeConvertBytesToString(s))
			} else {
				kys[r] = encoding.EncodeStringDescending(b, encoding.UnsafeConvertBytesToString(s))
			}
		}
	case types.TimestampFamily, types.TimestampTZFamily:
		ts := vec.Timestamp()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			if dir == encoding.Ascending {
				kys[r] = encoding.EncodeTimeAscending(b, ts.Get(r+start))
			} else {
				kys[r] = encoding.EncodeTimeDescending(b, ts.Get(r+start))
			}
		}
	case types.IntervalFamily:
		ds := vec.Interval()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			var err error
			if dir == encoding.Ascending {
				b, err = encoding.EncodeDurationAscending(b, ds.Get(r+start))
			} else {
				b, err = encoding.EncodeDurationDescending(b, ds.Get(r+start))
			}
			if err != nil {
				return err
			}
			kys[r] = b
		}
	case types.JsonFamily:
		jsonVector := vec.JSON()
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			var err error
			jsonObj := jsonVector.Get(r + start)
			b, err = jsonObj.EncodeForwardIndex(b, dir)
			if err != nil {
				return err
			}
			kys[r] = b
		}
	default:
		if buildutil.CrdbTestBuild {
			if typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) != typeconv.DatumVecCanonicalTypeFamily {
				return errors.AssertionFailedf("type %v wasn't a datum backed type, maybe new type was added?", typ)
			}
		}
		for r := 0; r < count; r++ {
			b := kys[r]
			if partialIndexAndNullCheck(kys, r, start, nulls, dir) {
				continue
			}
			var err error
			b, err = keyside.Encode(b, vec.Datum().Get(r+start).(tree.Datum), dir)
			if err != nil {
				return err
			}
			kys[r] = b
		}
	}
	return nil
}

// encodeIndexKey is the vector version of rowenc.EncodeIndexKey. nulls will tell
// the caller if there are null values in any of the columns and which rows as
// well.
func (b *BatchEncoder) encodeIndexKey(
	keyCols []fetchpb.IndexFetchSpec_KeyColumn, nulls *coldata.Nulls,
) error {
	kys := b.keys
	for _, k := range keyCols {
		if k.IsComposite {
			b.compositeColumnIDs.Add(int(k.ColumnID))
		}
		dir, err := catalogkeys.IndexColumnEncodingDirection(k.Direction)
		if err != nil {
			return err
		}
		col, ok := b.colMap.Get(k.ColumnID)
		var vec *coldata.Vec
		if ok {
			vec = b.b.ColVec(col)
		} else {
			nulls.SetNulls()
		}
		if err := encodeKeys(kys, dir, vec, b.start, b.end); err != nil {
			return err
		}
		if vec.Nulls().MaybeHasNulls() {
			newnulls := nulls.Or(*vec.Nulls())
			*nulls = newnulls
		}
	}
	return nil
}
