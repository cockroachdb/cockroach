// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

func invertedColToDatum(vec *coldata.Vec, row int) tree.Datum {
	if vec.Nulls().NullAt(row) {
		return tree.DNull
	}
	switch vec.Type().Family() {
	case types.JsonFamily:
		return tree.NewDJSON(vec.JSON().Get(row))
	case types.StringFamily:
		b := vec.Bytes().Get(row)
		s := encoding.UnsafeConvertBytesToString(b)
		return tree.NewDString(s)
	}
	// This handles arrays, geo etc.
	return vec.Datum().Get(row).(tree.Datum)
}

// encodeInvertedSecondaryIndex pretty much just delegates to the row engine and
// doesn't attempt to do bulk KV operations. TODO(cucaroach): optimize
// inverted index encoding to do bulk allocations and bulk KV puts.
func (b *BatchEncoder) encodeInvertedSecondaryIndex(
	ctx context.Context, index catalog.Index, kys []roachpb.Key, extraKeys [][]byte,
) error {
	var err error
	if kys, err = b.encodeInvertedIndexPrefixKeys(kys, index); err != nil {
		return err
	}
	var vec *coldata.Vec
	if i, ok := b.colMap.Get(index.InvertedColumnID()); ok {
		vec = b.b.ColVecs()[i]
	}
	indexGeoConfig := index.GetGeoConfig()
	for row := 0; row < b.count; row++ {
		if kys[row] == nil {
			continue
		}
		var keys [][]byte
		val := invertedColToDatum(vec, row+b.start)
		if !indexGeoConfig.IsEmpty() {
			if keys, err = rowenc.EncodeGeoInvertedIndexTableKeys(ctx, val, kys[row], indexGeoConfig); err != nil {
				return err
			}
		} else {
			if keys, err = rowenc.EncodeInvertedIndexTableKeys(val, kys[row], index.GetVersion()); err != nil {
				return err
			}
		}
		for _, key := range keys {
			if !index.IsUnique() {
				key = append(key, extraKeys[row]...)
			}
			if err = b.encodeInvertedSecondaryIndexNoFamiliesOneRow(index, key, row); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BatchEncoder) encodeInvertedSecondaryIndexNoFamiliesOneRow(
	ind catalog.Index, key roachpb.Key, row int,
) error {
	var value []byte
	// If we aren't encoding index keys with families, all index keys use the sentinel family 0.
	key = keys.MakeFamilyKey(key, 0)
	cols := rowenc.GetValueColumns(ind)
	var err error
	value, err = writeColumnValueOneRow(value, b.colMap, b.b.ColVecs(), cols, row+b.start)
	if err != nil {
		return err
	}
	var kvValue roachpb.Value
	kvValue.SetBytes(value)
	b.p.CPut(&key, &kvValue, nil /* expValue */)
	return b.checkMemory()
}

func (b *BatchEncoder) encodeInvertedIndexPrefixKeys(
	kys []roachpb.Key, index catalog.Index,
) ([]roachpb.Key, error) {
	numColumns := index.NumKeyColumns()
	var err error
	// If the index is a multi-column inverted index, we encode the non-inverted
	// columns in the key prefix.
	if numColumns > 1 {
		// Do not encode the last column, which is the inverted column, here. It
		// is encoded below this block.
		colIDs := index.IndexDesc().KeyColumnIDs[:numColumns-1]
		dirs := index.IndexDesc().KeyColumnDirections

		err = encodeColumns(colIDs, dirs, b.colMap, b.start, b.end, b.b.ColVecs(), kys)
		if err != nil {
			return nil, err
		}
	}
	return kys, nil
}

func writeColumnValueOneRow(
	value []byte,
	colMap catalog.TableColMap,
	vecs []*coldata.Vec,
	cols []rowenc.ValueEncodedColumn,
	row int,
) ([]byte, error) {
	var err error
	var lastColID catid.ColumnID

	for _, col := range cols {
		idx, ok := colMap.Get(col.ColID)
		if !ok {
			// Column not being updated or inserted.
			continue
		}
		vec := vecs[idx]
		if vec.Nulls().NullAt(row) {
			continue
		}
		if col.IsComposite && !isComposite(vec, row) {
			continue
		}
		if lastColID > col.ColID {
			return nil, errors.AssertionFailedf("cannot write column id %d after %d", col.ColID, lastColID)
		}
		colIDDelta := valueside.MakeColumnIDDelta(lastColID, col.ColID)
		lastColID = col.ColID
		value, err = valuesideEncodeCol(value, colIDDelta, vec, row)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
