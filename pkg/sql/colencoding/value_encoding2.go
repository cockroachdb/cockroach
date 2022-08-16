// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colencoding

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the rowIdx'th position of the vecIdx'th vector in
// coldata.TypedVecs.
// See the analog in rowenc/column_type_encoding.go.
func DecodeTableValueToCol2(
	da *tree.DatumAlloc,
	colID descpb.ColumnID,
	vecs *coldata.TypedVecs,
	vecIdx int,
	rowIdx int,
	typ encoding.Type,
	dataOffset int,
	valTyp *types.T,
	buf []byte,
) ([]byte, error) {

	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vecs.Nulls[vecIdx].SetNull(rowIdx)
		return buf[dataOffset:], nil
	}

	// Find the position of the target vector among the typed columns of its
	// type.
	colIdx := vecs.ColsMap[vecIdx]

	var err error
	switch valTyp.Family() {
	case types.IntFamily:
		var foundColID uint32
		buf, foundColID, err = encoding.DecodeUint32Ascending(buf)
		if err != nil {
			return nil, err
		}
		if foundColID != uint32(colID) {
			return buf, errors.AssertionFailedf("mismatch in col id %d != %d", foundColID, colID)
		}
		var i uint64
		_, i, err = encoding.DecodeUint64Ascending(buf[4:])
		if err != nil {
			return nil, err
		}
		//fmt.Printf("Just successfully decoded int %d for colID %d\n", i, colID)
		switch valTyp.Width() {
		case 16:
			vecs.Int16Cols[colIdx][rowIdx] = int16(i)
		case 32:
			vecs.Int32Cols[colIdx][rowIdx] = int32(i)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vecs.Int64Cols[colIdx][rowIdx] = int64(i)
		}
	default:
		return nil, errors.AssertionFailedf("unsupported type %s", valTyp)
	}
	return buf, err
}
