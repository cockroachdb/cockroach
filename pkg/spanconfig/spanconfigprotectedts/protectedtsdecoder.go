// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// protectedTimestampDecoder decodes rows from the protected_timestamp_records
// table.
type protectedTimestampDecoder struct {
	codec     keys.SQLCodec
	alloc     rowenc.DatumAlloc
	colIdxMap catalog.TableColMap
}

// newProtectedTimestampDecoder instantiates a zonesDecoder.
func newProtectedTimestampDecoder(codec keys.SQLCodec) *protectedTimestampDecoder {
	return &protectedTimestampDecoder{
		codec: codec,
		colIdxMap: row.ColIDtoRowIndexFromCols(
			systemschema.ProtectedTimestampsRecordsTable.PublicColumns(),
		),
	}
}

// RawValue contains a raw-value / value-type pair, corresponding to the value
// and valueType columns of the settings table.
type RawValue struct {
	Timestamp hlc.Timestamp
	Target    *ptpb.Target
}

// DecodeRow decodes a row of the system.protected_ts_records table. If the
// value is not present, the record key will be returned but the value will be
// zero and the tombstone bool will be set.
func (d *protectedTimestampDecoder) DecodeRow(
	kv roachpb.KeyValue,
) (id uuid.UUID, val RawValue, _ error) {
	tbl := systemschema.ProtectedTimestampsRecordsTable
	// First we need to decode the pts record id field from the index key.
	{
		types := []*types.T{tbl.PublicColumns()[0].GetType()}
		idRow := make([]rowenc.EncDatum, 1)
		_, matches, _, err := rowenc.DecodeIndexKey(d.codec, types, idRow, nil, kv.Key)
		if err != nil {
			return uuid.Nil, RawValue{}, errors.Wrap(err, "failed to decode key")
		}
		if !matches {
			return uuid.Nil, RawValue{}, errors.Errorf("unexpected non-pts-records KV with pts-record prefix: %v", kv.Key)
		}
		if err := idRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
			return uuid.Nil, RawValue{}, err
		}
		id = idRow[0].Datum.(*tree.DUuid).UUID
	}
	if !kv.Value.IsPresent() {
		return id, RawValue{}, nil
	}

	// The rest of the columns are stored as a family, packed with diff-encoded
	// column IDs followed by their values.
	{
		// column valueType can be null (missing) so we default it to "s".
		bytes, err := kv.Value.GetTuple()
		if err != nil {
			return uuid.Nil, RawValue{}, err
		}
		var colIDDiff uint32
		var lastColID descpb.ColumnID
		var res tree.Datum
		var dataOffset int
		var typ encoding.Type
		neededCols := map[descpb.ColumnID]struct{}{2: {}, 8: {}}
		for len(bytes) > 0 {
			_, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(bytes)
			if err != nil {
				return uuid.Nil, RawValue{}, err
			}
			colID := lastColID + descpb.ColumnID(colIDDiff)
			lastColID = colID

			if _, ok := neededCols[colID]; !ok {
				// This column wasn't requested, so read its length and skip it.
				len, err := encoding.PeekValueLengthWithOffsetsAndType(bytes, dataOffset, typ)
				if err != nil {
					return uuid.Nil, RawValue{}, err
				}
				bytes = bytes[len:]
				continue
			}

			if idx, ok := d.colIdxMap.Get(colID); ok {
				res, bytes, err = rowenc.DecodeTableValue(&d.alloc, tbl.PublicColumns()[idx].GetType(), bytes)
				if err != nil {
					return uuid.Nil, RawValue{}, err
				}
				switch colID {
				case tbl.PublicColumns()[1].GetID(): // ts
					tsDecimal := tree.MustBeDDecimal(res)
					ts, err := tree.DecimalToHLC(&tsDecimal.Decimal)
					if err != nil {
						return uuid.Nil, RawValue{}, err
					}
					val.Timestamp = ts
				case tbl.PublicColumns()[7].GetID(): // target
					targetBytes := tree.MustBeDBytes(res)
					target := &ptpb.Target{}
					if err := protoutil.Unmarshal([]byte(targetBytes), target); err != nil {
						return uuid.Nil, RawValue{}, errors.Wrapf(err, "failed to unmarshal target for %v", id)
					}
					val.Target = target
				default:
					return uuid.Nil, RawValue{}, errors.Errorf("unknown column: %v", colID)
				}
			}
		}
	}
	return id, val, nil
}

// TestingProtectedTimestampDecoder constructs a protectedTimestampDecoder using
// the given codec and decodes the supplied keyValue using it. This wrapper is
// exported for testing purposes to ensure the struct remains private.
func TestingProtectedTimestampDecoder(
	codec keys.SQLCodec, keyValue roachpb.KeyValue,
) (uuid.UUID, RawValue, error) {
	return newProtectedTimestampDecoder(codec).DecodeRow(keyValue)
}
