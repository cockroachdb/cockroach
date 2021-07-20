// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// RowDecoder decodes rows from the settings table.
type RowDecoder struct {
	codec     keys.SQLCodec
	alloc     rowenc.DatumAlloc
	colIdxMap catalog.TableColMap
}

// MakeRowDecoder makes a new RowDecoder for the settings table.
func MakeRowDecoder(codec keys.SQLCodec) RowDecoder {
	return RowDecoder{
		codec: codec,
		colIdxMap: row.ColIDtoRowIndexFromCols(
			systemschema.SettingsTable.PublicColumns(),
		),
	}
}

// DecodeRow decodes a row of the system.settings table. If the value is not
// present, the setting key will be returned but the other two fields will be
// zero and the tombstone bool will be set.
func (d *RowDecoder) DecodeRow(
	kv roachpb.KeyValue,
) (setting, val, valType string, tombstone bool, _ error) {
	tbl := systemschema.SettingsTable
	// First we need to decode the setting name field from the index key.
	{
		types := []*types.T{tbl.PublicColumns()[0].GetType()}
		nameRow := make([]rowenc.EncDatum, 1)
		_, matches, _, err := rowenc.DecodeIndexKey(d.codec, tbl, tbl.GetPrimaryIndex(), types, nameRow, nil, kv.Key)
		if err != nil {
			return "", "", "", false, errors.Wrap(err, "failed to decode key")
		}
		if !matches {
			return "", "", "", false, errors.Errorf("unexpected non-settings KV with settings prefix: %v", kv.Key)
		}
		if err := nameRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
			return "", "", "", false, err
		}
		setting = string(tree.MustBeDString(nameRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return setting, "", "", true, nil
	}

	// The rest of the columns are stored as a family, packed with diff-encoded
	// column IDs followed by their values.
	{
		// column valueType can be null (missing) so we default it to "s".
		valType = "s"
		bytes, err := kv.Value.GetTuple()
		if err != nil {
			return "", "", "", false, err
		}
		var colIDDiff uint32
		var lastColID descpb.ColumnID
		var res tree.Datum
		for len(bytes) > 0 {
			_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
			if err != nil {
				return "", "", "", false, err
			}
			colID := lastColID + descpb.ColumnID(colIDDiff)
			lastColID = colID
			if idx, ok := d.colIdxMap.Get(colID); ok {
				res, bytes, err = rowenc.DecodeTableValue(&d.alloc, tbl.PublicColumns()[idx].GetType(), bytes)
				if err != nil {
					return "", "", "", false, err
				}
				switch colID {
				case tbl.PublicColumns()[1].GetID(): // value
					val = string(tree.MustBeDString(res))
				case tbl.PublicColumns()[3].GetID(): // valueType
					valType = string(tree.MustBeDString(res))
				case tbl.PublicColumns()[2].GetID(): // lastUpdated
					// TODO(dt): we could decode just the len and then seek `bytes` past
					// it, without allocating/decoding the unused timestamp.
				default:
					return "", "", "", false, errors.Errorf("unknown column: %v", colID)
				}
			}
		}
	}
	return setting, val, valType, false, nil
}
