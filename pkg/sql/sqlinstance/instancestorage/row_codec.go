// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// RowCodec encodes/decodes rows from the sql_instances table.
type RowCodec struct {
	codec     keys.SQLCodec
	alloc     rowenc.DatumAlloc
	colIdxMap catalog.TableColMap
	columns   []catalog.Column
}

// MakeRowCodec makes a new RowCodec for the sql_instances table.
func makeRowCodec(codec keys.SQLCodec) RowCodec {
	return RowCodec{
		codec: codec,
		colIdxMap: row.ColIDtoRowIndexFromCols(
			systemschema.SQLInstancesTable.PublicColumns(),
		),
		columns: systemschema.SQLInstancesTable.PublicColumns(),
	}
}

// encodeRow encodes a row of the sql_instances table.
func (d *RowCodec) encodeRow(i *instance) (v roachpb.Value, err error) {
	addrDatum := tree.NewDString(i.httpAddr)
	var valueBuf []byte
	valueBuf, err = rowenc.EncodeTableValue(
		[]byte(nil), d.columns[1].GetID(), addrDatum, []byte(nil))
	if err != nil {
		return v, err
	}
	sessionDatum := tree.NewDBytes(tree.DBytes(i.sessionID.UnsafeBytes()))
	sessionColDiff := d.columns[2].GetID() - d.columns[1].GetID()
	valueBuf, err = rowenc.EncodeTableValue(valueBuf, sessionColDiff, sessionDatum, []byte(nil))
	if err != nil {
		return v, err
	}
	v.SetTuple(valueBuf)
	return v, nil
}

// encodeEmptyRow sets all columns other than instance id to null
// to indicate that the instance id can be reused.
func (d *RowCodec) encodeEmptyRow() (v roachpb.Value, err error) {
	var valueBuf []byte
	valueBuf, err = rowenc.EncodeTableValue(
		[]byte(nil), d.columns[1].GetID(), tree.DNull, []byte(nil))
	if err != nil {
		return v, err
	}
	sessionColDiff := d.columns[2].GetID() - d.columns[1].GetID()
	valueBuf, err = rowenc.EncodeTableValue(valueBuf, sessionColDiff, tree.DNull, []byte(nil))
	if err != nil {
		return v, err
	}
	v.SetTuple(valueBuf)
	return v, err
}

// decodeRow decodes a row of the sql_instances table.
func (d *RowCodec) decodeRow(kv kv.KeyValue) (*instance, error) {
	tbl := systemschema.SQLInstancesTable
	var i instance
	// First, decode the id field from the index key.
	{
		types := []*types.T{tbl.PublicColumns()[0].GetType()}
		row := make([]rowenc.EncDatum, 1)
		_, _, _, err := rowenc.DecodeIndexKey(d.codec, tbl, tbl.GetPrimaryIndex(), types, row, nil, kv.Key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode key")
		}
		if err := row[0].EnsureDecoded(types[0], &d.alloc); err != nil {
			return nil, err
		}
		i.id = base.SQLInstanceID(tree.MustBeDInt(row[0].Datum))
	}

	// The rest of the columns are stored as a family, packed with diff-encoded
	// column IDs followed by their values.
	{
		bytes, err := kv.Value.GetTuple()
		if err != nil {
			return nil, err
		}
		var colIDDiff uint32
		var lastColID descpb.ColumnID
		var res tree.Datum
		for len(bytes) > 0 {
			_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
			if err != nil {
				return nil, err
			}
			colID := lastColID + descpb.ColumnID(colIDDiff)
			lastColID = colID
			if idx, ok := d.colIdxMap.Get(colID); ok {
				res, bytes, err = rowenc.DecodeTableValue(&d.alloc, tbl.PublicColumns()[idx].GetType(), bytes)
				if err != nil {
					return nil, err
				}
				switch colID {
				case tbl.PublicColumns()[1].GetID(): // httpAddr
					if res != tree.DNull {
						i.httpAddr = string(tree.MustBeDString(res))
					}
				case tbl.PublicColumns()[2].GetID(): // sessionID
					if res != tree.DNull {
						i.sessionID = sqlliveness.SessionID(tree.MustBeDBytes(res))
					}
				default:
					return nil, errors.Errorf("unknown column: %v", colID)
				}
			}
		}
	}
	return &i, nil
}
