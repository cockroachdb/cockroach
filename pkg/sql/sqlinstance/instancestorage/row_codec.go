// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// rowCodec encodes/decodes rows from the sql_instances table.
type rowCodec struct {
	codec     keys.SQLCodec
	colIdxMap catalog.TableColMap
	columns   []catalog.Column
}

// MakeRowCodec makes a new rowCodec for the sql_instances table.
func makeRowCodec(codec keys.SQLCodec) rowCodec {
	return rowCodec{
		codec: codec,
		colIdxMap: row.ColIDtoRowIndexFromCols(
			systemschema.SQLInstancesTable.PublicColumns(),
		),
		columns: systemschema.SQLInstancesTable.PublicColumns(),
	}
}

// encodeRow encodes a row of the sql_instances table.
func (d *rowCodec) encodeRow(
	instanceID base.SQLInstanceID,
	addr string,
	sessionID sqlliveness.SessionID,
	codec keys.SQLCodec,
	tableID descpb.ID,
) (kv kv.KeyValue, err error) {
	addrDatum := tree.NewDString(addr)
	var valueBuf []byte
	valueBuf, err = rowenc.EncodeTableValue(
		[]byte(nil), d.columns[1].GetID(), addrDatum, []byte(nil))
	if err != nil {
		return kv, err
	}
	sessionDatum := tree.NewDBytes(tree.DBytes(sessionID.UnsafeBytes()))
	sessionColDiff := d.columns[2].GetID() - d.columns[1].GetID()
	valueBuf, err = rowenc.EncodeTableValue(valueBuf, sessionColDiff, sessionDatum, []byte(nil))
	if err != nil {
		return kv, err
	}
	var v roachpb.Value
	v.SetTuple(valueBuf)
	kv.Value = &v
	kv.Key = makeInstanceKey(codec, tableID, instanceID)
	return kv, nil
}

// decodeRow decodes a row of the sql_instances table.
func (d *rowCodec) decodeRow(
	kv kv.KeyValue,
) (
	instanceID base.SQLInstanceID,
	addr string,
	sessionID sqlliveness.SessionID,
	timestamp hlc.Timestamp,
	_ error,
) {
	tbl := systemschema.SQLInstancesTable
	var alloc rowenc.DatumAlloc
	// First, decode the id field from the index key.
	{
		types := []*types.T{tbl.PublicColumns()[0].GetType()}
		row := make([]rowenc.EncDatum, 1)
		_, _, _, err := rowenc.DecodeIndexKey(d.codec, tbl, tbl.GetPrimaryIndex(), types, row, nil, kv.Key)
		if err != nil {
			return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, errors.Wrap(err, "failed to decode key")
		}
		if err := row[0].EnsureDecoded(types[0], &alloc); err != nil {
			return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, err
		}
		instanceID = base.SQLInstanceID(tree.MustBeDInt(row[0].Datum))
	}

	// The rest of the columns are stored as a family, packed with diff-encoded
	// column IDs followed by their values.
	{
		bytes, err := kv.Value.GetTuple()
		timestamp = kv.Value.Timestamp
		if err != nil {
			return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, err
		}
		var colIDDiff uint32
		var lastColID descpb.ColumnID
		var res tree.Datum
		for len(bytes) > 0 {
			_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
			if err != nil {
				return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, err
			}
			colID := lastColID + descpb.ColumnID(colIDDiff)
			lastColID = colID
			if idx, ok := d.colIdxMap.Get(colID); ok {
				res, bytes, err = rowenc.DecodeTableValue(&alloc, tbl.PublicColumns()[idx].GetType(), bytes)
				if err != nil {
					return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, err
				}
				switch colID {
				case tbl.PublicColumns()[1].GetID(): // addr
					if res != tree.DNull {
						addr = string(tree.MustBeDString(res))
					}
				case tbl.PublicColumns()[2].GetID(): // sessionID
					if res != tree.DNull {
						sessionID = sqlliveness.SessionID(tree.MustBeDBytes(res))
					}
				default:
					return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, errors.Errorf("unknown column: %v", colID)
				}
			}
		}
	}
	return instanceID, addr, sessionID, timestamp, nil
}

func makeTablePrefix(codec keys.SQLCodec, tableID descpb.ID) roachpb.Key {
	return codec.IndexPrefix(uint32(tableID), 1)
}

func makeInstanceKey(
	codec keys.SQLCodec, tableID descpb.ID, instanceID base.SQLInstanceID,
) roachpb.Key {
	return keys.MakeFamilyKey(encoding.EncodeVarintAscending(makeTablePrefix(codec, tableID), int64(instanceID)), 0)
}
