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
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// rowCodec encodes/decodes rows from the sql_instances table.
type rowCodec struct {
	codec   keys.SQLCodec
	columns []catalog.Column
	decoder valueside.Decoder
}

// MakeRowCodec makes a new rowCodec for the sql_instances table.
func makeRowCodec(codec keys.SQLCodec) rowCodec {
	columns := systemschema.SQLInstancesTable.PublicColumns()
	return rowCodec{
		codec:   codec,
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
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
	valueBuf, err = valueside.Encode(
		[]byte(nil), valueside.MakeColumnIDDelta(0, d.columns[1].GetID()), addrDatum, []byte(nil))
	if err != nil {
		return kv, err
	}
	sessionDatum := tree.NewDBytes(tree.DBytes(sessionID.UnsafeBytes()))
	sessionColDiff := valueside.MakeColumnIDDelta(d.columns[1].GetID(), d.columns[2].GetID())
	valueBuf, err = valueside.Encode(valueBuf, sessionColDiff, sessionDatum, []byte(nil))
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
	tombstone bool,
	_ error,
) {
	var alloc tree.DatumAlloc
	// First, decode the id field from the index key.
	{
		types := []*types.T{d.columns[0].GetType()}
		row := make([]rowenc.EncDatum, 1)
		_, _, err := rowenc.DecodeIndexKey(d.codec, types, row, nil, kv.Key)
		if err != nil {
			return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, false, errors.Wrap(err, "failed to decode key")
		}
		if err := row[0].EnsureDecoded(types[0], &alloc); err != nil {
			return base.SQLInstanceID(0), "", "", hlc.Timestamp{}, false, err
		}
		instanceID = base.SQLInstanceID(tree.MustBeDInt(row[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return instanceID, "", "", hlc.Timestamp{}, true, nil
	}
	timestamp = kv.Value.Timestamp
	// The rest of the columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return instanceID, "", "", hlc.Timestamp{}, false, err
	}

	datums, err := d.decoder.Decode(&alloc, bytes)
	if err != nil {
		return instanceID, "", "", hlc.Timestamp{}, false, err
	}

	if addrVal := datums[1]; addrVal != tree.DNull {
		addr = string(tree.MustBeDString(addrVal))
	}
	if sessionIDVal := datums[2]; sessionIDVal != tree.DNull {
		sessionID = sqlliveness.SessionID(tree.MustBeDBytes(sessionIDVal))
	}

	return instanceID, addr, sessionID, timestamp, false, nil
}

func makeTablePrefix(codec keys.SQLCodec, tableID descpb.ID) roachpb.Key {
	return codec.IndexPrefix(uint32(tableID), 1)
}

func makeInstanceKey(
	codec keys.SQLCodec, tableID descpb.ID, instanceID base.SQLInstanceID,
) roachpb.Key {
	return keys.MakeFamilyKey(encoding.EncodeVarintAscending(makeTablePrefix(codec, tableID), int64(instanceID)), 0)
}
