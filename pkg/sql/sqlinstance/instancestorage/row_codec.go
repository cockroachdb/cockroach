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
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// rowCodec encodes/decodes rows from the sql_instances table.
type rowCodec struct {
	codec   keys.SQLCodec
	columns []catalog.Column
	decoder valueside.Decoder
	tableID descpb.ID
}

// MakeRowCodec makes a new rowCodec for the sql_instances table.
func makeRowCodec(codec keys.SQLCodec, tableID descpb.ID) rowCodec {
	columns := systemschema.SQLInstancesTable.PublicColumns()
	return rowCodec{
		codec:   codec,
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
		tableID: tableID,
	}
}

// decodeRow converts the key and value into an instancerow. value may be nil
// or uninitialized. If it is, the fields stored in the value will be left with
// their default values.
func (d *rowCodec) decodeRow(key roachpb.Key, value *roachpb.Value) (instancerow, error) {
	instanceID, err := d.decodeKey(key)
	if err != nil {
		return instancerow{}, err
	}

	r := instancerow{
		instanceID: instanceID,
	}
	if value == nil || !value.IsPresent() {
		return r, nil
	}

	r.addr, r.sessionID, r.locality, r.timestamp, err = d.decodeValue(*value)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "failed to decode value for: %v", key)
	}

	return r, nil
}

// makeIndexPrefix returns a roachpb.Key that is the prefix for all encoded
// keys and can be used to scan the entire table.
func (d *rowCodec) makeIndexPrefix() roachpb.Key {
	return d.codec.IndexPrefix(uint32(d.tableID), 1)
}

// encodeKey converts the instanceID into an encoded key for the table.
func (d *rowCodec) encodeKey(instanceID base.SQLInstanceID) roachpb.Key {
	key := d.makeIndexPrefix()
	key = encoding.EncodeVarintAscending(key, int64(instanceID))
	return keys.MakeFamilyKey(key, 0)
}

// encodeValue encodes the sql_instance columns into a kv value.
func (d *rowCodec) encodeValue(
	addr string, sessionID sqlliveness.SessionID, locality roachpb.Locality,
) (*roachpb.Value, error) {
	var valueBuf []byte

	addrDatum := tree.DNull
	if addr != "" {
		addrDatum = tree.NewDString(addr)
	}
	valueBuf, err := valueside.Encode(
		[]byte(nil), valueside.MakeColumnIDDelta(0, d.columns[1].GetID()), addrDatum, []byte(nil))
	if err != nil {
		return nil, err
	}

	sessionDatum := tree.DNull
	if len(sessionID) > 0 {
		sessionDatum = tree.NewDBytes(tree.DBytes(sessionID.UnsafeBytes()))
	}
	sessionColDiff := valueside.MakeColumnIDDelta(d.columns[1].GetID(), d.columns[2].GetID())
	valueBuf, err = valueside.Encode(valueBuf, sessionColDiff, sessionDatum, []byte(nil))
	if err != nil {
		return nil, err
	}

	// Preserve the ordering of locality.Tiers, even though we convert it to json.
	localityDatum := tree.DNull
	if len(locality.Tiers) > 0 {
		builder := json.NewObjectBuilder(1)
		builder.Add("Tiers", json.FromString(locality.String()))
		localityDatum = tree.NewDJSON(builder.Build())
	}
	localityColDiff := valueside.MakeColumnIDDelta(d.columns[2].GetID(), d.columns[3].GetID())
	valueBuf, err = valueside.Encode(valueBuf, localityColDiff, localityDatum, []byte(nil))
	if err != nil {
		return nil, err
	}

	v := &roachpb.Value{}
	v.SetTuple(valueBuf)
	return v, nil
}

// decodeKey decodes a sql_instance key into its logical components.
func (d *rowCodec) decodeKey(key roachpb.Key) (base.SQLInstanceID, error) {
	types := []*types.T{d.columns[0].GetType()}
	row := make([]rowenc.EncDatum, 1)
	_, _, err := rowenc.DecodeIndexKey(d.codec, types, row, nil, key)
	if err != nil {
		return base.SQLInstanceID(0), errors.Wrap(err, "failed to decode key")
	}
	var alloc tree.DatumAlloc
	if err := row[0].EnsureDecoded(types[0], &alloc); err != nil {
		return base.SQLInstanceID(0), err
	}
	return base.SQLInstanceID(tree.MustBeDInt(row[0].Datum)), nil
}

// decodeRow decodes a row of the sql_instances table.
func (d *rowCodec) decodeValue(
	value roachpb.Value,
) (
	addr string,
	sessionID sqlliveness.SessionID,
	locality roachpb.Locality,
	timestamp hlc.Timestamp,
	_ error,
) {
	// The rest of the columns are stored as a family.
	bytes, err := value.GetTuple()
	if err != nil {
		return "", "", roachpb.Locality{}, hlc.Timestamp{}, err
	}

	datums, err := d.decoder.Decode(&tree.DatumAlloc{}, bytes)
	if err != nil {
		return "", "", roachpb.Locality{}, hlc.Timestamp{}, err
	}

	if addrVal := datums[1]; addrVal != tree.DNull {
		addr = string(tree.MustBeDString(addrVal))
	}
	if sessionIDVal := datums[2]; sessionIDVal != tree.DNull {
		sessionID = sqlliveness.SessionID(tree.MustBeDBytes(sessionIDVal))
	}
	if localityVal := datums[3]; localityVal != tree.DNull {
		localityJ := tree.MustBeDJSON(localityVal)
		v, err := localityJ.FetchValKey("Tiers")
		if err != nil {
			return "", "", roachpb.Locality{}, hlc.Timestamp{}, errors.Wrap(err, "failed to find Tiers attribute in locality")
		}
		if v != nil {
			vStr, err := v.AsText()
			if err != nil {
				return "", "", roachpb.Locality{}, hlc.Timestamp{}, err
			}
			if len(*vStr) > 0 {
				if err := locality.Set(*vStr); err != nil {
					return "", "", roachpb.Locality{}, hlc.Timestamp{}, err
				}
			}
		}
	}

	return addr, sessionID, locality, value.Timestamp, nil
}
