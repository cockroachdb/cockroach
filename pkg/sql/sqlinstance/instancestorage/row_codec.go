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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type keyCodec interface {
	// makeIndexPrefix returns a roachpb.Key that is the prefix for all encoded
	// keys and can be used to scan the entire table.
	makeIndexPrefix() roachpb.Key

	// makeRegionPrefix returns a roachpb.Key that is the prefix for all keys
	// in the region and can be used to scan the region.
	makeRegionPrefix(region []byte) roachpb.Key

	// encodeKey makes a key for the sql_instance table.
	encodeKey(region []byte, id base.SQLInstanceID) roachpb.Key

	// decodeKey decodes a sql_instance table key into its logical components.
	decodeKey(key roachpb.Key) (region []byte, id base.SQLInstanceID, err error)
}

// rowCodec encodes/decodes rows from the sql_instances table.
type rowCodec struct {
	keyCodec
	codec   keys.SQLCodec
	columns []catalog.Column
	decoder valueside.Decoder
	tableID descpb.ID
}

// rbrKeyCodec is used by the regional by row compatible sql_instances index format.
type rbrKeyCodec struct {
	indexPrefix roachpb.Key
}

func (c rbrKeyCodec) makeIndexPrefix() roachpb.Key {
	return c.indexPrefix.Clone()
}

func (c rbrKeyCodec) makeRegionPrefix(region []byte) roachpb.Key {
	return encoding.EncodeBytesAscending(c.indexPrefix.Clone(), region)
}

func (c rbrKeyCodec) encodeKey(region []byte, instanceID base.SQLInstanceID) roachpb.Key {
	key := c.makeRegionPrefix(region)
	key = encoding.EncodeVarintAscending(key, int64(instanceID))
	key = keys.MakeFamilyKey(key, 0)
	return key
}

func (c rbrKeyCodec) decodeKey(key roachpb.Key) (region []byte, id base.SQLInstanceID, err error) {
	if !bytes.HasPrefix(key, c.indexPrefix) {
		return nil, 0, errors.Newf("sql_instances table key has an invalid prefix: %v", key)
	}
	rem := key[len(c.indexPrefix):]

	rem, region, err = encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return nil, 0, errors.Newf("failed to decode region from sql_instances key: %v", key)
	}

	_, rawID, err := encoding.DecodeVarintAscending(rem)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to decode sql instance id from key: %v", key)
	}

	return region, base.SQLInstanceID(rawID), nil
}

// rbtKeyCodec is used by the legacy sql_instances index format.
type rbtKeyCodec struct {
	indexPrefix roachpb.Key
}

func (c rbtKeyCodec) makeIndexPrefix() roachpb.Key {
	return c.indexPrefix.Clone()
}

func (c rbtKeyCodec) makeRegionPrefix(region []byte) roachpb.Key {
	return c.indexPrefix.Clone()
}

func (c *rbtKeyCodec) encodeKey(_ []byte, instanceID base.SQLInstanceID) roachpb.Key {
	key := c.indexPrefix.Clone()
	key = encoding.EncodeVarintAscending(key, int64(instanceID))
	return keys.MakeFamilyKey(key, 0)
}

func (c *rbtKeyCodec) decodeKey(key roachpb.Key) (region []byte, id base.SQLInstanceID, err error) {
	if !bytes.HasPrefix(key, c.indexPrefix) {
		return nil, 0, errors.Newf("sql_instances table key has an invalid prefix: %v", key)
	}
	rem := key[len(c.indexPrefix):]

	_, rawID, err := encoding.DecodeVarintAscending(rem)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to decode sql instance id from key: %v", key)
	}

	return enum.One, base.SQLInstanceID(rawID), nil
}

// MakeRowCodec makes a new rowCodec for the sql_instances table.
func makeRowCodec(codec keys.SQLCodec, tableID descpb.ID) rowCodec {
	columns := systemschema.SQLInstancesTable().PublicColumns()

	var key keyCodec
	if systemschema.TestSupportMultiRegion() {
		key = &rbrKeyCodec{
			indexPrefix: codec.IndexPrefix(uint32(tableID), 2),
		}
	} else {
		key = &rbtKeyCodec{
			indexPrefix: codec.IndexPrefix(uint32(tableID), 1),
		}
	}

	return rowCodec{
		keyCodec: key,
		codec:    codec,
		columns:  columns,
		decoder:  valueside.MakeDecoder(columns),
		tableID:  tableID,
	}
}

// decodeRow converts the key and value into an instancerow. value may be nil
// or uninitialized. If it is, the fields stored in the value will be left with
// their default values.
func (d *rowCodec) decodeRow(key roachpb.Key, value *roachpb.Value) (instancerow, error) {
	region, instanceID, err := d.decodeKey(key)
	if err != nil {
		return instancerow{}, err
	}

	r := instancerow{
		region:     region,
		instanceID: instanceID,
	}
	if !value.IsPresent() {
		return r, nil
	}

	r.rpcAddr, r.sqlAddr, r.sessionID, r.locality, r.timestamp, err = d.decodeValue(*value)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "failed to decode value for: %v", key)
	}

	return r, nil
}

// encodeValue encodes the sql_instance columns into a kv value.
func (d *rowCodec) encodeValue(
	rpcAddr string, sqlAddr string, sessionID sqlliveness.SessionID, locality roachpb.Locality,
) (*roachpb.Value, error) {
	var valueBuf []byte

	rpcAddrDatum := tree.DNull
	if rpcAddr != "" {
		rpcAddrDatum = tree.NewDString(rpcAddr)
	}
	sqlAddrDatum := tree.DNull
	if sqlAddr != "" {
		sqlAddrDatum = tree.NewDString(sqlAddr)
	}

	// Ordering of the values below needs to remain stable for backwards
	// compatibility. New values should be appended to the end.
	valueBuf, err := valueside.Encode(
		[]byte(nil), valueside.MakeColumnIDDelta(0, d.columns[1].GetID()), rpcAddrDatum, []byte(nil))
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

	valueBuf, err = valueside.Encode(valueBuf,
		valueside.MakeColumnIDDelta(d.columns[3].GetID(), d.columns[4].GetID()), sqlAddrDatum, []byte(nil))
	if err != nil {
		return nil, err
	}

	v := &roachpb.Value{}
	v.SetTuple(valueBuf)
	return v, nil
}

func (d *rowCodec) encodeAvailableValue() (*roachpb.Value, error) {
	value, err := d.encodeValue("", "", sqlliveness.SessionID([]byte{}), roachpb.Locality{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode available sql_instances value")
	}
	return value, nil
}

// decodeRow decodes a row of the sql_instances table.
func (d *rowCodec) decodeValue(
	value roachpb.Value,
) (
	rpcAddr string,
	sqlAddr string,
	sessionID sqlliveness.SessionID,
	locality roachpb.Locality,
	timestamp hlc.Timestamp,
	_ error,
) {
	// The rest of the columns are stored as a family.
	bytes, err := value.GetTuple()
	if err != nil {
		return "", "", "", roachpb.Locality{}, hlc.Timestamp{}, err
	}

	datums, err := d.decoder.Decode(&tree.DatumAlloc{}, bytes)
	if err != nil {
		return "", "", "", roachpb.Locality{}, hlc.Timestamp{}, err
	}

	if addrVal := datums[1]; addrVal != tree.DNull {
		rpcAddr = string(tree.MustBeDString(addrVal))
	}
	if len(datums) == 5 {
		if sqlAddrVal := datums[4]; sqlAddrVal != tree.DNull {
			sqlAddr = string(tree.MustBeDString(sqlAddrVal))
		} else {
			// Backwards compatible with single-address version.
			sqlAddr = rpcAddr
		}
	} else {
		// Backwards compatible with single-address version.
		sqlAddr = rpcAddr
	}
	if sessionIDVal := datums[2]; sessionIDVal != tree.DNull {
		sessionID = sqlliveness.SessionID(tree.MustBeDBytes(sessionIDVal))
	}
	if localityVal := datums[3]; localityVal != tree.DNull {
		localityJ := tree.MustBeDJSON(localityVal)
		v, err := localityJ.FetchValKey("Tiers")
		if err != nil {
			return "", "", "", roachpb.Locality{}, hlc.Timestamp{}, errors.Wrap(err, "failed to find Tiers attribute in locality")
		}
		if v != nil {
			vStr, err := v.AsText()
			if err != nil {
				return "", "", "", roachpb.Locality{}, hlc.Timestamp{}, err
			}
			if len(*vStr) > 0 {
				if err := locality.Set(*vStr); err != nil {
					return "", "", "", roachpb.Locality{}, hlc.Timestamp{}, err
				}
			}
		}
	}

	return rpcAddr, sqlAddr, sessionID, locality, value.Timestamp, nil
}
