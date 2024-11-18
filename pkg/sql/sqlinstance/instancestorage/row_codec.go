// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	codec               keys.SQLCodec
	columns             []catalog.Column
	decoder             valueside.Decoder
	tableID             descpb.ID
	valueColumnIDs      [numValueColumns]descpb.ColumnID
	valueColumnOrdinals [numValueColumns]int
}

type valueColumnIdx int

const numValueColumns = 6

const (
	addrColumnIdx valueColumnIdx = iota
	sessionIDColumnIdx
	localityColumnIdx
	sqlAddrColumnIdx
	binaryVersionColumnIdx
	isDrainingColumnIdx

	// Ensure we have the right number of value columns.
	_ uint = iota - numValueColumns
	_ uint = numValueColumns - (iota - 1)
)

var valueColumnNames = [numValueColumns]string{
	addrColumnIdx:          "addr",
	sessionIDColumnIdx:     "session_id",
	localityColumnIdx:      "locality",
	sqlAddrColumnIdx:       "sql_addr",
	binaryVersionColumnIdx: "binary_version",
	isDrainingColumnIdx:    "is_draining",
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
func makeRowCodec(
	codec keys.SQLCodec, table catalog.TableDescriptor, useRegionalByRow bool,
) rowCodec {
	columns := table.PublicColumns()
	var key keyCodec
	if useRegionalByRow {
		key = &rbrKeyCodec{
			indexPrefix: codec.IndexPrefix(uint32(table.GetID()), uint32(table.GetPrimaryIndexID())),
		}
	} else {
		const rbtIndexID = 1
		key = &rbtKeyCodec{
			indexPrefix: codec.IndexPrefix(uint32(table.GetID()), uint32(rbtIndexID)),
		}
	}
	rc := rowCodec{
		keyCodec: key,
		codec:    codec,
		columns:  columns,
		decoder:  valueside.MakeDecoder(columns),
		tableID:  table.GetID(),
	}
	for i := 0; i < numValueColumns; i++ {
		col := catalog.FindColumnByName(table, valueColumnNames[i])
		rc.valueColumnIDs[i] = col.GetID()
		rc.valueColumnOrdinals[i] = col.Ordinal()
	}
	return rc
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

	r.rpcAddr, r.sqlAddr, r.sessionID, r.locality, r.binaryVersion, r.isDraining, r.timestamp, err = d.decodeValue(*value)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "failed to decode value for: %v", key)
	}

	return r, nil
}

// encodeValue encodes the sql_instance columns into a kv value.
func (d *rowCodec) encodeValue(
	rpcAddr string,
	sqlAddr string,
	sessionID sqlliveness.SessionID,
	locality roachpb.Locality,
	binaryVersion roachpb.Version,
	encodeIsDraining bool,
	isDraining bool,
) (*roachpb.Value, error) {
	var valueBuf []byte
	columnsToEncode := []func() tree.Datum{
		addrColumnIdx: func() tree.Datum {
			if rpcAddr == "" {
				return tree.DNull
			}
			return tree.NewDString(rpcAddr)
		},
		sessionIDColumnIdx: func() tree.Datum {
			if len(sessionID) == 0 {
				return tree.DNull
			}
			return tree.NewDBytes(tree.DBytes(sessionID.UnsafeBytes()))
		},
		localityColumnIdx: func() tree.Datum {
			if len(locality.Tiers) == 0 {
				return tree.DNull
			}
			builder := json.NewObjectBuilder(1)
			builder.Add("Tiers", json.FromString(locality.String()))
			return tree.NewDJSON(builder.Build())
		},
		sqlAddrColumnIdx: func() tree.Datum {
			if sqlAddr == "" {
				return tree.DNull
			}
			return tree.NewDString(sqlAddr)
		},
		binaryVersionColumnIdx: func() tree.Datum {
			return tree.NewDString(clusterversion.StringForPersistence(binaryVersion))
		},
	}

	if encodeIsDraining {
		columnsToEncode = append(columnsToEncode, func() tree.Datum {
			return tree.MakeDBool(tree.DBool(isDraining))
		})
	}

	for i, f := range columnsToEncode {
		var err error
		var prev descpb.ColumnID
		if i > 0 {
			prev = d.valueColumnIDs[i-1]
		}
		delta := valueside.MakeColumnIDDelta(prev, d.valueColumnIDs[i])
		if valueBuf, err = valueside.Encode(valueBuf, delta, f()); err != nil {
			return nil, err
		}
	}

	v := &roachpb.Value{}
	v.SetTuple(valueBuf)
	return v, nil
}

func (d *rowCodec) encodeAvailableValue(encodeIsDraining bool) (*roachpb.Value, error) {
	value, err := d.encodeValue("", "", sqlliveness.SessionID([]byte{}),
		roachpb.Locality{}, roachpb.Version{}, encodeIsDraining, false)
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
	binaryVersion roachpb.Version,
	isDraining bool,
	timestamp hlc.Timestamp,
	_ error,
) {
	// The rest of the columns are stored as a single family.
	bytes, err := value.GetTuple()
	if err != nil {
		return "", "", "", roachpb.Locality{}, roachpb.Version{}, false, hlc.Timestamp{}, err
	}
	datums, err := d.decoder.Decode(&tree.DatumAlloc{}, bytes)
	if err != nil {
		return "", "", "", roachpb.Locality{}, roachpb.Version{}, false, hlc.Timestamp{}, err
	}
	for i, f := range [numValueColumns]func(datum tree.Datum) error{
		addrColumnIdx: func(datum tree.Datum) error {
			if datum != tree.DNull {
				rpcAddr = string(tree.MustBeDString(datum))
			}
			return nil
		},
		sessionIDColumnIdx: func(datum tree.Datum) error {
			if datum != tree.DNull {
				sessionID = sqlliveness.SessionID(tree.MustBeDBytes(datum))
			}
			return nil
		},
		localityColumnIdx: func(datum tree.Datum) error {
			if datum == tree.DNull {
				return nil
			}
			localityJ := tree.MustBeDJSON(datum)
			v, err := localityJ.FetchValKey("Tiers")
			if err != nil {
				return errors.Wrap(err, "failed to find Tiers attribute in locality")
			}
			if v != nil {
				vStr, err := v.AsText()
				if err != nil {
					return err
				}
				if len(*vStr) > 0 {
					if err := locality.Set(*vStr); err != nil {
						return err
					}
				}
			}
			return nil
		},
		sqlAddrColumnIdx: func(datum tree.Datum) error {
			if datum != tree.DNull {
				sqlAddr = string(tree.MustBeDString(datum))
			} else {
				sqlAddr = rpcAddr
			}
			return nil
		},
		binaryVersionColumnIdx: func(datum tree.Datum) error {
			if datum != tree.DNull {
				var err error
				binaryVersion, err = roachpb.ParseVersion(string(tree.MustBeDString(datum)))
				return err
			}
			return nil
		},
		isDrainingColumnIdx: func(datum tree.Datum) error {
			if datum == tree.DNull {
				isDraining = false
			} else {
				isDraining = bool(tree.MustBeDBool(datum))
			}
			return nil
		},
	} {
		ord := d.valueColumnOrdinals[i]
		// Deal with the fact that new columns may not yet have been added.
		datum := tree.DNull
		if ord < len(datums) {
			datum = datums[ord]
		}
		if err := f(datum); err != nil {
			return "", "", "", roachpb.Locality{}, roachpb.Version{}, false, hlc.Timestamp{}, err
		}
	}
	return rpcAddr, sqlAddr, sessionID, locality, binaryVersion, isDraining, value.Timestamp, nil
}
