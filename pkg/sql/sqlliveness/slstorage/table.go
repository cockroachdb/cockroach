// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Table is an interface to system.sqlliveness table. The api was created to
// encapsulate the migration to a RBR sqlliveness table.
//
// # Migrating to Rbr
// When the binary is upgraded, it creates a session that encodes the region in
// the session id. sessions with an encoded region are dual written to the RBT
// and RBR index.
//
// When an upgraded binary encounters a new session id, it consults the RBR
// index to see if it is valid. If a legacy binary encounters an RBR session,
// it treats the id as a black box and consults the RBT index.
//
// Before the RbrSqlliveness version gate is flipped the sqlliveness
// descriptor is upgraded to the new format. This is safe because the version
// can only advance after all servers are running the new binary and that
// implies all legacy sessions are inactive.
//
// After the version gate is flipped, servers stop dual writing. The legacy RBT
// index is eventually cleaned up by deleteExpiredSessions.
type Table struct {
	rbtIndex roachpb.Key
	rbrIndex roachpb.Key
	settings *cluster.Settings
}

// MakeTable constructs a typed interface to the system.sqlliveness table.
func MakeTable(settings *cluster.Settings, codec keys.SQLCodec, tableID catid.DescID) Table {
	return Table{
		rbtIndex: codec.IndexPrefix(uint32(tableID), 1),
		rbrIndex: codec.IndexPrefix(uint32(tableID), 2),
		settings: settings,
	}
}

// MakeTestTable constructs a table with specific index ids. This is needed to
// test the interface with a RBR table created via sql, since the index id will
// be wrong.
func MakeTestTable(
	settings *cluster.Settings, codec keys.SQLCodec, tableID catid.DescID, rbrIndex, rbtIndex uint32,
) Table {
	return Table{
		rbtIndex: codec.IndexPrefix(uint32(tableID), rbtIndex),
		rbrIndex: codec.IndexPrefix(uint32(tableID), rbrIndex),
		settings: settings,
	}
}

// GetExpiration retrieves the expiration for the session if the session
// exists.
func (t *Table) GetExpiration(
	ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID,
) (exists bool, expiration hlc.Timestamp, err error) {
	region, uuid, err := UnsafeDecodeSessionID(sid)
	if err != nil {
		return false, hlc.Timestamp{}, err
	}

	var key roachpb.Key
	if 0 < len(region) {
		key = t.makeRbrSessionKey(region, uuid)
	} else {
		key = t.makeRbtSessionKey(sid)
	}
	kv, err := txn.Get(ctx, key)
	if err != nil {
		return false, hlc.Timestamp{}, err
	}
	if kv.Value == nil {
		return false, hlc.Timestamp{}, err
	}
	expiration, err = decodeValue(kv)
	if err != nil {
		return false, hlc.Timestamp{}, errors.Wrapf(err, "failed to decode expiration for %s", redact.SafeString(sid.String()))
	}
	return true, expiration, nil
}

// SetExpiration upserts the session with the given expiration.
func (t *Table) SetExpiration(
	ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID, expiration hlc.Timestamp,
) error {
	region, uuid, err := UnsafeDecodeSessionID(sid)
	if err != nil {
		return err
	}
	if len(region) == 0 && t.settings.Version.IsActive(ctx, clusterversion.RbrSqlliveness) {
		// SetExpiration is only used to create and heartbeat a node's own
		// session. It should never be called with a session that does not
		// include a region.
		return errors.Newf("attempted to set the expiration of a legacy session: '%s'", sid.String())
	}

	batch := txn.NewBatch()
	if 0 < len(region) {
		batch.Put(t.makeRbrSessionKey(region, uuid), encodeValue(expiration))
	}
	if !t.settings.Version.IsActive(ctx, clusterversion.RbrSqlliveness) {
		batch.Put(t.makeRbtSessionKey(sid), encodeValue(expiration))
	}
	return txn.Run(ctx, batch)
}

// Delete deletes the session.
func (t *Table) Delete(ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID) error {
	region, uuid, err := UnsafeDecodeSessionID(sid)
	if err != nil {
		return err
	}
	batch := txn.NewBatch()
	if 0 < len(region) {
		batch.Del(t.makeRbrSessionKey(region, uuid))
	}
	if len(region) == 0 || !t.settings.Version.IsActive(ctx, clusterversion.RbrSqlliveness) {
		batch.Del(t.makeRbtSessionKey(sid))
	}
	return txn.Run(ctx, batch)
}

func (t *Table) makeRbrSessionKey(region []byte, uuid []byte) roachpb.Key {
	key := t.rbrIndex.Clone()
	key = encoding.EncodeBytesAscending(key, region)
	key = encoding.EncodeBytesAscending(key, uuid)
	key = keys.MakeFamilyKey(key, 0)
	return key
}

func (t *Table) makeRbtSessionKey(id sqlliveness.SessionID) roachpb.Key {
	return keys.MakeFamilyKey(encoding.EncodeBytesAscending(t.rbtIndex.Clone(), id.UnsafeBytes()), 0)
}

func decodeValue(kv kv.KeyValue) (hlc.Timestamp, error) {
	tup, err := kv.Value.GetTuple()
	if err != nil {
		return hlc.Timestamp{},
			errors.Wrapf(err, "failed to decode tuple from key %v", kv.Key)
	}
	_, dec, err := encoding.DecodeDecimalValue(tup)
	if err != nil {
		return hlc.Timestamp{},
			errors.Wrapf(err, "failed to decode decimal from key %v", kv.Key)
	}
	return hlc.DecimalToHLC(&dec)
}

func encodeValue(expiration hlc.Timestamp) *roachpb.Value {
	var v roachpb.Value
	dec := eval.TimestampToDecimal(expiration)
	v.SetTuple(encoding.EncodeDecimalValue(nil, 2, &dec))
	return &v
}
