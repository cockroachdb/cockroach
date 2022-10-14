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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Table is an interface to system.sqlliveness table.
type Table struct {
	rbtIndex roachpb.Key
}

// MakeTable constructs a typed interface to the system.sqlliveness table.
func MakeTable(codec keys.SQLCodec, tableID catid.DescID) Table {
	return Table{
		rbtIndex: codec.IndexPrefix(uint32(tableID), 1),
	}
}

// GetExpiration retrieves the expiration for the session if the session
// exists.
func (t *Table) GetExpiration(
	ctx context.Context, txn *kv.Txn, sid sqlliveness.SessionID,
) (exists bool, expiration hlc.Timestamp, err error) {
	k := t.makeSessionKey(sid)
	kv, err := txn.Get(ctx, k)
	if err != nil {
		return false, hlc.Timestamp{}, err
	}
	// The session is not alive.
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
	ctx context.Context, txn *kv.Txn, id sqlliveness.SessionID, expiration hlc.Timestamp,
) error {
	k := t.makeSessionKey(id)
	v := encodeValue(expiration)
	return txn.Put(ctx, k, &v)
}

// Delete deletes the session.
func (t *Table) Delete(ctx context.Context, txn *kv.Txn, id sqlliveness.SessionID) error {
	key := t.makeSessionKey(id)
	_, err := txn.Del(ctx, key)
	return err
}

func (t *Table) makeSessionKey(id sqlliveness.SessionID) roachpb.Key {
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

func encodeValue(expiration hlc.Timestamp) roachpb.Value {
	var v roachpb.Value
	dec := eval.TimestampToDecimal(expiration)
	v.SetTuple(encoding.EncodeDecimalValue(nil, 2, &dec))
	return v
}
