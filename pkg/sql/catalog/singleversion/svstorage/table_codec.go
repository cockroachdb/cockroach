// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svstorage

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

func makeRows(
	a Action, descriptors catalog.DescriptorIDSet, sessionID sqlliveness.SessionID,
) []Row {
	rows := make([]Row, 0, descriptors.Len())
	descriptors.ForEach(func(id descpb.ID) {
		rows = append(rows, Row{
			Action:     a,
			Descriptor: id,
			Session:    sessionID,
		})
	})
	return rows
}

// ActionIDsToRows constructs rows corresponding to prefixes row entries which
// share the specified action and represent each of the ids in descriptors.
func ActionIDsToRows(a Action, descriptors catalog.DescriptorIDSet) []Row {
	return makeRows(a, descriptors, "")
}

// Action corresponds to a prefix of the single_version_descriptor_lease table.
type Action int

//go:generate stringer --type Action --linecomment

func (a Action) validate() error {
	if int(a) <= 0 || int(a) >= numActions {
		return errors.AssertionFailedf("unknown Action %d", a)
	}
	return nil
}

const (
	_ Action = iota

	// Notify outstanding leases to release.
	Notify // notify

	// Lease marks existing lease keys
	Lease // lease

	// Lock marks the key prefix to which writes are issued by transaction
	// attempting to change a descriptor.
	Lock // lock

	numActions int = iota
)

var actionStrings = func() map[string]Action {
	ret := make(map[string]Action, numActions)
	for a := Action(1); int(a) <= numActions; a++ {
		ret[a.String()] = a
	}
	return ret
}()

// Row corresponds to a row of the single_version_descriptor_lease table.
type Row struct {

	// Action is the prefix of the row.
	Action Action
	// Descriptor ID keys the row in the action.
	Descriptor descpb.ID
	// Session is the session corresponding to the suffix of the row.
	// An empty session value is meaningfully used in a number of places.
	// In the scan code, it implies a scan for all sessions with the above
	// prefix.
	Session sqlliveness.SessionID
}

type tableCodec struct {
	codec keys.SQLCodec
	// tableID is the ID of the table storing the single version descriptor Lease
	// state.
	tableID descpb.ID
}

func (th tableCodec) encodeActionPrefix(a Action) (roachpb.Key, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	key := th.codec.IndexPrefix(
		uint32(th.tableID), keys.SingleVersionDescriptorLeasePrimaryKeyIndexID,
	)
	key = encoding.EncodeStringAscending(key, a.String())
	return key, nil
}

func (th tableCodec) encodeActionDescriptorPrefix(a Action, id descpb.ID) (roachpb.Key, error) {
	key, err := th.encodeActionPrefix(a)
	if err != nil {
		return nil, err
	}
	key = encoding.EncodeVarintAscending(key, int64(id))
	return key, nil
}

func encode(th tableCodec, r Row, prefix bool) (roachpb.Key, error) {
	key, err := th.encodeActionDescriptorPrefix(r.Action, r.Descriptor)
	if err != nil {
		return nil, err
	}
	if prefix && r.Session == "" {
		return key, nil
	}
	key = encoding.EncodeBytesAscending(key, r.Session.UnsafeBytes())
	key = keys.MakeFamilyKey(key, keys.SingleVersionDescriptorLeasePrimaryColFamID)
	return key, nil
}

// encode encodes the Row to a roachpb.Key. If prefix is true, it will return
// not encode the Session column if it has a zero value.
func (th tableCodec) encode(r Row) (roachpb.Key, error) {
	return encode(th, r, false /* prefix */)
}

func (th tableCodec) encodePrefix(r Row) (roachpb.Key, error) {
	return encode(th, r, true /* prefix */)
}

func (th tableCodec) decode(k roachpb.Key, r *Row) error {
	rem, err := th.codec.StripTenantPrefix(k)
	if err != nil {
		return errors.Wrapf(err, "failed to strip tenant prefix of %v", k)
	}
	rem, tableID, indexID, err := keys.DecodeTableIDIndexID(rem)
	if err != nil {
		return errors.Wrapf(err, "decoding tableID, indexID of %v", k)
	}
	if descpb.ID(tableID) != th.tableID {
		return errors.AssertionFailedf(
			"singleversion: key corresponds to table %d, expected %d",
			tableID, th.tableID,
		)
	}
	if indexID != keys.SingleVersionDescriptorLeasePrimaryKeyIndexID {
		return errors.AssertionFailedf(
			"singleversion: key corresponds to index %d, expected %d",
			indexID, keys.SingleVersionDescriptorLeasePrimaryKeyIndexID,
		)
	}
	rem, actionStr, err := encoding.DecodeUnsafeStringAscending(rem, nil)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "singleversion: decoding Action",
		)
	}
	action, err := decodeAction(actionStr)
	if err != nil {
		return err
	}
	rem, descriptorID, err := encoding.DecodeVarintAscending(rem)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "singleversion: decoding descriptor_id",
		)
	}
	_, sessionID, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "singleversion: decoding descriptor_id",
		)
	}
	*r = Row{
		Action:     action,
		Descriptor: descpb.ID(descriptorID),
		Session:    sqlliveness.SessionID(sessionID),
	}
	return nil
}

func decodeAction(actionStr string) (Action, error) {
	a, ok := actionStrings[actionStr]
	if !ok {
		return 0, errors.AssertionFailedf("singleversion: invalid Action: %s", actionStr)
	}
	return a, nil
}
