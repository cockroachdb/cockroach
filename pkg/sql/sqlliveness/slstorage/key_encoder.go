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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// keyCodec manages the SessionID <-> roachpb.Key mapping.
type keyCodec interface {
	encode(sid sqlliveness.SessionID) (roachpb.Key, error)
	decode(key roachpb.Key) (sqlliveness.SessionID, error)
	prefix() roachpb.Key
}

// makeKeyCodec constructs a key codec. It consults the
// COCKROACH_MR_SYSTEM_DATABASE environment variable to determine if it should
// use the regional by table or regional by row index format.
func makeKeyCodec(codec keys.SQLCodec, tableID catid.DescID, rbrIndex catid.IndexID) keyCodec {
	if systemschema.SupportMultiRegion() {
		return &rbrEncoder{codec.IndexPrefix(uint32(tableID), uint32(rbrIndex))}
	}
	return &rbtEncoder{codec.IndexPrefix(uint32(tableID), 1)}
}

type rbrEncoder struct {
	rbrIndex roachpb.Key
}

func (e *rbrEncoder) encode(session sqlliveness.SessionID) (roachpb.Key, error) {
	region, uuid, err := UnsafeDecodeSessionID(session)
	if err != nil {
		return nil, err
	}
	if len(region) == 0 {
		return nil, errors.Newf("legacy session passed to rbr table: '%s'", session.String())
	}

	key := e.rbrIndex.Clone()
	key = encoding.EncodeBytesAscending(key, region)
	key = encoding.EncodeBytesAscending(key, uuid)
	key = keys.MakeFamilyKey(key, 0)
	return key, nil
}

func (e *rbrEncoder) decode(key roachpb.Key) (sqlliveness.SessionID, error) {
	if !bytes.HasPrefix(key, e.rbrIndex) {
		return "", errors.Newf("sqlliveness table key has an invalid prefix: %v", key)
	}
	rem := key[len(e.rbrIndex):]

	rem, region, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode region from session key")
	}

	rem, rawUUID, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode uuid from session key")
	}

	id, err := uuid.FromBytes(rawUUID)
	if err != nil {
		return "", errors.Wrap(err, "failed to convert uuid bytes to uuid id for session key")
	}

	return MakeSessionID(region, id)
}

func (e *rbrEncoder) prefix() roachpb.Key {
	return e.rbrIndex
}

type rbtEncoder struct {
	rbtIndex roachpb.Key
}

func (e *rbtEncoder) encode(id sqlliveness.SessionID) (roachpb.Key, error) {
	return keys.MakeFamilyKey(encoding.EncodeBytesAscending(e.rbtIndex.Clone(), id.UnsafeBytes()), 0), nil
}

func (e *rbtEncoder) decode(key roachpb.Key) (sqlliveness.SessionID, error) {
	if !bytes.HasPrefix(key, e.rbtIndex) {
		return "", errors.Newf("sqlliveness table key has an invalid prefix: %v", key)
	}
	rem := key[len(e.rbtIndex):]

	rem, session, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode region from session key")
	}

	return sqlliveness.SessionID(session), nil
}

func (e *rbtEncoder) prefix() roachpb.Key {
	return e.rbtIndex
}
