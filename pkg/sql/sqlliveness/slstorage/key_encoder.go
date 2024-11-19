// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// keyCodec manages the SessionID <-> roachpb.Key mapping.
type keyCodec interface {
	encode(sid sqlliveness.SessionID) (roachpb.Key, string, error)
	decode(key roachpb.Key) (sqlliveness.SessionID, error)
	validate(session sqlliveness.SessionID) error

	// indexPrefix returns the prefix for an encoded key. encode() will return
	// something with the prefix and decode will expect a key with this prefix.
	//
	// indexPrefix() and indexPrefix.PrefixEnd() may be used to scan the
	// content of the table.
	indexPrefix() roachpb.Key
}

type rbrEncoder struct {
	rbrIndex roachpb.Key
}

func (e *rbrEncoder) validate(session sqlliveness.SessionID) error {
	return ValidateSessionID(session)
}

func (e *rbrEncoder) encode(session sqlliveness.SessionID) (roachpb.Key, string, error) {
	region, _, err := SafeDecodeSessionID(session)
	if err != nil {
		return nil, "", err
	}
	if len(region) == 0 {
		return nil, "", errors.Newf("legacy session passed to rbr table: '%s'", session.String())
	}

	const columnFamilyID = 0

	key := e.indexPrefix()
	key = encoding.EncodeBytesAscending(key, encoding.UnsafeConvertStringToBytes(region))
	key = encoding.EncodeBytesAscending(key, session.UnsafeBytes())
	return keys.MakeFamilyKey(key, columnFamilyID), region, nil
}

func (e *rbrEncoder) decode(key roachpb.Key) (sqlliveness.SessionID, error) {
	if !bytes.HasPrefix(key, e.rbrIndex) {
		return "", errors.Newf("sqlliveness table key has an invalid prefix: %v", key)
	}
	rem := key[len(e.rbrIndex):]

	rem, _, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode region from session key")
	}

	rem, id, err := encoding.DecodeBytesAscending(rem, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode uuid from session key")
	}

	return sqlliveness.SessionID(id), nil
}

func (e *rbrEncoder) indexPrefix() roachpb.Key {
	return e.rbrIndex.Clone()
}
