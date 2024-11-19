// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	sessionIDVersion    uint8 = 1
	legacyLen                 = uuid.Size
	versionLen                = 1
	regionLengthLen           = 1
	minimumRegionLen          = 1
	minimumPrefixLen          = versionLen + regionLengthLen + minimumRegionLen
	minimumNonLegacyLen       = minimumPrefixLen + uuid.Size
)

// MakeSessionID encodes the region and uuid into a binary string. Most callers
// should treat the format of SessionID as opaque. The basic format is:
//
//		byte[] {
//			version = 1,
//	        len(region),
//	        region...,
//			uuid...,
//		}
//
// One of the goals of the encoding is every (region, uuid) pair should have
// exactly one valid binary encoding. Unique encodings make it safe to use the
// encoded version in maps. The goal of a single canonical representation
// disqualified the following encoding schemes:
//  1. protobufs: protobufs do not have a canonical encoding scheme. The order
//     of fields is not guaranteed.
//  2. region length is encoded as a single byte instead of a varint. Small
//     numbers have multiple valid varint encodings. E.g 0x8001 and 0x01 are both
//     valid encodings of 1.
func MakeSessionID(region []byte, id uuid.UUID) (sqlliveness.SessionID, error) {
	if len(region) == 0 {
		return sqlliveness.SessionID(""), errors.New("session id requires a non-empty region")
	}
	if int(uint8(len(region))) != len(region) {
		return sqlliveness.SessionID(""), errors.Newf("region is too long: %d", len(region))
	}

	sessionLength := versionLen + regionLengthLen + len(region) + uuid.Size
	b := make([]byte, 0, sessionLength)
	b = append(b, sessionIDVersion)
	b = append(b, byte(len(region)))
	b = append(b, region...)
	b = append(b, id.GetBytes()...)
	return sqlliveness.SessionID(b), nil
}

// UnsafeDecodeSessionID decodes the region and id from the SessionID. The
// function is unsafe, because the byte slices index into the session and must
// not be mutated.
func UnsafeDecodeSessionID(session sqlliveness.SessionID) (region, id []byte, err error) {
	b := session.UnsafeBytes()
	if err = ValidateSessionID(sqlliveness.SessionID(b)); err != nil {
		return nil, nil, err
	}
	regionLen := int(b[1])
	rest := b[2:]

	// Decode and validate the length of the region.
	if len(rest) != regionLen+uuid.Size {
		return nil, nil, errors.Newf("session id with length %d is the wrong size to include a region with length %d", len(b), regionLen)
	}

	return rest[:regionLen], rest[regionLen:], nil
}

// ValidateSessionID validates that the SessionID has the correct format.
func ValidateSessionID(session sqlliveness.SessionID) error {
	if len(session) == legacyLen {
		return errors.Newf("unexpected legacy SessionID format")
	}
	if len(session) < minimumNonLegacyLen {
		// The smallest valid v1 session id is a [version, 1, single_byte_region, uuid...],
		// which is three bytes larger than a uuid.
		return errors.New("session id is too short")
	}
	// Decode the version.
	if session[0] != sessionIDVersion {
		return errors.Newf("invalid session id version: %d", session[0])
	}
	return nil
}

// SafeDecodeSessionID decodes the region and id from the SessionID.
func SafeDecodeSessionID(session sqlliveness.SessionID) (region, id string, err error) {
	if err = ValidateSessionID(session); err != nil {
		return "", "", err
	}
	regionLen := int(session[1])
	rest := session[2:]
	// Decode and validate the length of the region.
	if len(rest) != regionLen+uuid.Size {
		return "", "", errors.Newf("session id with length %d is the wrong size to include a region with length %d", len(session), regionLen)
	}

	return string(rest[:regionLen]), string(rest[regionLen:]), nil
}
