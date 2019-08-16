// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(id uint32) roachpb.Key {
	return keys.ZoneKeyPrefix(id)
}

// MakeZoneKey returns the key for id's entry in the system.zones table.
func MakeZoneKey(id uint32) roachpb.Key {
	return keys.ZoneKey(id)
}

// DecodeObjectID decodes the object ID from the front of key. It returns the
// decoded object ID, the remainder of the key, and whether the result is valid
// (i.e., whether the key was within the structured key space).
func DecodeObjectID(key roachpb.RKey) (uint32, []byte, bool) {
	if key.Equal(roachpb.RKeyMax) {
		return 0, nil, false
	}
	if encoding.PeekType(key) != encoding.Int {
		// TODO(marc): this should eventually return SystemDatabaseID.
		return 0, nil, false
	}
	// Consume first encoded int.
	rem, id64, err := encoding.DecodeUvarintAscending(key)
	return uint32(id64), rem, err == nil
}
