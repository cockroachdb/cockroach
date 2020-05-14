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
)

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(id uint32) roachpb.Key {
	return keys.SystemSQLCodec.ZoneKeyPrefix(id)
}

// MakeZoneKey returns the key for id's entry in the system.zones table.
func MakeZoneKey(id uint32) roachpb.Key {
	return keys.SystemSQLCodec.ZoneKey(id)
}

// DecodeObjectID decodes the object ID from the front of key. It returns the
// decoded object ID, the remainder of the key, and whether the result is valid
// (i.e., whether the key was within the structured key space).
func DecodeObjectID(key roachpb.RKey) (uint32, []byte, bool) {
	rem, id, err := keys.TODOSQLCodec.DecodeTablePrefix(key.AsRawKey())
	return id, rem, err == nil
}
