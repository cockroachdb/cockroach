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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(codec keys.SQLCodec, id descpb.ID) roachpb.Key {
	return codec.ZoneKeyPrefix(uint32(id))
}

// MakeZoneKey returns the key for a given id's entry in the system.zones table.
func MakeZoneKey(codec keys.SQLCodec, id descpb.ID) roachpb.Key {
	return codec.ZoneKey(uint32(id))
}

// DecodeSystemTenantObjectID decodes the object ID for the system-tenant from
// the front of key. It returns the decoded object ID, the remainder of the key,
// and whether the result is valid (i.e., whether the key was within the system
// tenant's structured key space).
func DecodeSystemTenantObjectID(key roachpb.RKey) (SystemTenantObjectID, []byte, bool) {
	rem, id, err := keys.SystemSQLCodec.DecodeTablePrefix(key.AsRawKey())
	return SystemTenantObjectID(id), rem, err == nil
}
