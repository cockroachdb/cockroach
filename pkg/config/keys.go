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
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(codec keys.SQLCodec, id descpb.ID) roachpb.Key {
	k := codec.IndexPrefix(keys.ZonesTableID, keys.ZonesTablePrimaryIndexID)
	return encoding.EncodeUvarintAscending(k, uint64(id))
}

// MakeZoneKey returns the key for a given id's entry in the system.zones table.
func MakeZoneKey(codec keys.SQLCodec, id descpb.ID) roachpb.Key {
	k := MakeZoneKeyPrefix(codec, id)
	return keys.MakeFamilyKey(k, keys.ZonesTableConfigColFamID)
}

// DecodeObjectID decodes the object ID for the system-tenant from
// the front of key. It returns the decoded object ID, the remainder of the key,
// and whether the result is valid (i.e., whether the key was within the system
// tenant's structured key space).
func DecodeObjectID(codec keys.SQLCodec, key roachpb.RKey) (ObjectID, []byte, bool) {
	rem, id, err := codec.DecodeTablePrefix(key.AsRawKey())
	return ObjectID(id), rem, err == nil
}
