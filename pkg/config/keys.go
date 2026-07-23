// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ZonesPrimaryIndexPrefix returns key prefix for the primary index in the system.zones
// table.
func ZonesPrimaryIndexPrefix(codec keys.SQLCodec) roachpb.Key {
	return codec.IndexPrefix(keys.ZonesTableID, keys.ZonesTablePrimaryIndexID)
}

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(codec keys.SQLCodec, id descpb.ID) roachpb.Key {
	return encoding.EncodeUvarintAscending(ZonesPrimaryIndexPrefix(codec), uint64(id))
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
