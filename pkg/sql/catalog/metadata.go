// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// ZoneConfig encapsulates zone config and raw bytes read from storage.
type ZoneConfig interface {
	// ZoneConfigProto returns the zone config proto buf struct.
	ZoneConfigProto() *zonepb.ZoneConfig

	// GetRawBytesInStorage returns the corresponding raw bytes read from storage.
	GetRawBytesInStorage() []byte

	// Size returns the underlying bytes used by the instance.
	Size() int

	// Clone returns a deep copy of the object.
	Clone() ZoneConfig
}

// ZoneConfigHydrationHelper is a helper interface for zone config hydration.
type ZoneConfigHydrationHelper interface {
	// MaybeGetTable either return a table descriptor if exists or a nil if not
	// exists.
	MaybeGetTable(ctx context.Context, txn *kv.Txn, id descpb.ID) (TableDescriptor, error)
	// MaybeGetZoneConfig either return a zone config if exists or a nil if not
	// exists.
	MaybeGetZoneConfig(ctx context.Context, txn *kv.Txn, id descpb.ID) (ZoneConfig, error)
}
