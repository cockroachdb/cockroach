// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = GetZoneConfig
}

func getZoneConfig(
	id uint32,
	get func(roachpb.Key) (*roachpb.Value, error),
) (config.ZoneConfig, bool, error) {
	// Look in the zones table.
	if zoneVal, err := get(sqlbase.MakeZoneKey(sqlbase.ID(id))); err != nil {
		return config.ZoneConfig{}, false, err
	} else if zoneVal != nil {
		// We're done.
		zone, err := config.MigrateZoneConfig(zoneVal)
		return zone, true, err
	}

	// No zone config for this ID. We need to figure out if it's a database
	// or table. Lookup its descriptor.
	if descVal, err := get(sqlbase.MakeDescMetadataKey(sqlbase.ID(id))); err != nil {
		return config.ZoneConfig{}, false, err
	} else if descVal != nil {
		// Determine whether this is a database or table.
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return config.ZoneConfig{}, false, err
		}
		if tableDesc := desc.GetTable(); tableDesc != nil {
			// This is a table descriptor. Lookup its parent database zone config.
			return getZoneConfig(uint32(tableDesc.ParentID), get)
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		return getZoneConfig(keys.RootNamespaceID, get)
	}

	// No descriptor or not a table.
	return config.ZoneConfig{}, false, nil
}

// GetZoneConfig returns the zone config for the object with 'id' using the
// cached system config.
func GetZoneConfig(cfg config.SystemConfig, id uint32) (config.ZoneConfig, bool, error) {
	return getZoneConfig(id, func(key roachpb.Key) (*roachpb.Value, error) {
		return cfg.GetValue(key), nil
	})
}

// GetZoneConfigInTxn is like GetZoneConfig, but uses the provided transaction
// to perform lookups instead of the cached system config.
func GetZoneConfigInTxn(
	ctx context.Context,
	txn *client.Txn,
	id uint32,
) (config.ZoneConfig, bool, error) {
	return getZoneConfig(id, func(key roachpb.Key) (*roachpb.Value, error) {
		kv, err := txn.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return kv.Value, nil
	})
}

// GetTableDesc returns the table descriptor for the table with 'id'.
// Returns nil if the descriptor is not present, or is present but is not a
// table.
func GetTableDesc(cfg config.SystemConfig, id sqlbase.ID) (*sqlbase.TableDescriptor, error) {
	if descVal := cfg.GetValue(sqlbase.MakeDescMetadataKey(id)); descVal != nil {
		desc := &sqlbase.Descriptor{}
		if err := descVal.GetProto(desc); err != nil {
			return nil, err
		}
		return desc.GetTable(), nil
	}
	return nil, nil
}
