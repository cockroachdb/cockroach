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
	"github.com/pkg/errors"
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = GetZoneConfig
}

var errNoZoneConfigApplies = errors.New("no zone config applies")

// getZoneConfig recursively looks up entries in system.zones until an entry
// that applies to the object with the specified id is found.
//
// This function must be kept in sync with ascendZoneSpecifier.
func getZoneConfig(
	id uint32, keySuffix []byte, get func(roachpb.Key) (*roachpb.Value, error),
) (config.ZoneConfig, uint32, error) {
	// Look in the zones table.
	if zoneVal, err := get(config.MakeZoneKey(id)); err != nil {
		return config.ZoneConfig{}, 0, err
	} else if zoneVal != nil {
		// We found a matching entry.
		var zone config.ZoneConfig
		if err := zoneVal.GetProto(&zone); err != nil {
			return config.ZoneConfig{}, 0, err
		}
		if subzone, found := zone.GetSubzoneForKeySuffix(keySuffix); found {
			// The ZoneConfig has a more-specific index or partition subzone; use
			// that.
			return subzone, id, nil
		}
		// No subzone matched. If the parent zone specifies zero replicas, it
		// existed only as a home for subzones, and we should keep recursing up the
		// hierarchy. Otherwise, return the parent zone.
		if zone.NumReplicas != 0 {
			return zone, id, nil
		}
	}

	// No zone config for this ID. We need to figure out if it's a table, so we
	// look up its descriptor.
	if descVal, err := get(sqlbase.MakeDescMetadataKey(sqlbase.ID(id))); err != nil {
		return config.ZoneConfig{}, 0, err
	} else if descVal != nil {
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return config.ZoneConfig{}, 0, err
		}
		if tableDesc := desc.GetTable(); tableDesc != nil {
			// This is a table descriptor. Look up its parent database zone config.
			// Don't forward keySuffix, because only tables can have subzones.
			return getZoneConfig(uint32(tableDesc.ParentID), nil, get)
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		return getZoneConfig(keys.RootNamespaceID, nil, get)
	}

	// No descriptor or not a table.
	return config.ZoneConfig{}, 0, errNoZoneConfigApplies
}

// GetZoneConfig returns the zone config for the object with 'id' using the
// cached system config.
func GetZoneConfig(
	cfg config.SystemConfig, id uint32, keySuffix []byte,
) (config.ZoneConfig, bool, error) {
	zone, _, err := getZoneConfig(id, keySuffix, func(key roachpb.Key) (*roachpb.Value, error) {
		return cfg.GetValue(key), nil
	})
	if err == errNoZoneConfigApplies {
		return config.ZoneConfig{}, false, nil
	}
	found := err == nil
	return zone, found, err
}

// GetZoneConfigInTxn is like GetZoneConfig, but uses the provided transaction
// to perform lookups instead of the cached system config.
func GetZoneConfigInTxn(
	ctx context.Context, txn *client.Txn, id uint32, keySuffix []byte,
) (config.ZoneConfig, uint32, error) {
	return getZoneConfig(id, keySuffix, func(key roachpb.Key) (*roachpb.Value, error) {
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
