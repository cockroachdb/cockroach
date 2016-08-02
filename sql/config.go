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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = GetZoneConfig
}

// GetZoneConfig returns the zone config for the object with 'id'.
func GetZoneConfig(cfg config.SystemConfig, id uint32) (config.ZoneConfig, bool, error) {
	// Look in the zones table.
	if zoneVal := cfg.GetValue(sqlbase.MakeZoneKey(sqlbase.ID(id))); zoneVal != nil {
		var zone config.ZoneConfig
		// We're done.
		return zone, true, zoneVal.GetProto(&zone)
	}

	// No zone config for this ID. We need to figure out if it's a database
	// or table. Lookup its descriptor.
	if descVal := cfg.GetValue(sqlbase.MakeDescMetadataKey(sqlbase.ID(id))); descVal != nil {
		// Determine whether this is a database or table.
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return config.ZoneConfig{}, false, err
		}
		if tableDesc := desc.GetTable(); tableDesc != nil {
			// This is a table descriptor. Lookup its parent database zone config.
			return GetZoneConfig(cfg, uint32(tableDesc.ParentID))
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		return GetZoneConfig(cfg, keys.RootNamespaceID)
	}

	// No descriptor or not a table.
	return config.ZoneConfig{}, false, nil
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
