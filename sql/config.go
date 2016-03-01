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
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = GetZoneConfig
}

// GetZoneConfig returns the zone config for the object with 'id'.
func GetZoneConfig(cfg config.SystemConfig, id uint32) (*config.ZoneConfig, error) {
	// Look in the zones table.
	if zoneVal := cfg.GetValue(MakeZoneKey(ID(id))); zoneVal != nil {
		zone := &config.ZoneConfig{}
		if err := zoneVal.GetProto(zone); err != nil {
			return nil, err
		}
		// We're done.
		return zone, nil
	}

	// No zone config for this ID. We need to figure out if it's a database
	// or table. Lookup its descriptor.
	if descVal := cfg.GetValue(MakeDescMetadataKey(ID(id))); descVal != nil {
		// Determine whether this is a database or table.
		desc := &Descriptor{}
		if err := descVal.GetProto(desc); err != nil {
			return nil, err
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
	return nil, nil
}
