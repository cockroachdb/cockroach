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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

func init() {
	// TODO(marc): we use a hook to avoid a depency on the sql package.
	// We should probably move keys/protos elsewhere.
	config.ZoneConfigHook = GetZoneConfig
}

// GetZoneConfig returns the zone config for the object with 'id'.
func GetZoneConfig(cfg *config.SystemConfig, id uint32) (*config.ZoneConfig, error) {
	// Look in the zones table.
	if val, ok := cfg.GetValue(MakeZoneKey(ID(id))); ok {
		zone := &config.ZoneConfig{}
		if err := gogoproto.Unmarshal(val, zone); err != nil {
			return nil, err
		}
		// We're done.
		return zone, nil
	}

	// No zone config for this ID. We need to figure out if it's a database
	// or table. Lookup its descriptor.
	rawDesc, ok := cfg.GetValue(MakeDescMetadataKey(ID(id)))
	if !ok {
		// No descriptor. This table/db could have been deleted,
		// just return the default config.
		return config.DefaultZoneConfig, nil
	}

	// Determine whether this is a database or table.
	// TODO(marc): we need a better way of doing this. Options include:
	// - add a type field on the descriptor table
	// - separate descriptor tables for databases and tables
	// - prebuild list of databases and tables in the system config
	var dbDesc DatabaseDescriptor
	if err := gogoproto.Unmarshal(rawDesc, &dbDesc); err == nil {
		// parses as a database: return default config.
		return config.DefaultZoneConfig, nil
	}

	var tableDesc TableDescriptor
	if err := gogoproto.Unmarshal(rawDesc, &tableDesc); err != nil {
		// does not parse as a table either: this means an entry in the
		// descriptor table we're not familiar with.
		return nil, util.Errorf("descriptor for object ID %d is not a table or database", id)
	}

	// This is a table descriptor. Lookup its parent database zone config.
	return GetZoneConfig(cfg, uint32(tableDesc.ParentID))
}
