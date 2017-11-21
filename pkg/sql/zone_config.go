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
	"fmt"

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
	config.ZoneConfigHook = ZoneConfigHook
}

var errNoZoneConfigApplies = errors.New("no zone config applies")

var getSubzoneNoop = func(config.ZoneConfig) *config.Subzone { return nil }

// getZoneConfig recursively looks up entries in system.zones until an entry
// that applies to the object with the specified id is found.
//
// This function must be kept in sync with ascendZoneSpecifier.
func getZoneConfig(
	id uint32,
	getKey func(roachpb.Key) (*roachpb.Value, error),
	getSubzone func(config.ZoneConfig) *config.Subzone,
) (uint32, config.ZoneConfig, *config.Subzone, error) {
	// Look in the zones table.
	if zoneVal, err := getKey(config.MakeZoneKey(id)); err != nil {
		return 0, config.ZoneConfig{}, nil, err
	} else if zoneVal != nil {
		// We found a matching entry.
		var zone config.ZoneConfig
		if err := zoneVal.GetProto(&zone); err != nil {
			return 0, config.ZoneConfig{}, nil, err
		}
		subzone := getSubzone(zone)
		if !zone.IsSubzonePlaceholder() || subzone != nil {
			return id, zone, subzone, nil
		}
		// No subzone matched, and the zone is just a placeholder for subzones. Keep
		// recursing up the hierarchy.
	}

	// No zone config for this ID. We need to figure out if it's a table, so we
	// look up its descriptor.
	if descVal, err := getKey(sqlbase.MakeDescMetadataKey(sqlbase.ID(id))); err != nil {
		return 0, config.ZoneConfig{}, nil, err
	} else if descVal != nil {
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return 0, config.ZoneConfig{}, nil, err
		}
		if tableDesc := desc.GetTable(); tableDesc != nil {
			// This is a table descriptor. Look up its parent database zone config.
			// Don't forward getSubzone, because only tables can have subzones.
			return getZoneConfig(uint32(tableDesc.ParentID), getKey, getSubzoneNoop)
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		return getZoneConfig(keys.RootNamespaceID, getKey, getSubzoneNoop)
	}

	// No descriptor or not a table.
	return 0, config.ZoneConfig{}, nil, errNoZoneConfigApplies
}

// ZoneConfigHook returns the zone config for the object with id using the
// cached system config. If keySuffix is within a subzone, the subzone's config
// is returned instead.
func ZoneConfigHook(
	cfg config.SystemConfig, id uint32, keySuffix []byte,
) (config.ZoneConfig, bool, error) {
	_, zone, subzone, err := getZoneConfig(
		id,
		func(key roachpb.Key) (*roachpb.Value, error) {
			return cfg.GetValue(key), nil
		},
		func(zone config.ZoneConfig) *config.Subzone {
			return zone.GetSubzoneForKeySuffix(keySuffix)
		},
	)
	if err == errNoZoneConfigApplies {
		return config.ZoneConfig{}, false, nil
	} else if err != nil {
		return config.ZoneConfig{}, false, err
	} else if subzone != nil {
		return subzone.Config, true, nil
	}
	return zone, true, nil
}

// GetZoneConfigInTxn looks up the zone and subzone for the specified object ID,
// index, and partition.
func GetZoneConfigInTxn(
	ctx context.Context, txn *client.Txn, id uint32, index *sqlbase.IndexDescriptor, partition string,
) (uint32, config.ZoneConfig, *config.Subzone, error) {
	return getZoneConfig(
		id,
		func(key roachpb.Key) (*roachpb.Value, error) {
			kv, err := txn.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			return kv.Value, nil
		},
		func(zone config.ZoneConfig) *config.Subzone {
			if index == nil {
				return nil
			}
			return zone.GetSubzone(uint32(index.ID), partition)
		},
	)
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

// GenerateSubzoneSpans is a hook point for a CCL function that constructs from
// a TableDescriptor the entries mapping zone config spans to subzones for use
// in the SubzonzeSpans field of config.ZoneConfig. If no CCL hook is installed,
// it returns an error that directs users to use a CCL binary.
var GenerateSubzoneSpans = func(
	*sqlbase.TableDescriptor, []config.Subzone,
) ([]config.SubzoneSpan, error) {
	return nil, fmt.Errorf("setting zone configs on indexes or partitions requires a CCL binary")
}
