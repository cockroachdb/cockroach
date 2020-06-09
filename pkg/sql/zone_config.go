// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

func init() {
	// TODO(marc): we use a hook to avoid a dependency on the sql package. We
	// should probably move keys/protos elsewhere.
	config.ZoneConfigHook = zoneConfigHook
}

var errNoZoneConfigApplies = errors.New("no zone config applies")

// getZoneConfig recursively looks up entries in system.zones until an
// entry that applies to the object with the specified id is
// found. Returns the ID of the matching zone, its zone config, and an
// optional placeholder ID and config if the looked-for ID was a table
// with a zone config specifying only indexes and/or partitions.
//
// This function must be kept in sync with ascendZoneSpecifier.
//
// If getInheritedDefault is true, the direct zone configuration, if it exists, is
// ignored, and the default that would apply if it did not exist is returned instead.
func getZoneConfig(
	id config.SystemTenantObjectID,
	getKey func(roachpb.Key) (*roachpb.Value, error),
	getInheritedDefault bool,
) (
	config.SystemTenantObjectID,
	*zonepb.ZoneConfig,
	config.SystemTenantObjectID,
	*zonepb.ZoneConfig,
	error,
) {
	var placeholder *zonepb.ZoneConfig
	var placeholderID config.SystemTenantObjectID
	if !getInheritedDefault {
		// Look in the zones table.
		if zoneVal, err := getKey(config.MakeZoneKey(id)); err != nil {
			return 0, nil, 0, nil, err
		} else if zoneVal != nil {
			// We found a matching entry.
			var zone zonepb.ZoneConfig
			if err := zoneVal.GetProto(&zone); err != nil {
				return 0, nil, 0, nil, err
			}
			// If the zone isn't a subzone placeholder, we're done.
			if !zone.IsSubzonePlaceholder() {
				return id, &zone, 0, nil, nil
			}
			// If the zone is just a placeholder for subzones, keep recursing
			// up the hierarchy.
			placeholder = &zone
			placeholderID = id
		}
	}

	// No zone config for this ID. We need to figure out if it's a table, so we
	// look up its descriptor.
	if descVal, err := getKey(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(id))); err != nil {
		return 0, nil, 0, nil, err
	} else if descVal != nil {
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return 0, nil, 0, nil, err
		}
		if tableDesc := desc.Table(descVal.Timestamp); tableDesc != nil {
			// This is a table descriptor. Look up its parent database zone config.
			dbID, zone, _, _, err := getZoneConfig(config.SystemTenantObjectID(tableDesc.ParentID), getKey, false /* getInheritedDefault */)
			if err != nil {
				return 0, nil, 0, nil, err
			}
			return dbID, zone, placeholderID, placeholder, nil
		}
	}

	// Retrieve the default zone config, but only as long as that wasn't the ID
	// we were trying to retrieve (avoid infinite recursion).
	if id != keys.RootNamespaceID {
		rootID, zone, _, _, err := getZoneConfig(keys.RootNamespaceID, getKey, false /* getInheritedDefault */)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		return rootID, zone, placeholderID, placeholder, nil
	}

	// No descriptor or not a table.
	return 0, nil, 0, nil, errNoZoneConfigApplies
}

// completeZoneConfig takes a zone config pointer and fills in the
// missing fields by following the chain of inheritance.
// In the worst case, will have to inherit from the default zone config.
// NOTE: This will not work for subzones. To complete subzones, find a complete
// parent zone (index or table) and apply InheritFromParent to it.
func completeZoneConfig(
	cfg *zonepb.ZoneConfig,
	id config.SystemTenantObjectID,
	getKey func(roachpb.Key) (*roachpb.Value, error),
) error {
	if cfg.IsComplete() {
		return nil
	}
	// Check to see if its a table. If so, inherit from the database.
	// For all other cases, inherit from the default.
	if descVal, err := getKey(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(id))); err != nil {
		return err
	} else if descVal != nil {
		var desc sqlbase.Descriptor
		if err := descVal.GetProto(&desc); err != nil {
			return err
		}
		if tableDesc := desc.Table(descVal.Timestamp); tableDesc != nil {
			_, dbzone, _, _, err := getZoneConfig(config.SystemTenantObjectID(tableDesc.ParentID), getKey, false /* getInheritedDefault */)
			if err != nil {
				return err
			}
			cfg.InheritFromParent(dbzone)
		}
	}

	// Check if zone is complete. If not, inherit from the default zone config
	if cfg.IsComplete() {
		return nil
	}
	_, defaultZone, _, _, err := getZoneConfig(keys.RootNamespaceID, getKey, false /* getInheritedDefault */)
	if err != nil {
		return err
	}
	cfg.InheritFromParent(defaultZone)
	return nil
}

// zoneConfigHook returns the zone config and optional placeholder config for
// the object with id using the cached system config. The returned boolean is
// set to true when the zone config returned can be cached.
//
// zoneConfigHook is a pure function whose only inputs are a system config and
// an object ID. It does not make any external KV calls to look up additional
// state.
func zoneConfigHook(
	cfg *config.SystemConfig, id config.SystemTenantObjectID,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, bool, error) {
	getKey := func(key roachpb.Key) (*roachpb.Value, error) {
		return cfg.GetValue(key), nil
	}
	zoneID, zone, _, placeholder, err := getZoneConfig(
		id, getKey, false /* getInheritedDefault */)
	if errors.Is(err, errNoZoneConfigApplies) {
		return nil, nil, true, nil
	} else if err != nil {
		return nil, nil, false, err
	}
	if err = completeZoneConfig(zone, zoneID, getKey); err != nil {
		return nil, nil, false, err
	}
	return zone, placeholder, true, nil
}

// GetZoneConfigInTxn looks up the zone and subzone for the specified object ID,
// index, and partition. See the documentation on getZoneConfig for information
// about the getInheritedDefault parameter.
//
// Unlike ZoneConfigHook, GetZoneConfigInTxn does not used a cached system
// config. Instead, it uses the provided txn to make transactionally consistent
// KV lookups.
func GetZoneConfigInTxn(
	ctx context.Context,
	txn *kv.Txn,
	id config.SystemTenantObjectID,
	index *sqlbase.IndexDescriptor,
	partition string,
	getInheritedDefault bool,
) (config.SystemTenantObjectID, *zonepb.ZoneConfig, *zonepb.Subzone, error) {
	getKey := func(key roachpb.Key) (*roachpb.Value, error) {
		kv, err := txn.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return kv.Value, nil
	}
	zoneID, zone, placeholderID, placeholder, err := getZoneConfig(
		id, getKey, getInheritedDefault)
	if err != nil {
		return 0, nil, nil, err
	}
	if err = completeZoneConfig(zone, zoneID, getKey); err != nil {
		return 0, nil, nil, err
	}
	var subzone *zonepb.Subzone
	if index != nil {
		if placeholder != nil {
			if subzone = placeholder.GetSubzone(uint32(index.ID), partition); subzone != nil {
				if indexSubzone := placeholder.GetSubzone(uint32(index.ID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
				return placeholderID, placeholder, subzone, nil
			}
		} else {
			if subzone = zone.GetSubzone(uint32(index.ID), partition); subzone != nil {
				if indexSubzone := zone.GetSubzone(uint32(index.ID), ""); indexSubzone != nil {
					subzone.Config.InheritFromParent(&indexSubzone.Config)
				}
				subzone.Config.InheritFromParent(zone)
			}
		}
	}
	return zoneID, zone, subzone, nil
}

func zoneSpecifierNotFoundError(zs tree.ZoneSpecifier) error {
	if zs.NamedZone != "" {
		return pgerror.Newf(
			pgcode.InvalidCatalogName, "zone %q does not exist", zs.NamedZone)
	} else if zs.Database != "" {
		return sqlbase.NewUndefinedDatabaseError(string(zs.Database))
	} else {
		return sqlbase.NewUndefinedRelationError(&zs.TableOrIndex)
	}
}

// resolveTableForZone ensures that the table part of the zone
// specifier is resolved (or resolvable) and, if the zone specifier
// points to an index, that the index name is expanded to a valid
// table.
// Returns res = nil if the zone specifier is not for a table or index.
func (p *planner) resolveTableForZone(
	ctx context.Context, zs *tree.ZoneSpecifier,
) (res sqlbase.DescriptorInterface, err error) {
	if zs.TargetsIndex() {
		var mutRes *MutableTableDescriptor
		_, mutRes, err = expandMutableIndexName(ctx, p, &zs.TableOrIndex, true /* requireTable */)
		if mutRes != nil {
			res = mutRes
		}
	} else if zs.TargetsTable() {
		var immutRes *ImmutableTableDescriptor
		p.runWithOptions(resolveFlags{skipCache: true}, func() {
			flags := tree.ObjectLookupFlagsWithRequired()
			flags.IncludeOffline = true
			immutRes, err = resolver.ResolveExistingTableObject(ctx, p, &zs.TableOrIndex.Table, flags, resolver.ResolveAnyDescType)
		})
		if err != nil {
			return nil, err
		} else if immutRes != nil {
			res = immutRes
		}
	}
	return res, err
}

// resolveZone resolves a zone specifier to a zone ID.  If the zone
// specifier points to a table, index or partition, the table part
// must be properly normalized already. It is the caller's
// responsibility to do this using e.g .resolveTableForZone().
func resolveZone(ctx context.Context, txn *kv.Txn, zs *tree.ZoneSpecifier) (sqlbase.ID, error) {
	errMissingKey := errors.New("missing key")
	id, err := zonepb.ResolveZoneSpecifier(zs,
		func(parentID uint32, name string) (uint32, error) {
			found, id, err := sqlbase.LookupPublicTableID(ctx, txn, keys.SystemSQLCodec, sqlbase.ID(parentID), name)
			if err != nil {
				return 0, err
			}
			if !found {
				return 0, errMissingKey
			}
			return uint32(id), nil
		},
	)
	if err != nil {
		if errors.Is(err, errMissingKey) {
			return 0, zoneSpecifierNotFoundError(*zs)
		}
		return 0, err
	}
	return sqlbase.ID(id), nil
}

func resolveSubzone(
	zs *tree.ZoneSpecifier, table sqlbase.DescriptorInterface,
) (*sqlbase.IndexDescriptor, string, error) {
	if !zs.TargetsTable() || zs.TableOrIndex.Index == "" && zs.Partition == "" {
		return nil, "", nil
	}

	indexName := string(zs.TableOrIndex.Index)
	var index *sqlbase.IndexDescriptor
	if indexName == "" {
		index = &table.TableDesc().PrimaryIndex
		indexName = index.Name
	} else {
		var err error
		index, _, err = table.TableDesc().FindIndexByName(indexName)
		if err != nil {
			return nil, "", err
		}
	}

	partitionName := string(zs.Partition)
	if partitionName != "" {
		if partitioning := index.FindPartitionByName(partitionName); partitioning == nil {
			return nil, "", fmt.Errorf("partition %q does not exist on index %q", partitionName, indexName)
		}
	}

	return index, partitionName, nil
}

func deleteRemovedPartitionZoneConfigs(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	oldPartDesc *sqlbase.PartitioningDescriptor,
	newPartDesc *sqlbase.PartitioningDescriptor,
	execCfg *ExecutorConfig,
) error {
	newNames := map[string]struct{}{}
	for _, n := range newPartDesc.PartitionNames() {
		newNames[n] = struct{}{}
	}
	removedNames := []string{}
	for _, n := range oldPartDesc.PartitionNames() {
		if _, exists := newNames[n]; !exists {
			removedNames = append(removedNames, n)
		}
	}
	if len(removedNames) == 0 {
		return nil
	}
	zone, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, tableDesc.ID)
	if err != nil {
		return err
	} else if zone == nil {
		zone = zonepb.NewZoneConfig()
	}
	for _, n := range removedNames {
		zone.DeleteSubzone(uint32(idxDesc.ID), n)
	}
	hasNewSubzones := false
	_, err = writeZoneConfig(ctx, txn, tableDesc.ID, tableDesc, zone, execCfg, hasNewSubzones)
	return err
}
