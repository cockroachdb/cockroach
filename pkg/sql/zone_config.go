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
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// GenerateSubzoneSpans is a hook point for a CCL function that constructs from
// a TableDescriptor the entries mapping zone config spans to subzones for use
// in the SubzonzeSpans field of config.ZoneConfig. If no CCL hook is installed,
// it returns an error that directs users to use a CCL binary.
var GenerateSubzoneSpans = func(
	st *cluster.Settings,
	clusterID uuid.UUID,
	tableDesc *sqlbase.TableDescriptor,
	subzones []config.Subzone,
	newSubzones bool,
) ([]config.SubzoneSpan, error) {
	return nil, sqlbase.NewCCLRequiredError(errors.New(
		"setting zone configs on indexes or partitions requires a CCL binary"))
}

func zoneSpecifierNotFoundError(zs tree.ZoneSpecifier) error {
	if zs.NamedZone != "" {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidCatalogNameError, "zone %q does not exist", zs.NamedZone)
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
) (res *sqlbase.TableDescriptor, err error) {
	if zs.TargetsIndex() {
		_, res, err = expandIndexName(ctx, p, &zs.TableOrIndex, true /* requireTable */)
	} else if zs.TargetsTable() {
		tn, err := zs.TableOrIndex.Table.Normalize()
		if err != nil {
			return nil, err
		}
		res, err = ResolveExistingObject(ctx, p, tn, true /*required*/, anyDescType)
		if err != nil {
			return nil, err
		}
	}
	return res, err
}

// resolveZone resolves a zone specifier to a zone ID.  If the zone
// specifier points to a table, index or partition, the table part
// must be properly normalized already. It is the caller's
// responsibility to do this using e.g .resolveTableForZone().
func resolveZone(ctx context.Context, txn *client.Txn, zs *tree.ZoneSpecifier) (sqlbase.ID, error) {
	errMissingKey := errors.New("missing key")
	id, err := config.ResolveZoneSpecifier(zs,
		func(parentID uint32, name string) (uint32, error) {
			kv, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(sqlbase.ID(parentID), name))
			if err != nil {
				return 0, err
			}
			if kv.Value == nil {
				return 0, errMissingKey
			}
			id, err := kv.Value.GetInt()
			if err != nil {
				return 0, err
			}
			return uint32(id), nil
		},
	)
	if err != nil {
		if err == errMissingKey {
			return 0, zoneSpecifierNotFoundError(*zs)
		}
		return 0, err
	}
	return sqlbase.ID(id), nil
}

func resolveSubzone(
	ctx context.Context,
	txn *client.Txn,
	zs *tree.ZoneSpecifier,
	targetID sqlbase.ID,
	table *sqlbase.TableDescriptor,
) (*sqlbase.IndexDescriptor, string, error) {
	if !zs.TargetsTable() {
		return nil, "", nil
	}
	if indexName := string(zs.TableOrIndex.Index); indexName != "" {
		index, _, err := table.FindIndexByName(indexName)
		if err != nil {
			return nil, "", err
		}
		return &index, "", nil
	} else if partitionName := string(zs.Partition); partitionName != "" {
		_, index, err := table.FindNonDropPartitionByName(partitionName)
		if err != nil {
			return nil, "", err
		}
		zs.TableOrIndex.Index = tree.UnrestrictedName(index.Name)
		return index, partitionName, nil
	}
	return nil, "", nil
}

func deleteRemovedPartitionZoneConfigs(
	ctx context.Context,
	txn *client.Txn,
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
	zone, err := getZoneConfigRaw(ctx, txn, tableDesc.ID)
	if err != nil {
		return err
	}
	for _, n := range removedNames {
		zone.DeleteSubzone(uint32(idxDesc.ID), n)
	}
	hasNewSubzones := false
	_, err = writeZoneConfig(ctx, txn, tableDesc.ID, tableDesc, zone, execCfg, hasNewSubzones)
	return err
}
