// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// resolveTableForZone ensures that the table part of the zone
// specifier is resolved (or resolvable) and, if the zone specifier
// points to an index, that the index name is expanded to a valid
// table.
// Returns res = nil if the zone specifier is not for a table or index.
func (p *planner) resolveTableForZone(
	ctx context.Context, zs *tree.ZoneSpecifier,
) (res catalog.TableDescriptor, err error) {
	if zs.TargetsIndex() {
		var mutRes *tabledesc.Mutable
		_, mutRes, err = expandMutableIndexName(ctx, p, &zs.TableOrIndex, true /* requireTable */)
		if mutRes != nil {
			res = mutRes
		}
	} else if zs.TargetsTable() {
		var immutRes catalog.TableDescriptor
		p.runWithOptions(resolveFlags{skipCache: true}, func() {
			flags := tree.ObjectLookupFlags{
				Required:          true,
				IncludeOffline:    true,
				DesiredObjectKind: tree.TableObject,
			}
			_, immutRes, err = resolver.ResolveExistingTableObject(ctx, p, &zs.TableOrIndex.Table, flags)
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
func resolveZone(
	ctx context.Context,
	txn *kv.Txn,
	col *descs.Collection,
	zs *tree.ZoneSpecifier,
	version clusterversion.Handle,
) (descpb.ID, error) {
	errMissingKey := errors.New("missing key")
	id, err := zonepb.ResolveZoneSpecifier(ctx, zs,
		func(parentID uint32, schemaID uint32, name string) (uint32, error) {
			id, err := col.LookupObjectID(ctx, txn, descpb.ID(parentID), descpb.ID(schemaID), name)
			if err != nil {
				return 0, err
			}
			if id == descpb.InvalidID {
				return 0, errMissingKey
			}
			return uint32(id), nil
		},
		version,
	)
	if err != nil {
		if errors.Is(err, errMissingKey) {
			return 0, zoneSpecifierNotFoundError(*zs)
		}
		return 0, err
	}
	return descpb.ID(id), nil
}

func resolveSubzone(
	zs *tree.ZoneSpecifier, table catalog.TableDescriptor,
) (catalog.Index, string, error) {
	if !zs.TargetsTable() || zs.TableOrIndex.Index == "" && zs.Partition == "" {
		return nil, "", nil
	}

	indexName := string(zs.TableOrIndex.Index)
	var index catalog.Index
	if indexName == "" {
		index = table.GetPrimaryIndex()
		indexName = index.GetName()
	} else {
		var err error
		index, err = catalog.MustFindIndexByName(table, indexName)
		if err != nil {
			return nil, "", err
		}
	}

	partitionName := string(zs.Partition)
	if partitionName != "" {
		if index.GetPartitioning().FindPartitionByName(partitionName) == nil {
			return nil, "", fmt.Errorf("partition %q does not exist on index %q", partitionName, indexName)
		}
	}

	return index, partitionName, nil
}

func prepareRemovedPartitionZoneConfigs(
	ctx context.Context,
	txn descs.Txn,
	tableDesc catalog.TableDescriptor,
	indexID descpb.IndexID,
	oldPart catalog.Partitioning,
	newPart catalog.Partitioning,
	execCfg *ExecutorConfig,
) (*zoneConfigUpdate, error) {
	newNames := map[string]struct{}{}
	_ = newPart.ForEachPartitionName(func(newName string) error {
		newNames[newName] = struct{}{}
		return nil
	})
	removedNames := make([]string, 0, len(newNames))
	_ = oldPart.ForEachPartitionName(func(oldName string) error {
		if _, exists := newNames[oldName]; !exists {
			removedNames = append(removedNames, oldName)
		}
		return nil
	})
	if len(removedNames) == 0 {
		return nil, nil
	}
	zoneWithRaw, err := txn.Descriptors().GetZoneConfig(ctx, txn.KV(), tableDesc.GetID())
	if err != nil {
		return nil, err
	}
	if zoneWithRaw == nil {
		zoneWithRaw = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}
	for _, n := range removedNames {
		zoneWithRaw.ZoneConfigProto().DeleteSubzone(uint32(indexID), n)
	}
	return prepareZoneConfigWrites(
		ctx, execCfg, tableDesc.GetID(), tableDesc, zoneWithRaw.ZoneConfigProto(), zoneWithRaw.GetRawBytesInStorage(), false, /* hasNewSubzones */
	)
}

func deleteRemovedPartitionZoneConfigs(
	ctx context.Context,
	txn descs.Txn,
	tableDesc catalog.TableDescriptor,
	indexID descpb.IndexID,
	oldPart catalog.Partitioning,
	newPart catalog.Partitioning,
	execCfg *ExecutorConfig,
	kvTrace bool,
) error {
	update, err := prepareRemovedPartitionZoneConfigs(
		ctx, txn, tableDesc, indexID, oldPart, newPart, execCfg,
	)
	if update == nil || err != nil {
		return err
	}
	return writeZoneConfigUpdate(ctx, txn, kvTrace, update)
}

func zoneSpecifierNotFoundError(zs tree.ZoneSpecifier) error {
	if zs.NamedZone != "" {
		return pgerror.Newf(
			pgcode.InvalidCatalogName, "zone %q does not exist", zs.NamedZone)
	} else if zs.Database != "" {
		return sqlerrors.NewUndefinedDatabaseError(string(zs.Database))
	} else {
		return sqlerrors.NewUndefinedRelationError(&zs.TableOrIndex)
	}
}
