// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replication

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// SetupOrAdvanceStandbyReaderCatalog sets up a reader catalog that reads from
// that tenant as of the passed timestamp -- potentially replacing the current
// reader catalog if needed -- or advances the current reader catalog to read as
// of that timestamp if that is possible to do in place by updating the ts and
// bumping the descriptor versions. In addition to mirroring all user-created
// catalog entries, this also updates the system.users, roles and role options,
// and database role settings tables in this cluster to read from those tables in
// the other cluster (and fails if they have incompatible indexes). Changes to
// the catalog are atomic.
func SetupOrAdvanceStandbyReaderCatalog(
	ctx context.Context,
	fromID roachpb.TenantID,
	asOf hlc.Timestamp,
	descsCol descs.DB,
	st *cluster.Settings,
) error {
	extracted, err := getCatalogForTenantAsOf(ctx, st, descsCol.KV(), fromID, asOf)
	if err != nil {
		return err
	}
	return descsCol.DescsTxn(
		ctx, func(ctx context.Context, txn descs.Txn) error {
			// Descriptors that no longer exist
			descriptorsUpdated := catalog.DescriptorIDSet{}
			namespaceUpdated := catalog.DescriptorIDSet{}
			allExistingDescs, err := txn.Descriptors().GetAll(ctx, txn.KV())
			if err != nil {
				return err
			}
			// Resolve any existing descriptors within the tenant, which
			// will be use to compute old values for writing.
			b := txn.KV().NewBatch()
			if err := extracted.ForEachDescriptor(func(desc catalog.Descriptor) error {
				if !shouldSetupForReader(desc.GetID(), desc.GetParentID()) {
					return nil
				}
				// Track this descriptor was updated.
				descriptorsUpdated.Add(desc.GetID())
				// If there is an existing descriptor with the same ID, we should
				// determine the old bytes in storage for the upsert.
				var existingRawBytes []byte
				existingDesc, err := txn.Descriptors().MutableByID(txn.KV()).Desc(ctx, desc.GetID())
				if err == nil {
					existingRawBytes = existingDesc.GetRawBytesInStorage()
				} else if errors.Is(err, catalog.ErrDescriptorNotFound) {
					err = nil
				} else {
					return err
				}
				// Existing descriptor should never be a system descriptor.
				if existingDesc != nil &&
					existingDesc.GetParentID() == keys.SystemDatabaseID &&
					desc.GetParentID() != keys.SystemDatabaseID {
					return errors.AssertionFailedf("system database from replicated tenant " +
						"and reader have a system table that collide, ensure both are created on the " +
						"same version of CRDB.")
				}
				var mut catalog.MutableDescriptor
				switch t := desc.DescriptorProto().GetUnion().(type) {
				case *descpb.Descriptor_Table:
					t.Table.Version = 1
					var mutBuilder tabledesc.TableDescriptorBuilder
					var mutTbl *tabledesc.Mutable
					if existingRawBytes != nil {
						t.Table.Version = existingDesc.GetVersion()
						mutBuilder = existingDesc.NewBuilder().(tabledesc.TableDescriptorBuilder)
						newDescBytes, err := protoutil.Marshal(t.Table)
						if err != nil {
							return err
						}
						if err := protoutil.Unmarshal(newDescBytes, mutBuilder.BuildExistingMutableTable().TableDesc()); err != nil {
							return err
						}
						mutTbl = mutBuilder.BuildExistingMutableTable()
					} else {
						mutBuilder = tabledesc.NewBuilder(t.Table)
						mutTbl = mutBuilder.BuildCreatedMutableTable()
					}
					mut = mutTbl
					// Convert any physical tables into external row tables.
					// Note: Materialized views will be converted, but their
					// view definition will be wiped.
					if mutTbl.IsPhysicalTable() {
						mutTbl.ViewQuery = ""
						mutTbl.SetExternalRowData(&descpb.ExternalRowData{TenantID: fromID, TableID: desc.GetID(), AsOf: asOf})
					}
				case *descpb.Descriptor_Database:
					t.Database.Version = 1
					var mutBuilder dbdesc.DatabaseDescriptorBuilder
					if existingRawBytes != nil {
						mutBuilder = existingDesc.NewBuilder().(dbdesc.DatabaseDescriptorBuilder)
						newDescBytes, err := protoutil.Marshal(t.Database)
						if err != nil {
							return err
						}
						if err := protoutil.Unmarshal(newDescBytes, mutBuilder.BuildExistingMutableDatabase().DatabaseDesc()); err != nil {
							return err
						}
						mut = mutBuilder.BuildExistingMutable()
					} else {
						mutBuilder = dbdesc.NewBuilder(t.Database)
						mut = mutBuilder.BuildCreatedMutable()
					}
				case *descpb.Descriptor_Schema:
					t.Schema.Version = 1
					var mutBuilder schemadesc.SchemaDescriptorBuilder
					if existingRawBytes != nil {
						mutBuilder = existingDesc.NewBuilder().(schemadesc.SchemaDescriptorBuilder)
						newDescBytes, err := protoutil.Marshal(t.Schema)
						if err != nil {
							return err
						}
						if err := protoutil.Unmarshal(newDescBytes, mutBuilder.BuildExistingMutableSchema().SchemaDesc()); err != nil {
							return err
						}
						mut = mutBuilder.BuildExistingMutable()
					} else {
						mutBuilder = schemadesc.NewBuilder(t.Schema)
						mut = mutBuilder.BuildCreatedMutable()
					}
				case *descpb.Descriptor_Function:
					t.Function.Version = 1
					var mutBuilder funcdesc.FunctionDescriptorBuilder
					if existingRawBytes != nil {
						mutBuilder = existingDesc.NewBuilder().(funcdesc.FunctionDescriptorBuilder)
						newDescBytes, err := protoutil.Marshal(t.Function)
						if err != nil {
							return err
						}
						if err := protoutil.Unmarshal(newDescBytes, mutBuilder.BuildExistingMutableFunction().FuncDesc()); err != nil {
							return err
						}
						mut = mutBuilder.BuildExistingMutable()
					} else {
						mutBuilder = funcdesc.NewBuilder(t.Function)
						mut = mutBuilder.BuildCreatedMutable()
					}
				case *descpb.Descriptor_Type:
					t.Type.Version = 1
					var mutBuilder typedesc.TypeDescriptorBuilder
					if existingRawBytes != nil {
						mutBuilder = existingDesc.NewBuilder().(typedesc.TypeDescriptorBuilder)
						newDescBytes, err := protoutil.Marshal(t.Type)
						if err != nil {
							return err
						}
						if err := protoutil.Unmarshal(newDescBytes, mutBuilder.BuildExistingMutableType()); err != nil {
							return err
						}
						mut = mutBuilder.BuildExistingMutable()
					} else {
						mutBuilder = typedesc.NewBuilder(t.Type)
						mut = mutBuilder.BuildCreatedMutable()
					}
				}
				return errors.Wrapf(txn.Descriptors().WriteDescToBatch(ctx, true, mut, b),
					"unable to create replicated descriptor: %d %T", mut.GetID(), mut)
			}); err != nil {
				return err
			}
			if err := extracted.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
				if !shouldSetupForReader(e.GetID(), e.GetParentID()) {
					return nil
				}
				namespaceUpdated.Add(e.GetID())
				return errors.Wrapf(txn.Descriptors().UpsertNamespaceEntryToBatch(ctx, true, e, b), "namespace entry %v", e)
			}); err != nil {
				return err
			}
			// Figure out which descriptors should be deleted.
			if err := allExistingDescs.ForEachDescriptor(func(desc catalog.Descriptor) error {
				// Skip descriptors that were updated above
				if !shouldSetupForReader(desc.GetID(), desc.GetParentID()) ||
					descriptorsUpdated.Contains(desc.GetID()) {
					return nil
				}
				// Delete the descriptor from the batch
				return errors.Wrapf(txn.Descriptors().DeleteDescToBatch(ctx, true, desc.GetID(), b),
					"deleting descriptor")
			}); err != nil {
				return err
			}
			// Figure out which namespaces should be deleted.
			if err := allExistingDescs.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
				// Skip descriptors that were updated above
				if !shouldSetupForReader(e.GetID(), e.GetParentID()) ||
					descriptorsUpdated.Contains(e.GetID()) {
					return nil
				}
				return errors.Wrapf(txn.Descriptors().DeleteNamespaceEntryToBatch(ctx, true, e, b),
					"deleting namespace")
			}); err != nil {
				return err
			}
			return errors.Wrap(txn.KV().Run(ctx, b), "executing bach for updating catalog")
		})
}

// shouldSetupForReader determines if a descriptor should be setup
// access via external row data.
func shouldSetupForReader(id descpb.ID, parentID descpb.ID) bool {
	switch id {
	case keys.UsersTableID, keys.RoleMembersTableID, keys.RoleOptionsTableID,
		keys.DatabaseRoleSettingsTableID, keys.TableStatisticsTableID:
		return true
	default:
		return parentID != keys.SystemDatabaseID &&
			id != keys.SystemDatabaseID
	}
}

// getCatalogForTenantAsOf reads the descriptors from a given tenant
// at the given timestamp.
func getCatalogForTenantAsOf(
	ctx context.Context,
	st *cluster.Settings,
	db *kv.DB,
	tenantID roachpb.TenantID,
	asOf hlc.Timestamp,
) (all nstree.Catalog, _ error) {
	cf := descs.NewBareBonesCollectionFactory(st, keys.MakeSQLCodec(tenantID))
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		descsCol := cf.NewCollection(ctx)
		defer descsCol.ReleaseAll(ctx)
		all, err = descsCol.GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}

		return nil
	})
	return all, err
}
