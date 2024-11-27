// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// SetupOrAdvanceStandbyReaderCatalog when invoked inside the reader
// tenant will replicate the descriptors from the tenant specified
// by fromID. The replicated descriptors will be setup such that they
// will access data from fromID. If the descriptors are already replicated
// then this function will advance the timestamp.
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
			// Allow modifications to the reader catalog mutable.
			txn.Descriptors().SetReaderCatalogSetup()
			// Track which descriptors / namespaces that have been updated,
			// the difference between any existing tenant in the reader
			// catalog will be deleted (i.e. these are descriptors that exist
			// in the reader tenant, but not in the from tenant which we are
			// replicating).
			descriptorsUpdated := catalog.DescriptorIDSet{}
			allExistingDescs, err := txn.Descriptors().GetAll(ctx, txn.KV())
			if err != nil {
				return err
			}
			// Resolve any existing descriptors within the tenant, which
			// will be use to compute old values for writing.
			b := txn.KV().NewBatch()
			if err := extracted.ForEachDescriptor(func(fromDesc catalog.Descriptor) error {
				if !shouldSetupForReader(fromDesc.GetID(), fromDesc.GetParentID()) {
					return nil
				}
				// Track this descriptor was updated.
				descriptorsUpdated.Add(fromDesc.GetID())
				// If there is an existing descriptor with the same ID, we should
				// determine the old bytes in storage for the upsert.
				existingDesc, err := txn.Descriptors().MutableByID(txn.KV()).Desc(ctx, fromDesc.GetID())
				if err != nil &&
					!errors.Is(err, catalog.ErrDescriptorNotFound) {
					return err
				} else {
					err = nil
				}
				// Existing descriptor should have the parent ID, since we should only
				// be updating it to reflect the latest timestamp.
				if existingDesc != nil &&
					existingDesc.GetParentID() != fromDesc.GetParentID() {
					return errors.AssertionFailedf("existing descriptor in the reader catalog "+
						"collides with a descriptor in the from tenant, with differring parent databases.\n"+
						"existing descriptor %s (id: %d, parentID: %d)\n "+
						"from descriptor: %s (id: %d, parentID: %d)\n",
						existingDesc.GetName(), existingDesc.GetID(), existingDesc.GetParentID(),
						fromDesc.GetName(), fromDesc.GetID(), fromDesc.GetParentID())
				}

				var mut catalog.MutableDescriptor
				// If the descriptor has not been modified, then extend the timestamp only.
				if existingDesc != nil && existingDesc.GetReplicatedPCRVersion() == fromDesc.GetVersion() {
					if existingDesc.DescriptorType() != catalog.Table {
						return nil
					}
					mut, err = advanceTimestampForTable(existingDesc, asOf)
					if err != nil || mut == nil {
						return err
					}
				} else {
					// Its possible that multiple descriptor versions could have gone by on
					// the from cluster, which could mean fairly large schema changes have
					// occured. This should be fine, because the leasing subsystem will look
					// at the modification time of a descriptor to determine which version should
					// be picked up by a txn. Since, we are publishing all descriptors in a single txn,
					// this would ensure that we don't end up using a mix of "new" descriptors in old txns,
					// since read timestamp will be below the modification time. The only potential hazard
					// that exists here is that newer txns could pick up older descriptors if the range feed
					// is behind.
					// TODO(fqazi): We should either change how big version jumps are handled here
					// or adjust leasing to publish in batches (latter would be more usable).
					mut, err = replicateDescriptorForReader(fromDesc, existingDesc, fromID, asOf)
					if err != nil {
						return err
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
				// Do not upsert entries if one already exists.
				entry := allExistingDescs.LookupNamespaceEntry(catalog.MakeNameInfo(e))
				if entry != nil && e.GetID() == entry.GetID() {
					return nil
				}
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
			if err := maybeBlockSchemaChangesOnSystemDatabase(ctx, txn, b); err != nil {
				return errors.Wrapf(err, "blocking schema changes")
			}
			return errors.Wrap(txn.KV().Run(ctx, b), "executing bach for updating catalog")
		})
}

func advanceTimestampForTable(
	descriptor catalog.MutableDescriptor, asOf hlc.Timestamp,
) (catalog.MutableDescriptor, error) {
	tbl := descriptor.(*tabledesc.Mutable)
	// Views can be skipped.
	if !tbl.IsPhysicalTable() {
		return nil, nil
	}
	return tbl, tbl.BumpExternalAsOf(asOf)
}

func replicateDescriptorForReader(
	fromDesc catalog.Descriptor,
	existingDesc catalog.MutableDescriptor,
	fromID roachpb.TenantID,
	asOf hlc.Timestamp,
) (catalog.MutableDescriptor, error) {
	var mut catalog.MutableDescriptor
	var existingRawBytes []byte
	if existingDesc != nil {
		existingRawBytes = existingDesc.GetRawBytesInStorage()
	}
	switch t := fromDesc.DescriptorProto().GetUnion().(type) {
	case *descpb.Descriptor_Table:
		t.Table.ReplicatedPCRVersion = t.Table.Version
		t.Table.Version = 1
		var mutBuilder tabledesc.TableDescriptorBuilder
		var mutTbl *tabledesc.Mutable
		if existingRawBytes != nil {
			t.Table.Version = existingDesc.GetVersion()
			mutBuilder = existingDesc.NewBuilder().(tabledesc.TableDescriptorBuilder)
			mutTbl = mutBuilder.BuildExistingMutableTable()
			mutTbl.TableDescriptor = *protoutil.Clone(t.Table).(*descpb.TableDescriptor)
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
			mutTbl.SetExternalRowData(&descpb.ExternalRowData{TenantID: fromID, TableID: fromDesc.GetID(), AsOf: asOf})
		}
	case *descpb.Descriptor_Database:
		t.Database.ReplicatedPCRVersion = t.Database.Version
		t.Database.Version = 1
		var mutBuilder dbdesc.DatabaseDescriptorBuilder
		if existingRawBytes != nil {
			t.Database.Version = existingDesc.GetVersion()
			mutBuilder = existingDesc.NewBuilder().(dbdesc.DatabaseDescriptorBuilder)
			mutDB := mutBuilder.BuildExistingMutableDatabase()
			mutDB.DatabaseDescriptor = *protoutil.Clone(t.Database).(*descpb.DatabaseDescriptor)
			mut = mutDB
		} else {
			mutBuilder = dbdesc.NewBuilder(t.Database)
			mut = mutBuilder.BuildCreatedMutable()
		}
	case *descpb.Descriptor_Schema:
		t.Schema.ReplicatedPCRVersion = t.Schema.Version
		t.Schema.Version = 1
		var mutBuilder schemadesc.SchemaDescriptorBuilder
		if existingRawBytes != nil {
			t.Schema.Version = existingDesc.GetVersion()
			mutBuilder = existingDesc.NewBuilder().(schemadesc.SchemaDescriptorBuilder)
			mutSchema := mutBuilder.BuildExistingMutableSchema()
			mutSchema.SchemaDescriptor = *protoutil.Clone(t.Schema).(*descpb.SchemaDescriptor)
			mut = mutSchema
		} else {
			mutBuilder = schemadesc.NewBuilder(t.Schema)
			mut = mutBuilder.BuildCreatedMutable()
		}
	case *descpb.Descriptor_Function:
		t.Function.ReplicatedPCRVersion = t.Function.Version
		t.Function.Version = 1
		var mutBuilder funcdesc.FunctionDescriptorBuilder
		if existingRawBytes != nil {
			t.Function.Version = existingDesc.GetVersion()
			mutBuilder = existingDesc.NewBuilder().(funcdesc.FunctionDescriptorBuilder)
			mutFunction := mutBuilder.BuildExistingMutableFunction()
			mutFunction.FunctionDescriptor = *protoutil.Clone(t.Function).(*descpb.FunctionDescriptor)
			mut = mutFunction
		} else {
			mutBuilder = funcdesc.NewBuilder(t.Function)
			mut = mutBuilder.BuildCreatedMutable()
		}
	case *descpb.Descriptor_Type:
		t.Type.ReplicatedPCRVersion = t.Type.Version
		t.Type.Version = 1
		var mutBuilder typedesc.TypeDescriptorBuilder
		if existingRawBytes != nil {
			t.Type.Version = existingDesc.GetVersion()
			mutBuilder = existingDesc.NewBuilder().(typedesc.TypeDescriptorBuilder)
			mutType := mutBuilder.BuildExistingMutableType()
			mutType.TypeDescriptor = *protoutil.Clone(t.Type).(*descpb.TypeDescriptor)
			mut = mutType
		} else {
			mutBuilder = typedesc.NewBuilder(t.Type)
			mut = mutBuilder.BuildCreatedMutable()
		}
	default:
		return nil, errors.AssertionFailedf("unknown descriptor type: %T", t)
	}
	return mut, nil
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

// maybeBlockSchemaChangesOnSystemDatabase blocks schema changes on the system
// database.
func maybeBlockSchemaChangesOnSystemDatabase(
	ctx context.Context, txn descs.Txn, ba *kv.Batch,
) error {
	systemDatabase, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return err
	}
	if systemDatabase.ReplicatedPCRVersion != 0 {
		return nil
	}
	systemDatabase.ReplicatedPCRVersion = 1
	return txn.Descriptors().WriteDescToBatch(ctx, true, systemDatabase, ba)
}
