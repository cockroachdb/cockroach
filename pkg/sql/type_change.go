// Copyright 2020 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// findTransitioningMembers returns a list of all physical representations that
// are being mutated (either being added or removed) in the current txn by
// diffing mutated type descriptor against the one read from the cluster.
func findTransitioningMembers(desc *typedesc.Mutable) [][]byte {
	var transitioningMembers [][]byte

	// If the type descriptor was created fresh in the current transaction, then
	// there is no cluster version to diff against. All members the type is
	// initially created with are PUBLIC. If any non-PUBLIC enum member exists on
	// the type, it must be the case that it was a result of an ALTER TYPE command
	// in the same transaction. As such, the job created for the transaction is
	// responsible to transition those members appropriately.
	if desc.IsNew() {
		for _, member := range desc.EnumMembers {
			if member.Capability != descpb.TypeDescriptor_EnumMember_ALL {
				transitioningMembers = append(transitioningMembers, member.PhysicalRepresentation)
			}
		}
		return transitioningMembers
	}

	// We diff against the cluster version in the general case.
	for _, member := range desc.EnumMembers {
		found := false
		for _, clusterMember := range desc.ClusterVersion.EnumMembers {
			if bytes.Equal(member.PhysicalRepresentation, clusterMember.PhysicalRepresentation) {
				found = true
				if member.Capability != clusterMember.Capability {
					transitioningMembers = append(transitioningMembers, member.PhysicalRepresentation)
				}
				break
			}
		}

		if !found {
			transitioningMembers = append(transitioningMembers, member.PhysicalRepresentation)
		}
	}
	return transitioningMembers
}

// writeTypeSchemaChange should be called on a mutated type descriptor to ensure that
// the descriptor gets written to a batch, as well as ensuring that a job is
// created to perform the schema change on the type.
func (p *planner) writeTypeSchemaChange(
	ctx context.Context, typeDesc *typedesc.Mutable, jobDesc string,
) error {
	// Check if there is an active job for this type, otherwise create one.
	job, jobExists := p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID]
	transitioningMembers := findTransitioningMembers(typeDesc)
	if jobExists {
		// Update it.
		newDetails := jobspb.TypeSchemaChangeDetails{
			TypeID:               typeDesc.ID,
			TransitioningMembers: transitioningMembers,
		}
		if err := job.SetDetails(ctx, p.txn, newDetails); err != nil {
			return err
		}
		if err := job.SetDescription(ctx, p.txn,
			func(ctx context.Context, description string) (string, error) {
				return description + "; " + jobDesc, nil
			},
		); err != nil {
			return err
		}
		log.Infof(ctx, "job %d: updated with type change for type %d", job.ID(), typeDesc.ID)
	} else {
		// Or, create a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descpb.IDs{typeDesc.ID},
			Details: jobspb.TypeSchemaChangeDetails{
				TypeID:               typeDesc.ID,
				TransitioningMembers: transitioningMembers,
			},
			Progress: jobspb.TypeSchemaChangeProgress{},
			// Type change jobs are not cancellable.
			NonCancelable: true,
		}
		newJob, err := p.extendedEvalCtx.QueueJob(ctx, jobRecord)
		if err != nil {
			return err
		}
		p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID] = newJob
		log.Infof(ctx, "queued new type change job %d for type %d", newJob.ID(), typeDesc.ID)
	}

	return p.writeTypeDesc(ctx, typeDesc)
}

func (p *planner) writeTypeDesc(ctx context.Context, typeDesc *typedesc.Mutable) error {
	// Write the type out to a batch.
	b := p.txn.NewBatch()
	if err := p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), typeDesc, b,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

// typeSchemaChanger is the struct that actually runs the type schema change.
type typeSchemaChanger struct {
	typeID descpb.ID
	// transitioningMembers is a list of enum members, represented by their
	// physical representation, that need to be transitioned in the job created
	// for a typeSchemaChanger. This is used to group transitions together and
	// ensure proper rollback semantics on job failure.
	transitioningMembers [][]byte
	execCfg              *ExecutorConfig
}

// TypeSchemaChangerTestingKnobs contains testing knobs for the typeSchemaChanger.
type TypeSchemaChangerTestingKnobs struct {
	// TypeSchemaChangeJobNoOp returning true will cause the job to be a no-op.
	TypeSchemaChangeJobNoOp func() bool
	// RunBeforeExec runs at the start of the typeSchemaChanger.
	RunBeforeExec func() error
	// RunBeforeEnumMemberPromotion runs before enum members are promoted from
	// readable to all permissions in the typeSchemaChanger.
	RunBeforeEnumMemberPromotion func()
	// RunAfterOnFailOrCancel runs after OnFailOrCancel completes, if
	// OnFailOrCancel is triggered.
	RunAfterOnFailOrCancel func() error
	// RunBeforeMultiRegionUpdates is a multi-region specific testing knob which
	// runs after enum promotion and before multi-region updates (such as
	// repartitioning tables, applying zone configs etc.)
	RunBeforeMultiRegionUpdates func() error
}

// ModuleTestingKnobs implements the ModuleTestingKnobs interface.
func (TypeSchemaChangerTestingKnobs) ModuleTestingKnobs() {}

func (t *typeSchemaChanger) getTypeDescFromStore(ctx context.Context) (*typedesc.Immutable, error) {
	var typeDesc *typedesc.Immutable
	if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := catalogkv.GetDescriptorByID(ctx, txn, t.execCfg.Codec, t.typeID,
			catalogkv.Immutable, catalogkv.TypeDescriptorKind, true /* required */)
		if err != nil {
			return err
		}
		typeDesc = desc.(*typedesc.Immutable)
		return nil
	}); err != nil {
		return nil, err
	}
	return typeDesc, nil
}

// exec is the entry point for the type schema change process.
func (t *typeSchemaChanger) exec(ctx context.Context) error {
	if t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeExec != nil {
		if err := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeExec(); err != nil {
			return err
		}
	}
	ctx = logtags.AddTags(ctx, t.logTags())
	leaseMgr := t.execCfg.LeaseManager
	codec := t.execCfg.Codec

	typeDesc, err := t.getTypeDescFromStore(ctx)
	if err != nil {
		return err
	}

	// If there are any names to drain, then do so.
	if len(typeDesc.DrainingNames) > 0 {
		if err := drainNamesForDescriptor(
			ctx, t.execCfg.Settings, typeDesc.GetID(), t.execCfg.DB, t.execCfg.InternalExecutor,
			leaseMgr, codec, nil,
		); err != nil {
			return err
		}
	}

	// Make sure all of the leases have dropped before attempting to validate.
	if err := WaitToUpdateLeases(ctx, leaseMgr, t.typeID); err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil
		}
		return err
	}

	// For all the read only members the current job is responsible for, either
	// promote them to writeable or remove them from the descriptor entirely,
	// as dictated by the direction.
	if (typeDesc.Kind == descpb.TypeDescriptor_ENUM ||
		typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM) &&
		len(t.transitioningMembers) != 0 {
		if fn := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeEnumMemberPromotion; fn != nil {
			fn()
		}

		// First, we check if any of the enum values that are being removed are in
		// use and fail. This is done in a separate txn to the one that mutates the
		// descriptor, as this validation can take arbitrarily long.
		validateDrops := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			typeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, t.typeID)
			if err != nil {
				return err
			}
			for _, member := range typeDesc.EnumMembers {
				if t.isTransitioningInCurrentJob(&member) && enumMemberIsRemoving(&member) {
					if err := t.canRemoveEnumValue(ctx, typeDesc, txn, &member, descsCol); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := descs.Txn(
			ctx, t.execCfg.Settings, t.execCfg.LeaseManager,
			t.execCfg.InternalExecutor, t.execCfg.DB, validateDrops,
		); err != nil {
			return err
		}

		// A list of multi-region tables that were repartitioned as a result of
		// promotion/demotion of enum values. This is used to track tables whose
		// leases need to be invalidated.
		var repartitionedTables []descpb.ID

		// Now that we've ascertained that the enum values can be removed, we can
		// actually go about modifying the type descriptor.

		// The version of the array type needs to get bumped as well so that
		// changes to the underlying type are picked up.
		run := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			typeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, t.typeID)
			if err != nil {
				return err
			}
			// First, deal with all members that need to be promoted to writable.
			for i := range typeDesc.EnumMembers {
				member := &typeDesc.EnumMembers[i]
				if t.isTransitioningInCurrentJob(member) && enumMemberIsAdding(member) {
					member.Capability = descpb.TypeDescriptor_EnumMember_ALL
					member.Direction = descpb.TypeDescriptor_EnumMember_NONE
				}
			}
			// Next, deal with all the members that need to be removed from the slice.
			applyFilterOnEnumMembers(typeDesc, func(member *descpb.TypeDescriptor_EnumMember) bool {
				return t.isTransitioningInCurrentJob(member) && enumMemberIsRemoving(member)
			})

			b := txn.NewBatch()
			if err := descsCol.WriteDescToBatch(
				ctx, true /* kvTrace */, typeDesc, b,
			); err != nil {
				return err
			}

			// Additional work must be performed once the promotion/demotion of enum
			// members has been taken care of. In particular, index partitions for
			// REGIONAL BY ROW tables must be updated to reflect the new region values
			// available.
			if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
				immut, err := descsCol.GetImmutableTypeByID(ctx, txn, t.typeID, tree.ObjectLookupFlags{})
				if err != nil {
					return err
				}
				if fn := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeMultiRegionUpdates; fn != nil {
					return fn()
				}
				repartitionedTables, err = performMultiRegionFinalization(
					ctx,
					immut,
					txn,
					t.execCfg,
					descsCol,
				)
				if err != nil {
					return err
				}
			}

			return txn.Run(ctx, b)
		}
		if err := descs.Txn(
			ctx, t.execCfg.Settings, t.execCfg.LeaseManager,
			t.execCfg.InternalExecutor, t.execCfg.DB, run,
		); err != nil {
			return err
		}

		// If any tables were repartitioned, make sure their leases are updated as
		// well.
		for _, tbID := range repartitionedTables {
			if err := WaitToUpdateLeases(ctx, leaseMgr, tbID); err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					// Swallow.
					log.Infof(ctx,
						"could not find table %d to be repartitioned when adding/removing regions on "+
							"enum %d, assuming it was dropped and moving on",
						tbID,
						t.typeID,
					)
				}
				return err
			}
		}

		// Finally, make sure all of the type descriptor leases are updated.
		if err := WaitToUpdateLeases(ctx, leaseMgr, t.typeID); err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				return nil
			}
			return err
		}
	}

	// If the type is being dropped, remove the descriptor here.
	if typeDesc.Dropped() {
		if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			b.Del(catalogkeys.MakeDescMetadataKey(codec, typeDesc.ID))
			return txn.Run(ctx, b)
		}); err != nil {
			return err
		}
	}
	return nil
}

// performMultiRegionFinalization updates the zone configurations on the
// database and re-partitions all REGIONAL BY ROW tables after REGION ADD/DROP
// has completed. A list of re-partitioned tables, if any, is returned.
func performMultiRegionFinalization(
	ctx context.Context,
	typeDesc *typedesc.Immutable,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
) ([]descpb.ID, error) {
	_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
		ctx, txn, typeDesc.ParentID, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, err
	}
	// Once the region promotion/demotion is complete, we update the
	// zone configuration on the database.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		dbDesc.ID,
		*dbDesc.RegionConfig,
		txn,
		execCfg,
	); err != nil {
		return nil, err
	}

	return repartitionRegionalByRowTables(ctx, typeDesc, dbDesc, txn, execCfg, descsCol)
}

// repartitionRegionalByRowTables takes a multi-region enum and re-partitions
// all REGIONAL BY ROW tables in the enclosing database such that there is a
// partition and corresponding zone configuration for all PUBLIC enum members
// (regions).
// This currently doesn't work too well if there are READ ONLY member on the
// type. This is because we create the partitioning clause based on the regions
// on the database descriptor, which may include the READ ONLY member, and
// partitioning on a READ ONLY member doesn't work. This will go away once
// https://github.com/cockroachdb/cockroach/issues/60620 is fixed.
func repartitionRegionalByRowTables(
	ctx context.Context,
	typeDesc *typedesc.Immutable,
	dbDesc *dbdesc.Immutable,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	descsCol *descs.Collection,
) ([]descpb.ID, error) {
	var repartitionedTableIDs []descpb.ID
	if typeDesc.GetKind() != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return repartitionedTableIDs, errors.AssertionFailedf(
			"expected multi-region enum, but found type descriptor of kind: %v", typeDesc.GetKind(),
		)
	}
	p, cleanup := NewInternalPlanner(
		"repartition-regional-by-row-tables",
		txn,
		security.RootUserName(),
		&MemoryMetrics{},
		execCfg,
		sessiondatapb.SessionData{},
		WithDescCollection(descsCol),
	)
	defer cleanup()
	localPlanner := p.(*planner)

	allDescs, err := localPlanner.Descriptors().GetAllDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}
	lCtx := newInternalLookupCtx(ctx, allDescs, dbDesc, nil /* fallback */)

	b := txn.NewBatch()
	for _, tbID := range lCtx.tbIDs {
		tableDesc, err := localPlanner.Descriptors().GetMutableTableByID(
			ctx, txn, tbID, tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       true,
					IncludeDropped: true,
				},
			})
		if err != nil {
			return nil, err
		}

		if !tableDesc.IsLocalityRegionalByRow() || tableDesc.Dropped() {
			// We only need to re-partition REGIONAL BY ROW tables. Even then, we
			// don't need to (can't) repartition a REGIONAL BY ROW table if it has
			// been dropped.
			continue
		}

		colName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return nil, err
		}
		partitionAllBy := partitionByForRegionalByRow(*dbDesc.RegionConfig, colName)

		// oldPartitioningDescs saves the old partitioning descriptors for each
		// index that is repartitioned. This is later used to remove zone
		// configurations from any partitions that are removed.
		oldPartitioningDescs := make(map[descpb.IndexID]descpb.PartitioningDescriptor)

		// Update the partitioning on all indexes of the table that aren't being
		// dropped.
		for _, index := range tableDesc.NonDropIndexes() {
			newIdx, err := CreatePartitioning(
				ctx,
				localPlanner.extendedEvalCtx.Settings,
				localPlanner.EvalContext(),
				tableDesc,
				*index.IndexDesc(),
				partitionAllBy,
				nil,  /* allowedNewColumnName*/
				true, /* allowImplicitPartitioning */
			)
			if err != nil {
				return nil, err
			}

			oldPartitioningDescs[index.GetID()] = index.IndexDesc().Partitioning

			// Update the index descriptor proto's partitioning.
			index.IndexDesc().Partitioning = newIdx.Partitioning
		}

		// Remove zone configurations that applied to partitions that were removed
		// in the previous step. This requires all indexes to have been
		// repartitioned such that there is no partitioning on the removed enum
		// value. This is because `deleteRemovedPartitionZoneConfigs` generates
		// subzone spans for the entire table (all indexes) downstream for each
		// index. Spans can only be generated if partitioning values are present on
		// the type descriptor (removed enum values obviously aren't), so we must
		// remove the partition from all indexes before trying to delete zone
		// configurations.
		for _, index := range tableDesc.NonDropIndexes() {
			oldPartitioning := oldPartitioningDescs[index.GetID()]

			// Remove zone configurations that reference partition values we removed
			// in the previous step.
			if err = deleteRemovedPartitionZoneConfigs(
				ctx,
				txn,
				tableDesc,
				index.IndexDesc(),
				&oldPartitioning,
				&index.IndexDesc().Partitioning,
				execCfg,
			); err != nil {
				return nil, err
			}
		}

		// Update the zone configurations now that the partition's been added.
		if err := ApplyZoneConfigForMultiRegionTable(
			ctx,
			txn,
			localPlanner.ExecCfg(),
			*dbDesc.RegionConfig,
			tableDesc,
			ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
		); err != nil {
			return nil, err
		}

		if err := localPlanner.Descriptors().WriteDescToBatch(ctx, false /* kvTrace */, tableDesc, b); err != nil {
			return nil, err
		}

		repartitionedTableIDs = append(repartitionedTableIDs, tbID)
	}
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}

	return repartitionedTableIDs, nil
}

// isTransitioningInCurrentJob returns true if the given member is either being
// added or removed in the current job.
func (t *typeSchemaChanger) isTransitioningInCurrentJob(
	member *descpb.TypeDescriptor_EnumMember,
) bool {
	for _, rep := range t.transitioningMembers {
		if bytes.Equal(member.PhysicalRepresentation, rep) {
			return true
		}
	}
	return false
}

// applyFilterOnEnumMembers modifies the supplied typeDesc by removing all enum
// members as dictated by shouldRemove.
func applyFilterOnEnumMembers(
	typeDesc *typedesc.Mutable, shouldRemove func(member *descpb.TypeDescriptor_EnumMember) bool,
) {
	idx := 0
	for _, member := range typeDesc.EnumMembers {
		if shouldRemove(&member) {
			// By not updating the index, the truncation logic below will remove
			// this label from the list of members.
			continue
		}
		typeDesc.EnumMembers[idx] = member
		idx++
	}
	typeDesc.EnumMembers = typeDesc.EnumMembers[:idx]
}

// cleanupEnumValues performs cleanup if any of the enum value transitions
// fails. In particular:
// 1. If an enum value was being added as part of this txn, we remove it
// from the descriptor.
// 2. If an enum value was being removed as part of this txn, we promote
// it back to writable.
func (t *typeSchemaChanger) cleanupEnumValues(ctx context.Context) error {
	// Cleanup:
	cleanup := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		typeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, t.typeID)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		// No cleanup required.
		if !enumHasNonPublic(&typeDesc.Immutable) {
			return nil
		}

		// First, we deal with cleanup specific to the multi-region enum.
		// TODO(arul): This can go away once we stop copying regions on the dataabse
		// descriptor.
		if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
			for _, member := range typeDesc.EnumMembers {
				if t.isTransitioningInCurrentJob(&member) {
					_, dbDesc, err := descsCol.GetMutableDatabaseByID(
						ctx, txn, typeDesc.ParentID, tree.DatabaseLookupFlags{Required: true})
					if err != nil {
						return err
					}

					// If the enum member was being removed, we need to add it back to the
					// database region config.
					if enumMemberIsRemoving(&member) {
						err = addRegionToRegionConfig(dbDesc, descpb.RegionName(member.LogicalRepresentation))
						if err != nil {
							return err
						}
						if err := descsCol.WriteDescToBatch(ctx, true /* kvTrace */, dbDesc, b); err != nil {
							return err
						}
					}

					// If the enum member was being added, we must remove it from the
					// database region config.
					if enumMemberIsAdding(&member) {
						idx := 0
						found := false
						for i, region := range dbDesc.RegionConfig.Regions {
							if region.Name == descpb.RegionName(member.LogicalRepresentation) {
								idx = i
								found = true
								break
							}
						}

						if !found {
							// This shouldn't happen and is simply a sanity check to ensure the
							// database descriptor regions and multi-region enum regions are
							// in sync.
							return errors.AssertionFailedf(
								"expected to find region on database %d during cleanup", dbDesc.GetID(),
							)
						}
						dbDesc.RegionConfig.Regions = append(dbDesc.RegionConfig.Regions[:idx],
							dbDesc.RegionConfig.Regions[idx+1:]...)
						if err := descsCol.WriteDescToBatch(ctx, true /* kvTrace */, dbDesc, b); err != nil {
							return err
						}
					}
				}
			}
		}

		// Next, deal with all members that we initially hoped to remove but
		// now need to be promoted back to writable.
		for i := range typeDesc.EnumMembers {
			member := &typeDesc.EnumMembers[i]
			if t.isTransitioningInCurrentJob(member) && enumMemberIsRemoving(member) {
				member.Capability = descpb.TypeDescriptor_EnumMember_ALL
				member.Direction = descpb.TypeDescriptor_EnumMember_NONE
			}
		}
		// Now deal with all members that we initially hoped to add but now need
		// to be removed from the descriptor.
		applyFilterOnEnumMembers(typeDesc, func(member *descpb.TypeDescriptor_EnumMember) bool {
			return t.isTransitioningInCurrentJob(member) && enumMemberIsAdding(member)
		})

		if err := descsCol.WriteDescToBatch(
			ctx, true /* kvTrace */, typeDesc, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}
	if err := descs.Txn(ctx, t.execCfg.Settings, t.execCfg.LeaseManager, t.execCfg.InternalExecutor,
		t.execCfg.DB, cleanup); err != nil {
		return err
	}

	// Finally, make sure all of the leases are updated.
	if err := WaitToUpdateLeases(ctx, t.execCfg.LeaseManager, t.typeID); err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// canRemoveEnumValue returns an error if the enum value is in use and therefore
// can't be removed.
func (t *typeSchemaChanger) canRemoveEnumValue(
	ctx context.Context,
	typeDesc *typedesc.Mutable,
	txn *kv.Txn,
	member *descpb.TypeDescriptor_EnumMember,
	descsCol *descs.Collection,
) error {
	// convertToSQLStringRepresentation takes an array of bytes (the physical
	// representation of an enum) and converts it into a string that can be used
	// in a SQL predicate.
	convertToSQLStringRepresentation := func(bytes []byte) string {
		var byteRep strings.Builder
		byteRep.WriteString("b'")
		for _, b := range bytes {
			byteRep.WriteByte(b)
		}
		byteRep.WriteString("'")
		return byteRep.String()
	}

	for _, ID := range typeDesc.ReferencingDescriptorIDs {
		desc, err := descsCol.GetImmutableTableByID(ctx, txn, ID, tree.ObjectLookupFlags{})
		if err != nil {
			return errors.Wrapf(err,
				"could not validate enum value removal for %q", member.LogicalRepresentation)
		}
		var query strings.Builder
		colSelectors := tabledesc.ColumnsSelectors(desc.PublicColumns())
		columns := tree.AsStringWithFlags(&colSelectors, tree.FmtSerializable)
		query.WriteString(fmt.Sprintf("SELECT %s FROM [%d as t] WHERE", columns, ID))
		firstClause := true
		validationQueryConstructed := false
		for _, col := range desc.PublicColumns() {
			if typeDesc.ID == typedesc.GetTypeDescID(col.GetType()) {
				if !firstClause {
					query.WriteString(" OR")
				}
				query.WriteString(fmt.Sprintf(" t.%s = %s", col.GetName(),
					convertToSQLStringRepresentation(member.PhysicalRepresentation)))
				firstClause = false
				validationQueryConstructed = true
			}
		}
		query.WriteString(" LIMIT 1")

		// NB: A type descriptor reference does not imply at-least one column in the
		// table is of the type whose value is being removed. The notable exception
		// being REGIONAL BY TABLE multi-region tables. In this case, no valid query
		// is constructed and there's nothing to execute. Instead, their validation
		// is handled as a special case below.
		if validationQueryConstructed {
			// We need to override the internal executor's current database (which would
			// be unset by default) when executing the query constructed above. This is
			// because the enum value may be used in a view expression, which is
			// name resolved in the context of the type's database.
			_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
				ctx, txn, typeDesc.ParentID, tree.DatabaseLookupFlags{Required: true})
			const validationErr = "could not validate removal of enum value %q"
			if err != nil {
				return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
			}
			override := sessiondata.InternalExecutorOverride{
				User:     security.RootUserName(),
				Database: dbDesc.Name,
			}
			rows, err := t.execCfg.InternalExecutor.QueryRowEx(ctx, "count-value-usage", txn, override, query.String())
			if err != nil {
				return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
			}
			// Check if the above query returned a result. If it did, then the
			// enum value is being used by some place.
			if len(rows) > 0 {
				return pgerror.Newf(pgcode.DependentObjectsStillExist,
					"could not remove enum value %q as it is being used by %q in row: %s",
					member.LogicalRepresentation, desc.GetName(), labeledRowValues(desc.PublicColumns(), rows))
			}
		}

		// If the type descriptor is a multi-region enum and the table descriptor
		// belongs to a regional (by table) table, we disallow dropping the region
		// if it is being used as the homed region for that table.
		if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM && desc.IsLocalityRegionalByTable() {
			homedRegion, err := desc.GetRegionalByTableRegion()
			if err != nil {
				return err
			}
			if descpb.RegionName(member.LogicalRepresentation) == homedRegion {
				return errors.Newf("could not remove enum value %q as it is the home region for table %q",
					member.LogicalRepresentation, desc.GetName())
			}
		}
	}
	// We have ascertained that the value is not in use, and can therefore be
	// safely removed.
	return nil
}

func enumHasNonPublic(typeDesc *typedesc.Immutable) bool {
	hasNonPublic := false
	for _, member := range typeDesc.EnumMembers {
		if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY {
			hasNonPublic = true
			break
		}
	}
	return hasNonPublic
}

func enumMemberIsAdding(member *descpb.TypeDescriptor_EnumMember) bool {
	if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY &&
		member.Direction == descpb.TypeDescriptor_EnumMember_ADD {
		return true
	}
	return false
}

func enumMemberIsRemoving(member *descpb.TypeDescriptor_EnumMember) bool {
	if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY &&
		member.Direction == descpb.TypeDescriptor_EnumMember_REMOVE {
		return true
	}
	return false
}

// execWithRetry is a wrapper around exec that retries the type schema change
// on retryable errors.
func (t *typeSchemaChanger) execWithRetry(ctx context.Context) error {
	// Set up the type changer to be retried.
	opts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     20 * time.Second,
		Multiplier:     1.5,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		tcErr := t.exec(ctx)
		switch {
		case tcErr == nil:
			return nil
		case errors.Is(tcErr, catalog.ErrDescriptorNotFound):
			// If the descriptor for the ID can't be found, we assume that another
			// job executed already and dropped the type.
			log.Infof(
				ctx,
				"descriptor %d not found for type change job; assuming it was dropped, and exiting",
				t.typeID,
			)
			return nil
		case !isPermanentSchemaChangeError(tcErr):
			// If this isn't a permanent error, then retry.
			log.Infof(ctx, "retrying type schema change due to retriable error %v", tcErr)
		default:
			return tcErr
		}
	}
	return nil
}

func (t *typeSchemaChanger) logTags() *logtags.Buffer {
	buf := &logtags.Buffer{}
	buf.Add("typeChangeExec", nil)
	buf.Add("type", t.typeID)
	return buf
}

// typeChangeResumer is the anchor struct for the type change job.
type typeChangeResumer struct {
	job *jobs.Job
}

// Resume implements the jobs.Resumer interface.
func (t *typeChangeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	if p.ExecCfg().TypeSchemaChangerTestingKnobs.TypeSchemaChangeJobNoOp != nil {
		if p.ExecCfg().TypeSchemaChangerTestingKnobs.TypeSchemaChangeJobNoOp() {
			return nil
		}
	}
	tc := &typeSchemaChanger{
		typeID:               t.job.Details().(jobspb.TypeSchemaChangeDetails).TypeID,
		transitioningMembers: t.job.Details().(jobspb.TypeSchemaChangeDetails).TransitioningMembers,
		execCfg:              p.ExecCfg(),
	}
	return tc.execWithRetry(ctx)
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t *typeChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	// If the job failed, just try again to clean up any draining names.
	tc := &typeSchemaChanger{
		typeID:               t.job.Details().(jobspb.TypeSchemaChangeDetails).TypeID,
		transitioningMembers: t.job.Details().(jobspb.TypeSchemaChangeDetails).TransitioningMembers,
		execCfg:              execCtx.(JobExecContext).ExecCfg(),
	}

	if rollbackErr := func() error {
		if err := tc.cleanupEnumValues(ctx); err != nil {
			return err
		}

		if err := drainNamesForDescriptor(
			ctx, tc.execCfg.Settings, tc.typeID, tc.execCfg.DB,
			tc.execCfg.InternalExecutor, tc.execCfg.LeaseManager, tc.execCfg.Codec, nil,
		); err != nil {
			return err
		}

		if fn := tc.execCfg.TypeSchemaChangerTestingKnobs.RunAfterOnFailOrCancel; fn != nil {
			return fn()
		}

		return nil
	}(); rollbackErr != nil {
		switch {
		case errors.Is(rollbackErr, catalog.ErrDescriptorNotFound):
			// If the descriptor for the ID can't be found, we assume that another
			// job executed already and dropped the type.
			log.Infof(
				ctx,
				"descriptor %d not found for type change job; assuming it was dropped, and exiting",
				tc.typeID,
			)
		case !isPermanentSchemaChangeError(rollbackErr):
			return jobs.NewRetryJobError(rollbackErr.Error())
		default:
			return rollbackErr
		}
	}

	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &typeChangeResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeTypeSchemaChange, createResumerFn)
}
