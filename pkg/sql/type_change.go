// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// findTransitioningMembers returns a list of all physical representations that
// are being mutated (either being added or removed) in the current txn by
// diffing mutated type descriptor against the one read from the cluster. The
// second return parameter indicates whether at least one of the members is
// being dropped.
func findTransitioningMembers(desc *typedesc.Mutable) ([][]byte, bool) {
	var transitioningMembers [][]byte
	beingDropped := false

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
				if member.Direction == descpb.TypeDescriptor_EnumMember_REMOVE {
					beingDropped = true
				}
			}
		}
		return transitioningMembers, beingDropped
	}

	// We diff against the cluster version in the general case.
	for _, member := range desc.EnumMembers {
		found := false
		for _, clusterMember := range desc.ClusterVersion.EnumMembers {
			if bytes.Equal(member.PhysicalRepresentation, clusterMember.PhysicalRepresentation) {
				found = true
				if member.Capability != clusterMember.Capability {
					transitioningMembers = append(transitioningMembers, member.PhysicalRepresentation)
					if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY &&
						member.Direction == descpb.TypeDescriptor_EnumMember_REMOVE {
						beingDropped = true
					}
				}
				break
			}
		}

		if !found {
			transitioningMembers = append(transitioningMembers, member.PhysicalRepresentation)
		}
	}
	return transitioningMembers, beingDropped
}

// writeTypeSchemaChange should be called on a mutated type descriptor to ensure that
// the descriptor gets written to a batch, as well as ensuring that a job is
// created to perform the schema change on the type.
func (p *planner) writeTypeSchemaChange(
	ctx context.Context, typeDesc *typedesc.Mutable, jobDesc string,
) error {
	// Exit early with an error if the table is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(typeDesc) {
		return scerrors.ConcurrentSchemaChangeError(typeDesc)
	}
	// Check if there is a cached specification for this type, otherwise create one.
	record, recordExists := p.extendedEvalCtx.jobs.uniqueToCreate[typeDesc.ID]
	transitioningMembers, beingDropped := findTransitioningMembers(typeDesc)
	if recordExists {
		// Update it.
		newDetails := jobspb.TypeSchemaChangeDetails{
			TypeID:               typeDesc.ID,
			TransitioningMembers: transitioningMembers,
		}
		record.Details = newDetails
		record.AppendDescription(jobDesc)
		record.SetNonCancelable(ctx,
			func(ctx context.Context, nonCancelable bool) bool {
				// If the job is already cancelable, then it should stay as such
				// regardless of if a member is being dropped or not in the current
				// statement.
				if !nonCancelable {
					return nonCancelable
				}
				// Type change jobs are non-cancelable unless an enum member is being
				// dropped.
				return !beingDropped
			})
		log.Infof(ctx, "job %d: updated with type change for type %d", record.JobID, typeDesc.ID)
	} else {
		// Or, create a new job.
		newRecord := jobs.Record{
			JobID:         p.extendedEvalCtx.ExecCfg.JobRegistry.MakeJobID(),
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descpb.IDs{typeDesc.ID},
			Details: jobspb.TypeSchemaChangeDetails{
				TypeID:               typeDesc.ID,
				TransitioningMembers: transitioningMembers,
			},
			Progress: jobspb.TypeSchemaChangeProgress{},
			// Type change jobs in general are not cancelable, unless they include
			// a transition that drops an enum member.
			NonCancelable: !beingDropped,
		}
		p.extendedEvalCtx.jobs.uniqueToCreate[typeDesc.ID] = &newRecord
		log.Infof(ctx, "queued new type change job %d for type %d", newRecord.JobID, typeDesc.ID)
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

func (p *planner) writeDescToBatch(
	ctx context.Context, typeDesc catalog.MutableDescriptor, b *kv.Batch,
) error {
	return p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), typeDesc, b,
	)
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
	RunBeforeEnumMemberPromotion func(ctx context.Context) error
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

func (t *typeSchemaChanger) getTypeDescFromStore(
	ctx context.Context,
) (catalog.TypeDescriptor, error) {
	var typeDesc catalog.TypeDescriptor
	if err := DescsTxn(ctx, t.execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		// Avoid GetImmutableTypeByID, downstream logic relies on
		// catalog.ErrDescriptorNotFound.
		desc, err := col.ByIDWithoutLeased(txn.KV()).Get().Desc(ctx, t.typeID)
		if err != nil {
			return err
		}
		typeDesc, err = catalog.AsTypeDescriptor(desc)
		return err
	}); err != nil {
		return nil, err
	}
	return typeDesc, nil
}

// refreshTypeDescriptorLeases refreshes the lease on both the type descriptor
// and its array type descriptor (if one exists). If a descriptor is not found,
// it is assumed dropped, and the error is swallowed.
func refreshTypeDescriptorLeases(
	ctx context.Context, leaseMgr *lease.Manager, db *kv.DB, typeDesc catalog.TypeDescriptor,
) error {
	var err error
	var ids = []descpb.ID{typeDesc.GetID()}
	if aID := typeDesc.TypeDesc().ArrayTypeID; aID != descpb.InvalidID {
		ids = append(ids, aID)
	}
	cachedRegions, err := regions.NewCachedDatabaseRegions(ctx, db, leaseMgr)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, updateErr := WaitToUpdateLeases(ctx, leaseMgr, cachedRegions, id); updateErr != nil {
			// Swallow the descriptor not found error.
			if errors.Is(updateErr, catalog.ErrDescriptorNotFound) {
				log.Infof(ctx,
					"could not find type descriptor %d to refresh lease; "+
						"assuming it was dropped and moving on",
					id,
				)
			} else {
				err = errors.CombineErrors(err, updateErr)
			}
		}
	}
	return err
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
	const kvTrace = true

	typeDesc, err := t.getTypeDescFromStore(ctx)
	if err != nil {
		return err
	}

	// Make sure all of the leases have dropped before attempting to validate.
	if err := refreshTypeDescriptorLeases(ctx, leaseMgr, t.execCfg.DB, typeDesc); err != nil {
		return err
	}

	// For all the read only members the current job is responsible for, either
	// promote them to writeable or remove them from the descriptor entirely,
	// as dictated by the direction.
	if typeDesc.AsEnumTypeDescriptor() != nil && len(t.transitioningMembers) != 0 {
		if fn := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeEnumMemberPromotion; fn != nil {
			if err := fn(ctx); err != nil {
				return err
			}
		}

		// In the case where we're dropping elements from a multi-region enum,
		// we first re-partition all REGIONAL BY ROW tables. This is to handle
		// the dependency which exist between the partitioning and the enum.
		//
		// There are places in the query path (specifically, when we decode
		// the partitioning tuple) where we validate that for a given partition,
		// that it's respective value exists in the multi-region enum. In cases
		// where we're in the process of a DROP REGION however, if we don't
		// repartition the table first, we can get into a situation where the
		// query holds the new version of the enum type descriptor (in which
		// the partition has already been dropped) and the old version of the
		// table descriptor (in which the partition still exists). This
		// situation causes a panic, and the query fails.
		//
		// To address this issue, and only in the DROP REGION case, we
		// repartition the tables first, and drop the value from the enum in a
		// separate transaction. Note that we must refresh the table descriptors
		// before we proceed to the drop enum portion, so that we ensure that
		// any concurrent queries see the descriptor updates in the correct
		// order.
		//
		// It's also worth noting that we don't need to be concerned about
		// exposing things in the right order in OnFailOrCancel. This is because
		// OnFailOrCancel doesn't expose any new state in the type descriptor
		// (it just cleans up non-public states).
		var isDroppingMultiRegionEnumMember bool
		withDatabaseRegionChangeFinalizer := func(
			ctx context.Context, txn descs.Txn,
			f func(finalizer *databaseRegionChangeFinalizer) error,
		) error {
			typeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, t.typeID)
			if err != nil {
				return err
			}
			regionChangeFinalizer, err := newDatabaseRegionChangeFinalizer(
				ctx,
				txn,
				t.execCfg,
				typeDesc.GetParentID(),
				typeDesc.GetID(),
			)
			if err != nil {
				return err
			}
			defer regionChangeFinalizer.cleanup()
			return f(regionChangeFinalizer)
		}
		prepareRepartitionedRegionalByRowTables := func(
			ctx context.Context, txn descs.Txn,
		) (repartitioned []*tabledesc.Mutable, err error) {
			err = withDatabaseRegionChangeFinalizer(ctx, txn, func(
				finalizer *databaseRegionChangeFinalizer,
			) (err error) {
				repartitioned, _, err = finalizer.repartitionRegionalByRowTables(ctx, txn)
				return err
			})
			return repartitioned, err
		}
		repartitionRegionalByRowTables := func(
			ctx context.Context, txn descs.Txn,
		) error {
			return withDatabaseRegionChangeFinalizer(ctx, txn, func(
				finalizer *databaseRegionChangeFinalizer,
			) error {
				return finalizer.preDrop(ctx, txn)
			})
		}

		// First, we check if any of the enum values that are being removed are in
		// use and fail. This is done in a separate txn to the one that mutates the
		// descriptor, as this validation can take arbitrarily long.
		validateDrops := func(ctx context.Context, txn descs.Txn) error {
			typeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, t.typeID)
			if err != nil {
				return err
			}
			var toDrop []descpb.TypeDescriptor_EnumMember
			for _, member := range typeDesc.EnumMembers {
				if t.isTransitioningInCurrentJob(&member) && enumMemberIsRemoving(&member) {
					if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
						isDroppingMultiRegionEnumMember = true
					}
					toDrop = append(toDrop, member)
				}
			}

			// If we're dropping a multi-region enum value, we are also going to
			// repartition all of the regional by row tables during this job. We
			// don't want to disallow this operation and return an error because
			// those tables are indeed partitioned by this value. To deal with this
			// fact, we ask the databaseRegionChangeFinalizer to plan out what
			// repartitioning it is going to do and inject those synthesized
			// descriptors into out collection for the purpose of checking whether
			// it is safe to remove this enum value. We'll not actually write the
			// changes in this transaction because we don't want this long-running
			// transaction to be a writing transaction; it would have a heck of
			// a lot of data to refresh. We instead defer the repartitioning until
			// after this checking confirms the safety of the change.
			if isDroppingMultiRegionEnumMember {
				repartitioned, err := prepareRepartitionedRegionalByRowTables(ctx, txn)
				if err != nil {
					return err
				}
				synthetic := make([]catalog.Descriptor, len(repartitioned))
				for i, d := range repartitioned {
					synthetic[i] = d
				}
				txn.Descriptors().SetSyntheticDescriptors(synthetic)
			}
			for _, member := range toDrop {
				if err := t.canRemoveEnumValue(ctx, typeDesc, txn, &member, txn.Descriptors()); err != nil {
					return err
				}
			}
			return nil
		}

		var idsToRemove []int
		populateIDsToRemove := func(holder context.Context, txn descs.Txn) error {
			typeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, t.typeID)
			if err != nil {
				return err
			}
			for _, member := range typeDesc.EnumMembers {
				if !t.isTransitioningInCurrentJob(&member) ||
					!enumMemberIsRemoving(&member) ||
					typeDesc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
					continue
				}
				rows, err := txn.QueryBufferedEx(ctx, "select-invalid-instances", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, `SELECT id FROM system.sql_instances 
 							WHERE crdb_region = $1`, member.PhysicalRepresentation)
				if err != nil {
					return err
				}
				for _, row := range rows {
					idsToRemove = append(idsToRemove, int(tree.MustBeDInt(row[0])))
				}
			}
			return nil
		}

		removeReferences := func(ctx context.Context, txn descs.Txn) error {
			for _, id := range idsToRemove {
				deleteQuery := fmt.Sprintf(
					`DELETE FROM system.sql_instances WHERE id = %d`, id)
				if _, err := txn.ExecEx(ctx, "delete-dropped-region-ref", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, deleteQuery); err != nil {
					return err
				}

			}
			return nil
		}
		if err := t.execCfg.InternalDB.DescsTxn(ctx, validateDrops); err != nil {
			return err
		}
		if isDroppingMultiRegionEnumMember {
			if err := t.execCfg.InternalDB.DescsTxn(ctx, populateIDsToRemove); err != nil {
				return err
			}
			if err := t.execCfg.InternalDB.DescsTxn(ctx, repartitionRegionalByRowTables); err != nil {
				return err
			}
		}

		// Now that we've ascertained that the enum values can be removed, and
		// have performed any necessary pre-drop work, we can actually go about
		// modifying the type descriptor.
		run := func(ctx context.Context, txn descs.Txn) error {
			typeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, t.typeID)
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

			// We need to initialize the finalizer before we write the type descriptor.
			// Otherwise, we run into a chicken and egg problem:
			// * If we write the type descriptor first, the validator expects all the
			//   regions in the type enum to be a partition on the table descriptor,
			//   failing validation.
			// * We cannot write the partitions first as the members are not yet public.
			regionChangeFinalizer, err := newDatabaseRegionChangeFinalizer(
				ctx,
				txn,
				t.execCfg,
				typeDesc.GetParentID(),
				typeDesc.GetID(),
			)
			if err != nil {
				return err
			}
			defer regionChangeFinalizer.cleanup()

			b := txn.KV().NewBatch()
			if err := txn.Descriptors().WriteDescToBatch(
				ctx, kvTrace, typeDesc, b,
			); err != nil {
				return err
			}

			// The version of the array type needs to get bumped as well so that
			// changes to the underlying type are picked up. Simply reading the
			// mutable descriptor and writing it back should do the trick.
			arrayTypeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, typeDesc.ArrayTypeID)
			if err != nil {
				return err
			}
			if err := txn.Descriptors().WriteDescToBatch(
				ctx, kvTrace, arrayTypeDesc, b,
			); err != nil {
				return err
			}

			if err := txn.KV().Run(ctx, b); err != nil {
				return err
			}

			// Additional work must be performed once the promotion/demotion of enum
			// members has been taken care of. In particular, index partitions for
			// REGIONAL BY ROW tables must be updated to reflect the new region values
			// available.
			if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
				if fn := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeMultiRegionUpdates; fn != nil {
					if err := fn(); err != nil {
						return err
					}
				}
				if err := regionChangeFinalizer.finalize(ctx, txn); err != nil {
					return err
				}
			}

			return nil
		}
		if err := t.execCfg.InternalDB.DescsTxn(ctx, run); err != nil {
			return err
		}

		// Finally, make sure all of the type descriptor leases are updated.
		if err := refreshTypeDescriptorLeases(ctx, leaseMgr, t.execCfg.DB, typeDesc); err != nil {
			return err
		}

		if isDroppingMultiRegionEnumMember && len(idsToRemove) != 0 {
			if err := t.execCfg.InternalDB.DescsTxn(ctx, removeReferences); err != nil {
				return err
			}
		}
	}

	// If the type is being dropped, remove the descriptor here only
	// if the declarative schema changer is not in use.
	if typeDesc.Dropped() && typeDesc.GetDeclarativeSchemaChangerState() == nil {
		if err := DescsTxn(ctx, t.execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			b := txn.KV().NewBatch()
			if err := col.DeleteDescToBatch(ctx, kvTrace, typeDesc.GetID(), b); err != nil {
				return err
			}
			return txn.KV().Run(ctx, b)
		}); err != nil {
			return err
		}
	}
	return nil
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
	var regionChangeFinalizer *databaseRegionChangeFinalizer
	// Cleanup:
	cleanup := func(ctx context.Context, txn descs.Txn) error {
		typeDesc, err := txn.Descriptors().MutableByID(txn.KV()).Type(ctx, t.typeID)
		if err != nil {
			return err
		}
		// No cleanup required.
		if !enumHasNonPublic(typeDesc) {
			return nil
		}

		if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
			regionChangeFinalizer, err = newDatabaseRegionChangeFinalizer(
				ctx,
				txn,
				t.execCfg,
				typeDesc.GetParentID(),
				typeDesc.GetID(),
			)
			if err != nil {
				return err
			}
			defer regionChangeFinalizer.cleanup()
		}

		// Deal with all members that we initially hoped to remove but now need to
		// be promoted back to writable.
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

		if err := txn.Descriptors().WriteDesc(ctx, true /* kvTrace */, typeDesc, txn.KV()); err != nil {
			return err
		}

		if regionChangeFinalizer != nil {
			if err := regionChangeFinalizer.finalize(ctx, txn); err != nil {
				return err
			}
		}

		return nil
	}
	return t.execCfg.InternalDB.DescsTxn(ctx, cleanup)
}

// convertToSQLStringRepresentation takes an array of bytes (the physical
// representation of an enum) and converts it into a string that can be used
// in a SQL predicate.
func convertToSQLStringRepresentation(bytes []byte) (string, error) {
	var byteRep strings.Builder
	byteRep.WriteString("x'")
	if _, err := hex.NewEncoder(&byteRep).Write(bytes); err != nil {
		return "", err
	}
	byteRep.WriteString("'")
	return byteRep.String(), nil
}

// doesArrayContainEnumValues takes an array of enum values represented
// as a string, along with an EnumMember, and checks if the
// array contains the given EnumMember. Only works for arrays in the form
// of something like {a, b, c}. Used to capture dependencies in
// expressions in the form of something like '{a, b, c}'::typ[]
func doesArrayContainEnumValues(s string, member *descpb.TypeDescriptor_EnumMember) bool {
	enumValues := strings.Split(s[1:len(s)-1], ",")
	for _, val := range enumValues {
		if strings.TrimSpace(val) == member.LogicalRepresentation {
			return true
		}
	}
	return false
}

func visitExprToCheckEnumValueUsage(
	expr tree.Expr, typeID descpb.ID, member *descpb.TypeDescriptor_EnumMember,
) (foundUsage bool, recurse bool, newExpr tree.Expr, err error) {
	foundUsage = false
	switch t := expr.(type) {
	// Case for types being used regularly, which are serialized like '\x80':::@100053.
	case *tree.AnnotateTypeExpr:
		// Check if this expr's type is the one we're dropping the enum value from.
		typeOid, ok := t.Type.(*tree.OIDTypeReference)
		if !ok {
			return foundUsage, true, expr, nil
		}
		id := typedesc.UserDefinedTypeOIDToID(typeOid.OID)
		if id != typeID {
			return foundUsage, true, expr, nil
		}

		// Check if this expr uses the enum value we're dropping.
		strVal, ok := t.Expr.(*tree.StrVal)
		if !ok {
			return foundUsage, true, expr, nil
		}
		physicalRep := []byte(strVal.RawString())
		if bytes.Equal(physicalRep, member.PhysicalRepresentation) {
			foundUsage = true
		}
		return foundUsage, false, expr, nil

	// Case for types used in string arrays, serialized like '{a, b, c}':::STRING::@100053.
	case *tree.CastExpr:
		typeOid, ok := t.Type.(*tree.OIDTypeReference)
		if !ok {
			return foundUsage, true, expr, nil
		}
		id := typedesc.UserDefinedTypeOIDToID(typeOid.OID)
		// -1 since the type of this CastExpr is the array type.
		id = id - 1
		if id != typeID {
			return foundUsage, true, expr, nil
		}

		// Extract the array and check if it contains the enum member.
		annotateType, ok := t.Expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return foundUsage, true, expr, nil
		}
		strVal, ok := annotateType.Expr.(*tree.StrVal)
		if !ok {
			return foundUsage, true, expr, nil
		}
		foundUsage = doesArrayContainEnumValues(strVal.RawString(), member)
		return foundUsage, false, expr, nil
	default:
		return foundUsage, true, expr, nil
	}
}

// findUsagesOfEnumValue takes an expr, type ID and a enum member of that type,
// and checks if the expr uses that enum member.
func findUsagesOfEnumValue(
	exprStr string, member *descpb.TypeDescriptor_EnumMember, typeID descpb.ID,
) (bool, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return false, err
	}
	var foundUsage, foundUsageInCurrentWalk bool

	visitFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		foundUsageInCurrentWalk, recurse, newExpr, err = visitExprToCheckEnumValueUsage(expr, typeID, member)
		// Set foundUsage to true if enum usage is detected in any expression in the AST walk.
		foundUsage = foundUsage || foundUsageInCurrentWalk
		return recurse, newExpr, err
	}

	_, err = tree.SimpleVisit(expr, visitFunc)
	if err != nil {
		return false, err
	}
	return foundUsage, nil
}

// findUsagesOfEnumValueInViewQuery takes a view query, type ID and an
// enum member of that type, and checks if the view query uses that enum member.
func findUsagesOfEnumValueInViewQuery(
	viewQuery string, member *descpb.TypeDescriptor_EnumMember, typeID descpb.ID,
) (bool, error) {
	var foundUsage, foundUsageInCurrentWalk bool
	visitFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		foundUsageInCurrentWalk, recurse, newExpr, err = visitExprToCheckEnumValueUsage(expr, typeID, member)
		// Set foundUsage to true if enum usage is detected in any expression in the AST walk.
		foundUsage = foundUsage || foundUsageInCurrentWalk
		return recurse, newExpr, err
	}

	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return false, err
	}
	_, err = tree.SimpleStmtVisit(stmt.AST, visitFunc)
	if err != nil {
		return false, err
	}
	return foundUsage, nil
}

// canRemoveEnumValueFromUDF checks if the enum value is being used within the
// function body or within DEFAULT expressions for input arguments.
func (t *typeSchemaChanger) canRemoveEnumValueFromUDF(
	typeDesc *typedesc.Mutable,
	member *descpb.TypeDescriptor_EnumMember,
	udfDesc catalog.FunctionDescriptor,
) error {
	makeError := func() error {
		return pgerror.Newf(pgcode.DependentObjectsStillExist,
			"could not remove enum value %q as it is being used in a routine %q",
			member.LogicalRepresentation, udfDesc.GetName())
	}
	var foundUsage, foundUsageInCurrentWalk bool
	visitFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		foundUsageInCurrentWalk, recurse, newExpr, err = visitExprToCheckEnumValueUsage(expr, typeDesc.ID, member)
		// Set foundUsage to true if enum usage is detected in any expression in the AST walk.
		foundUsage = foundUsage || foundUsageInCurrentWalk
		return recurse, newExpr, err
	}
	switch udfDesc.GetLanguage() {
	case catpb.Function_SQL:
		parsedStmts, err := parser.Parse(udfDesc.GetFunctionBody())
		if err != nil {
			return err
		}
		for _, stmt := range parsedStmts {
			_, err = tree.SimpleStmtVisit(stmt.AST, visitFunc)
			if err != nil {
				return errors.Wrapf(err, "failed to parse routine %s", udfDesc.GetName())
			}
			if foundUsage {
				return makeError()
			}
		}
	case catpb.Function_PLPGSQL:
		stmt, err := plpgsql.Parse(udfDesc.GetFunctionBody())
		if err != nil {
			return errors.Wrapf(err, "failed to parse routine %s", udfDesc.GetName())
		}
		v := plpgsqltree.SQLStmtVisitor{Fn: visitFunc}
		plpgsqltree.Walk(&v, stmt.AST)
		if v.Err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(v.Err, "failed to parse routine %s", udfDesc.GetName())
		}
		if foundUsage {
			return makeError()
		}
	}
	for _, param := range udfDesc.GetParams() {
		if param.DefaultExpr != nil {
			expr, err := parser.ParseExpr(*param.DefaultExpr)
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(
					err, "when parsing DEFAULT expression for parameter %q of routine %s",
					param.Name, udfDesc.GetName())
			}
			_, err = tree.SimpleVisit(expr, visitFunc)
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(
					err, "failed to parse DEFAULT expression for parameter %q of routine %s",
					param.Name, udfDesc.GetName())
			}
			if foundUsage {
				return makeError()
			}
		}
	}
	return nil
}

func (t *typeSchemaChanger) canRemoveEnumValueFromTable(
	ctx context.Context,
	typeDesc *typedesc.Mutable,
	txn isql.Txn,
	member *descpb.TypeDescriptor_EnumMember,
	descsCol *descs.Collection,
	desc catalog.TableDescriptor,
	ID descpb.ID,
) error {
	if desc.IsView() {
		foundUsage, err := findUsagesOfEnumValueInViewQuery(desc.GetViewQuery(), member, typeDesc.ID)
		if err != nil {
			return err
		}
		if foundUsage {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"could not remove enum value %q as it is being used in view %q",
				member.LogicalRepresentation, desc.GetName())
		}
	}

	// If the descriptor has any inaccessible columns, we need to scan those.
	var syntheticDescs []catalog.Descriptor
	if len(desc.AccessibleColumns()) != len(desc.PublicColumns()) {
		descBuilder := desc.NewBuilder()
		fullyAccessibleDesc := descBuilder.BuildExistingMutable().(*tabledesc.Mutable)
		for colIdx := range fullyAccessibleDesc.Columns {
			if col := &fullyAccessibleDesc.Columns[colIdx]; col.Inaccessible {
				col.Inaccessible = false
			}
		}
		syntheticDescs = []catalog.Descriptor{descBuilder.BuildImmutable().(catalog.TableDescriptor)}
	}

	var query strings.Builder
	colSelectors := tabledesc.ColumnsSelectors(desc.AccessibleColumns())
	columns := tree.AsStringWithFlags(&colSelectors, tree.FmtSerializable)
	query.WriteString(fmt.Sprintf("SELECT %s FROM [%d as t] WHERE", columns, ID))
	firstClause := true
	validationQueryConstructed := false

	// Note that we examine all indexes as opposed to non-drop indexes so we
	// do not remove a partitioning value which is in use on an index which
	// is in the process of being dropped but gets re-added due to a failure
	// in that schema change.
	for _, idx := range desc.AllIndexes() {
		if pred := idx.GetPredicate(); pred != "" {
			foundUsage, err := findUsagesOfEnumValue(pred, member, typeDesc.ID)
			if err != nil {
				return err
			}
			if foundUsage {
				return pgerror.Newf(pgcode.DependentObjectsStillExist,
					"could not remove enum value %q as it is being used in a predicate of index %s",
					member.LogicalRepresentation, &tree.TableIndexName{
						Table: tree.MakeUnqualifiedTableName(tree.Name(desc.GetName())),
						Index: tree.UnrestrictedName(idx.GetName()),
					})
			}
		}
		keyColumns := make([]catalog.Column, 0, idx.NumKeyColumns())
		for i := 0; i < idx.NumKeyColumns(); i++ {
			col, err := catalog.MustFindColumnByID(desc, idx.GetKeyColumnID(i))
			if err != nil {
				return errors.WithAssertionFailure(err)
			}
			keyColumns = append(keyColumns, col)
		}
		foundUsage, err := findUsagesOfEnumValueInPartitioning(
			idx.GetPartitioning(), t.execCfg.Codec, keyColumns, desc, idx, member, nil, typeDesc,
		)
		if err != nil {
			return err
		}
		if foundUsage {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"could not remove enum value %q as it is being used in the partitioning of index %s",
				member.LogicalRepresentation, &tree.TableIndexName{
					Table: tree.MakeUnqualifiedTableName(tree.Name(desc.GetName())),
					Index: tree.UnrestrictedName(idx.GetName()),
				})
		}
	}

	// Examine all check constraints.
	for _, chk := range desc.CheckConstraints() {
		foundUsage, err := findUsagesOfEnumValue(chk.GetExpr(), member, typeDesc.ID)
		if err != nil {
			return err
		}
		if foundUsage {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"could not remove enum value %q as it is being used in a check constraint of %q",
				member.LogicalRepresentation, desc.GetName())
		}
	}

	for _, col := range desc.PublicColumns() {
		// If this column has a default expression, check if it uses the enum member being dropped.
		if col.HasDefault() {
			foundUsage, err := findUsagesOfEnumValue(col.GetDefaultExpr(), member, typeDesc.ID)
			if err != nil {
				return err
			}
			if foundUsage {
				return pgerror.Newf(pgcode.DependentObjectsStillExist,
					"could not remove enum value %q as it is being used in a default expresion of %q",
					member.LogicalRepresentation, desc.GetName())
			}
		}

		// If this column is computed, check if it uses the enum member being dropped.
		if col.IsComputed() {
			foundUsage, err := findUsagesOfEnumValue(col.GetComputeExpr(), member, typeDesc.ID)
			if err != nil {
				return err
			}
			if foundUsage {
				return pgerror.Newf(pgcode.DependentObjectsStillExist,
					"could not remove enum value %q as it is being used in a computed column of %q",
					member.LogicalRepresentation, desc.GetName())
			}
		}

		// If this column has an ON UPDATE expression, check if it uses the enum
		// member being dropped.
		if col.HasOnUpdate() {
			foundUsage, err := findUsagesOfEnumValue(col.GetOnUpdateExpr(), member, typeDesc.ID)
			if err != nil {
				return err
			}
			if foundUsage {
				return pgerror.Newf(pgcode.DependentObjectsStillExist,
					"could not remove enum value %q as it is being used in an ON UPDATE expression"+
						" of %q",
					member.LogicalRepresentation, desc.GetName())
			}
		}

		if col.GetType().UserDefined() {
			tid := typedesc.GetUserDefinedTypeDescID(col.GetType())
			if typeDesc.ID == tid {
				if !firstClause {
					query.WriteString(" OR")
				}
				sqlPhysRep, err := convertToSQLStringRepresentation(member.PhysicalRepresentation)
				if err != nil {
					return err
				}
				colName := col.ColName()
				query.WriteString(fmt.Sprintf(
					" t.%s = %s",
					colName.String(),
					sqlPhysRep,
				))
				firstClause = false
				validationQueryConstructed = true
			}
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
		dbDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, typeDesc.ParentID)
		const validationErr = "could not validate removal of enum value %q"
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		override := sessiondata.InternalExecutorOverride{
			User:     username.NodeUserName(),
			Database: dbDesc.GetName(),
		}
		var rows tree.Datums
		err = txn.WithSyntheticDescriptors(syntheticDescs, func() error {
			var err error
			rows, err = txn.QueryRowEx(ctx, "count-value-usage", txn.KV(), override, query.String())
			return err
		})
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		// Check if the above query returned a result. If it did, then the
		// enum value is being used by some place.
		if len(rows) > 0 {
			// If our enum member is being removed, we can skip this check
			// because we need to wait until the region is removed from our
			// multiregion enum before we can drop the reference entirely.
			// We will perform said cleanup later on during the type schema
			// change. We have to do this because
			// instancestorage.RunInstanceIDReclaimLoop will add prewarmed
			// entries in the instances table for each public region.
			if member.Direction == descpb.TypeDescriptor_EnumMember_REMOVE {
				if desc.GetID() == keys.SQLInstancesTableID {
					return nil
				}
			}
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"could not remove enum value %q as it is being used by %q in row: %s",
				member.LogicalRepresentation, desc.GetName(), labeledRowValues(desc.AccessibleColumns(), rows))
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
		if catpb.RegionName(member.LogicalRepresentation) == homedRegion {
			return errors.Newf("could not remove enum value %q as it is the home region for table %q",
				member.LogicalRepresentation, desc.GetName())
		}
	}
	return nil
}

// canRemoveEnumValue returns an error if the enum value is in use and therefore
// can't be removed. An enum value can be referenced in a UDF and a relation. This method
// should be updated if any other data element is added/updated to reference an enum
// value.
func (t *typeSchemaChanger) canRemoveEnumValue(
	ctx context.Context,
	typeDesc *typedesc.Mutable,
	txn isql.Txn,
	member *descpb.TypeDescriptor_EnumMember,
	descsCol *descs.Collection,
) error {
	descGetter := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get()
	for _, id := range typeDesc.ReferencingDescriptorIDs {
		desc, err := descGetter.Desc(ctx, id)
		if err != nil {
			return errors.Wrapf(err,
				"could not validate enum value removal for %q", member.LogicalRepresentation)
		}

		// An enum value can be used within a table and a UDF.
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			err = t.canRemoveEnumValueFromTable(ctx, typeDesc, txn, member, descsCol, desc, id)
			if err != nil {
				return err
			}
		case catalog.FunctionDescriptor:
			err = t.canRemoveEnumValueFromUDF(typeDesc, member, desc)
			if err != nil {
				return err
			}
		default:
			// Enum value is being referenced by some other type of descriptor, return error.
			// This should never happen.
			return errors.Newf(
				"enum value being referenced by descriptor type %s, name %s, unable to delete",
				desc.DescriptorType(),
				desc.GetName())
		}
	}

	// Do validation for the array type now.
	arrayTypeDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, typeDesc.ArrayTypeID)
	if err != nil {
		return err
	}

	return t.canRemoveEnumValueFromArrayUsages(ctx, arrayTypeDesc, member, txn, descsCol)
}

// findUsagesOfEnumValueInPartitioning is a recursive function to explore all of
// the values used in partitioning and its subpartitions. The fakePrefixDatums
// should be nil when first calling this function. They are needed to support
// calling rowenc.DecodePartitionTuple which needs them to synthesize an index
// key that we don't actually need. As we traverse the subpartitions, it will
// be populated with tree.DNull values for each column in the already explored
// prefix.
func findUsagesOfEnumValueInPartitioning(
	partitioning catalog.Partitioning,
	codec keys.SQLCodec,
	columns []catalog.Column,
	table catalog.TableDescriptor,
	index catalog.Index,
	member *descpb.TypeDescriptor_EnumMember,
	fakePrefixDatums []tree.Datum,
	typ *typedesc.Mutable,
) (foundUsage bool, _ error) {
	if partitioning == nil || partitioning.NumColumns() == 0 {
		return false, nil
	}

	var colsToCheck intsets.Fast
	for i, c := range columns[:partitioning.NumColumns()] {
		typT := c.GetType()
		if !typT.UserDefined() {
			continue
		}
		id := typedesc.UserDefinedTypeOIDToID(typT.Oid())
		if id != typ.GetID() {
			continue
		}
		colsToCheck.Add(i)
	}
	makeFakeDatums := func(n int) []tree.Datum {
		ret := fakePrefixDatums
		for i := 0; i < n; i++ {
			ret = append(ret, tree.DNull)
		}
		return ret
	}
	// Note that we do not currently support indexing on array types so we don't
	// need to check for arrays of the enums.
	maybeFindUsageInValues := func(values [][]byte) (err error) {
		if colsToCheck.Empty() {
			return
		}
		for _, v := range values {
			foundUsage, err = findUsageOfEnumValueInEncodedPartitioningValue(
				codec, table, index, partitioning, v, fakePrefixDatums, colsToCheck, foundUsage, member,
			)
			if foundUsage {
				err = iterutil.StopIteration()
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := partitioning.ForEachList(func(
		name string, values [][]byte, subPartitioning catalog.Partitioning,
	) (err error) {
		if err = maybeFindUsageInValues(values); err != nil {
			return err
		}
		foundUsage, err = findUsagesOfEnumValueInPartitioning(
			subPartitioning, codec, columns[partitioning.NumColumns():],
			table, index, member, makeFakeDatums(partitioning.NumColumns()), typ)
		if err != nil && foundUsage {
			err = iterutil.StopIteration()
		}
		return err
	}); err != nil || foundUsage {
		return foundUsage, err
	}
	if err := partitioning.ForEachRange(func(
		name string, from, to []byte,
	) error {
		return maybeFindUsageInValues([][]byte{from, to})
	}); err != nil || foundUsage {
		return foundUsage, err
	}
	return false, nil
}

func findUsageOfEnumValueInEncodedPartitioningValue(
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	index catalog.Index,
	partitioning catalog.Partitioning,
	v []byte,
	fakePrefixDatums []tree.Datum,
	colsToCheck intsets.Fast,
	foundUsage bool,
	member *descpb.TypeDescriptor_EnumMember,
) (bool, error) {
	var d tree.DatumAlloc
	tuple, _, err := rowenc.DecodePartitionTuple(
		&d, codec, table, index, partitioning, v, fakePrefixDatums,
	)
	if err != nil {
		return false, err
	}
	colsToCheck.ForEach(func(i int) {
		foundUsage = foundUsage ||
			bytes.Equal(
				member.PhysicalRepresentation,
				tuple.Datums[i].(*tree.DEnum).PhysicalRep,
			)
	})
	return foundUsage, nil
}

// canRemoveEnumValueFromArrayUsages returns an error if the enum member is used
// as a value by a table/view column which type resolves to a the given array
// type.
func (t *typeSchemaChanger) canRemoveEnumValueFromArrayUsages(
	ctx context.Context,
	arrayTypeDesc catalog.TypeDescriptor,
	member *descpb.TypeDescriptor_EnumMember,
	txn isql.Txn,
	descsCol *descs.Collection,
) error {
	const validationErr = "could not validate removal of enum value %q"
	for i := 0; i < arrayTypeDesc.NumReferencingDescriptors(); i++ {
		id := arrayTypeDesc.GetReferencingDescriptorID(i)
		desc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Desc(ctx, id)
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		// We install a backreference to both the type descriptor and
		// its array alias even when referenced within a UDF. We only need to
		// execute the following code when its referenced with a relation. So check for descriptor
		// type and skip if it is not a relation.
		tblDesc, isTable := desc.(catalog.TableDescriptor)
		if !isTable {
			continue
		}
		var unionUnnests strings.Builder
		var query strings.Builder

		// Construct a query of the form to count usage of this enum value:
		// SELECT COUNT(unnest) FROM (
		//	SELECT unnest(c1) FROM [SELECT %d AS t]
		//	UNION
		//	SELECT unnest(c2) FROM [SELECT %d AS t]
		// 		...
		//	) WHERE unnest = 'enum_value'
		firstClause := true
		for _, col := range tblDesc.PublicColumns() {
			if !col.GetType().UserDefined() {
				continue
			}
			tid := typedesc.GetUserDefinedTypeDescID(col.GetType())
			if arrayTypeDesc.GetID() == tid {
				if !firstClause {
					unionUnnests.WriteString(" UNION ")
				}
				colName := col.ColName()
				unionUnnests.WriteString(fmt.Sprintf(
					"SELECT unnest(t.%s) FROM [%d AS t]",
					colName.String(),
					id,
				))
				firstClause = false
			}
		}
		// Unfortunately, we install a backreference to both the type descriptor and
		// its array alias type regardless of the actual type of the table column.
		// This means we may not actually construct a valid query after going
		// through the columns, in which case there's no validation to do.
		if firstClause {
			continue
		}
		query.WriteString("SELECT count(unnest) FROM (")
		query.WriteString(unionUnnests.String())

		sqlPhysRep, err := convertToSQLStringRepresentation(member.PhysicalRepresentation)
		if err != nil {
			return err
		}
		query.WriteString(fmt.Sprintf(") WHERE unnest = %s", sqlPhysRep))

		dbDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, arrayTypeDesc.GetParentID())
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		override := sessiondata.InternalExecutorOverride{
			User:     username.NodeUserName(),
			Database: dbDesc.GetName(),
		}
		row, err := txn.QueryRowEx(
			ctx,
			"count-array-type-value-usage",
			txn.KV(),
			override,
			query.String(),
		)
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		if row == nil {
			return errors.New("failed to count array type value usage")
		}
		if int64(tree.MustBeDInt(row[0])) != 0 {
			// Use an FQN in the error message.
			parentSchema, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, desc.GetParentSchemaID())
			if err != nil {
				return err
			}
			fqName := tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.GetName()),
				tree.Name(parentSchema.GetName()),
				tree.Name(desc.GetName()),
			)
			return pgerror.Newf(pgcode.DependentObjectsStillExist, "could not remove enum value %q as it is being used by table %q",
				member.LogicalRepresentation, fqName.FQString(),
			)
		}
	}
	// None of the tables use the enum member in their rows.
	return nil
}

func enumHasNonPublic(typeDesc catalog.TypeDescriptor) bool {
	e := typeDesc.AsEnumTypeDescriptor()
	if e == nil {
		return false
	}
	for i := 0; i < e.NumEnumMembers(); i++ {
		if e.IsMemberReadOnly(i) {
			return true
		}
	}
	return false
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
		if err := t.execCfg.JobRegistry.CheckPausepoint("typeschemachanger.before.exec"); err != nil {
			return err
		}
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
		case !IsPermanentSchemaChangeError(tcErr):
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
	buf = buf.Add("typeChangeExec", nil)
	buf = buf.Add("type", t.typeID)
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
func (t *typeChangeResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
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

		if fn := tc.execCfg.TypeSchemaChangerTestingKnobs.RunAfterOnFailOrCancel; fn != nil {
			return fn()
		}

		return nil
	}(); rollbackErr != nil {
		switch {
		case errors.Is(rollbackErr, catalog.ErrDescriptorNotFound) ||
			pgerror.GetPGCode(rollbackErr) == pgcode.UndefinedObject:
			// If the descriptor for the ID can't be found, we assume that another
			// job executed already and dropped the type.
			log.Infof(
				ctx,
				"descriptor %d not found for type change job; assuming it was dropped, and exiting",
				tc.typeID,
			)
		case !IsPermanentSchemaChangeError(rollbackErr):
			return jobs.MarkAsRetryJobError(rollbackErr)
		default:
			return rollbackErr
		}
	}

	return nil
}

// CollectProfile supports the jobs.Resumer interface.
func (t *typeChangeResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &typeChangeResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeTypeSchemaChange, createResumerFn, jobs.UsesTenantCostControl)
}
