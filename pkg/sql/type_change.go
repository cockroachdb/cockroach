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
	"encoding/hex"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	// Check if there is an active job for this type, otherwise create one.
	job, jobExists := p.extendedEvalCtx.SchemaChangeJobCache[typeDesc.ID]
	transitioningMembers, beingDropped := findTransitioningMembers(typeDesc)
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
		if err := job.SetNonCancelable(ctx, p.txn,
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
			}); err != nil {
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
			// Type change jobs in general are not cancelable, unless they include
			// a transition that drops an enum member.
			NonCancelable: !beingDropped,
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
	RunBeforeEnumMemberPromotion func() error
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
	if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		typeDesc, err = catalogkv.MustGetTypeDescByID(ctx, txn, t.execCfg.Codec, t.typeID)
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
	ctx context.Context, leaseMgr *lease.Manager, typeDesc catalog.TypeDescriptor,
) error {
	var err error
	var ids = []descpb.ID{typeDesc.GetID()}
	if typeDesc.GetArrayTypeID() != descpb.InvalidID {
		ids = append(ids, typeDesc.GetArrayTypeID())
	}
	for _, id := range ids {
		if updateErr := WaitToUpdateLeases(ctx, leaseMgr, id); updateErr != nil {
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
	codec := t.execCfg.Codec

	typeDesc, err := t.getTypeDescFromStore(ctx)
	if err != nil {
		return err
	}

	// If there are any names to drain, then do so.
	if len(typeDesc.GetDrainingNames()) > 0 {
		if err := drainNamesForDescriptor(
			ctx, t.execCfg.Settings, typeDesc.GetID(), t.execCfg.DB, t.execCfg.InternalExecutor,
			leaseMgr, codec, nil,
		); err != nil {
			return err
		}
	}

	// Make sure all of the leases have dropped before attempting to validate.
	if err := refreshTypeDescriptorLeases(ctx, leaseMgr, typeDesc); err != nil {
		return err
	}

	// For all the read only members the current job is responsible for, either
	// promote them to writeable or remove them from the descriptor entirely,
	// as dictated by the direction.
	if (typeDesc.GetKind() == descpb.TypeDescriptor_ENUM ||
		typeDesc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM) &&
		len(t.transitioningMembers) != 0 {
		if fn := t.execCfg.TypeSchemaChangerTestingKnobs.RunBeforeEnumMemberPromotion; fn != nil {
			if err := fn(); err != nil {
				return err
			}
		}

		multiRegionPreDropIsNecessary := false

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
					if typeDesc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
						multiRegionPreDropIsNecessary = true
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

		var regionChangeFinalizer *databaseRegionChangeFinalizer

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
		if multiRegionPreDropIsNecessary {
			preDrop := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
				typeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, t.typeID)
				if err != nil {
					return err
				}

				regionChangeFinalizer, err = newDatabaseRegionChangeFinalizer(
					ctx,
					txn,
					t.execCfg,
					descsCol,
					typeDesc.GetParentID(),
					typeDesc.GetID(),
				)
				if err != nil {
					return err
				}
				defer regionChangeFinalizer.cleanup()

				if err := regionChangeFinalizer.preDrop(ctx, txn); err != nil {
					return err
				}
				return nil
			}
			if err := descs.Txn(
				ctx, t.execCfg.Settings, t.execCfg.LeaseManager,
				t.execCfg.InternalExecutor, t.execCfg.DB, preDrop,
			); err != nil {
				return err
			}

			// Now update the leases to ensure the that new table descriptor is
			// visible to all nodes.
			if err := regionChangeFinalizer.waitToUpdateLeases(ctx, leaseMgr); err != nil {
				return err
			}
		}

		// Now that we've ascertained that the enum values can be removed, and
		// have performed any necessary pre-drop work, we can actually go about
		// modifying the type descriptor.
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

			// We need to initialize the finalizer before we write the type descriptor.
			// Otherwise, we run into a chicken and egg problem:
			// * If we write the type descriptor first, the validator expects all the
			//   regions in the type enum to be a partition on the table descriptor,
			//   failing validation.
			// * We cannot write the partitions first as the members are not yet public.
			regionChangeFinalizer, err = newDatabaseRegionChangeFinalizer(
				ctx,
				txn,
				t.execCfg,
				descsCol,
				typeDesc.GetParentID(),
				typeDesc.GetID(),
			)
			if err != nil {
				return err
			}
			defer regionChangeFinalizer.cleanup()

			b := txn.NewBatch()
			if err := descsCol.WriteDescToBatch(
				ctx, true /* kvTrace */, typeDesc, b,
			); err != nil {
				return err
			}

			// The version of the array type needs to get bumped as well so that
			// changes to the underlying type are picked up. Simply reading the
			// mutable descriptor and writing it back should do the trick.
			arrayTypeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, typeDesc.ArrayTypeID)
			if err != nil {
				return err
			}
			if err := descsCol.WriteDescToBatch(
				ctx, true /* kvTrace */, arrayTypeDesc, b,
			); err != nil {
				return err
			}

			if err := txn.Run(ctx, b); err != nil {
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
		if err := descs.Txn(
			ctx, t.execCfg.Settings, t.execCfg.LeaseManager,
			t.execCfg.InternalExecutor, t.execCfg.DB, run,
		); err != nil {
			return err
		}

		// If any tables were repartitioned, make sure their leases are updated as
		// well.
		if regionChangeFinalizer != nil {
			if err := regionChangeFinalizer.waitToUpdateLeases(ctx, leaseMgr); err != nil {
				return err
			}
		}

		// Finally, make sure all of the type descriptor leases are updated.
		if err := refreshTypeDescriptorLeases(ctx, leaseMgr, typeDesc); err != nil {
			return err
		}
	}

	// If the type is being dropped, remove the descriptor here.
	if typeDesc.Dropped() {
		if err := t.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := txn.NewBatch()
			b.Del(catalogkeys.MakeDescMetadataKey(codec, typeDesc.GetID()))
			return txn.Run(ctx, b)
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
	cleanup := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		typeDesc, err := descsCol.GetMutableTypeVersionByID(ctx, txn, t.typeID)
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
				descsCol,
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

		if err := descsCol.WriteDesc(ctx, true /* kvTrace */, typeDesc, txn); err != nil {
			return err
		}

		if regionChangeFinalizer != nil {
			if err := regionChangeFinalizer.finalize(ctx, txn); err != nil {
				return err
			}
		}

		return nil
	}
	if err := descs.Txn(ctx, t.execCfg.Settings, t.execCfg.LeaseManager, t.execCfg.InternalExecutor,
		t.execCfg.DB, cleanup); err != nil {
		return err
	}

	if regionChangeFinalizer != nil {
		if err := regionChangeFinalizer.waitToUpdateLeases(ctx, t.execCfg.LeaseManager); err != nil {
			return err
		}
	}

	return nil
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

// findUsagesOfEnumValue takes an expr, type ID and a enum member of that type,
// and checks if the expr uses that enum member.
func findUsagesOfEnumValue(
	exprStr string, member *descpb.TypeDescriptor_EnumMember, typeID descpb.ID,
) (bool, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return false, err
	}
	var foundUsage bool

	visitFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		switch t := expr.(type) {
		// Case for types being used regularly, which are serialized like '\x80':::@100053.
		case *tree.AnnotateTypeExpr:
			// Check if this expr's type is the one we're dropping the enum value from.
			typeOid, ok := t.Type.(*tree.OIDTypeReference)
			if !ok {
				return true, expr, nil
			}
			id, err := typedesc.UserDefinedTypeOIDToID(typeOid.OID)
			if err != nil {
				return false, expr, err
			}
			if id != typeID {
				return true, expr, nil
			}

			// Check if this expr uses the enum value we're dropping.
			strVal, ok := t.Expr.(*tree.StrVal)
			if !ok {
				return true, expr, nil
			}
			physicalRep := []byte(strVal.RawString())
			if bytes.Equal(physicalRep, member.PhysicalRepresentation) {
				foundUsage = true
			}
			return false, expr, nil

		// Case for types used in string arrays, serialized like '{a, b, c}':::STRING::@100053.
		case *tree.CastExpr:
			typeOid, ok := t.Type.(*tree.OIDTypeReference)
			if !ok {
				return true, expr, nil
			}
			id, err := typedesc.UserDefinedTypeOIDToID(typeOid.OID)
			if err != nil {
				return false, expr, err
			}
			// -1 since the type of this CastExpr is the array type.
			id = id - 1
			if id != typeID {
				return true, expr, nil
			}

			// Extract the array and check if it contains the enum member.
			annotateType, ok := t.Expr.(*tree.AnnotateTypeExpr)
			if !ok {
				return true, expr, nil
			}
			strVal, ok := annotateType.Expr.(*tree.StrVal)
			if !ok {
				return true, expr, nil
			}
			foundUsage = doesArrayContainEnumValues(strVal.RawString(), member)
			return false, expr, nil
		default:
			return true, expr, nil
		}
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
	var foundUsage bool
	visitFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		annotateType, ok := expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return true, expr, nil
		}

		// Check if this expr's type is the one we're dropping the enum value from.
		typeOid, ok := annotateType.Type.(*tree.OIDTypeReference)
		if !ok {
			return true, expr, nil
		}
		id, err := typedesc.UserDefinedTypeOIDToID(typeOid.OID)
		if err != nil {
			return false, expr, err
		}
		if id != typeID {
			return true, expr, nil
		}

		// Check if this expr uses the enum value we're dropping.
		strVal, ok := annotateType.Expr.(*tree.StrVal)
		if !ok {
			return true, expr, nil
		}
		physicalRep := []byte(strVal.RawString())
		if bytes.Equal(physicalRep, member.PhysicalRepresentation) {
			foundUsage = true
			return false, expr, nil
		}

		return false, expr, nil
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

// canRemoveEnumValue returns an error if the enum value is in use and therefore
// can't be removed.
func (t *typeSchemaChanger) canRemoveEnumValue(
	ctx context.Context,
	typeDesc *typedesc.Mutable,
	txn *kv.Txn,
	member *descpb.TypeDescriptor_EnumMember,
	descsCol *descs.Collection,
) error {
	for _, ID := range typeDesc.ReferencingDescriptorIDs {
		desc, err := descsCol.GetImmutableTableByID(ctx, txn, ID, tree.ObjectLookupFlags{})
		if err != nil {
			return errors.Wrapf(err,
				"could not validate enum value removal for %q", member.LogicalRepresentation)
		}
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

		var query strings.Builder
		colSelectors := tabledesc.ColumnsSelectors(desc.PublicColumns())
		columns := tree.AsStringWithFlags(&colSelectors, tree.FmtSerializable)
		query.WriteString(fmt.Sprintf("SELECT %s FROM [%d as t] WHERE", columns, ID))
		firstClause := true
		validationQueryConstructed := false
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

			if col.GetType().UserDefined() {
				tid, terr := typedesc.GetUserDefinedTypeDescID(col.GetType())
				if terr != nil {
					return terr
				}
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
			_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
				ctx, txn, typeDesc.ParentID, tree.DatabaseLookupFlags{Required: true})
			const validationErr = "could not validate removal of enum value %q"
			if err != nil {
				return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
			}
			override := sessiondata.InternalExecutorOverride{
				User:     security.RootUserName(),
				Database: dbDesc.GetName(),
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

	// Do validation for the array type now.
	arrayTypeDesc, err := descsCol.GetImmutableTypeByID(
		ctx, txn, typeDesc.ArrayTypeID, tree.ObjectLookupFlags{})
	if err != nil {
		return err
	}

	return t.canRemoveEnumValueFromArrayUsages(ctx, arrayTypeDesc, member, txn, descsCol)
}

// canRemoveEnumValueFromArrayUsages returns an error if the enum member is used
// as a value by a table/view column which type resolves to a the given array
// type.
func (t *typeSchemaChanger) canRemoveEnumValueFromArrayUsages(
	ctx context.Context,
	arrayTypeDesc catalog.TypeDescriptor,
	member *descpb.TypeDescriptor_EnumMember,
	txn *kv.Txn,
	descsCol *descs.Collection,
) error {
	const validationErr = "could not validate removal of enum value %q"
	for i := 0; i < arrayTypeDesc.NumReferencingDescriptors(); i++ {
		id := arrayTypeDesc.GetReferencingDescriptorID(i)
		desc, err := descsCol.GetImmutableTableByID(ctx, txn, id, tree.ObjectLookupFlags{})
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		var unionUnnests strings.Builder
		var query strings.Builder

		// Construct a query of the form:
		// SELECT unnest FROM (
		//	SELECT unnest(c1) FROM [SELECT %d AS t]
		//	UNION
		//	SELECT unnest(c2) FROM [SELECT %d AS t]
		// 		...
		//	) WHERE unnest = 'enum_value'
		firstClause := true
		for _, col := range desc.PublicColumns() {
			if !col.GetType().UserDefined() {
				continue
			}
			tid, terr := typedesc.GetUserDefinedTypeDescID(col.GetType())
			if terr != nil {
				return terr
			}
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
		query.WriteString("SELECT unnest FROM (")
		query.WriteString(unionUnnests.String())

		sqlPhysRep, err := convertToSQLStringRepresentation(member.PhysicalRepresentation)
		if err != nil {
			return err
		}
		query.WriteString(fmt.Sprintf(") WHERE unnest = %s", sqlPhysRep))

		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx, txn, arrayTypeDesc.GetParentID(), tree.DatabaseLookupFlags{Required: true})
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		override := sessiondata.InternalExecutorOverride{
			User:     security.RootUserName(),
			Database: dbDesc.GetName(),
		}
		rows, err := t.execCfg.InternalExecutor.QueryRowEx(
			ctx,
			"count-array-type-value-usage",
			txn,
			override,
			query.String(),
		)
		if err != nil {
			return errors.Wrapf(err, validationErr, member.LogicalRepresentation)
		}
		if len(rows) > 0 {
			// Use an FQN in the error message.
			parentSchema, err := descsCol.GetImmutableSchemaByID(
				ctx, txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{})
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
	hasNonPublic := false
	for i := 0; i < typeDesc.NumEnumMembers(); i++ {
		if typeDesc.IsMemberReadOnly(i) {
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
		case !IsPermanentSchemaChangeError(rollbackErr):
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
