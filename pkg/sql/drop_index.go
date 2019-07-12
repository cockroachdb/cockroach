// Copyright 2017 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type dropIndexNode struct {
	n        *tree.DropIndex
	idxNames []fullIndexName
}

// DropIndex drops an index.
// Privileges: CREATE on table.
//   Notes: postgres allows only the index owner to DROP an index.
//          mysql requires the INDEX privilege on the table.
func (p *planner) DropIndex(ctx context.Context, n *tree.DropIndex) (planNode, error) {
	// Keep a track of the indexes that exist to check. When the IF EXISTS
	// options are provided, we will simply not include any indexes that
	// don't exist and continue execution.
	idxNames := make([]fullIndexName, 0, len(n.IndexList))
	for _, index := range n.IndexList {
		tn, tableDesc, err := expandMutableIndexName(ctx, p, index, !n.IfExists /* requireTable */)
		if err != nil {
			// Error or table did not exist.
			return nil, err
		}
		if tableDesc == nil {
			// IfExists specified and table did not exist.
			continue
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
			return nil, err
		}

		idxNames = append(idxNames, fullIndexName{tn: tn, idxName: index.Index})
	}
	return &dropIndexNode{n: n, idxNames: idxNames}, nil
}

func (n *dropIndexNode) startExec(params runParams) error {
	ctx := params.ctx
	for _, index := range n.idxNames {
		// Need to retrieve the descriptor again for each index name in
		// the list: when two or more index names refer to the same table,
		// the mutation list and new version number created by the first
		// drop need to be visible to the second drop.
		tableDesc, err := params.p.ResolveMutableTableDescriptor(
			ctx, index.tn, true /*required*/, ResolveRequireTableDesc)
		if err != nil {
			// Somehow the descriptor we had during newPlan() is not there
			// any more.
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"table descriptor for %q became unavailable within same txn",
				tree.ErrString(index.tn))
		}

		if err := params.p.dropIndexByName(
			ctx, index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkIdxConstraint,
			tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}
	}
	return nil
}

func (*dropIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*dropIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropIndexNode) Close(context.Context)        {}

type fullIndexName struct {
	tn      *tree.TableName
	idxName tree.UnrestrictedName
}

// dropIndexConstraintBehavior is used when dropping an index to signal whether
// it is okay to do so even if it is in use as a constraint (outbound FK or
// unique). This is a subset of what is implied by DropBehavior CASCADE, which
// implies dropping *all* dependencies. This is used e.g. when the element
// constrained is being dropped anyway.
type dropIndexConstraintBehavior bool

const (
	checkIdxConstraint  dropIndexConstraintBehavior = true
	ignoreIdxConstraint dropIndexConstraintBehavior = false
)

func (p *planner) dropIndexByName(
	ctx context.Context,
	tn *tree.TableName,
	idxName tree.UnrestrictedName,
	tableDesc *sqlbase.MutableTableDescriptor,
	ifExists bool,
	behavior tree.DropBehavior,
	constraintBehavior dropIndexConstraintBehavior,
	jobDesc string,
) error {
	idx, dropped, err := tableDesc.FindIndexByName(string(idxName))
	if err != nil {
		// Only index names of the form "table@idx" throw an error here if they
		// don't exist.
		if ifExists {
			// Noop.
			return nil
		}
		// Index does not exist, but we want it to: error out.
		return err
	}
	if dropped {
		return nil
	}

	// Check if requires CCL binary for eventual zone config removal.
	_, zone, _, err := GetZoneConfigInTxn(ctx, p.txn, uint32(tableDesc.ID), nil, "", false)
	if err != nil {
		return err
	}

	for _, s := range zone.Subzones {
		if s.IndexID != uint32(idx.ID) {
			_, err = GenerateSubzoneSpans(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), tableDesc.TableDesc(), zone.Subzones, false /* newSubzones */)
			if sqlbase.IsCCLRequiredError(err) {
				return sqlbase.NewCCLRequiredError(fmt.Errorf("schema change requires a CCL binary "+
					"because table %q has at least one remaining index or partition with a zone config",
					tableDesc.Name))
			}
			break
		}
	}

	// Queue the mutation.
	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	// Now, we have to make sure that all of the foreign keys that point at the
	// table that this index is a part of still have sufficient unique indexes to
	// continue being semantically valid.

	// First check if there's indexes for the outgoing FKs.
	// TODO(jordan, radu) remove this restriction.

	// An FK can be removed by DROP CASCADE only if it is not being added/validated.
	removableFKs := make(map[string]struct{})
	for _, fk := range tableDesc.OutboundFKs {
		if fk.Validity != sqlbase.ConstraintValidity_Validating {
			removableFKs[fk.Name] = struct{}{}
		}
	}
	fksToDrop := make(map[string]struct{})
	for _, fk := range tableDesc.AllActiveAndInactiveForeignKeys() {
		// First, check if the index we're dropping matches this outbound FK.
		if !sqlbase.ColumnIDs(fk.OriginColumnIDs).EqualSets(idx.ColumnIDs) {
			// If there's no match, then there's no need to check anything further.
			continue
		}
		foundOtherIndexThatSatisfiesFK := false
		for _, otherIdx := range tableDesc.Indexes {
			// Skip ourselves.
			if otherIdx.ID == idx.ID {
				continue
			}
			if sqlbase.ColumnIDs(fk.OriginColumnIDs).EqualSets(otherIdx.ColumnIDs) {
				foundOtherIndexThatSatisfiesFK = true
				break
			}
		}
		if sqlbase.ColumnIDs(fk.OriginColumnIDs).EqualSets(tableDesc.PrimaryIndex.ColumnIDs) {
			foundOtherIndexThatSatisfiesFK = true
		}
		if !foundOtherIndexThatSatisfiesFK {
			if _, ok := removableFKs[fk.Name]; !ok {
				// TODO (lucy): !!! improve this error message
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint %q in the middle of being added, try again later", fk.Name)
			}
			if behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
				return errors.Errorf("index %q is in use as a foreign key constraint", idx.Name)
			}
			// Now, drop the reference from the table that the index we're
			// dropping.
			// TODO (lucy): !!! also remove the FK
			if err := p.removeFKBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	outputIdx := 0
	for _, fk := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[outputIdx] = fk
		if _, ok := fksToDrop[fk.Name]; !ok {
			outputIdx++
		}
	}
	tableDesc.OutboundFKs = tableDesc.OutboundFKs[:outputIdx]
	outputIdx = 0
	for _, ref := range tableDesc.InboundFKs {
		// We're updating the tableDesc.InboundFKs list in place, since during this
		// loop we're going to remove those inbound FKs that are only matched by the
		// index that we're dropping (if we're in cascade mode).
		tableDesc.InboundFKs[outputIdx] = ref
		outputIdx++
		// First, check if the index we're dropping matches this inbound FK.
		if !sqlbase.ColumnIDs(ref.ReferencedColumnIDs).EqualSets(idx.ColumnIDs) {
			// If there's no match, then there's no need to check anything further.
			continue
		}

		foundOtherIndexThatSatisfiesFK := false
		for _, otherIdx := range tableDesc.Indexes {
			// Skip ourselves.
			if otherIdx.ID == idx.ID {
				continue
			}
			// Skip non-unique indexes, which can't satisfy FKs.
			if !otherIdx.Unique {
				continue
			}
			if sqlbase.ColumnIDs(ref.ReferencedColumnIDs).EqualSets(otherIdx.ColumnIDs) {
				foundOtherIndexThatSatisfiesFK = true
				break
			}
		}
		if sqlbase.ColumnIDs(ref.ReferencedColumnIDs).EqualSets(tableDesc.PrimaryIndex.ColumnIDs) {
			foundOtherIndexThatSatisfiesFK = true
		}

		if !foundOtherIndexThatSatisfiesFK {
			if err := p.canRemoveFKBackreference(ctx, idx.Name, ref, behavior); err != nil {
				return err
			}
			// Now, drop the backreference from the table that the index we're
			// dropping. All we have to do is decrement our outputIdx, which
			// effectively deletes the element that we're currently looking at from
			// the list.
			outputIdx--

			// Now, we must delete the forward reference from the other table.
			originTable, err := p.Tables().getMutableTableVersionByID(ctx, ref.OriginTableID, p.txn)
			if err != nil {
				return err
			}
			foundIdx := -1
			for i, fk := range originTable.OutboundFKs {
				if fk.ReferencedTableID == tableDesc.ID &&
					sqlbase.ColumnIDs(fk.ReferencedColumnIDs).EqualSets(ref.ReferencedColumnIDs) &&
					sqlbase.ColumnIDs(fk.OriginColumnIDs).EqualSets(ref.OriginColumnIDs) {
					foundIdx = i
				}
			}
			if foundIdx == -1 {
				return errors.AssertionFailedf("missing forward ref for backref %v", ref)
			}
			originTable.OutboundFKs = append(originTable.OutboundFKs[:foundIdx], originTable.OutboundFKs[foundIdx+1:]...)
			if err := p.writeSchemaChange(ctx, originTable, sqlbase.InvalidMutationID); err != nil {
				return err
			}
		}
	}
	tableDesc.InboundFKs = tableDesc.InboundFKs[:outputIdx]

	for _, ref := range idx.InterleavedBy {
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if idx.Unique && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", idx.Name)
	}

	var droppedViews []string
	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID == idx.ID {
			// Ensure that we have DROP privilege on all dependent views
			err := p.canRemoveDependentViewGeneric(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef, behavior)
			if err != nil {
				return err
			}
			viewDesc, err := p.getViewDescForCascade(
				ctx, "index", idx.Name, tableDesc.ParentID, tableRef.ID, behavior,
			)
			if err != nil {
				return err
			}
			cascadedViews, err := p.removeDependentView(ctx, tableDesc, viewDesc)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, viewDesc.Name)
			droppedViews = append(droppedViews, cascadedViews...)
		}
	}

	// Overwriting tableDesc.Index may mess up with the idx object we collected above. Make a copy.
	idxCopy := *idx
	idx = &idxCopy

	found := false
	for i, idxEntry := range tableDesc.Indexes {
		if idxEntry.ID == idx.ID {
			// Unsplit all manually split ranges in the index so they can be
			// automatically merged by the merge queue.
			span := tableDesc.IndexSpan(idxEntry.ID)
			ranges, err := ScanMetaKVs(ctx, p.txn, span)
			if err != nil {
				return err
			}
			for _, r := range ranges {
				var desc roachpb.RangeDescriptor
				if err := r.ValueProto(&desc); err != nil {
					return err
				}
				// We have to explicitly check that the range descriptor's start key
				// lies within the span of the index since ScanMetaKVs returns all
				// intersecting spans.
				if (desc.GetStickyBit() != hlc.Timestamp{}) && span.Key.Compare(desc.StartKey.AsRawKey()) <= 0 {
					// Swallow "key is not the start of a range" errors because it would
					// mean that the sticky bit was removed and merged concurrently. DROP
					// INDEX should not fail because of this.
					if err := p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
						return err
					}
				}
			}

			// Remove all foreign key references and backreferences from the index.
			// TODO (lucy): This is incorrect for two reasons: The first is that FKs
			// won't be restored if the DROP INDEX is rolled back, and the second is
			// that validated constraints should be dropped in the schema changer in
			// multiple steps to avoid inconsistencies. We should be queuing a
			// mutation to drop the FK instead. The reason why the FK is removed here
			// is to keep the index state consistent with the earlier removal of the
			// reference on the other table involved in the FK, in case of rollbacks
			// (#38733).
			idxEntry.ForeignKey = sqlbase.ForeignKeyReference{}
			idxEntry.ReferencedBy = nil

			// the idx we picked up with FindIndexByID at the top may not
			// contain the same field any more due to other schema changes
			// intervening since the initial lookup. So we send the recent
			// copy idxEntry for drop instead.
			if err := tableDesc.AddIndexMutation(&idxEntry, sqlbase.DescriptorMutation_DROP); err != nil {
				return err
			}
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("index %q in the middle of being added, try again later", idxName)
	}

	/* TODO(jordan): this doesn't work because it doesn't see the
	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}
	*/
	mutationID, err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc)
	if err != nil {
		return err
	}
	if err := p.writeSchemaChange(ctx, tableDesc, mutationID); err != nil {
		return err
	}
	// Record index drop in the event log. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	return MakeEventLogger(p.extendedEvalCtx.ExecCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropIndex,
		int32(tableDesc.ID),
		int32(p.extendedEvalCtx.NodeID),
		struct {
			TableName           string
			IndexName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{tn.FQString(), string(idxName), jobDesc, p.SessionData().User, uint32(mutationID),
			droppedViews},
	)
}
