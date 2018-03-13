// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
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
		var tn *tree.TableName
		var tableDesc *TableDescriptor
		var err error
		// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
		// TODO(vivek): check if the cache can be used.
		p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
			tn, tableDesc, err = expandIndexName(ctx, p, index, !n.IfExists /* requireTable */)
		})
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
		var tableDesc *TableDescriptor
		var err error
		params.p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
			tableDesc, err = ResolveExistingObject(
				ctx, params.p, index.tn, true /*required*/, requireTableDesc)
		})
		if err != nil {
			// Somehow the descriptor we had during newPlan() is not there
			// any more.
			return errors.Wrapf(err, "table descriptor for %q became unavailable within same txn",
				tree.ErrString(index.tn))
		}

		if err := params.p.dropIndexByName(
			ctx, index.tn, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkIdxConstraint,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames),
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
	tableDesc *sqlbase.TableDescriptor,
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

	// Queue the mutation.
	var droppedViews []string
	if idx.ForeignKey.IsSet() {
		if behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
			return errors.Errorf("index %q is in use as a foreign key constraint", idx.Name)
		}
		if err := p.removeFKBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	if len(idx.Interleave.Ancestors) > 0 {
		if err := p.removeInterleaveBackReference(ctx, tableDesc, idx); err != nil {
			return err
		}
	}

	for _, ref := range idx.ReferencedBy {
		fetched, err := p.canRemoveFK(ctx, idx.Name, ref, behavior)
		if err != nil {
			return err
		}
		if err := p.removeFK(ctx, ref, fetched); err != nil {
			return err
		}
	}
	for _, ref := range idx.InterleavedBy {
		if err := p.removeInterleave(ctx, ref); err != nil {
			return err
		}
	}

	if idx.Unique && behavior != tree.DropCascade && constraintBehavior != ignoreIdxConstraint {
		return errors.Errorf("index %q is in use as unique constraint (use CASCADE if you really want to drop it)", idx.Name)
	}

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
	found := false
	for i := range tableDesc.Indexes {
		if tableDesc.Indexes[i].ID == idx.ID {
			if err := tableDesc.AddIndexMutation(tableDesc.Indexes[i], sqlbase.DescriptorMutation_DROP); err != nil {
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

	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}
	mutationID, err := p.createSchemaChangeJob(ctx, tableDesc, jobDesc)
	if err != nil {
		return err
	}
	if err := p.writeTableDesc(ctx, tableDesc); err != nil {
		return err
	}
	// Record index drop in the event log. This is an auditable log event
	// and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(p.extendedEvalCtx.ExecCfg).InsertEventRecord(
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
	); err != nil {
		return err
	}
	p.notifySchemaChange(tableDesc, mutationID)

	return nil
}
