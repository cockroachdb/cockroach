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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
		tn, err := p.expandIndexName(ctx, index, false /* requireTable */)
		if err != nil {
			return nil, err
		} else if tn == nil {
			// Only index names of the form "idx" throw an error here if they
			// don't exist.
			if n.IfExists {
				// Skip this index and don't return an error.
				continue
			}
			// Index does not exist, but we want it to error out.
			return nil, fmt.Errorf("index %q not found", index.Index)
		}

		tableDesc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /*allowAdding*/)
		if err != nil {
			return nil, err
		}

		if err := p.CheckPrivilege(tableDesc, privilege.CREATE); err != nil {
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
		tableDesc, err := getTableDesc(ctx, params.p.txn, params.p.getVirtualTabler(), index.tn)
		if err != nil || tableDesc == nil {
			// newPlan() and Start() ultimately run within the same
			// transaction. If we got a descriptor during newPlan(), we
			// must have it here too.
			panic(fmt.Sprintf("table descriptor for %s became unavailable within same txn", index.tn))
		}

		if err := params.p.dropIndexByName(
			ctx, index.idxName, tableDesc, n.n.IfExists, n.n.DropBehavior, checkOutboundFK,
			tree.AsStringWithFlags(n.n, tree.FmtSimpleQualified),
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

// dropIdxFKCheck is used when dropping an index to signal whether it is okay to
// do so even if it is in use as an *outbound FK*. This is a subset of what is
// implied by DropBehavior CASCADE, which implies dropping *all* dependencies.
// This is used e.g. when the element constrained is being dropped anyway.
type dropIdxFKCheck bool

const (
	checkOutboundFK  dropIdxFKCheck = true
	ignoreOutboundFK dropIdxFKCheck = false
)

func (p *planner) dropIndexByName(
	ctx context.Context,
	idxName tree.UnrestrictedName,
	tableDesc *sqlbase.TableDescriptor,
	ifExists bool,
	behavior tree.DropBehavior,
	outboundFKCheck dropIdxFKCheck,
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
	if behavior == tree.DropRestrict {
		name, constraint, err := findIndexUniqueConstraint(ctx, p.txn, tableDesc, &idx)
		if err != nil {
			return err
		}
		if constraint != nil {
			return pgerror.NewErrorf(pgerror.CodeInvalidObjectDefinitionError,
				"index %q is in use by unique constraint %q", idx.Name, name)
		}
	}
	// Queue the mutation.
	var droppedViews []string
	if idx.ForeignKey.IsSet() {
		if behavior != tree.DropCascade && outboundFKCheck != ignoreOutboundFK {
			return fmt.Errorf("index %q is in use as a foreign key constraint", idx.Name)
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

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
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
	if err := MakeEventLogger(p.LeaseMgr()).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropIndex,
		int32(tableDesc.ID),
		int32(p.evalCtx.NodeID),
		struct {
			TableName           string
			IndexName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{tableDesc.Name, string(idxName), jobDesc, p.session.User, uint32(mutationID),
			droppedViews},
	); err != nil {
		return err
	}
	p.notifySchemaChange(tableDesc, mutationID)

	return nil
}

// findIndexUniqueConstraint will find a unique constraint that the
// index is used by, if one exists.
func findIndexUniqueConstraint(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	idx *sqlbase.IndexDescriptor,
) (string, *sqlbase.ConstraintDetail, error) {
	details, err := tableDesc.GetConstraintInfo(ctx, txn)
	if err != nil {
		return "", nil, err
	}
	for name := range details {
		if details[name].Kind == sqlbase.ConstraintTypeUnique &&
			details[name].Index.ID == idx.ID {
			constraint := details[name]
			return name, &constraint, nil
		}
	}
	return "", nil, nil
}
