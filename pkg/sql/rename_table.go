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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RenameTable renames the table, view or sequence.
// Privileges: DROP on source table/view/sequence, CREATE on destination database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(ctx context.Context, n *tree.RenameTable) (planNode, error) {
	oldTn, err := n.Name.NormalizeWithDatabaseName(p.SessionData().Database)
	if err != nil {
		return nil, err
	}
	newTn, err := n.NewName.NormalizeWithDatabaseName(p.SessionData().Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), oldTn.Schema())
	if err != nil {
		return nil, err
	}

	// Check if source table, view or sequence exists.
	// Note that Postgres's behavior here is a little lenient - it'll let you
	// modify views by running ALTER TABLE, but won't let you modify tables
	// by running ALTER VIEW. Our behavior is strict for now, but can be
	// made more lenient down the road if needed.
	var tableDesc *sqlbase.TableDescriptor
	if n.IsView {
		tableDesc, err = getViewDesc(ctx, p.txn, p.getVirtualTabler(), oldTn)
		if err != nil {
			return nil, err
		}
	} else if n.IsSequence {
		tableDesc, err = getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), oldTn)
		if err != nil {
			return nil, err
		}
	} else {
		tableDesc, err = getTableDesc(ctx, p.txn, p.getVirtualTabler(), oldTn)
		if err != nil {
			return nil, err
		}
	}

	if tableDesc == nil {
		if n.IfExists {
			// Noop.
			return &zeroNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, sqlbase.NewUndefinedRelationError(oldTn)
	}
	if tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
		return nil, sqlbase.NewUndefinedRelationError(oldTn)
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	// Check if any views depend on this table/view. Because our views
	// are currently just stored as strings, they explicitly specify the name
	// of everything they depend on. Rather than trying to rewrite the view's
	// query with the new name, we simply disallow such renames for now.
	if len(tableDesc.DependedOnBy) > 0 {
		return nil, p.dependentViewRenameError(
			ctx, tableDesc.TypeName(), oldTn.String(), tableDesc.ParentID, tableDesc.DependedOnBy[0].ID)
	}

	// Check if target database exists.
	targetDbDesc, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), newTn.Schema())
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, targetDbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Schema() == newTn.Schema() && oldTn.Table() == newTn.Table() {
		// Noop.
		return &zeroNode{}, nil
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.ID

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	newTbKey := tableKey{targetDbDesc.ID, newTn.Table()}.Key()

	if err := tableDesc.Validate(ctx, p.txn); err != nil {
		return nil, err
	}

	descID := tableDesc.GetID()
	descDesc := sqlbase.WrapDescriptor(tableDesc)

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	renameDetails := sqlbase.TableDescriptor_NameInfo{
		ParentID: dbDesc.ID,
		Name:     oldTn.Table()}
	tableDesc.DrainingNames = append(tableDesc.DrainingNames, renameDetails)
	if err := p.writeTableDesc(ctx, tableDesc); err != nil {
		return nil, err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &client.Batch{}
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
		log.VEventf(ctx, 2, "CPut %s -> %d", newTbKey, descID)
	}
	b.Put(descKey, descDesc)
	b.CPut(newTbKey, descID, nil)

	if err := p.txn.Run(ctx, b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return nil, sqlbase.NewRelationAlreadyExistsError(newTn.Table())
		}
		return nil, err
	}
	p.notifySchemaChange(tableDesc, sqlbase.InvalidMutationID)

	return &zeroNode{}, nil
}

// TODO(a-robinson): Support renaming objects depended on by views once we have
// a better encoding for view queries (#10083).
func (p *planner) dependentViewRenameError(
	ctx context.Context, typeName, objName string, parentID, viewID sqlbase.ID,
) error {
	viewDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, viewID)
	if err != nil {
		return err
	}
	viewName := viewDesc.Name
	if viewDesc.ParentID != parentID {
		var err error
		viewName, err = p.getQualifiedTableName(ctx, viewDesc)
		if err != nil {
			log.Warningf(ctx, "unable to retrieve name of view %d: %v", viewID, err)
			msg := fmt.Sprintf("cannot rename %s %q because a view depends on it",
				typeName, objName)
			return sqlbase.NewDependentObjectError(msg)
		}
	}
	msg := fmt.Sprintf("cannot rename %s %q because view %q depends on it",
		typeName, objName, viewName)
	hint := fmt.Sprintf("you can drop %s instead.", viewName)
	return sqlbase.NewDependentObjectErrorWithHint(msg, hint)
}
