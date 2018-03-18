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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyIndexName = pgerror.NewError(pgerror.CodeSyntaxError, "empty index name")

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(ctx context.Context, n *tree.RenameIndex) (planNode, error) {
	var tableDesc *TableDescriptor
	var err error
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		_, tableDesc, err = expandIndexName(ctx, p, n.Index, true /* requireTable */)
	})
	if err != nil {
		return nil, err
	}

	idx, _, err := tableDesc.FindIndexByName(string(n.Index.Index))
	if err != nil {
		if n.IfExists {
			// Noop.
			return newZeroNode(nil /* columns */), nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	for _, tableRef := range tableDesc.DependedOnBy {
		if tableRef.IndexID != idx.ID {
			continue
		}
		return nil, p.dependentViewRenameError(
			ctx, "index", n.Index.Index.String(), tableDesc.ParentID, tableRef.ID)
	}

	if n.NewName == "" {
		return nil, errEmptyIndexName
	}

	if n.Index.Index == n.NewName {
		// Noop.
		return newZeroNode(nil /* columns */), nil
	}

	if _, _, err := tableDesc.FindIndexByName(string(n.NewName)); err == nil {
		return nil, fmt.Errorf("index name %q already exists", string(n.NewName))
	}

	tableDesc.RenameIndexDescriptor(idx, string(n.NewName))

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return nil, err
	}
	if err := p.txn.Put(ctx, descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}

	// TODO(vivek): the code above really should really be replaced by a call
	// to writeTableDesc(). However if we do that then a couple of logic tests
	// start failing. How can that be?
	//
	// if err := p.writeTableDesc(ctx, tableDesc); err != nil {
	//	return nil, err
	// }
	p.notifySchemaChange(tableDesc, sqlbase.InvalidMutationID)
	return newZeroNode(nil /* columns */), nil
}
