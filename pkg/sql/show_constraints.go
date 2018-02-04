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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ShowConstraints returns all the constraints for a table.
// Privileges: Any privilege on table.
//   Notes: postgres does not have a SHOW CONSTRAINTS statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowConstraints(ctx context.Context, n *tree.ShowConstraints) (planNode, error) {
	tn, err := n.Table.Normalize()
	if err != nil {
		return nil, err
	}

	var desc *TableDescriptor
	// We avoid the cache so that we can observe the constraints without
	// taking a lease, like other SHOW commands. We also use
	// allowAdding=true so we can look at the constraints of a table
	// added in the same transaction.
	//
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		desc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	})
	if err != nil {
		return nil, sqlbase.NewUndefinedRelationError(tn)
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return nil, err
	}

	columns := sqlbase.ResultColumns{
		{Name: "Table", Typ: types.String},
		{Name: "Name", Typ: types.String},
		{Name: "Type", Typ: types.String},
		{Name: "Column(s)", Typ: types.String},
		{Name: "Details", Typ: types.String},
	}

	return &delayedNode{
		name:    "SHOW CONSTRAINTS FROM " + tn.String(),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(columns, 0)

			info, err := desc.GetConstraintInfo(ctx, p.txn)
			if err != nil {
				return nil, err
			}
			for name, c := range info {
				detailsDatum := tree.DNull
				if c.Details != "" {
					detailsDatum = tree.NewDString(c.Details)
				}
				columnsDatum := tree.DNull
				if c.Columns != nil {
					columnsDatum = tree.NewDString(strings.Join(c.Columns, ", "))
				}
				kind := string(c.Kind)
				if c.Unvalidated {
					kind += " (UNVALIDATED)"
				}
				newRow := []tree.Datum{
					tree.NewDString(tn.Table()),
					tree.NewDString(name),
					tree.NewDString(kind),
					columnsDatum,
					detailsDatum,
				}
				if _, err := v.rows.AddRow(ctx, newRow); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}

			// Sort the results by constraint name.
			return &sortNode{
				plan: v,
				ordering: sqlbase.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Ascending},
				},
				columns: v.columns,
			}, nil
		},
	}, nil
}
