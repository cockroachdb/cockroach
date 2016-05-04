// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

type alterTableNode struct {
	p         *planner
	n         *parser.AlterTable
	tableDesc *TableDescriptor
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(n *parser.AlterTable) (planNode, error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		return nil, databaseDoesNotExistError(n.Table.Database())
	}

	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		if n.IfExists {
			return &emptyNode{}, nil
		}
		return nil, tableDoesNotExistError(n.Table.String())
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	return &alterTableNode{n: n, p: p, tableDesc: tableDesc}, nil
}

func (n *alterTableNode) Start() *roachpb.Error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			col, idx, err := makeColumnDefDescs(d)
			if err != nil {
				return roachpb.NewError(err)
			}
			status, i, err := n.tableDesc.FindColumnByName(col.Name)
			if err == nil {
				if status == DescriptorIncomplete && n.tableDesc.Mutations[i].Direction == DescriptorMutation_DROP {
					return roachpb.NewUErrorf("column %q being dropped, try again later", col.Name)
				}
			}
			n.tableDesc.addColumnMutation(*col, DescriptorMutation_ADD)
			if idx != nil {
				n.tableDesc.addIndexMutation(*idx, DescriptorMutation_ADD)
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				if d.PrimaryKey {
					return roachpb.NewUErrorf("multiple primary keys for table %q are not allowed", n.tableDesc.Name)
				}
				name := string(d.Name)
				idx := IndexDescriptor{
					Name:             name,
					Unique:           true,
					StoreColumnNames: d.Storing,
				}
				if err := idx.fillColumns(d.Columns); err != nil {
					return roachpb.NewError(err)
				}
				status, i, err := n.tableDesc.FindIndexByName(name)
				if err == nil {
					if status == DescriptorIncomplete && n.tableDesc.Mutations[i].Direction == DescriptorMutation_DROP {
						return roachpb.NewUErrorf("index %q being dropped, try again later", name)
					}
				}
				n.tableDesc.addIndexMutation(idx, DescriptorMutation_ADD)

			default:
				return roachpb.NewUErrorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			status, i, err := n.tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return roachpb.NewError(err)
			}
			switch status {
			case DescriptorActive:
				col := n.tableDesc.Columns[i]
				if n.tableDesc.PrimaryIndex.containsColumnID(col.ID) {
					return roachpb.NewUErrorf("column %q is referenced by the primary key", col.Name)
				}
				for _, idx := range n.tableDesc.allNonDropIndexes() {
					if idx.containsColumnID(col.ID) {
						return roachpb.NewUErrorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
				n.tableDesc.addColumnMutation(col, DescriptorMutation_DROP)
				n.tableDesc.Columns = append(n.tableDesc.Columns[:i], n.tableDesc.Columns[i+1:]...)

			case DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return roachpb.NewUErrorf("column %q in the middle of being added, try again later", t.Column)

				case DescriptorMutation_DROP:
					// Noop.
				}
			}

		case *parser.AlterTableDropConstraint:
			status, i, err := n.tableDesc.FindIndexByName(t.Constraint)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return roachpb.NewError(err)
			}
			switch status {
			case DescriptorActive:
				n.tableDesc.addIndexMutation(n.tableDesc.Indexes[i], DescriptorMutation_DROP)
				n.tableDesc.Indexes = append(n.tableDesc.Indexes[:i], n.tableDesc.Indexes[i+1:]...)

			case DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return roachpb.NewUErrorf("constraint %q in the middle of being added, try again later", t.Constraint)

				case DescriptorMutation_DROP:
					// Noop.
				}
			}

		case parser.ColumnMutationCmd:
			// Column mutations
			status, i, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return roachpb.NewError(err)
			}

			switch status {
			case DescriptorActive:
				if err := applyColumnMutation(&n.tableDesc.Columns[i], t); err != nil {
					return roachpb.NewError(err)
				}
				descriptorChanged = true

			case DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return roachpb.NewUErrorf("column %q in the middle of being added, try again later", t.GetColumn())
				case DescriptorMutation_DROP:
					return roachpb.NewUErrorf("column %q in the middle of being dropped", t.GetColumn())
				}
			}

		default:
			return roachpb.NewUErrorf("unsupported alter cmd: %T", cmd)
		}
	}
	// Were some changes made?
	//
	// This is only really needed for the unittests that add dummy mutations
	// before calling ALTER TABLE commands. We do not want to apply those
	// dummy mutations. Most tests trigger errors above
	// this line, but tests that run redundant operations like dropping
	// a column when it's already dropped will hit this condition and exit.
	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		return nil
	}

	mutationID := invalidMutationID
	var err error
	if addedMutations {
		mutationID, err = n.tableDesc.finalizeMutation()
	} else {
		err = n.tableDesc.setUpVersion()
	}
	if err != nil {
		return roachpb.NewError(err)
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return roachpb.NewError(err)
	}

	if err := n.p.writeTableDesc(n.tableDesc); err != nil {
		return roachpb.NewError(err)
	}
	n.p.notifySchemaChange(n.tableDesc.ID, mutationID)

	return nil
}

func (n *alterTableNode) Next() bool                   { return false }
func (n *alterTableNode) Columns() []ResultColumn      { return make([]ResultColumn, 0) }
func (n *alterTableNode) Ordering() orderingInfo       { return orderingInfo{} }
func (n *alterTableNode) Values() parser.DTuple        { return parser.DTuple{} }
func (n *alterTableNode) DebugValues() debugValues     { return debugValues{} }
func (n *alterTableNode) PErr() *roachpb.Error         { return nil }
func (n *alterTableNode) SetLimitHint(_ int64, _ bool) {}
func (n *alterTableNode) MarkDebug(mode explainMode)   {}
func (n *alterTableNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "alter table", "", nil
}

func applyColumnMutation(col *ColumnDescriptor, mut parser.ColumnMutationCmd) error {
	switch t := mut.(type) {
	case *parser.AlterTableSetDefault:
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			colDatumType := col.Type.toDatumType()
			if err := sanitizeDefaultExpr(t.Default, colDatumType); err != nil {
				return err
			}
			s := t.Default.String()
			col.DefaultExpr = &s
		}

	case *parser.AlterTableDropNotNull:
		col.Nullable = true
	}
	return nil
}
