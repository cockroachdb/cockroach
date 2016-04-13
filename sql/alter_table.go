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

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(n *parser.AlterTable) (planNode, *roachpb.Error) {
	if err := n.Table.NormalizeTableName(p.session.Database); err != nil {
		return nil, roachpb.NewError(err)
	}

	dbDesc, err := p.getDatabaseDesc(n.Table.Database())
	if err != nil {
		return nil, err
	}

	// Check if table exists.
	tbKey := tableKey{dbDesc.ID, n.Table.Table()}.Key()
	gr, pErr := p.txn.Get(tbKey)
	if pErr != nil {
		return nil, pErr
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, roachpb.NewUErrorf("table %q does not exist", n.Table.Table())
	}

	tableDesc, pErr := p.getTableDesc(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(&tableDesc, privilege.CREATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(tableDesc.Mutations)

	for _, cmd := range n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			col, idx, err := makeColumnDefDescs(d)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			status, i, err := tableDesc.FindColumnByName(col.Name)
			if err == nil {
				if status == DescriptorIncomplete && tableDesc.Mutations[i].Direction == DescriptorMutation_DROP {
					return nil, roachpb.NewUErrorf("column %q being dropped, try again later", col.Name)
				}
			}
			tableDesc.addColumnMutation(*col, DescriptorMutation_ADD)
			if idx != nil {
				tableDesc.addIndexMutation(*idx, DescriptorMutation_ADD)
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				if d.PrimaryKey {
					return nil, roachpb.NewUErrorf("multiple primary keys for table %q are not allowed", tableDesc.Name)
				}
				name := string(d.Name)
				idx := IndexDescriptor{
					Name:             name,
					Unique:           true,
					StoreColumnNames: d.Storing,
				}
				if err := idx.fillColumns(d.Columns); err != nil {
					return nil, roachpb.NewError(err)
				}
				status, i, err := tableDesc.FindIndexByName(name)
				if err == nil {
					if status == DescriptorIncomplete && tableDesc.Mutations[i].Direction == DescriptorMutation_DROP {
						return nil, roachpb.NewUErrorf("index %q being dropped, try again later", name)
					}
				}
				tableDesc.addIndexMutation(idx, DescriptorMutation_ADD)

			default:
				return nil, roachpb.NewErrorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			status, i, err := tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, roachpb.NewError(err)
			}
			switch status {
			case DescriptorActive:
				col := tableDesc.Columns[i]
				if tableDesc.PrimaryIndex.containsColumnID(col.ID) {
					return nil, roachpb.NewUErrorf("column %q is referenced by the primary key", col.Name)
				}
				for _, idx := range tableDesc.allNonDropIndexes() {
					if idx.containsColumnID(col.ID) {
						return nil, roachpb.NewUErrorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
				tableDesc.addColumnMutation(col, DescriptorMutation_DROP)
				tableDesc.Columns = append(tableDesc.Columns[:i], tableDesc.Columns[i+1:]...)

			case DescriptorIncomplete:
				switch tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return nil, roachpb.NewUErrorf("column %q in the middle of being added, try again later", t.Column)

				case DescriptorMutation_DROP:
					// Noop.
				}
			}

		case *parser.AlterTableDropConstraint:
			status, i, err := tableDesc.FindIndexByName(t.Constraint)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, roachpb.NewError(err)
			}
			switch status {
			case DescriptorActive:
				tableDesc.addIndexMutation(tableDesc.Indexes[i], DescriptorMutation_DROP)
				tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)

			case DescriptorIncomplete:
				switch tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return nil, roachpb.NewUErrorf("constraint %q in the middle of being added, try again later", t.Constraint)

				case DescriptorMutation_DROP:
					// Noop.
				}
			}

		case parser.ColumnMutationCmd:
			// Column mutations
			status, i, err := tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return nil, roachpb.NewError(err)
			}

			switch status {
			case DescriptorActive:
				applyColumnMutation(&tableDesc.Columns[i], t)
				descriptorChanged = true

			case DescriptorIncomplete:
				switch tableDesc.Mutations[i].Direction {
				case DescriptorMutation_ADD:
					return nil, roachpb.NewUErrorf("column %q in the middle of being added, try again later", t.GetColumn())
				case DescriptorMutation_DROP:
					return nil, roachpb.NewUErrorf("column %q in the middle of being dropped", t.GetColumn())
				}
			}

		default:
			return nil, roachpb.NewErrorf("unsupported alter cmd: %T", cmd)
		}
	}
	// Were some changes made?
	//
	// This is only really needed for the unittests that add dummy mutations
	// before calling ALTER TABLE commands. We do not want to apply those
	// dummy mutations. Most tests trigger errors above
	// this line, but tests that run redundant operations like dropping
	// a column when it's already dropped will hit this condition and exit.
	mutationID := invalidMutationID
	if len(tableDesc.Mutations) > origNumMutations {
		mutationID = tableDesc.NextMutationID
		tableDesc.NextMutationID++
	} else if !descriptorChanged {
		return &emptyNode{}, nil
	}
	tableDesc.UpVersion = true

	if err := tableDesc.AllocateIDs(); err != nil {
		return nil, roachpb.NewError(err)
	}

	if pErr := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), wrapDescriptor(&tableDesc)); err != nil {
		return nil, pErr
	}
	p.notifySchemaChange(tableDesc.ID, mutationID)

	return &emptyNode{}, nil
}

func applyColumnMutation(col *ColumnDescriptor, mut parser.ColumnMutationCmd) {
	switch t := mut.(type) {
	case *parser.AlterTableSetDefault:
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			s := t.Default.String()
			col.DefaultExpr = &s
		}

	case *parser.AlterTableDropNotNull:
		col.Nullable = true
	}
}
