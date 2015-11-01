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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
)

// AlterTable creates a table.
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

	// Check if table exists.
	tbKey := tableKey{dbDesc.ID, n.Table.Table()}.Key()
	gr, err := p.txn.Get(tbKey)
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		if n.IfExists {
			// Noop.
			return &valuesNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", n.Table.Table())
	}

	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	for _, cmd := range n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			col, idx, err := makeColumnDefDescs(d)
			if err != nil {
				return nil, err
			}
			mutation := TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_AddColumn{AddColumn: col}}
			if err := tableDesc.appendMutation(mutation, &p.session); err != nil {
				return nil, err
			}
			if idx != nil {
				mutation = TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_AddIndex{AddIndex: idx}}
				if err := tableDesc.appendMutation(mutation, &p.session); err != nil {
					return nil, err
				}
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				if d.PrimaryKey {
					return nil, fmt.Errorf("multiple primary keys for table %q are not allowed", tableDesc.Name)
				}
				idx := IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					ColumnNames:      d.Columns,
					StoreColumnNames: d.Storing,
				}
				mutation := TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_AddIndex{AddIndex: &idx}}
				if err := tableDesc.appendMutation(mutation, &p.session); err != nil {
					return nil, err
				}
			default:
				return nil, util.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			i, err := tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, err
			}

			col := tableDesc.Columns[i]
			if tableDesc.PrimaryIndex.containsColumnID(col.ID) {
				return nil, fmt.Errorf("column %q is referenced by the primary key", col.Name)
			}
			for _, idx := range tableDesc.Indexes {
				if idx.containsColumnID(col.ID) {
					return nil, fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
				}
			}
			mutation := TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_DropColumn{DropColumn: &col}}
			if err := tableDesc.appendMutation(mutation, &p.session); err != nil {
				return nil, err
			}

		case *parser.AlterTableDropConstraint:
			i, err := tableDesc.FindIndexByName(t.Constraint)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, err
			}
			mutation := TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_DropIndex{DropIndex: &tableDesc.Indexes[i]}}
			if err := tableDesc.appendMutation(mutation, &p.session); err != nil {
				return nil, err
			}

		default:
			return nil, util.Errorf("unsupported alter cmd: %T", cmd)
		}
	}

	if err := tableDesc.put(p); err != nil {
		return nil, err
	}

	return &valuesNode{}, nil
}
