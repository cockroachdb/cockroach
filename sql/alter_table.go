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
			if err := tableDesc.appendMutation(mutation); err != nil {
				return nil, err
			}
			if idx != nil {
				mutation = TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_AddIndex{AddIndex: idx}}
				if err := tableDesc.appendMutation(mutation); err != nil {
					return nil, err
				}
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				idx := &IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					ColumnNames:      d.Columns,
					StoreColumnNames: d.Storing,
				}
				mutation := TableDescriptor_Mutation{Descriptor_: &TableDescriptor_Mutation_AddIndex{AddIndex: idx}, Primary: d.PrimaryKey}
				if err := tableDesc.appendMutation(mutation); err != nil {
					return nil, err
				}
			default:
				return nil, util.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		default:
			return nil, util.Errorf("unsupported alter cmd: %T", cmd)
		}
	}
	if err := p.txn.Put(MakeDescMetadataKey(tableDesc.GetID()), tableDesc); err != nil {
		return nil, err
	}
	p.txn.SetSystemDBTrigger()

	if err := p.txn.Commit(); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}
