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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
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

	newTableDesc := proto.Clone(tableDesc).(*TableDescriptor)

	b := client.Batch{}
	for _, cmd := range n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			col, idx, err := makeColumnDefDescs(d)
			if err != nil {
				return nil, err
			}
			newTableDesc.AddColumn(*col)
			if idx != nil {
				if err := newTableDesc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return nil, err
				}
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				idx := IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					ColumnNames:      d.Columns,
					StoreColumnNames: d.Storing,
				}
				if err := newTableDesc.AddIndex(idx, d.PrimaryKey); err != nil {
					return nil, err
				}
			default:
				return nil, util.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			i, err := newTableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, err
			}

			col := newTableDesc.Columns[i]
			if newTableDesc.PrimaryIndex.containsColumnID(col.ID) {
				return nil, fmt.Errorf("column %q is referenced by the primary key", col.Name)
			}
			for _, idx := range newTableDesc.Indexes {
				if idx.containsColumnID(col.ID) {
					return nil, fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
				}
			}

			newTableDesc.Columns = append(newTableDesc.Columns[:i], newTableDesc.Columns[i+1:]...)

		case *parser.AlterTableDropConstraint:
			i, err := newTableDesc.FindIndexByName(t.Constraint)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return nil, err
			}
			newTableDesc.Indexes = append(newTableDesc.Indexes[:i], newTableDesc.Indexes[i+1:]...)

		default:
			return nil, util.Errorf("unsupported alter cmd: %T", cmd)
		}
	}

	if err := newTableDesc.AllocateIDs(); err != nil {
		return nil, err
	}

	if err := p.backfillBatch(&b, n.Table, tableDesc, newTableDesc); err != nil {
		return nil, err
	}

	// TODO(pmattis): This is a hack. Remove when schema change operations work
	// properly.
	p.hackNoteSchemaChange(newTableDesc)

	b.Put(MakeDescMetadataKey(newTableDesc.GetID()), wrapDescriptor(newTableDesc))

	if err := p.txn.Run(&b); err != nil {
		return nil, convertBatchError(newTableDesc, b, err)
	}

	return &valuesNode{}, nil
}
