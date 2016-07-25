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
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

type alterTableNode struct {
	p         *planner
	n         *parser.AlterTable
	tableDesc *sqlbase.TableDescriptor
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(n *parser.AlterTable) (planNode, error) {
	tableDesc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		if n.IfExists {
			return &emptyNode{}, nil
		}
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	return &alterTableNode{n: n, p: p, tableDesc: tableDesc}, nil
}

func (n *alterTableNode) expandPlan() error {
	return nil
}

func (n *alterTableNode) Start() error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			col, idx, err := sqlbase.MakeColumnDefDescs(d)
			if err != nil {
				return err
			}
			status, i, err := n.tableDesc.FindColumnByName(col.Name)
			if err == nil {
				if status == sqlbase.DescriptorIncomplete &&
					n.tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_DROP {
					return fmt.Errorf("column %q being dropped, try again later", col.Name)
				}
				if status == sqlbase.DescriptorActive && t.IfNotExists {
					continue
				}
			}

			n.tableDesc.AddColumnMutation(*col, sqlbase.DescriptorMutation_ADD)
			if idx != nil {
				n.tableDesc.AddIndexMutation(*idx, sqlbase.DescriptorMutation_ADD)
			}
			if t.ColumnDef.Family.Create || len(t.ColumnDef.Family.Name) > 0 {
				err := n.tableDesc.AddColumnToFamilyMaybeCreate(
					col.Name, string(t.ColumnDef.Family.Name), t.ColumnDef.Family.Create,
					t.ColumnDef.Family.IfNotExists)
				if err != nil {
					return err
				}
			}

		case *parser.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				if d.PrimaryKey {
					return fmt.Errorf("multiple primary keys for table %q are not allowed", n.tableDesc.Name)
				}
				name := string(d.Name)
				idx := sqlbase.IndexDescriptor{
					Name:             name,
					Unique:           true,
					StoreColumnNames: d.Storing,
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}
				status, i, err := n.tableDesc.FindIndexByName(name)
				if err == nil {
					if status == sqlbase.DescriptorIncomplete &&
						n.tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_DROP {
						return fmt.Errorf("index %q being dropped, try again later", name)
					}
				}
				n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD)

			default:
				return fmt.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			status, i, err := n.tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return err
			}
			switch status {
			case sqlbase.DescriptorActive:
				col := n.tableDesc.Columns[i]
				if n.tableDesc.PrimaryIndex.ContainsColumnID(col.ID) {
					return fmt.Errorf("column %q is referenced by the primary key", col.Name)
				}
				for _, idx := range n.tableDesc.AllNonDropIndexes() {
					if idx.ContainsColumnID(col.ID) {
						return fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
				n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_DROP)
				n.tableDesc.Columns = append(n.tableDesc.Columns[:i], n.tableDesc.Columns[i+1:]...)

			case sqlbase.DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case sqlbase.DescriptorMutation_ADD:
					return fmt.Errorf("column %q in the middle of being added, try again later", t.Column)

				case sqlbase.DescriptorMutation_DROP:
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
				return err
			}
			switch status {
			case sqlbase.DescriptorActive:
				n.tableDesc.AddIndexMutation(n.tableDesc.Indexes[i], sqlbase.DescriptorMutation_DROP)
				n.tableDesc.Indexes = append(n.tableDesc.Indexes[:i], n.tableDesc.Indexes[i+1:]...)

			case sqlbase.DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case sqlbase.DescriptorMutation_ADD:
					return fmt.Errorf("constraint %q in the middle of being added, try again later", t.Constraint)

				case sqlbase.DescriptorMutation_DROP:
					// Noop.
				}
			}

		case parser.ColumnMutationCmd:
			// Column mutations
			status, i, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}

			switch status {
			case sqlbase.DescriptorActive:
				if err := applyColumnMutation(&n.tableDesc.Columns[i], t); err != nil {
					return err
				}
				descriptorChanged = true

			case sqlbase.DescriptorIncomplete:
				switch n.tableDesc.Mutations[i].Direction {
				case sqlbase.DescriptorMutation_ADD:
					return fmt.Errorf("column %q in the middle of being added, try again later", t.GetColumn())
				case sqlbase.DescriptorMutation_DROP:
					return fmt.Errorf("column %q in the middle of being dropped", t.GetColumn())
				}
			}

		default:
			return fmt.Errorf("unsupported alter cmd: %T", cmd)
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

	mutationID := sqlbase.InvalidMutationID
	var err error
	if addedMutations {
		mutationID, err = n.tableDesc.FinalizeMutation()
	} else {
		err = n.tableDesc.SetUpVersion()
	}
	if err != nil {
		return err
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	if err := n.p.writeTableDesc(n.tableDesc); err != nil {
		return err
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(n.p.leaseMgr).InsertEventRecord(n.p.txn,
		EventLogAlterTable,
		int32(n.tableDesc.ID),
		int32(n.p.evalCtx.NodeID),
		struct {
			TableName  string
			Statement  string
			User       string
			MutationID uint32
		}{n.tableDesc.Name, n.n.String(), n.p.session.User, uint32(mutationID)},
	); err != nil {
		return err
	}

	n.p.notifySchemaChange(n.tableDesc.ID, mutationID)

	return nil
}

func (n *alterTableNode) Next() (bool, error)                 { return false, nil }
func (n *alterTableNode) Columns() []ResultColumn             { return make([]ResultColumn, 0) }
func (n *alterTableNode) Ordering() orderingInfo              { return orderingInfo{} }
func (n *alterTableNode) Values() parser.DTuple               { return parser.DTuple{} }
func (n *alterTableNode) DebugValues() debugValues            { return debugValues{} }
func (n *alterTableNode) ExplainTypes(_ func(string, string)) {}
func (n *alterTableNode) SetLimitHint(_ int64, _ bool)        {}
func (n *alterTableNode) MarkDebug(mode explainMode)          {}
func (n *alterTableNode) ExplainPlan(v bool) (string, string, []planNode) {
	return "alter table", "", nil
}

func applyColumnMutation(col *sqlbase.ColumnDescriptor, mut parser.ColumnMutationCmd) error {
	switch t := mut.(type) {
	case *parser.AlterTableSetDefault:
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			colDatumType := col.Type.ToDatumType()
			if err := sqlbase.SanitizeVarFreeExpr(t.Default, colDatumType, "DEFAULT"); err != nil {
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
