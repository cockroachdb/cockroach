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
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var (
	errEmptyColumnName = errors.New("empty column name")
	errEmptyIndexName  = errors.New("empty index name")
)

// RenameDatabase renames the database.
// Privileges: security.RootUser user, DROP on source database.
//   Notes: postgres requires superuser, db owner, or "CREATEDB".
//          mysql >= 5.1.23 does not allow database renames.
func (p *planner) RenameDatabase(n *parser.RenameDatabase) (planNode, error) {
	if n.Name == "" || n.NewName == "" {
		return nil, errEmptyDatabaseName
	}

	if p.session.User != security.RootUser {
		return nil, fmt.Errorf("only %s is allowed to rename databases", security.RootUser)
	}

	dbDesc, err := p.mustGetDatabaseDesc(string(n.Name))
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	if n.Name == n.NewName {
		// Noop.
		return &emptyNode{}, nil
	}

	if err := p.renameDatabase(dbDesc, string(n.NewName)); err != nil {
		return nil, err
	}
	return &emptyNode{}, nil
}

// RenameTable renames the table or view.
// Privileges: DROP on source table/view, CREATE on destination database.
//   Notes: postgres requires the table owner.
//          mysql requires ALTER, DROP on the original table, and CREATE, INSERT
//          on the new table (and does not copy privileges over).
func (p *planner) RenameTable(n *parser.RenameTable) (planNode, error) {
	oldTn, err := n.Name.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	newTn, err := n.NewName.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	dbDesc, err := p.mustGetDatabaseDesc(oldTn.Database())
	if err != nil {
		return nil, err
	}

	// Check if source table or view exists.
	// Note that Postgres's behavior here is a little lenient - it'll let you
	// modify views by running ALTER TABLE, but won't let you modify tables
	// by running ALTER VIEW. Our behavior is strict for now, but can be
	// made more lenient down the road if needed.
	var tableDesc *sqlbase.TableDescriptor
	if n.IsView {
		tableDesc, err = p.getViewDesc(oldTn)
		if err != nil {
			return nil, err
		}
		if tableDesc == nil {
			if n.IfExists {
				// Noop.
				return &emptyNode{}, nil
			}
			// Key does not exist, but we want it to: error out.
			return nil, sqlbase.NewUndefinedViewError(oldTn.String())
		}
		if tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
			return nil, sqlbase.NewUndefinedViewError(oldTn.String())
		}
	} else {
		tableDesc, err = p.getTableDesc(oldTn)
		if err != nil {
			return nil, err
		}
		if tableDesc == nil {
			if n.IfExists {
				// Noop.
				return &emptyNode{}, nil
			}
			// Key does not exist, but we want it to: error out.
			return nil, sqlbase.NewUndefinedTableError(oldTn.String())
		}
		if tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
			return nil, sqlbase.NewUndefinedTableError(oldTn.String())
		}
	}

	if err := p.checkPrivilege(tableDesc, privilege.DROP); err != nil {
		return nil, err
	}

	// Check if target database exists.
	targetDbDesc, err := p.mustGetDatabaseDesc(newTn.Database())
	if err != nil {
		return nil, err
	}

	if err := p.checkPrivilege(targetDbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	// oldTn and newTn are already normalized, so we can compare directly here.
	if oldTn.Database() == newTn.Database() && oldTn.Table() == newTn.Table() {
		// Noop.
		return &emptyNode{}, nil
	}

	tableDesc.SetName(newTn.Table())
	tableDesc.ParentID = targetDbDesc.ID

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	newTbKey := tableKey{targetDbDesc.ID, newTn.Table()}.Key()

	if err := tableDesc.Validate(p.txn); err != nil {
		return nil, err
	}

	descID := tableDesc.GetID()
	descDesc := sqlbase.WrapDescriptor(tableDesc)

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	renameDetails := sqlbase.TableDescriptor_RenameInfo{
		OldParentID: dbDesc.ID,
		OldName:     oldTn.Table()}
	tableDesc.Renames = append(tableDesc.Renames, renameDetails)
	if err := p.writeTableDesc(tableDesc); err != nil {
		return nil, err
	}

	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &client.Batch{}
	b.Put(descKey, descDesc)
	b.CPut(newTbKey, descID, nil)

	if err := p.txn.Run(b); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return nil, sqlbase.NewRelationAlreadyExistsError(newTn.Table())
		}
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)

	p.setTestingVerifyMetadata(func(systemConfig config.SystemConfig) error {
		if err := expectDescriptorID(systemConfig, newTbKey, descID); err != nil {
			return err
		}
		if err := expectDescriptor(systemConfig, descKey, descDesc); err != nil {
			return err
		}
		return nil
	})

	return &emptyNode{}, nil
}

// RenameIndex renames the index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameIndex(n *parser.RenameIndex) (planNode, error) {
	tn, err := n.Index.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := p.mustGetTableDesc(tn)
	if err != nil {
		return nil, err
	}

	normIdxName := sqlbase.NormalizeName(n.Index.Index)
	status, i, err := tableDesc.FindIndexByNormalizedName(normIdxName)
	if err != nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Index does not exist, but we want it to: error out.
		return nil, err
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if n.NewName == "" {
		return nil, errEmptyIndexName
	}
	normNewIdxName := sqlbase.NormalizeName(n.NewName)

	if normIdxName == normNewIdxName {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindIndexByNormalizedName(normNewIdxName); err == nil {
		return nil, fmt.Errorf("index name %q already exists", n.NewName)
	}

	if status == sqlbase.DescriptorActive {
		tableDesc.Indexes[i].Name = normNewIdxName
	} else {
		tableDesc.Mutations[i].GetIndex().Name = normNewIdxName
	}

	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(p.txn); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)
	return &emptyNode{}, nil
}

// RenameColumn renames the column.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameColumn(n *parser.RenameColumn) (planNode, error) {
	// Check if table exists.
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}
	tableDesc, err := p.getTableDesc(tn)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		if n.IfExists {
			// Noop.
			return &emptyNode{}, nil
		}
		// Key does not exist, but we want it to: error out.
		return nil, fmt.Errorf("table %q does not exist", tn.Table())
	}

	if err := p.checkPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if n.NewName == "" {
		return nil, errEmptyColumnName
	}
	normNewColName := sqlbase.NormalizeName(n.NewName)
	normColName := sqlbase.NormalizeName(n.Name)

	status, i, err := tableDesc.FindColumnByNormalizedName(normColName)
	// n.IfExists only applies to table, no need to check here.
	if err != nil {
		return nil, err
	}
	var column *sqlbase.ColumnDescriptor
	if status == sqlbase.DescriptorActive {
		column = &tableDesc.Columns[i]
	} else {
		column = tableDesc.Mutations[i].GetColumn()
	}

	if normColName == normNewColName {
		// Noop.
		return &emptyNode{}, nil
	}

	if _, _, err := tableDesc.FindColumnByNormalizedName(normNewColName); err == nil {
		return nil, fmt.Errorf("column name %q already exists", n.NewName)
	}

	preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
		if vBase, ok := expr.(parser.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*parser.ColumnItem); ok {
				if sqlbase.NormalizeName(c.ColumnName) == normColName {
					c.ColumnName = n.NewName
				}
			}
			return nil, false, v
		}
		return nil, true, expr
	}

	exprStrings := make([]string, len(tableDesc.Checks))
	for i, check := range tableDesc.Checks {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprsTraditional(exprStrings)
	if err != nil {
		return nil, err
	}

	for i := range tableDesc.Checks {
		expr, err := parser.SimpleVisit(exprs[i], preFn)
		if err != nil {
			return nil, err
		}
		if after := expr.String(); after != tableDesc.Checks[i].Expr {
			tableDesc.Checks[i].Expr = after
		}
	}
	// Rename the column in the indexes.
	tableDesc.RenameColumnNormalized(column.ID, normNewColName)
	column.Name = normNewColName
	if err := tableDesc.SetUpVersion(); err != nil {
		return nil, err
	}

	descKey := sqlbase.MakeDescMetadataKey(tableDesc.GetID())
	if err := tableDesc.Validate(p.txn); err != nil {
		return nil, err
	}
	if err := p.txn.Put(descKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return nil, err
	}
	p.notifySchemaChange(tableDesc.ID, sqlbase.InvalidMutationID)
	return &emptyNode{}, nil
}
