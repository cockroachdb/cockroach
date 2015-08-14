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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

// ShowColumns of a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(n *parser.ShowColumns) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []string{"Field", "Type", "Null"}}
	for i, col := range desc.Columns {
		v.rows = append(v.rows, []parser.Datum{
			parser.DString(desc.Columns[i].Name),
			parser.DString(col.Type.SQLString()),
			parser.DBool(desc.Columns[i].Nullable),
		})
	}
	return v, nil
}

// ShowDatabases returns all the databases.
// Privileges: None.
//   Notes: postgres does not have a "show databases"
//          mysql has a "SHOW DATABASES" permission, but we have no system-level permissions.
func (p *planner) ShowDatabases(n *parser.ShowDatabases) (planNode, error) {
	prefix := structured.MakeNameMetadataKey(structured.RootNamespaceID, "")
	sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []string{"Database"}}
	for _, row := range sr {
		name := string(bytes.TrimPrefix(row.Key, prefix))
		v.rows = append(v.rows, []parser.Datum{parser.DString(name)})
	}
	return v, nil
}

// ShowGrants returns grant details for the specified objects and users.
// TODO(marc): implement multiple targets, or no targets (meaning full scan).
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(n *parser.ShowGrants) (planNode, error) {
	if n.Targets == nil {
		return nil, fmt.Errorf("TODO(marc): implement SHOW GRANT with no targets")
	}
	descriptor, err := p.getDescriptorFromTargetList(*n.Targets)
	if err != nil {
		return nil, err
	}

	objectType := "Database"
	if n.Targets.Tables != nil {
		objectType = "Table"
	}

	v := &valuesNode{columns: []string{objectType, "User", "Privileges"}}
	var wantedUsers map[string]struct{}
	if len(n.Grantees) != 0 {
		wantedUsers = make(map[string]struct{})
	}
	for _, u := range n.Grantees {
		wantedUsers[u] = struct{}{}
	}

	userPrivileges, err := descriptor.GetPrivileges().Show()
	if err != nil {
		return nil, err
	}
	for _, userPriv := range userPrivileges {
		if wantedUsers != nil {
			if _, ok := wantedUsers[userPriv.User]; !ok {
				continue
			}
		}
		v.rows = append(v.rows, []parser.Datum{
			parser.DString(descriptor.GetName()),
			parser.DString(userPriv.User),
			parser.DString(userPriv.Privileges),
		})
	}
	return v, nil
}

// ShowIndex returns all the indexes for a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW INDEX statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	v := &valuesNode{columns: []string{"Table", "Name", "Unique", "Seq", "Column"}}

	name := n.Table.Table()
	for _, index := range append([]structured.IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		for j, col := range index.ColumnNames {
			v.rows = append(v.rows, []parser.Datum{
				parser.DString(name),
				parser.DString(index.Name),
				parser.DBool(index.Unique),
				parser.DInt(j + 1),
				parser.DString(col),
			})
		}
	}
	return v, nil
}

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(n *parser.ShowTables) (planNode, error) {
	if n.Name == nil {
		if p.session.Database == "" {
			return nil, errNoDatabase
		}
		n.Name = &parser.QualifiedName{Base: parser.Name(p.session.Database)}
	}
	dbDesc, err := p.getDatabaseDesc(n.Name.String())
	if err != nil {
		return nil, err
	}

	tableNames, err := p.getTableNames(dbDesc)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []string{"Table"}}
	for _, name := range tableNames {
		v.rows = append(v.rows, []parser.Datum{parser.DString(name.Table())})
	}

	return v, nil
}
