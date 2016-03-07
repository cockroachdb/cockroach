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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Show a session-local variable name.
func (p *planner) Show(n *parser.Show) (planNode, error) {
	name := strings.ToUpper(n.Name)

	v := &valuesNode{columns: []ResultColumn{{Name: name, Typ: parser.DummyString}}}

	switch name {
	case `DATABASE`:
		v.rows = append(v.rows, []parser.Datum{parser.DString(p.session.Database)})
	case `TIME ZONE`:
		loc, err := p.evalCtx.GetLocation()
		if err != nil {
			return nil, err
		}
		v.rows = append(v.rows, []parser.Datum{parser.DString(loc.String())})
	case `SYNTAX`:
		v.rows = append(v.rows, []parser.Datum{parser.DString(parser.Syntax(p.session.Syntax).String())})
	case `DEFAULT_TRANSACTION_ISOLATION`:
		level := p.session.DefaultIsolationLevel.String()
		v.rows = append(v.rows, []parser.Datum{parser.DString(level)})
	case `TRANSACTION ISOLATION LEVEL`:
		v.rows = append(v.rows, []parser.Datum{parser.DString(p.txn.Proto.Isolation.String())})
	case `TRANSACTION PRIORITY`:
		v.rows = append(v.rows, []parser.Datum{parser.DString(p.txn.UserPriority.String())})
	default:
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	return v, nil
}

// ShowColumns of a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW COLUMNS statement.
//          mysql only returns columns you have privileges on.
func (p *planner) ShowColumns(n *parser.ShowColumns) (planNode, *roachpb.Error) {
	desc, pErr := p.getTableDesc(n.Table)
	if pErr != nil {
		return nil, pErr
	}
	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Field", Typ: parser.DummyString},
			{Name: "Type", Typ: parser.DummyString},
			{Name: "Null", Typ: parser.DummyBool},
			{Name: "Default", Typ: parser.DummyString},
		},
	}
	for i, col := range desc.Columns {
		defaultExpr := parser.Datum(parser.DNull)
		if e := desc.Columns[i].DefaultExpr; e != nil {
			defaultExpr = parser.DString(*e)
		}
		v.rows = append(v.rows, []parser.Datum{
			parser.DString(desc.Columns[i].Name),
			parser.DString(col.Type.SQLString()),
			parser.DBool(desc.Columns[i].Nullable),
			defaultExpr,
		})
	}
	return v, nil
}

// ShowDatabases returns all the databases.
// Privileges: None.
//   Notes: postgres does not have a "show databases"
//          mysql has a "SHOW DATABASES" permission, but we have no system-level permissions.
func (p *planner) ShowDatabases(n *parser.ShowDatabases) (planNode, *roachpb.Error) {
	// TODO(pmattis): This could be implemented as:
	//
	//   SELECT id FROM system.namespace WHERE parentID = 0

	prefix := MakeNameMetadataKey(keys.RootNamespaceID, "")
	sr, pErr := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if pErr != nil {
		return nil, pErr
	}
	v := &valuesNode{columns: []ResultColumn{{Name: "Database", Typ: parser.DummyString}}}
	for _, row := range sr {
		_, name, err := encoding.DecodeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		v.rows = append(v.rows, []parser.Datum{parser.DString(name)})
	}
	return v, nil
}

// ShowGrants returns grant details for the specified objects and users.
// TODO(marc): implement no targets (meaning full scan).
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(n *parser.ShowGrants) (planNode, *roachpb.Error) {
	if n.Targets == nil {
		return nil, roachpb.NewErrorf("TODO(marc): implement SHOW GRANT with no targets")
	}
	descriptors, pErr := p.getDescriptorsFromTargetList(*n.Targets)
	if pErr != nil {
		return nil, pErr
	}

	objectType := "Database"
	if n.Targets.Tables != nil {
		objectType = "Table"
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: objectType, Typ: parser.DummyString},
			{Name: "User", Typ: parser.DummyString},
			{Name: "Privileges", Typ: parser.DummyString},
		},
	}
	var wantedUsers map[string]struct{}
	if len(n.Grantees) != 0 {
		wantedUsers = make(map[string]struct{})
	}
	for _, u := range n.Grantees {
		wantedUsers[u] = struct{}{}
	}

	for _, descriptor := range descriptors {
		userPrivileges := descriptor.GetPrivileges().Show()
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
	}
	return v, nil
}

// ShowIndex returns all the indexes for a table.
// Privileges: None.
//   Notes: postgres does not have a SHOW INDEXES statement.
//          mysql requires some privilege for any column.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, *roachpb.Error) {
	desc, pErr := p.getTableDesc(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	v := &valuesNode{
		columns: []ResultColumn{
			{Name: "Table", Typ: parser.DummyString},
			{Name: "Name", Typ: parser.DummyString},
			{Name: "Unique", Typ: parser.DummyBool},
			{Name: "Seq", Typ: parser.DummyInt},
			{Name: "Column", Typ: parser.DummyString},
			{Name: "Direction", Typ: parser.DummyString},
			{Name: "Storing", Typ: parser.DummyBool},
		},
	}

	appendRow := func(index IndexDescriptor, colName string, sequence int,
		direction string, isStored bool) {
		v.rows = append(v.rows, []parser.Datum{
			parser.DString(n.Table.Table()),
			parser.DString(index.Name),
			parser.DBool(index.Unique),
			parser.DInt(sequence),
			parser.DString(colName),
			parser.DString(direction),
			parser.DBool(isStored),
		})
	}
	for _, index := range append([]IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...) {
		sequence := 1
		for i, col := range index.ColumnNames {
			appendRow(index, col, sequence, index.ColumnDirections[i].String(), false)
			sequence++
		}
		for _, col := range index.StoreColumnNames {
			appendRow(index, col, sequence, "N/A", true)
			sequence++
		}
	}
	return v, nil
}

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(n *parser.ShowTables) (planNode, *roachpb.Error) {
	// TODO(pmattis): This could be implemented as:
	//
	//   SELECT name FROM system.namespace
	//     WHERE parentID = (SELECT id FROM system.namespace
	//                       WHERE parentID = 0 AND name = <database>)

	name := n.Name
	if name == nil {
		if p.session.Database == "" {
			return nil, roachpb.NewError(errNoDatabase)
		}
		name = &parser.QualifiedName{Base: parser.Name(p.session.Database)}
	}
	dbDesc, pErr := p.getDatabaseDesc(string(name.Base))
	if pErr != nil {
		return nil, pErr
	}

	tableNames, pErr := p.getTableNames(dbDesc)
	if pErr != nil {
		return nil, pErr
	}
	v := &valuesNode{columns: []ResultColumn{{Name: "Table", Typ: parser.DummyString}}}
	for _, name := range tableNames {
		v.rows = append(v.rows, []parser.Datum{parser.DString(name.Table())})
	}

	return v, nil
}
