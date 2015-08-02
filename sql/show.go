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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

// ShowColumns of a table.
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
func (p *planner) ShowDatabases(n *parser.ShowDatabases) (planNode, error) {
	prefix := keys.MakeNameMetadataKey(structured.RootNamespaceID, "")
	sr, err := p.db.Scan(prefix, prefix.PrefixEnd(), 0)
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

// ShowIndex returns all the indexes for a table.
func (p *planner) ShowIndex(n *parser.ShowIndex) (planNode, error) {
	desc, err := p.getTableDesc(n.Table)
	if err != nil {
		return nil, err
	}

	v := &valuesNode{columns: []string{"Table", "Name", "Unique", "Seq", "Column"}}

	name := n.Table.Table()
	for i, index := range desc.Indexes {
		for j, col := range index.ColumnNames {
			v.rows = append(v.rows, []parser.Datum{
				parser.DString(name),
				parser.DString(desc.Indexes[i].Name),
				parser.DBool(desc.Indexes[i].Unique),
				parser.DInt(j + 1),
				parser.DString(col),
			})
		}
	}
	return v, nil
}

// ShowTables returns all the tables.
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
	prefix := keys.MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := p.db.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	v := &valuesNode{columns: []string{"Table"}}
	for _, row := range sr {
		name := string(bytes.TrimPrefix(row.Key, prefix))
		v.rows = append(v.rows, []parser.Datum{parser.DString(name)})
	}
	return v, nil
}
