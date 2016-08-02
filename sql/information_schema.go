// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"sort"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

var informationSchema = virtualSchema{
	name: "information_schema",
	tables: []virtualSchemaTable{
		informationSchemaTablesTable,
	},
}

// defString is used as the value for columns included in the sql standard
// of information_schema that don't make sense for CockroachDB. This is
// identical to the behavior of MySQL.
var defString = parser.NewDString("def")

var (
	tableTypeSystemView = parser.NewDString("SYSTEM VIEW")
	tableTypeBaseTable  = parser.NewDString("BASE TABLE")
)

var informationSchemaTablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.tables (
  TABLE_CATALOG STRING NOT NULL DEFAULT '',
  TABLE_SCHEMA STRING NOT NULL DEFAULT '',
  TABLE_NAME STRING NOT NULL DEFAULT '',
  TABLE_TYPE STRING NOT NULL DEFAULT '',
  VERSION INT
);`,
	populate: func(p *planner, addRow func(...parser.Datum)) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) {
				tableType := tableTypeBaseTable
				if isVirtualDescriptor(table) {
					tableType = tableTypeSystemView
				}
				addRow(
					defString,
					parser.NewDString(db.Name),
					parser.NewDString(table.Name),
					tableType,
					parser.NewDInt(parser.DInt(table.GetVersion())),
				)
			},
		)
	},
}

// forEachTableDesc retrieves all table descriptors and iterates through them in
// lexicographical order with respect primarily to database name and secondarily
// to table name. For each table, the function will call fn with its respective
// database and table descriptor.
func forEachTableDesc(
	p *planner,
	fn func(*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor),
) error {
	type dbDescTables struct {
		desc   *sqlbase.DatabaseDescriptor
		tables map[string]*sqlbase.TableDescriptor
	}
	databases := make(map[string]dbDescTables)

	// Handle virtual schemas.
	for dbName, schema := range virtualSchemaMap {
		dbTables := make(map[string]*sqlbase.TableDescriptor, len(schema.tables))
		for tableName, entry := range schema.tables {
			dbTables[tableName] = entry.desc
		}
		databases[dbName] = dbDescTables{
			desc:   schema.desc,
			tables: dbTables,
		}
	}

	// Handle real schemas.
	descs, err := p.getAllDescriptors()
	if err != nil {
		return err
	}
	dbIDsToName := make(map[sqlbase.ID]string)
	// First, iterate through all database descriptors, constructing dbDescTables
	// objects and populating a mapping from sqlbase.ID to database name.
	for _, desc := range descs {
		if db, ok := desc.(*sqlbase.DatabaseDescriptor); ok {
			dbIDsToName[db.GetID()] = db.GetName()
			databases[db.GetName()] = dbDescTables{
				desc:   db,
				tables: make(map[string]*sqlbase.TableDescriptor),
			}
		}
	}
	// Next, iterate through all table descriptors, using the mapping from sqlbase.ID
	// to database name to add descriptors to a dbDescTables' tables map.
	for _, desc := range descs {
		if table, ok := desc.(*sqlbase.TableDescriptor); ok {
			dbName, ok := dbIDsToName[table.GetParentID()]
			if !ok {
				return errors.Errorf("no database with ID %d found", table.GetParentID())
			}
			dbTables := databases[dbName]
			dbTables.tables[table.GetName()] = table
		}
	}

	// Below we use the same trick twice of sorting a slice of strings lexicographically
	// and iterating through these strings to index into a map. Effectively, this allows
	// us to iterate through a map in sorted order.
	dbNames := make([]string, 0, len(databases))
	for dbName := range databases {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		db := databases[dbName]
		dbTableNames := make([]string, 0, len(db.tables))
		for tableName := range db.tables {
			dbTableNames = append(dbTableNames, tableName)
		}
		sort.Strings(dbTableNames)
		for _, tableName := range dbTableNames {
			tableDesc := db.tables[tableName]
			if !userCanSeeTable(tableDesc, p.session.User) {
				continue
			}
			fn(db.desc, tableDesc)
		}
	}
	return nil
}

func userCanSeeTable(table *sqlbase.TableDescriptor, user string) bool {
	return table.State == sqlbase.TableDescriptor_PUBLIC &&
		(table.Privileges.AnyPrivilege(user) || isVirtualDescriptor(table))
}
