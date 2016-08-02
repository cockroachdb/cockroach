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
		var dbNames []string
		dbNamesToTables := make(map[string]map[string]*sqlbase.TableDescriptor)

		// Handle virtual schemas.
		for dbName, schema := range virtualSchemaMap {
			dbNames = append(dbNames, dbName)
			dbTables := make(map[string]*sqlbase.TableDescriptor)
			for tableName, entry := range schema.tables {
				dbTables[tableName] = entry.desc
			}
			dbNamesToTables[dbName] = dbTables
		}

		// Handle real schemas.
		descs, err := p.getAllDescriptors()
		if err != nil {
			return err
		}
		dbIDsToName := make(map[sqlbase.ID]string)
		for _, desc := range descs {
			if db, ok := desc.(*sqlbase.DatabaseDescriptor); ok {
				dbNames = append(dbNames, db.GetName())
				dbIDsToName[db.GetID()] = db.GetName()
				dbNamesToTables[db.GetName()] = make(map[string]*sqlbase.TableDescriptor)
			}
		}
		for _, desc := range descs {
			if table, ok := desc.(*sqlbase.TableDescriptor); ok {
				dbName, ok := dbIDsToName[table.GetParentID()]
				if !ok {
					return errors.Errorf("no database with ID %d found", table.GetParentID())
				}
				dbTables := dbNamesToTables[dbName]
				dbTables[table.GetName()] = table
			}
		}

		sort.Strings(dbNames)
		for _, dbName := range dbNames {
			var dbTableNames []string
			dbTables := dbNamesToTables[dbName]
			for tableName := range dbTables {
				dbTableNames = append(dbTableNames, tableName)
			}
			sort.Strings(dbTableNames)
			for _, tableName := range dbTableNames {
				table := dbTables[tableName]
				if !userCanSeeTable(table, p.session.User) {
					continue
				}
				tableType := tableTypeBaseTable
				if isVirtualDescriptor(table) {
					tableType = tableTypeSystemView
				}
				addRow(
					defString,
					parser.NewDString(dbName),
					parser.NewDString(tableName),
					tableType,
					parser.NewDInt(parser.DInt(table.GetVersion())),
				)
			}
		}
		return nil
	},
}

func userCanSeeTable(table *sqlbase.TableDescriptor, user string) bool {
	return table.State == sqlbase.TableDescriptor_PUBLIC &&
		(table.Privileges.AnyPrivilege(user) || isVirtualDescriptor(table))
}
