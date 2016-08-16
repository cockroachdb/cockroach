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
		informationSchemaColumnsTable,
		informationSchemaSchemataTable,
		informationSchemaTablesTable,
	},
}

var (
	// defString is used as the value for columns included in the sql standard
	// of information_schema that don't make sense for CockroachDB. This is
	// identical to the behavior of MySQL.
	defString = parser.NewDString("def")

	// information_schema was defined before the BOOLEAN data type was added to
	// the SQL specification. Because of this, boolean values are represented
	// as strings.
	yesString = parser.NewDString("YES")
	noString  = parser.NewDString("NO")
)

func yesOrNoDatum(b bool) parser.Datum {
	if b {
		return yesString
	}
	return noString
}

func dStringOrNull(s *string) parser.Datum {
	if s == nil {
		return parser.DNull
	}
	return parser.NewDString(*s)
}

func dIntFnOrNull(fn func() (int32, bool)) parser.Datum {
	if n, ok := fn(); ok {
		return parser.NewDInt(parser.DInt(n))
	}
	return parser.DNull
}

var informationSchemaColumnsTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.columns (
  TABLE_CATALOG STRING NOT NULL DEFAULT '',
  TABLE_SCHEMA STRING NOT NULL DEFAULT '',
  TABLE_NAME STRING NOT NULL DEFAULT '',
  COLUMN_NAME STRING NOT NULL DEFAULT '',
  ORDINAL_POSITION INT NOT NULL DEFAULT 0,
  COLUMN_DEFAULT STRING,
  IS_NULLABLE STRING NOT NULL DEFAULT '',
  DATA_TYPE STRING NOT NULL DEFAULT '',
  CHARACTER_MAXIMUM_LENGTH INT,
  CHARACTER_OCTET_LENGTH INT,
  NUMERIC_PRECISION INT,
  NUMERIC_SCALE INT,
  DATETIME_PRECISION INT
);
`,
	populate: func(p *planner, addRow func(...parser.Datum)) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) {
				// Table descriptors already holds columns in-order.
				visible := 0
				for _, column := range table.Columns {
					if column.Hidden {
						continue
					}
					visible++
					addRow(
						defString,                                    // table_catalog
						parser.NewDString(db.Name),                   // table_schema
						parser.NewDString(table.Name),                // table_name
						parser.NewDString(column.Name),               // column_name
						parser.NewDInt(parser.DInt(visible)),         // ordinal_position, 1-indexed
						dStringOrNull(column.DefaultExpr),            // column_default
						yesOrNoDatum(column.Nullable),                // is_nullable
						parser.NewDString(column.Type.Kind.String()), // data_type
						characterMaximumLength(column.Type),          // character_maximum_length
						characterOctetLength(column.Type),            // character_octet_length
						numericPrecision(column.Type),                // numeric_precision
						numericScale(column.Type),                    // numeric_scale
						datetimePrecision(column.Type),               // datetime_precision
					)
				}
			},
		)
	},
}

func characterMaximumLength(colType sqlbase.ColumnType) parser.Datum {
	return dIntFnOrNull(colType.MaxCharacterLength)
}

func characterOctetLength(colType sqlbase.ColumnType) parser.Datum {
	return dIntFnOrNull(colType.MaxOctetLength)
}

func numericPrecision(colType sqlbase.ColumnType) parser.Datum {
	return dIntFnOrNull(colType.NumericPrecision)
}

func numericScale(colType sqlbase.ColumnType) parser.Datum {
	return dIntFnOrNull(colType.NumericScale)
}

func datetimePrecision(colType sqlbase.ColumnType) parser.Datum {
	// We currently do not support a datetime precision.
	return parser.DNull
}

var informationSchemaSchemataTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.schemata (
  CATALOG_NAME STRING NOT NULL DEFAULT '',
  SCHEMA_NAME STRING NOT NULL DEFAULT '',
  DEFAULT_CHARACTER_SET_NAME STRING NOT NULL DEFAULT '',
  SQL_PATH STRING
);`,
	populate: func(p *planner, addRow func(...parser.Datum)) error {
		return forEachDatabaseDesc(p, func(db *sqlbase.DatabaseDescriptor) {
			addRow(
				defString,                  // catalog_name
				parser.NewDString(db.Name), // schema_name
				parser.DNull,               // default_character_set_name
				parser.DNull,               // sql_path
			)
		})
	},
}

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
					defString,                     // table_catalog
					parser.NewDString(db.Name),    // table_schema
					parser.NewDString(table.Name), // table_name
					tableType,                     // table_type
					parser.NewDInt(parser.DInt(table.Version)), // version
				)
			},
		)
	},
}

type sortedDBDescs []*sqlbase.DatabaseDescriptor

// sortedDBDescs implements sort.Interface. It sorts a slice of DatabaseDescriptors
// by the database name.
func (dbs sortedDBDescs) Len() int           { return len(dbs) }
func (dbs sortedDBDescs) Swap(i, j int)      { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs sortedDBDescs) Less(i, j int) bool { return dbs[i].Name < dbs[j].Name }

// forEachTableDesc retrieves all database descriptors and iterates through them in
// lexicographical order with respect to their name. For each database, the function
// will call fn with its descriptor.
func forEachDatabaseDesc(
	p *planner,
	fn func(*sqlbase.DatabaseDescriptor),
) error {
	// Handle real schemas
	dbDescs, err := p.getAllDatabaseDescs()
	if err != nil {
		return err
	}

	// Handle virtual schemas.
	for _, schema := range virtualSchemaMap {
		dbDescs = append(dbDescs, schema.desc)
	}

	sort.Sort(sortedDBDescs(dbDescs))
	for _, db := range dbDescs {
		if userCanSeeDatabase(db, p.session.User) {
			fn(db)
		}
	}
	return nil
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
			if userCanSeeTable(tableDesc, p.session.User) {
				fn(db.desc, tableDesc)
			}
		}
	}
	return nil
}

func userCanSeeDatabase(db *sqlbase.DatabaseDescriptor, user string) bool {
	return userCanSeeDescriptor(db, user)
}

func userCanSeeTable(table *sqlbase.TableDescriptor, user string) bool {
	return userCanSeeDescriptor(table, user) && table.State == sqlbase.TableDescriptor_PUBLIC
}
