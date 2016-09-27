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
		informationSchemaKeyColumnUsageTable,
		informationSchemaSchemataTable,
		informationSchemaSchemataTablePrivileges,
		informationSchemaTableConstraintTable,
		informationSchemaTablePrivileges,
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

func dStringOrNull(s string) parser.Datum {
	if s == "" {
		return parser.DNull
	}
	return parser.NewDString(s)
}

func dStringPtrOrNull(s *string) parser.Datum {
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
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				// Table descriptors already holds columns in-order.
				visible := 0
				for _, column := range table.Columns {
					if column.Hidden {
						continue
					}
					visible++
					if err := addRow(
						defString,                                    // table_catalog
						parser.NewDString(db.Name),                   // table_schema
						parser.NewDString(table.Name),                // table_name
						parser.NewDString(column.Name),               // column_name
						parser.NewDInt(parser.DInt(visible)),         // ordinal_position, 1-indexed
						dStringPtrOrNull(column.DefaultExpr),         // column_default
						yesOrNoDatum(column.Nullable),                // is_nullable
						parser.NewDString(column.Type.Kind.String()), // data_type
						characterMaximumLength(column.Type),          // character_maximum_length
						characterOctetLength(column.Type),            // character_octet_length
						numericPrecision(column.Type),                // numeric_precision
						numericScale(column.Type),                    // numeric_scale
						datetimePrecision(column.Type),               // datetime_precision
					); err != nil {
						return err
					}
				}
				return nil
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

var informationSchemaKeyColumnUsageTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.key_column_usage (
  CONSTRAINT_CATALOG STRING DEFAULT '',
  CONSTRAINT_SCHEMA STRING DEFAULT '',
  CONSTRAINT_NAME STRING DEFAULT '',
  TABLE_CATALOG STRING NOT NULL DEFAULT '',
  TABLE_SCHEMA STRING NOT NULL DEFAULT '',
  TABLE_NAME STRING NOT NULL DEFAULT '',
  COLUMN_NAME STRING NOT NULL DEFAULT '',
  ORDINAL_POSITION INT NOT NULL DEFAULT 0,
  POSITION_IN_UNIQUE_CONSTRAINT INT
);`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				type keyColumn struct {
					name        string
					columnNames []string
					primaryKey  bool
					foreignKey  bool
					unique      bool
				}
				var columns []keyColumn
				if table.IsPhysicalTable() {
					columns = append(columns, keyColumn{
						name:        table.PrimaryIndex.Name,
						columnNames: table.PrimaryIndex.ColumnNames,
						primaryKey:  true,
					})
				}
				for _, index := range table.Indexes {
					col := keyColumn{
						name:        index.Name,
						columnNames: index.ColumnNames,
						unique:      index.Unique,
						foreignKey:  index.ForeignKey.IsSet(),
					}
					if col.unique || col.foreignKey {
						columns = append(columns, col)
					}
				}
				for _, c := range columns {
					for pos, column := range c.columnNames {
						ordinalPos := parser.NewDInt(parser.DInt(pos + 1))
						uniquePos := parser.DNull
						if c.foreignKey {
							uniquePos = ordinalPos
						}
						if err := addRow(
							defString,                     // constraint_catalog
							parser.NewDString(db.Name),    // constraint_schema
							dStringOrNull(c.name),         // constraint_name
							defString,                     // table_catalog
							parser.NewDString(db.Name),    // table_schema
							parser.NewDString(table.Name), // table_name
							parser.NewDString(column),     // column_name
							ordinalPos,                    // ordinal_position, 1-indexed
							uniquePos,                     // position_in_unique_constraint
						); err != nil {
							return err
						}
					}
				}
				return nil
			},
		)
	},
}

var informationSchemaSchemataTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.schemata (
  CATALOG_NAME STRING NOT NULL DEFAULT '',
  SCHEMA_NAME STRING NOT NULL DEFAULT '',
  DEFAULT_CHARACTER_SET_NAME STRING NOT NULL DEFAULT '',
  SQL_PATH STRING
);`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachDatabaseDesc(p, func(db *sqlbase.DatabaseDescriptor) error {
			return addRow(
				defString,                  // catalog_name
				parser.NewDString(db.Name), // schema_name
				parser.DNull,               // default_character_set_name
				parser.DNull,               // sql_path
			)
		})
	},
}

var informationSchemaSchemataTablePrivileges = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.schema_privileges (
	GRANTEE STRING NOT NULL DEFAULT '',
	TABLE_CATALOG STRING NOT NULL DEFAULT '',
	TABLE_SCHEMA STRING NOT NULL DEFAULT '',
	PRIVILEGE_TYPE STRING NOT NULL DEFAULT '',
	IS_GRANTABLE BOOL NOT NULL DEFAULT FALSE
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachDatabaseDesc(p, func(db *sqlbase.DatabaseDescriptor) error {
			for _, u := range db.Privileges.Show() {
				for _, privilege := range u.Privileges {
					if err := addRow(
						parser.NewDString(u.User),    // grantee
						defString,                    // table_catalog,
						parser.NewDString(db.Name),   // table_schema
						parser.NewDString(privilege), // privilege_type
						parser.DNull,                 // is_grantable
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

var (
	constraintTypeCheck      = parser.NewDString("CHECK")
	constraintTypeForeignKey = parser.NewDString("FOREIGN KEY")
	constraintTypePrimaryKey = parser.NewDString("PRIMARY KEY")
	constraintTypeUnique     = parser.NewDString("UNIQUE")
)

// TODO(dt): switch using common GetConstraintInfo helper.
var informationSchemaTableConstraintTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.table_constraints (
  CONSTRAINT_CATALOG STRING NOT NULL DEFAULT '',
  CONSTRAINT_SCHEMA STRING NOT NULL DEFAULT '',
  CONSTRAINT_NAME STRING NOT NULL DEFAULT '',
  TABLE_SCHEMA STRING NOT NULL DEFAULT '',
  TABLE_NAME STRING NOT NULL DEFAULT '',
  CONSTRAINT_TYPE STRING NOT NULL DEFAULT ''
);`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				type constraint struct {
					name string
					typ  parser.Datum
				}
				var constraints []constraint
				if table.IsPhysicalTable() {
					constraints = append(constraints, constraint{
						name: table.PrimaryIndex.Name,
						typ:  constraintTypePrimaryKey,
					})
				}
				for _, index := range table.Indexes {
					c := constraint{name: index.Name}
					switch {
					case index.Unique:
						c.typ = constraintTypeUnique
						constraints = append(constraints, c)
					case index.ForeignKey.IsSet():
						c.typ = constraintTypeForeignKey
						constraints = append(constraints, c)
					}
				}
				for _, check := range table.Checks {
					constraints = append(constraints, constraint{
						name: check.Name,
						typ:  constraintTypeCheck,
					})
				}
				for _, c := range constraints {
					if err := addRow(
						defString,                     // constraint_catalog
						parser.NewDString(db.Name),    // constraint_schema
						dStringOrNull(c.name),         // constraint_name
						parser.NewDString(db.Name),    // table_schema
						parser.NewDString(table.Name), // table_name
						c.typ, // constraint_type
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
	},
}

var informationSchemaTablePrivileges = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.table_privileges (
	GRANTOR STRING NOT NULL DEFAULT '',
	GRANTEE STRING NOT NULL DEFAULT '',
	TABLE_CATALOG STRING NOT NULL DEFAULT '',
	TABLE_SCHEMA STRING NOT NULL DEFAULT '',
	TABLE_NAME STRING NOT NULL DEFAULT '',
	PRIVILEGE_TYPE STRING NOT NULL DEFAULT '',
	IS_GRANTABLE BOOL NOT NULL DEFAULT FALSE,
	WITH_HIERARCHY BOOL NOT NULL DEFAULT FALSE
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				for _, u := range table.Privileges.Show() {
					for _, privilege := range u.Privileges {
						if err := addRow(
							parser.DNull,                  // grantor
							parser.NewDString(u.User),     // grantee
							defString,                     // table_catalog,
							parser.NewDString(db.Name),    // table_schema
							parser.NewDString(table.Name), // table_name
							parser.NewDString(privilege),  // privilege_type
							parser.DNull,                  // is_grantable
							parser.DNull,                  // with_hierarchy
						); err != nil {
							return err
						}
					}
				}
				return nil
			},
		)
	},
}

var (
	tableTypeSystemView = parser.NewDString("SYSTEM VIEW")
	tableTypeBaseTable  = parser.NewDString("BASE TABLE")
	tableTypeView       = parser.NewDString("VIEW")
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
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableType := tableTypeBaseTable
				if isVirtualDescriptor(table) {
					tableType = tableTypeSystemView
				} else if table.IsView() {
					tableType = tableTypeView
				}
				return addRow(
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
	fn func(*sqlbase.DatabaseDescriptor) error,
) error {
	// Handle real schemas
	dbDescs, err := p.getAllDatabaseDescs()
	if err != nil {
		return err
	}

	// Handle virtual schemas.
	for _, schema := range p.virtualSchemas().entries {
		dbDescs = append(dbDescs, schema.desc)
	}

	sort.Sort(sortedDBDescs(dbDescs))
	for _, db := range dbDescs {
		if userCanSeeDatabase(db, p.session.User) {
			if err := fn(db); err != nil {
				return err
			}
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
	fn func(*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor) error,
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
	for dbName, schema := range p.virtualSchemas().entries {
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
				if err := fn(db.desc, tableDesc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func forEachIndexInTable(
	table *sqlbase.TableDescriptor,
	fn func(*sqlbase.IndexDescriptor) error,
) error {
	if table.IsPhysicalTable() {
		if err := fn(&table.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range table.Indexes {
		if err := fn(&table.Indexes[i]); err != nil {
			return err
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
