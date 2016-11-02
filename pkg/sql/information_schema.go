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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var informationSchema = virtualSchema{
	name: "information_schema",
	tables: []virtualSchemaTable{
		informationSchemaColumnsTable,
		informationSchemaKeyColumnUsageTable,
		informationSchemaSchemataTable,
		informationSchemaSchemataTablePrivileges,
		informationSchemaStatisticsTable,
		informationSchemaTableConstraintTable,
		informationSchemaTablePrivileges,
		informationSchemaTablesTable,
		informationSchemaViewsTable,
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
				return forEachColumnInTable(table, func(column *sqlbase.ColumnDescriptor) error {
					visible++
					return addRow(
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
					)
				})
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
	indexDirectionNA = "N/A"
)

var informationSchemaStatisticsTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.statistics (
	TABLE_CATALOG STRING NOT NULL DEFAULT '',
	TABLE_SCHEMA STRING NOT NULL DEFAULT '',
	TABLE_NAME STRING NOT NULL DEFAULT '',
	NON_UNIQUE BOOL NOT NULL DEFAULT FALSE,
	INDEX_SCHEMA STRING NOT NULL DEFAULT '',
	INDEX_NAME STRING NOT NULL DEFAULT '',
	SEQ_IN_INDEX INT NOT NULL DEFAULT 0,
	COLUMN_NAME STRING NOT NULL DEFAULT '',
	"COLLATION" STRING NOT NULL DEFAULT '',
	CARDINALITY INT NOT NULL DEFAULT 0,
	DIRECTION STRING NOT NULL DEFAULT '',
	STORING BOOL NOT NULL DEFAULT FALSE 
);`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				appendRow := func(index *sqlbase.IndexDescriptor, colName string, sequence int,
					direction string, isStored bool) error {
					return addRow(
						defString,                                    // table_catalog
						parser.NewDString(db.GetName()),              // table_schema
						parser.NewDString(table.GetName()),           // table_name
						parser.MakeDBool(parser.DBool(index.Unique)), // non_unique
						parser.NewDString(db.GetName()),              // index_schema
						parser.NewDString(index.Name),                // index_name
						parser.NewDInt(parser.DInt(sequence)),        // seq_in_index
						parser.NewDString(colName),                   // column_name
						parser.DNull,                                 // collation
						parser.DNull,                                 // cardinality
						parser.NewDString(direction),                 // direction
						parser.MakeDBool(parser.DBool(isStored)),     // storing
					)
				}

				return forEachIndexInTable(table, func(index *sqlbase.IndexDescriptor) error {
					sequence := 1
					for i, col := range index.ColumnNames {
						// We add a row for each column of index.
						if err := appendRow(index, col, sequence,
							index.ColumnDirections[i].String(), false); err != nil {
							return err
						}
						sequence++
					}
					for _, col := range index.StoreColumnNames {
						// We add a row for each stored column of index.
						if err := appendRow(index, col, sequence,
							indexDirectionNA, true); err != nil {
							return err
						}
						sequence++
					}
					return nil
				})
			},
		)
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

var informationSchemaViewsTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.views (
    TABLE_CATALOG STRING NOT NULL DEFAULT '',
    TABLE_SCHEMA STRING NOT NULL DEFAULT '',
    TABLE_NAME STRING NOT NULL DEFAULT '',
    VIEW_DEFINITION STRING NOT NULL DEFAULT '',
    CHECK_OPTION STRING NOT NULL DEFAULT '',
    IS_UPDATABLE BOOL NOT NULL DEFAULT FALSE,
    IS_INSERTABLE_INTO BOOL NOT NULL DEFAULT FALSE,
    IS_TRIGGER_UPDATABLE BOOL NOT NULL DEFAULT FALSE,
    IS_TRIGGER_DELETABLE BOOL NOT NULL DEFAULT FALSE,
    IS_TRIGGER_INSERTABLE_INTO BOOL NOT NULL DEFAULT FALSE
);`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		return forEachTableDesc(p,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				if !table.IsView() {
					return nil
				}
				// Note that the view query printed will not include any column aliases
				// specified outside the initial view query into the definition returned,
				// unlike Postgres. For example, for the view created via
				//  `CREATE VIEW (a) AS SELECT b FROM foo`
				// we'll only print `SELECT b FROM foo` as the view definition here,
				// while Postgres would more accurately print `SELECT b AS a FROM foo`.
				// TODO(a-robinson): Insert column aliases into view query once we
				// have a semantic query representation to work with (#10083).
				return addRow(
					defString,                          // table_catalog
					parser.NewDString(db.Name),         // table_schema
					parser.NewDString(table.Name),      // table_name
					parser.NewDString(table.ViewQuery), // view_definition
					parser.DNull,                       // check_option
					parser.DNull,                       // is_updatable
					parser.DNull,                       // is_insertable_into
					parser.DNull,                       // is_trigger_updatable
					parser.DNull,                       // is_trigger_deletable
					parser.DNull,                       // is_trigger_insertable_into
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
func forEachDatabaseDesc(p *planner, fn func(*sqlbase.DatabaseDescriptor) error) error {
	// Handle real schemas
	dbDescs, err := p.getAllDatabaseDescs()
	if err != nil {
		return err
	}

	// Handle virtual schemas.
	for _, schema := range p.session.virtualSchemas.entries {
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
	p *planner, fn func(*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor) error,
) error {
	return forEachTableDescWithTableLookup(p, func(
		db *sqlbase.DatabaseDescriptor,
		table *sqlbase.TableDescriptor,
		_ tableLookupFn,
	) error {
		return fn(db, table)
	})
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID. Both descriptors will be nil if a
// table with the provided ID was not found.
type tableLookupFn func(tableID sqlbase.ID) (*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor)

// forEachTableDescWithTableLookup acts like forEachTableDesc, except it also provides a
// tableLookupFn when calling fn to allow callers to lookup fetched table descriptors
// on demand. This is important for callers dealing with objects like foreign keys, where
// the metadata for each object must be augmented by looking at the referenced table.
func forEachTableDescWithTableLookup(
	p *planner, fn func(*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor, tableLookupFn) error,
) error {
	type dbDescTables struct {
		desc       *sqlbase.DatabaseDescriptor
		tables     map[string]*sqlbase.TableDescriptor
		tablesByID map[sqlbase.ID]*sqlbase.TableDescriptor
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
				desc:       db,
				tables:     make(map[string]*sqlbase.TableDescriptor),
				tablesByID: make(map[sqlbase.ID]*sqlbase.TableDescriptor),
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
			dbTables.tables[table.Name] = table
			dbTables.tablesByID[table.ID] = table
		}
	}

	// Handle virtual schemas.
	for dbName, schema := range p.session.virtualSchemas.entries {
		dbTables := make(map[string]*sqlbase.TableDescriptor, len(schema.tables))
		for tableName, entry := range schema.tables {
			dbTables[tableName] = entry.desc
		}
		databases[dbName] = dbDescTables{
			desc:   schema.desc,
			tables: dbTables,
		}
	}

	// Create table lookup function, which some callers of this function, like those
	// dealing with foreign keys, will need.
	tableLookup := func(id sqlbase.ID) (*sqlbase.DatabaseDescriptor, *sqlbase.TableDescriptor) {
		for _, db := range databases {
			table, ok := db.tablesByID[id]
			if ok {
				return db.desc, table
			}
		}
		return nil, nil
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
				if err := fn(db.desc, tableDesc, tableLookup); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func forEachIndexInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.IndexDescriptor) error,
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

func forEachColumnInTable(
	table *sqlbase.TableDescriptor, fn func(*sqlbase.ColumnDescriptor) error,
) error {
	// Table descriptors already hold columns in-order.
	for i := range table.Columns {
		if !table.Columns[i].Hidden {
			if err := fn(&table.Columns[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func forEachColumnInIndex(
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	fn func(*sqlbase.ColumnDescriptor) error,
) error {
	colMap := make(map[sqlbase.ColumnID]*sqlbase.ColumnDescriptor, len(table.Columns))
	for i, column := range table.Columns {
		colMap[column.ID] = &table.Columns[i]
	}
	for _, columnID := range index.ColumnIDs {
		if column := colMap[columnID]; !column.Hidden {
			if err := fn(column); err != nil {
				return err
			}
		}
	}
	return nil
}

func forEachUser(p *planner, fn func(username string) error) error {
	query := `SELECT username FROM system.users`
	plan, err := p.query(query)
	if err != nil {
		return nil
	}
	defer plan.Close()
	if err := plan.Start(); err != nil {
		return err
	}

	// TODO(cuongdo/asubiotto): Get rid of root user special-casing if/when a row
	// for "root" exists in system.user.
	if err := fn(security.RootUser); err != nil {
		return err
	}

	for {
		next, err := plan.Next()
		if err != nil {
			return err
		}
		if !next {
			break
		}
		row := plan.Values()
		username := row[0].(*parser.DString)
		if err := fn(string(*username)); err != nil {
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
