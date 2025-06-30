// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file

package workload_generator

import (
	"fmt"
	"strconv"
	"strings"
)

// Column stores column level schema information based on input ddl.
type Column struct {
	Name         string // Name: name of the column
	ColType      string // ColType: SQL data type of the column
	IsNullable   bool   // IsNullable: whether the column allows NULL values
	IsPrimaryKey bool   // IsPrimaryKey: whether the column is part of the primary key
	Default      string // Default: default value expression for the column
	IsUnique     bool   // IsUnique: whether the column has a UNIQUE constraint
	FKTable      string // FKTable: name of the referenced table if this is a foreign key
	FKColumn     string // FKColumn: name of the referenced column if this is a foreign key
	InlineCheck  string // InlineCheck: CHECK constraint expression if defined inline with the column
}

// String function converts the Column schema details into a parsable placeholder.
func (c *Column) String() string {
	parts := []string{c.Name, c.ColType}
	if c.IsNullable {
		parts = append(parts, sqlNull)
	} else {
		parts = append(parts, sqlNotNull)
	}
	if c.IsPrimaryKey {
		parts = append(parts, sqlPrimaryKey)
	}
	if c.Default != "" {
		parts = append(parts, fmt.Sprintf("%s %s", sqlDefault, c.Default))
	}
	if c.IsUnique {
		parts = append(parts, sqlUnique)
	}
	if c.FKTable != "" && c.FKColumn != "" {
		parts = append(parts, fmt.Sprintf("%s→%s.%s", sqlForeignKey, c.FKTable, c.FKColumn))
	}
	if c.InlineCheck != "" {
		parts = append(parts, fmt.Sprintf("%s(%s)", sqlCheck, c.InlineCheck))
	}
	return strings.Join(parts, " ")
}

// TableSchema stores table level schema information based on input ddl.
type TableSchema struct {
	rowCount          int                // rowCount: number of rows in the table (used internally)
	TableName         string             // TableName: fully qualified name of the table
	Columns           map[string]*Column // Columns: map of column names to their definitions
	PrimaryKeys       []string           // PrimaryKeys: list of column names that form the primary key
	UniqueConstraints [][]string         // UniqueConstraints: list of unique constraints, each containing a list of column names
	ForeignKeys       [][3]interface{}   // ForeignKeys: list of foreign keys: (local cols []string, table string, foreign cols []string)
	CheckConstraints  []string           // CheckConstraints: list of CHECK constraint expressions
	OriginalTable     string             // OriginalTable: original table name as it appears in the DDL
	ColumnOrder       []string           // ColumnOrder: order of columns as defined in the DDL
	TableNumber       int                // TableNumber: unique number assigned to the table for internal use
}

// NewTableSchema creates a new TableSchema instance with the given table name and original name.
// It initializes an empty columns map and returns a pointer to the new TableSchema.
func NewTableSchema(name string, original string) *TableSchema {
	return &TableSchema{
		TableName:     name,
		Columns:       make(map[string]*Column),
		OriginalTable: original,
	}
}

// String function converts the TableSchema object into a readable format - mostly for symmetry and testing.
func (ts *TableSchema) String() string {
	out := []string{fmt.Sprintf("Table: %s", ts.TableName), " Columns:"}
	if len(ts.ColumnOrder) > 0 {
		out = append(out, "ColumnOrder: "+strings.Join(ts.ColumnOrder, ", "))
	}
	out = append(out, "Table Number: "+strconv.Itoa(ts.TableNumber))
	for _, col := range ts.Columns {
		out = append(out, "  "+col.String())
	}
	if len(ts.PrimaryKeys) > 0 {
		out = append(out, " PKs: "+strings.Join(ts.PrimaryKeys, ", "))
	}
	if len(ts.UniqueConstraints) > 0 {
		tmp := []string{}
		for _, u := range ts.UniqueConstraints {
			tmp = append(tmp, "("+strings.Join(u, ",")+")")
		}
		out = append(out, " UNIQUE: "+strings.Join(tmp, "; "))
	}
	if len(ts.ForeignKeys) > 0 {
		tmp := []string{}
		for _, fk := range ts.ForeignKeys {
			l := fk[0].([]string)
			t := fk[1].(string)
			f := fk[2].([]string)
			tmp = append(tmp, fmt.Sprintf("(%s)→%s(%s)", strings.Join(l, ","), t, strings.Join(f, ",")))
		}
		out = append(out, " FKs: "+strings.Join(tmp, "; "))
	}
	if len(ts.CheckConstraints) > 0 {
		out = append(out, " CHECKs: "+strings.Join(ts.CheckConstraints, "; "))
	}
	return strings.Join(out, "\n") + "\n"
}

// AddColumn adds a Column to the TableSchema by storing it in the Columns map
// using the column name as the key.
func (ts *TableSchema) AddColumn(c *Column) {
	ts.Columns[c.Name] = c
}

// SetPrimaryKeys stores primary key information at table level and updates the
// corresponding column properties (IsPrimaryKey, IsNullable, IsUnique) accordingly.
func (ts *TableSchema) SetPrimaryKeys(pks []string) {
	ts.PrimaryKeys = pks
	single := len(pks) == 1
	// Columns labeled as primary key are all set to not nullable.
	// Primary key columns are only marked as unique if they are not part of a composite Primary Key
	for _, pk := range pks {
		if col, ok := ts.Columns[pk]; ok {
			col.IsPrimaryKey = true
			col.IsNullable = false
			col.IsUnique = single
		}
	}
}

//above structs are particularly focused around extracting data from DDL
//and converting it into a structured format for further use
//importantly - they were designed for anonymization part which is still at TODO stage

// following structs are used to build the schema for the data generator
// they are used to define the schema in a way that can be serialized to YAML when needed
// and have information necessary only for the data generator

// ColumnMeta is the per-column metadata (type, args, FK info, etc.) that
// drives our per batch generators.
type ColumnMeta struct {
	Type          string                 `yaml:"type"`
	Args          map[string]interface{} `yaml:"args"`
	IsPrimaryKey  bool                   `yaml:"isPrimaryKey"`
	IsUnique      bool                   `yaml:"isUnique"`
	HasForeignKey bool                   `yaml:"hasForeignKey"`

	FK          string  `yaml:"fk,omitempty"`
	FKMode      string  `yaml:"fk_mode,omitempty"`
	ParentSeed  float64 `yaml:"parent_seed,omitempty"`
	Fanout      int     `yaml:"fanout,omitempty"`
	CompositeID int     `yaml:"composite_id,omitempty"`

	Default     string  `yaml:"default,omitempty"`
	DefaultProb float64 `yaml:"default_prob,omitempty"`
}

// TableBlock stores extra information at table level that is used by the per batch generator.
type TableBlock struct {
	Count         int                   `yaml:"count"`
	Columns       map[string]ColumnMeta `yaml:"columns"`
	PK            []string              `yaml:"pk"`
	SortBy        []string              `yaml:"sort-by"`
	Unique        [][]string            `yaml:"unique,omitempty"`
	OriginalTable string                `yaml:"original_table"`
	ColumnOrder   []string              `yaml:"column_order"`
	TableNumber   int                   `yaml:"table_number"`
}
