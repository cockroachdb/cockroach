// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package structured

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/util"
	yaml "gopkg.in/yaml.v1"
)

// Column contains the schema for a column. The Key should be a
// shortened identifier related to Name. The Column Key is stored
// within the row value itself and is used to maintain human
// readability for debugging, as well as a stable identifier for
// column data. Column Keys must be unique within a Table.
type Column struct {
	// Name is used externally (i.e. in JSON or Go structs) to refer to
	// this column value.
	Name string `yaml:"column"`

	// Key should be a 1-3 character abbreviation of the Name. The key is
	// used internally for efficiency.
	Key string `yaml:"column_key"`

	// Type is one of "integer", "float", "string", "blob", "time",
	// "latlong", "integerset", "stringset", "integermap", or "stringmap".
	// Integers are int64s. Floats are float64s. Strings must be UTF8
	// encoded. Blobs are arbitrary byte arrays. If sending over JSON,
	// they should be base64 encoded. Latlong are (latitude, longitude,
	// altitude, accuracy) quadruplets, each a float64 (altitude and
	// accuracy are in meters). Integersets are a set of int64
	// values. Integermaps are map[string]int64. Stringsets and
	// stringmaps are similar, but with string values.
	Type string `yaml:"type"`

	// ForeignKey is a foreign key reference specified as
	// <table>[.<column>]. The column suffix is optional in the event
	// that the table has a non-composite primary key. Foreign keys
	// presuppose a secondary index, so specifying one isn't necessary
	// unless the foreign key is a one-to-one relation, in which case
	// a unique index should be specified.
	ForeignKey string `yaml:"foreign_key,omitempty"`

	// Index specifies that this column generates an index. The index
	// type is one of "fulltext", "location", "secondary", or "unique".
	// "fulltext" is valid only for "string"-type columns. It generates
	// a full text index by segmenting the text from a UTF8 string
	// column and indexing each word as a separate term. Full text
	// indexes support phrase searches. "location" is valid only for
	// "latlong"-type columns. It generates S2 geometry patches to
	// canvas the specified latitude/longitude location. "secondary"
	// creates an index with terms equal to this column value.
	// Secondary indexes are created automatically for foreign keys, in
	// which case their terms may be a concatenation of foreign key
	// values in the event the foreign key references a table with a
	// composite primary key. "unique" indexes are the same as
	// "secondary", except a one-to-one relation is enforced; that is,
	// only a single instance of a particular value for this column (or
	// values if part of a composite foreign key) is allowed.
	Index string `yaml:"index,omitempty"`

	// Interleave is specified with foreign keys to co-locate dependent
	// data. Interleaved data is physically proximate to data referenced
	// by this foreign key for faster lookups when querying complete
	// sets of data (e.g. a merchant and all of its inventory), and for
	// faster transactional writes in certain common cases.
	Interleave bool `yaml:"interleave,omitempty"`

	// OnDelete is specified with foreign keys to dictate database
	// behavior in the event the object which the foreign key column
	// references is deleted. The two supported values are "cascade" and
	// "setnull". "cascade" deletes the object with the foreign key
	// reference. "setnull" is less destructive, merely setting the
	// foreign key column to nil. If "interleave" was specified for this
	// foreign key, then "cascade" is the mandatory default value;
	// specifying setnull result in a schema validation error.
	OnDelete string `yaml:"ondelete,omitempty"`

	// PrimaryKey specifies this column is the primary key for the table
	// or part of a composite primary key. The order in which primary
	// key columns are declared dictates the order in which their values
	// are concatenated to form the key. This has obvious implications
	// for range queries.
	PrimaryKey bool `yaml:"primary_key,omitempty"`

	// Scatter is specified with primary keys to effectively randomize
	// the placement of the data within the table's keyspace. This is
	// accomplished by prepending a two-byte hash of the entire primary
	// key to the actual key used to store the value.
	Scatter bool `yaml:"scatter,omitempty"`

	// Auto specifies that the value of this column auto-increments from
	// a monotonically-increasing sequence starting at this field's
	// value. If Auto is nil, the column does not auto-increment.
	Auto *int64 `yaml:"auto_increment,omitempty"`
}

// Table contains the schema for a table. The Key should be a
// shortened identifier related to Name. The Table Key is stored
// as part of every row key within the database, so shorter is
// better. Table Keys must be unique within a Schema.
type Table struct {
	Name    string    `yaml:"table"`
	Key     string    `yaml:"table_key"`
	Columns []*Column `yaml:",omitempty"`

	// byName is a map from column name to *Column.
	byName map[string]*Column
	// byKey is a map from column key to *Column.
	byKey map[string]*Column
	// primaryKey is a slice of columns which make up primary key.
	// There must be one or more columns.
	primaryKey []*Column
	// foreignKeys is a map of outgoing foreign keys from this table.
	// The outer map is keyed by referenced table name. The inner map
	// is keyed by referenced column name and points to the local
	// (i.e. this table's) foreign key column.
	foreignKeys map[string]map[string]*Column
	// incomingForeignKeys is a map of incoming foreign keys
	// referencing this table. The outer map is keyed by referencing
	// table name. The inner map is keyed by referencing column name
	// and points to the referencing column. Note that this column is
	// from the other table! This map is used to implement the
	// ondelete policy specified in the referencing column to either
	// delete the referencing object ("cascade") or set the columns
	// null ("setnull").
	incomingForeignKeys map[string]map[string]*Column
}

// TableSlice helpfully implements the sort interface.
type TableSlice []*Table

func (ts TableSlice) Len() int           { return len(ts) }
func (ts TableSlice) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts TableSlice) Less(i, j int) bool { return ts[i].Name < ts[j].Name }

// Schema contains a named sequence of Table schemas. The Key should
// be a shortened identifier related to Name. The Schema Key is stored
// as part of every row key within the database, so size
// matters and small is better (for once). Schema Keys must be unique
// within a Cockroach cluster.
type Schema struct {
	Name   string     `yaml:"db"`
	Key    string     `yaml:"db_key"`
	Tables TableSlice `yaml:",omitempty"`

	// byName is a map from table name to *Table.
	byName map[string]*Table
	// byKey is a map from table key to *Table.
	byKey map[string]*Table
}

// Regular expression for capturing foreign key declarations. Valid
// declarations include "Table.Column" or just "Table".
var foreignKeyRE = regexp.MustCompile(`^([^\.]*)(?:\.([^\.]*))?$`)

// The maximum length for a {schema, table, column} key.
const maxKeyLength = 3

// Valid schema column types.
const (
	columnTypeInteger    = "integer"
	columnTypeFloat      = "float"
	columnTypeString     = "string"
	columnTypeBlob       = "blob"
	columnTypeTime       = "time"
	columnTypeLatLong    = "latlong"
	columnTypeIntegerSet = "integerset"
	columnTypeStringSet  = "stringset"
	columnTypeIntegerMap = "integermap"
	columnTypeStringMap  = "stringmap"
)

// Set containing all valid schema column types.
var validTypes = map[string]struct{}{
	columnTypeInteger:    struct{}{},
	columnTypeFloat:      struct{}{},
	columnTypeString:     struct{}{},
	columnTypeBlob:       struct{}{},
	columnTypeTime:       struct{}{},
	columnTypeLatLong:    struct{}{},
	columnTypeIntegerSet: struct{}{},
	columnTypeStringSet:  struct{}{},
	columnTypeIntegerMap: struct{}{},
	columnTypeStringMap:  struct{}{},
}

// Valid index types.
const (
	indexTypeFullText  = "fulltext"
	indexTypeLocation  = "location"
	indexTypeSecondary = "secondary"
	indexTypeUnique    = "unique"
)

// Set containing all valid index types.
var validIndexTypes = map[string]struct{}{
	indexTypeFullText:  struct{}{},
	indexTypeLocation:  struct{}{},
	indexTypeSecondary: struct{}{},
	indexTypeUnique:    struct{}{},
}

// NewGoSchema returns a schema using name and key and a map from
// Table Key to an instance of a Go struct corresponding to the
// Table (the Table name is derived from the Go struct name).
func NewGoSchema(name, key string, schemaMap map[string]interface{}) (*Schema, error) {
	s := &Schema{
		Name: name,
		Key:  key,
	}
	for key, strct := range schemaMap {
		table, err := getTableSchema(key, strct)
		if err != nil {
			return nil, err
		}
		s.Tables = append(s.Tables, table)
	}
	// Sort tables.
	sort.Sort(s.Tables)

	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// NewYAMLSchema returns a schema based on the YAML input string.
func NewYAMLSchema(in []byte) (*Schema, error) {
	s := &Schema{}
	if err := yaml.Unmarshal(in, s); err != nil {
		return nil, err
	}
	// Sort tables.
	sort.Sort(s.Tables)

	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// ToYAML marshals the Schema into YAML.
func (s *Schema) ToYAML() ([]byte, error) {
	return yaml.Marshal(s)
}

// Validate validates the schema for consistency, correctness and
// completeness. Foreign keys are matched to their respective
// tables. Parameters are verified as valid (e.g. OnDelete can only
// be "cascade" or "setnull"). Refer to the source for the complete
// list of checks.
func (s *Schema) Validate() error {
	if len(s.Key) < 1 || len(s.Key) > maxKeyLength {
		return fmt.Errorf("schema %q: key %q must be 1-%d characters", s.Name, s.Key, maxKeyLength)
	}

	s.byName = map[string]*Table{}
	s.byKey = map[string]*Table{}

	// First pass through validation validates all tables. This establishes
	// primary keys, necessary to validate columns in second pass.
	for _, t := range s.Tables {
		// Check for duplicate table names.
		if _, ok := s.byName[t.Name]; ok {
			return fmt.Errorf("table %q: duplicate name", t.Name)
		}
		s.byName[t.Name] = t

		// Check for duplicate table keys.
		if _, ok := s.byKey[t.Key]; ok {
			return fmt.Errorf("table %q: duplicate key %q", t.Name, t.Key)
		}
		s.byKey[t.Key] = t

		// Verify table key length.
		if len(t.Key) < 1 || len(t.Key) > maxKeyLength {
			return fmt.Errorf("table %q: key %q must be 1-%d characters", t.Name, t.Key, maxKeyLength)
		}

		// Init table data structures.
		t.primaryKey = make([]*Column, 0, 1)
		t.foreignKeys = map[string]map[string]*Column{}
		t.incomingForeignKeys = map[string]map[string]*Column{}

		// Validate table.
		if err := s.validateTable(t); err != nil {
			return fmt.Errorf("table %q: %v", t.Name, err)
		}

		if len(t.primaryKey) == 0 {
			return fmt.Errorf("table %q: no primary key(s)", t.Name)
		}
	}

	// Second pass: validate columns of each table.
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			if err := s.validateColumn(c, t); err != nil {
				return fmt.Errorf("table %q, column %q: %v", t.Name, c.Name, err)
			}
		}
	}

	// Third pass: validate foreign keys (need all columns validated first).
	for _, t := range s.Tables {
		for fkTable := range t.foreignKeys {
			if err := s.validateForeignKey(t, fkTable); err != nil {
				return fmt.Errorf("table %q, foreign key %q: %v", t.Name, fkTable, err)
			}
		}
	}

	return nil
}

// validateTable validates the table for consistency, correctness and
// completeness.
func (s *Schema) validateTable(t *Table) error {
	t.byName = map[string]*Column{}
	t.byKey = map[string]*Column{}

	for _, c := range t.Columns {
		// Check for duplicate column names.
		if _, ok := t.byName[c.Name]; ok {
			return fmt.Errorf("column %q: duplicate name", c.Name)
		}
		t.byName[c.Name] = c

		// Check for duplicate column keys.
		if _, ok := t.byKey[c.Key]; ok {
			return fmt.Errorf("column %q: duplicate key %q", c.Name, c.Key)
		}
		t.byKey[c.Key] = c

		// Verify column key length.
		if len(c.Key) < 1 || len(c.Key) > maxKeyLength {
			return fmt.Errorf("column %q: key %q is limited to 1-3 characters", c.Name, c.Key)
		}

		// Add to table's primary key.
		if c.PrimaryKey {
			t.primaryKey = append(t.primaryKey, c)
		}
	}

	return nil
}

// validateColumn validates the column options.
func (s *Schema) validateColumn(c *Column, t *Table) error {
	if _, ok := validTypes[c.Type]; !ok {
		return fmt.Errorf("invalid type %q", c.Type)
	}

	// Verify primary key options. Scatter is only valid on first
	// component of primary key.
	if c.Scatter {
		if c != t.primaryKey[0] {
			return fmt.Errorf("scatter may only be specified on first column of primary key")
		}
	}

	// Auto-increment columns must be type integer!
	if c.Auto != nil && c.Type != columnTypeInteger {
		return fmt.Errorf("auto may only be specified with columns of type integer")
	}

	// Verify foreign key & associated options.
	if c.ForeignKey != "" {
		fkTable, fkColumn, err := s.parseForeignKey(c)
		if err != nil {
			return err
		}

		// Set outgoing foreign key reference.
		if fkMap, ok := t.foreignKeys[fkTable]; ok {
			fkMap[fkColumn] = c
		} else {
			t.foreignKeys[fkTable] = map[string]*Column{fkColumn: c}
		}

		// Set incoming foreign key on referenced table.
		if incomingMap, ok := s.byName[fkTable].incomingForeignKeys[t.Name]; ok {
			incomingMap[c.Name] = c
		} else {
			s.byName[fkTable].incomingForeignKeys[t.Name] = map[string]*Column{c.Name: c}
		}

		// Check OnDelete spec (only valid for foreign keys).
		switch c.OnDelete {
		case "":
			// Set default values.
			if c.Interleave {
				c.OnDelete = "cascade"
			} else {
				c.OnDelete = "setnull"
			}
		case "cascade":
			// Do nothing, always a valid specification.
		case "setnull":
			if c.Interleave {
				return fmt.Errorf("interleaved tables must specify ondelete=%q", "cascade")
			}
		default:
			return fmt.Errorf("invalid ondelete value %q; must be one of (%q | %q)", c.OnDelete, "cascade", "setnull")
		}
	} else {
		if c.OnDelete != "" {
			return fmt.Errorf("ondelete cannot be specified outside foreign key column")
		}
		if c.Interleave {
			return fmt.Errorf("interleave cannot be specified outside foreign key column")
		}
	}

	if c.Index != "" {
		if _, ok := validIndexTypes[c.Index]; !ok {
			return fmt.Errorf("invalid index type %q", c.Index)
		}
		switch c.Index {
		case "fulltext":
			if c.Type != "string" {
				return fmt.Errorf("fulltext index only valid for string columns")
			}
		case "location":
			if c.Type != "latlong" {
				return fmt.Errorf("location index only valid for latlong columns")
			}
		}
	}

	return nil
}

// validateForeignKey verifies that foreign key references are
// complete. This means that each component of the referenced table's
// primary key is represented by the foreign key. When this method is
// invoked, we have already verified that "fkTable" is a valid table
// name.
func (s *Schema) validateForeignKey(t *Table, fkTable string) error {
	ft := s.byName[fkTable]
	// Verify number of components matches.
	if len(t.foreignKeys[fkTable]) != len(ft.primaryKey) {
		return fmt.Errorf("foreign key to table %q has %d components, expect %d", fkTable,
			len(t.foreignKeys[fkTable]), len(ft.primaryKey))
	}

	// Verify all columns in foreign key have same ondelete, interleave specs.
	var lastCol *Column
	for _, c := range t.foreignKeys[fkTable] {
		if lastCol != nil {
			if lastCol.OnDelete != c.OnDelete {
				return fmt.Errorf("inconsistent specification of ondelete between columns %q and %q", lastCol.Name, c.Name)
			}
			if lastCol.Interleave != c.Interleave {
				return fmt.Errorf("inconsistent specification of interleave between columns %q and %q", lastCol.Name, c.Name)
			}
		}
		lastCol = c
	}

	// Get two sorted lists of column names: for foreign key reference, and...
	fkColNames := make([]string, 0, len(t.foreignKeys[fkTable]))
	for key := range t.foreignKeys[fkTable] {
		fkColNames = append(fkColNames, key)
	}
	sort.Strings(fkColNames)

	// ...and for referenced table's primary key.
	ftPKColNames := make([]string, len(ft.primaryKey))
	for i, c := range ft.primaryKey {
		ftPKColNames[i] = c.Name
	}
	sort.Strings(ftPKColNames)

	// Verify two lists match exactly.
	if !reflect.DeepEqual(fkColNames, ftPKColNames) {
		return fmt.Errorf("component mismatch: foreign key has %s; primary key of ref'd table has %s", fkColNames, ftPKColNames)
	}
	return nil
}

// parseForeignKey parses a foreign key declaration of the form:
// <Table Name>.<Column Name> and returns table name and column
// name on success or empty strings and an error otherwise.
func (s *Schema) parseForeignKey(c *Column) (table, column string, err error) {
	matches := foreignKeyRE.FindStringSubmatch(c.ForeignKey)
	if matches == nil {
		err = fmt.Errorf("invalid foreign key format %q", c.ForeignKey)
		return
	}
	switch len(matches) {
	default:
		err = fmt.Errorf("invalid foreign key format %q", c.ForeignKey)
		return
	case 2:
		table = matches[1]
	case 3:
		table, column = matches[1], matches[2]
	}
	t, ok := s.byName[table]
	if !ok {
		err = fmt.Errorf("foreign key %q references non-existent table %q", c.ForeignKey, table)
		return
	}
	// If column is missing from regexp, use primary key of referenced
	// table if not composite.
	if column == "" {
		if len(t.primaryKey) != 1 {
			err = fmt.Errorf("foreign key %q references table %q with composite primary key; foreign key must specify <Table>.<Column>", c.ForeignKey, table)
			return
		}
		column = t.primaryKey[0].Name
	}
	// Verify that foreign key column is part of foreign key table's
	// primary key. Once found, also verify types match exactly.
	for _, fkColumn := range t.primaryKey {
		if column == fkColumn.Name {
			if c.Type == fkColumn.Type {
				return
			}
			err = fmt.Errorf("foreign key %q has type mismatch %q != %q", c.ForeignKey, fkColumn.Type, c.Type)
			return
		}
	}
	err = fmt.Errorf("foreign key %q does not reference a primary key column", c.ForeignKey)
	return
}

// getTableSchema returns a table schema based on the fields within
// the supplied table object's type. Field tags provide details on
// primary and foreign keys, indexes, and other schema-related
// details. See the package comments for more details.
func getTableSchema(key string, table interface{}) (*Table, error) {
	typ := reflect.TypeOf(table)
	t := &Table{
		Name:    typ.Name(),
		Key:     key,
		Columns: make([]*Column, 0, 1),
	}
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		c, err := getColumnSchema(sf)
		if err != nil {
			return nil, err
		}
		t.Columns = append(t.Columns, c)
	}
	return t, nil
}

// getColumnSchema unpacks the struct field into name and cockroach-
// specific schema directives via the struct field tag.
func getColumnSchema(sf reflect.StructField) (*Column, error) {
	schemaType, err := getSchemaType(sf)
	if err != nil {
		return nil, err
	}
	c := &Column{
		Name: sf.Name,
		Type: schemaType,
	}
	cr := sf.Tag.Get("roach")
	for i, spec := range strings.Split(cr, ",") {
		keyValueSplit := strings.SplitN(spec, "=", 2)
		if len(keyValueSplit) == 1 {
			keyValueSplit = append(keyValueSplit, "")
		}
		key, value := keyValueSplit[0], keyValueSplit[1]
		if i == 0 {
			if len(value) > 0 {
				return nil, util.Errorf("roach tag for field %s must begin with column key, a short (1-3) character designation related to column name: %s", c.Name, spec)
			}
			c.Key = key
		} else {
			if err := setColumnOption(c, key, value); err != nil {
				return nil, err
			}
		}
	}
	if c.Key == "" {
		return nil, util.Errorf("roach tag must include a column key")
	}
	return c, nil
}

// getSchemaType returns the schema type depending on the reflect.Type type.
// The valid schema types are defined in the constants above.
func getSchemaType(field reflect.StructField) (string, error) {
	switch t := reflect.New(field.Type).Interface().(type) {
	case *bool, *int, *int8, *int16, *int32, *int64:
		return columnTypeInteger, nil
	case *float32, *float64:
		return columnTypeFloat, nil
	case *string:
		return columnTypeString, nil
	case *[]byte:
		return columnTypeBlob, nil
	case *time.Time:
		return columnTypeTime, nil
	case *LatLong:
		return columnTypeLatLong, nil
	case *IntegerSet:
		return columnTypeIntegerSet, nil
	case *StringSet:
		return columnTypeStringSet, nil
	case *IntegerMap:
		return columnTypeIntegerMap, nil
	case *StringMap:
		return columnTypeStringMap, nil
	default:
		return "", util.Errorf("invalid type %v; only integer, float, string, time, latlong, integerset, stringset, integermap, stringmap are allowed", t)
	}
}

// Valid column options.
const (
	columnOptionPrimaryKey     = "pk"
	columnOptionForeignKey     = "fk"
	columnOptionAutoIncrement  = "auto"
	columnOptionFullTextIndex  = "fulltextindex"
	columnOptionInterleave     = "interleave"
	columnOptionLocationIndex  = "locationindex"
	columnOptionScatter        = "scatter"
	columnOptionSecondaryIndex = "secondaryindex"
	columnOptionUniqueIndex    = "uniqueindex"
	columnOptionOnDelete       = "ondelete"

	columnDeleteOptionCascade = "cascade"
	columnDeleteOptionSetNull = "setnull"
)

// setColumnOption sets column options based on the key/value pair.
// An error is returned if the option key or value is invalid.
func setColumnOption(c *Column, key, value string) error {
	switch key {
	case columnOptionAutoIncrement:
		c.Auto = new(int64)
		if len(value) > 0 {
			start, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return util.Errorf("error parsing auto-increment start value %q: %v", value, err)
			}
			*c.Auto = start
		} else {
			*c.Auto = 1
		}
	case columnOptionForeignKey:
		if len(value) == 0 {
			return util.Errorf("foreign key must specify reference as <Table>[.<Column>]")
		}
		c.ForeignKey = value
	case columnOptionFullTextIndex:
		c.Index = indexTypeFullText
	case columnOptionInterleave:
		c.Interleave = true
	case columnOptionLocationIndex:
		c.Index = indexTypeLocation
	case columnOptionOnDelete:
		switch value {
		case columnDeleteOptionCascade, columnDeleteOptionSetNull:
			c.OnDelete = value
		default:
			return util.Errorf("column option %q must specify either %q or %q", key, columnDeleteOptionCascade, columnDeleteOptionSetNull)
		}
	case columnOptionPrimaryKey:
		if len(value) > 0 {
			return util.Errorf("column option %q should not specify a value", key)
		}
		c.PrimaryKey = true
	case columnOptionScatter:
		if len(value) > 0 {
			return util.Errorf("column option %q should not specify a value", key)
		}
		c.Scatter = true
	case columnOptionSecondaryIndex:
		c.Index = indexTypeSecondary
	case columnOptionUniqueIndex:
		c.Index = indexTypeUnique
	default:
		return util.Errorf("unrecognized column option: %q", key)
	}
	return nil
}
