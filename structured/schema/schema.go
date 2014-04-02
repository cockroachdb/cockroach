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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package schema

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"gopkg.in/yaml.v1"
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
	// "latlong", "numberset", "stringset", "numbermap", or "stringmap".
	// Integers are int64s. Floats are float64s. Strings must be UTF8
	// encoded. Blobs are arbitrary byte arrays. If sending over JSON,
	// they should be base64 encoded. Latlong are (latitude, longitude,
	// altitude, accuracy) quadruplets, each a float64 (altitude and
	// accuracy are in meters). Numbersets are a set of int64
	// values. Numbermaps are map[string]int64. Stringsets and
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
	// or part of a composite primary key. If part of a composite
	// primary key the order in which the columns are declared dictates
	// the order in which their values are concatenated to form the key.
	// This has obvious implications for range queries.
	PrimaryKey bool `yaml:"primary_key,omitempty"`
	// Scatter is specified with primary keys to effectively randomize
	// the placement of the data within the table's keyspace. This is
	// accomplished by prepending a two-byte has of the entire primary
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
}

// Schema contains a named sequence of Table schemas. The Key should
// be a shortened identifier related to Name. The Schema Key is stored
// as part of every row key within the database, so size
// matters and small is better (for once). Schema Keys must be unique
// within a Cockroach cluster.
type Schema struct {
	Name   string   `yaml:"db"`
	Key    string   `yaml:"db_key"`
	Tables []*Table `yaml:",omitempty"`
}

// NewGoSchema returns a schema using name and key and a map from
// Table Key to an instance of a Go struct corresponding to the
// Table (the Table name is derived from the Go struct name).
func NewGoSchema(name, key string, schemaMap map[string]interface{}) (*Schema, error) {
	schema := &Schema{
		Name: name,
		Key:  key,
	}
	for key, strct := range schemaMap {
		table, err := getTableSchema(key, strct)
		if err != nil {
			return nil, err
		}
		schema.Tables = append(schema.Tables, table)
	}
	return schema, nil
}

// NewYAMLSchema returns a schema based on the YAML input string.
func NewYAMLSchema(in []byte) (*Schema, error) {
	s := &Schema{}
	if err := yaml.Unmarshal(in, s); err != nil {
		return nil, err
	}
	return s, nil
}

// ToYAML marshals the Schema into YAML.
func (s *Schema) ToYAML() ([]byte, error) {
	return yaml.Marshal(s)
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
	keys := make(map[string]struct{})
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		c, err := getColumnSchema(sf, keys)
		if err != nil {
			return nil, err
		}
		t.Columns = append(t.Columns, c)
	}
	return t, nil
}

// getColumnSchema unpacks the struct field into name and cockroach-
// specific schema directives via the struct field tag.
func getColumnSchema(sf reflect.StructField, keys map[string]struct{}) (*Column, error) {
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
			if value != "" || len(value) > 3 {
				return nil, util.Errorf("roach tag for field %s must begin with column key, a short (1-3) character designation related to column name: %s", c.Name, spec)
			}
			if _, ok := keys[key]; ok {
				return nil, util.Errorf("column key %s, specified for column %s not unique in table schema", key, c.Name)
			}
			keys[key] = struct{}{}
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
// The schema type is one of: (integer, float, string, blob, time, latlong,
// numberset, stringset, numbermap, stringmap).
func getSchemaType(field reflect.StructField) (string, error) {
	switch t := reflect.New(field.Type).Interface().(type) {
	case *bool, *int, *int8, *int16, *int32, *int64:
		return "integer", nil
	case *float32, *float64:
		return "float", nil
	case *string:
		return "string", nil
	case *[]byte:
		return "blob", nil
	case *time.Time:
		return "time", nil
	case *LatLong:
		return "latlong", nil
	case *NumberSet:
		return "numberset", nil
	case *StringSet:
		return "stringset", nil
	case *NumberMap:
		return "numbermap", nil
	case *StringMap:
		return "stringmap", nil
	default:
		return "", util.Errorf("invalid type %v; only integer, float, string, time, latlong, numberset, stringset, numbermap, stringmap are allowed", t)
	}
}

// setColumnOption sets column options based on the key/value pair.
// An error is returned if the option key or value is invalid.
func setColumnOption(c *Column, key, value string) error {
	switch key {
	case "auto":
		c.Auto = new(int64)
		if value != "" {
			start, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return util.Errorf("auto-increment start value: %v", err)
			}
			*c.Auto = start
		} else {
			*c.Auto = 1
		}
	case "fk":
		if value == "" {
			return util.Errorf("foreign key must specify reference as <Table>[.<Column>]")
		}
		c.ForeignKey = value
	case "fulltextindex":
		c.Index = "fulltext"
	case "interleave":
	case "locationindex":
		c.Index = "location"
	case "ondelete":
		switch value {
		case "cascade", "setnull":
			c.OnDelete = value
		default:
			return util.Errorf("column option %q must specify either %q or %q", key, "cascade", "setnull")
		}
	case "pk":
		if value != "" {
			return util.Errorf("column option %q should not specify a value", key)
		}
		c.PrimaryKey = true
	case "scatter":
		if value != "" {
			return util.Errorf("column option %q should not specify a value", key)
		}
		c.Scatter = true
	case "secondaryindex":
		c.Index = "secondary"
	case "unqieindex":
		c.Index = "unique"
	default:
		return util.Errorf("unrecognized column option: %q", key)
	}
	return nil
}
