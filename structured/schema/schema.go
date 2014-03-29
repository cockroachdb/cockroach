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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/go-yaml/go-yaml-v1"
)

// Column contains the schema for a column. The Key should be a
// shortened identifier related to Name. The Column Key is stored
// within the row value itself and is used to maintain human
// readability for debugging, as well as a stable indicator of
// Column type. Column Keys must be unique within a Table.
type Column struct {
	Name string `yaml:"column"`
	Key  string `yaml:"column_key"`
	Type string
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
			if value != "" {
				return nil, util.Errorf("roach tag for field %s must begin with column key, a short (1-3) character designation related to column name: %s", c.Name, spec)
			}
			if _, ok := keys[key]; ok {
				return nil, util.Errorf("column key %s, specified for column %s not unique in table schema", key, c.Name)
			}
			keys[key] = struct{}{}
			c.Key = key
		}
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
