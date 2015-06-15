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

package client

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
)

var schemaOptRE = regexp.MustCompile(`\s*((?:\w|\s)+)(?:\(([^)]+)\))?\s*`)

type columnsByName []proto.Column

func (s columnsByName) Len() int           { return len(s) }
func (s columnsByName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s columnsByName) Less(i, j int) bool { return s[i].Name < s[j].Name }

type indexesByName []proto.TableSchema_IndexByName

func (s indexesByName) Len() int      { return len(s) }
func (s indexesByName) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s indexesByName) Less(i, j int) bool {
	// Sort the name "primary" less than other names.
	if s[i].Name == proto.PrimaryKeyIndexName {
		return s[j].Name != proto.PrimaryKeyIndexName
	} else if s[j].Name == proto.PrimaryKeyIndexName {
		return false
	}
	return s[i].Name < s[j].Name
}

// SchemaFromModel allows the easy construction of a TableSchema from a Go
// struct. Columns are created for each exported field in the struct. The "db"
// struct tag is used to control the mapping of field name to column name and
// to indicate exported fields which should be skipped.
//
//   type User struct {
//     ID      int
//     Name    string `db:"old_name"`
//     Ignored int    `db:"-"`
//   }
//
// Indexes are specified using the "roach" struct tag declaration.
//
//   type User struct {
//     ID   int    `roach:"primary key"`
//     Name string `db:"old_name" roach:"index"`
//   }
//
// The following "roach" options are supported:
//
//   "primary key [(columns...)]" - creates a unique index on <columns> and
//   marks it as the primary key for the table. If <columns> is not specified
//   it defaults to the name of the column the option is associated with.
//
//   "index" [(columns...)]" - creates an index on <columns>.
//
//   "unique index" [(columns...)]" - creates a unique index on <columns>.
func SchemaFromModel(obj interface{}) (proto.TableSchema, error) {
	s := proto.TableSchema{}
	m, err := getDBFields(deref(reflect.TypeOf(obj)))
	if err != nil {
		return s, err
	}

	s.Table.Name = strings.ToLower(reflect.TypeOf(obj).Name())

	// Create the columns for the table.
	for name := range m {
		col := proto.Column{
			Name: name,
			Type: proto.Column_BYTES,
		}
		s.Columns = append(s.Columns, col)
	}

	// Create the indexes for the table.
	for name, f := range m {
		for _, opt := range strings.Split(f.Tag.Get("roach"), ";") {
			match := schemaOptRE.FindStringSubmatch(opt)
			if match == nil {
				return s, fmt.Errorf("invalid schema option: %s", opt)
			}
			cmd := match[1]
			var params []string
			if len(match[2]) > 0 {
				params = strings.Split(match[2], ",")
			} else {
				params = []string{name}
			}
			var index proto.Index
			switch strings.ToLower(cmd) {
			case "primary key":
				index.Name = proto.PrimaryKeyIndexName
				index.Unique = true
			case "unique index":
				index.Name = strings.Join(params, ":")
				index.Unique = true
			case "index":
				index.Name = strings.Join(params, ":")
			}
			s.Indexes = append(s.Indexes, proto.TableSchema_IndexByName{
				Index:       index,
				ColumnNames: params,
			})
		}
	}

	// Normalize the column and index order.
	sort.Sort(columnsByName(s.Columns))
	sort.Sort(indexesByName(s.Indexes))
	return s, nil
}
