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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSchemaFromModel(t *testing.T) {
	defer leaktest.AfterTest(t)
	type Foo struct {
		A int `roach:"primary key(a,b)"`
		B int `roach:"unique index"` // equivalent to: unique index(b)
		C int `roach:"index(c,b)"`
		D int // 0 options should not be an error
	}
	schema, err := SchemaFromModel(Foo{})
	if err != nil {
		t.Fatal(err)
	}

	expectedSchema := structured.TableSchema{
		Table: structured.Table{Name: "foo"},
		Columns: []structured.Column{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
			{Name: "d"},
		},
		Indexes: []structured.TableSchema_IndexByName{
			{Index: structured.Index{Name: "primary", Unique: true},
				ColumnNames: []string{"a", "b"}},
			{Index: structured.Index{Name: "b", Unique: true},
				ColumnNames: []string{"b"}},
			{Index: structured.Index{Name: "c:b"},
				ColumnNames: []string{"c", "b"}},
		},
	}
	if !reflect.DeepEqual(expectedSchema, schema) {
		t.Errorf("expected %+v, but got %+v", expectedSchema, schema)
	}
}
