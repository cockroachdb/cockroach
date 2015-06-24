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

package client_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var isError = testutils.IsError

func makeTestSchema(name string) structured.TableSchema {
	charType := structured.ColumnType{
		Kind: structured.ColumnType_CHAR,
	}
	return structured.TableSchema{
		Table: structured.Table{
			Name: name,
		},
		Columns: []structured.Column{
			{Name: "id", Type: charType},
			{Name: "name", Type: charType},
			{Name: "title", Type: charType},
		},
		Indexes: []structured.TableSchema_IndexByName{
			{Index: structured.Index{Name: "primary", Unique: true},
				ColumnNames: []string{"id"}},
		},
	}
}

func TestCreateNamespace(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	if err := db.CreateNamespace("foo"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNamespace("foo"); !isError(err, "namespace .* already exists") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}
	if err := db.CreateNamespace(""); !isError(err, "empty namespace name") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}
}

func TestListNamespaces(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	names := []string{"a", "b", "c", "d", "e", "f", "g", "i"}
	for i, name := range names {
		if err := db.CreateNamespace(name); err != nil {
			t.Fatal(err)
		}
		namespaces, err := db.ListNamespaces()
		if err != nil {
			t.Fatal(err)
		}
		expectedNamespaces := namespaces[:i+1]
		if !reflect.DeepEqual(expectedNamespaces, namespaces) {
			t.Errorf("expected %+v, but got %+v", expectedNamespaces, namespaces)
		}
	}
}

func TestCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	if err := db.CreateNamespace("t"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.DescribeTable("t.users"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}

	schema := makeTestSchema("t.users")
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}

	// Table names are case-insensitive.
	schema2, err := db.DescribeTable("T.USERS")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema, *schema2) {
		t.Errorf("expected %+v, but got %+v", schema, schema2)
	}

	// Creating the table again should fail as the table already exists.
	if err := db.CreateTable(schema); !isError(err, "table .* already exists") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}

	// Verify we were allocated a non-reserved table ID. This involves manually
	// retrieving the descriptor. Don't do this at home kiddies.
	gr, err := db.Get(keys.MakeNameMetadataKey(0, "t"))
	if err != nil {
		t.Fatal(err)
	}
	gr, err = db.Get(keys.MakeNameMetadataKey(uint32(gr.ValueInt()), "users"))
	if err != nil {
		t.Fatal(err)
	}
	desc := structured.TableDescriptor{}
	if err := db.GetProto(gr.ValueBytes(), &desc); err != nil {
		t.Fatal(err)
	}
	if desc.ID <= structured.MaxReservedDescID {
		t.Errorf("expected a non-reserved table ID, but got %d", desc.ID)
	}
}

func TestRenameTable(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	if err := db.CreateNamespace("t"); err != nil {
		t.Fatal(err)
	}

	// Cannot rename a non-exist table.
	if err := db.RenameTable("t.a", "t.b"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}

	if err := db.CreateTable(makeTestSchema("t.a")); err != nil {
		t.Fatal(err)
	}

	if err := db.RenameTable("t.a", "t.b"); err != nil {
		t.Fatal(err)
	}

	// A second rename should fail (the table is now named "b").
	if err := db.RenameTable("t.a", "t.b"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found '%+v'", err)
	}

	tables, err := db.ListTables("t")
	if err != nil {
		t.Fatal(err)
	}
	expectedTables := []string{"b"}
	if !reflect.DeepEqual(expectedTables, tables) {
		t.Errorf("expected %+v, but got %+v", expectedTables, tables)
	}
}

func TestListTables(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	if err := db.CreateNamespace("t"); err != nil {
		t.Fatal(err)
	}

	names := []string{"a", "b", "c", "d", "e", "f", "g", "i"}
	for i, name := range names {
		if err := db.CreateTable(makeTestSchema("t." + name)); err != nil {
			t.Fatal(err)
		}

		tables, err := db.ListTables("t")
		if err != nil {
			t.Fatal(err)
		}
		expectedTables := names[:i+1]
		if !reflect.DeepEqual(expectedTables, tables) {
			t.Errorf("expected %+v, but got %+v", expectedTables, tables)
		}
	}
}

func TestStruct(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setup()
	defer s.Stop()

	type User struct {
		ID         int    `db:"id" roach:"primary key"`
		Name       string `db:"name"`
		Title      string
		Delinquent int
	}

	if err := db.CreateNamespace("t"); err != nil {
		t.Fatal(err)
	}

	schema, err := client.SchemaFromModel(User{})
	if err != nil {
		t.Fatal(err)
	}
	schema.Name = "t.users"
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}

	// Bind our User model to the "users" table, specifying the "id" column as
	// the primary key.
	if err := db.BindModel("t.users", User{}); err != nil {
		t.Fatal(err)
	}

	// Insert 2 users.
	peter := &User{ID: 1, Name: "Peter"}
	if err := db.PutStruct(peter); err != nil {
		t.Fatal(err)
	}
	spencer := User{ID: 2, Name: "Spencer", Title: "CEO"}
	if err := db.PutStruct(spencer); err != nil {
		t.Fatal(err)
	}

	// Verify we can retrieve the users.
	u := &User{ID: 1}
	if err := db.GetStruct(u, "name"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(peter, u) {
		t.Errorf("expected '%+v', but got '%+v'", peter, u)
	}

	u = &User{ID: 2}
	if err := db.GetStruct(u); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spencer, *u) {
		t.Errorf("expected '%+v', but got '%+v'", spencer, *u)
	}

	// Verify we can delete a single column.
	if err := db.DelStruct(spencer, "title"); err != nil {
		t.Fatal(err)
	}
	if err := db.GetStruct(u); err != nil {
		t.Fatal(err)
	}
	spencer.Title = ""
	if !reflect.DeepEqual(spencer, *u) {
		t.Errorf("expected '%+v', but got '%+v'", spencer, *u)
	}

	// Increment a column.
	if err := db.IncStruct(&spencer, 7, "delinquent"); err != nil {
		t.Fatal(err)
	}
	if spencer.Delinquent != 7 {
		t.Errorf("expected 7, but got %d", spencer.Delinquent)
	}

	{
		// Scan into a slice of structures.
		var result []User
		if err := db.ScanStruct(&result, User{ID: 0}, User{ID: 1000}, 0); err != nil {
			t.Fatal(err)
		}
		expected := []User{*peter, spencer}
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("expected %+v, but got %+v", expected, result)
		}
	}

	{
		// Scan into a slice of pointers.
		var result []*User
		if err := db.ScanStruct(&result, User{ID: 0}, User{ID: 1000}, 0); err != nil {
			t.Fatal(err)
		}
		expected := []*User{peter, &spencer}
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("expected %+v, but got %+v", expected, result)
		}
	}
}
