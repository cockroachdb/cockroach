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
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
)

func makeTestSchema(name string) proto.TableSchema {
	return proto.TableSchema{
		Table: proto.Table{
			Name: name,
		},
		Columns: []proto.Column{
			{Name: "id", Type: proto.Column_BYTES},
			{Name: "name", Type: proto.Column_BYTES},
			{Name: "title", Type: proto.Column_BYTES},
		},
		Indexes: []proto.TableSchema_IndexByName{
			{Index: proto.Index{Name: "primary", Unique: true},
				ColumnNames: []string{"id"}},
		},
	}
}

func isError(err error, re string) bool {
	if err == nil {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}

func TestCreateTable(t *testing.T) {
	s, db := setup()
	defer s.Stop()

	if _, err := db.DescribeTable("users"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found success")
	}

	schema := makeTestSchema("users")
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}

	// Table names are case-insensitive.
	schema2, err := db.DescribeTable("USERS")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema, *schema2) {
		t.Errorf("expected %+v, but got %+v", schema, schema2)
	}

	// Creating the table again should fail as the table already exists.
	if err := db.CreateTable(schema); !isError(err, "table .* already exists") {
		t.Fatalf("expected failure, but found success")
	}

	// Verify we were allocated a non-reserved table ID. This involves manually
	// retrieving the descriptor. Don't do this at home kiddies.
	gr, err := db.Get(keys.MakeNameMetadataKey(0, "users"))
	if err != nil {
		t.Fatal(err)
	}
	desc := proto.TableDescriptor{}
	if err := db.GetProto(gr.ValueBytes(), &desc); err != nil {
		t.Fatal(err)
	}
	if desc.ID <= proto.MaxReservedDescID {
		t.Errorf("expected a non-reserved table ID, but got %d", desc.ID)
	}
}

func TestRenameTable(t *testing.T) {
	s, db := setup()
	defer s.Stop()

	// Cannot rename a non-exist table.
	if err := db.RenameTable("a", "b"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found success")
	}

	if err := db.CreateTable(makeTestSchema("a")); err != nil {
		t.Fatal(err)
	}

	if err := db.RenameTable("a", "b"); err != nil {
		t.Fatal(err)
	}

	// A second rename should fail (the table is now named "b").
	if err := db.RenameTable("a", "b"); !isError(err, "unable to find table") {
		t.Fatalf("expected failure, but found success")
	}

	tables, err := db.ListTables()
	if err != nil {
		t.Fatal(err)
	}
	expectedTables := []string{"b"}
	if !reflect.DeepEqual(expectedTables, tables) {
		t.Errorf("expected %+v, but got %+v", expectedTables, tables)
	}
}

func TestListTables(t *testing.T) {
	s, db := setup()
	defer s.Stop()

	names := []string{"a", "b", "c", "d", "e", "f", "g", "i"}
	for i, name := range names {
		if err := db.CreateTable(makeTestSchema(name)); err != nil {
			t.Fatal(err)
		}

		tables, err := db.ListTables()
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
	s, db := setup()
	defer s.Stop()

	type User struct {
		ID         int    `db:"id"`
		Name       string `db:"name"`
		Title      string
		Delinquent int
	}

	// Bind our User model to the "users" table, specifying the "id" column as
	// the primary key.
	if err := db.BindModel("users", User{}, "id"); err != nil {
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
