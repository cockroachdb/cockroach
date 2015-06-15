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
)

type User struct {
	ID         int    `db:"id"`
	Name       string `db:"name"`
	Title      string
	Delinquent int
}

func TestStruct(t *testing.T) {
	s, db := setup()
	defer s.Stop()

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
