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
	"log"
	"testing"

	"github.com/cockroachdb/cockroach/client"
)

func TestTableBuilder(t *testing.T) {
	db, err := client.Open("")
	if err != nil {
		log.Fatal(err)
	}

	// A builder for the "users" table. Not shown here is that the primary key is
	// the "id" column.
	users := client.TableBuilder{Name: "users"}

	// Put columns "id" and "name". On a Put, the primary key columns must be
	// specified. Note the "RowBuilder.Put()" call at the end which returns two
	// arguments in order to adapt the RowBuilder to the {DB,Txn,Batch}.Put()
	// signature.
	if err := db.Put(
		users.Columns("id", "name").Values(1).Put("Spencer Kimball")); err != nil {
		log.Fatal(err)
	}

	// Conditionally put the "name" column.
	if err := db.CPut(
		users.Columns("id", "name").Values(1).CPut("Spencer W Kimball", "Spencer Kimball")); err != nil {
		log.Fatal(err)
	}

	// Retrieve the "name" column.
	if _, err := db.Get(users.Columns("id", "name").Values(1)); err != nil {
		log.Fatal(err)
	}

	// Increment the "absent" column.
	if _, err = db.Inc(users.Columns("id", "absent").Values(1), 1); err != nil {
		log.Fatal(err)
	}

	// The builder approach also works with batches and inside of transactions.
	err = db.Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		// Delete the "absent" column.
		b.Put(users.Columns("id", "absent").Values(1).Put(nil))
		return txn.Commit(b)
	})

	pk := users.Columns("id")
	if err := db.DelRange(pk.Values(0), pk.Values(1000)); err != nil {
		log.Fatal(err)
	}
}

func TestTableStructTag(t *testing.T) {
	db, err := client.Open("")
	if err != nil {
		log.Fatal(err)
	}

	type User struct {
		ID     int    `db:"id"`
		Name   string `db:"name"`
		Absent int    `db:"absent"`
	}

	// Bind the User struct to the "users" table. Whenever a *User is passed to
	// one of the {DB,Txn,Batch} methods we'll look up the matching table.
	if err := db.Bind("users", &User{}); err != nil {
		log.Fatal(err)
	}

	// Blindly put a new row.
	if err := db.PutStruct(&User{ID: 1, Name: "Spencer Kimball"}); err != nil {
		log.Fatal(err)
	}

	// Conditionally put the name field for user id 1. First argument is the new
	// value. Second argument has the expected value for the non-primary-key
	// columns.
	if err := db.CPutStruct(&User{ID: 1, Name: "Spencer W Kimball"}, &User{Name: "Spencer Kimball"}); err != nil {
		log.Fatal(err)
	}

	// Retrieve all of the columns for a row.
	u := &User{ID: 1}
	if err := db.GetStruct(u); err != nil {
		log.Fatal(err)
	}

	// Retrieve the named columns for a row.
	if err := db.GetStruct(u, "name", "absent"); err != nil {
		log.Fatal(err)
	}

	// Increment the "absent" column.
	if err = db.IncStruct(&User{ID: 1}, 1, "absent"); err != nil {
		log.Fatal(err)
	}

	// Delete the specified columns from a row.
	if err := db.DelStruct(&User{ID: 1}, "absent"); err != nil {
		log.Fatal(err)
	}

	// Delete a range of rows.
	if err := db.DelRange(&User{ID: 0}, &User{ID: 1000}); err != nil {
		log.Fatal(err)
	}
}
