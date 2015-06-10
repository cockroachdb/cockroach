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
	"log"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
)

// CreateTable ...
func (db *DB) CreateTable(name string, schema proto.TableSchema) error {
	return nil
}

// RowBuilder ...
type RowBuilder struct {
	tableName string
	columns   []string
	values    []interface{}
}

// Values returns a new RowBuilder with the specified values replacing any
// existing values.
func (b RowBuilder) Values(values ...interface{}) RowBuilder {
	b.values = values
	return b
}

// Put adapts the RowBuilder for use in a {DB,Txn,Batch}.Put() call.
func (b RowBuilder) Put(values ...interface{}) (RowBuilder, []interface{}) {
	return b, values
}

// CPut adapts the RowBuilder for use in a {DB,Txn,Batch}.CPut() call.
func (b RowBuilder) CPut(newValue, expValue interface{}) (RowBuilder, error, interface{}) {
	return b, newValue, expValue
}

// TableBuilder facilitates the construction of RowBuilder objects which can be
// passed to the various {DB,Txn,Batch} methods to operate on tables and rows
// instead of raw keys and values.
type TableBuilder struct {
	Name string
	PK   RowBuilder
}

// Columns returns a new RowBuilder for the table and the specified columns.
func (b TableBuilder) Columns(columns ...string) RowBuilder {
	return RowBuilder{
		tableName: b.Name,
		columns:   columns,
	}
}

func examples() {
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

	// Retrieve the "id" and "name" columns.
	r, err := db.Get(users.Columns("id", "name").Values(1))
	if err != nil {
		log.Fatal(err)
	}

	// Increment the "absent" column.
	r, err = db.Inc(users.Columns("id", "absent").Values(1), 1)
	if err != nil {
		log.Fatal(err)
	}

	// The builder approach also works with batches and inside of transactions.
	err := db.Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		// Delete the "absent" column.
		b.Put(users.Columns("id", "absent").Values(1, nil))
		return txn.Commit(b)
	})

	pk := users.Columns("id")
	if err := db.DeleteRange(pk.Values(0), pk.Values(1000)); err != nil {
		log.Fatal(err)
	}
}
