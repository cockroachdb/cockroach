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

import "github.com/cockroachdb/cockroach/proto"

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
func (b RowBuilder) CPut(newValue, expValue interface{}) (RowBuilder, interface{}, interface{}) {
	return b, newValue, expValue
}

// TableBuilder facilitates the construction of RowBuilder objects which can be
// passed to the various {DB,Txn,Batch} methods to operate on tables and rows
// instead of raw keys and values.
type TableBuilder struct {
	Name string
}

// Columns returns a new RowBuilder for the table and the specified columns.
func (b TableBuilder) Columns(columns ...string) RowBuilder {
	return RowBuilder{
		tableName: b.Name,
		columns:   columns,
	}
}
