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

	"github.com/cockroachdb/cockroach/proto"
)

// Row ...
type Row []interface{}

// IndexKey ...
type IndexKey []interface{}

// Index ...
type Index struct {
}

// Scan retrieves 1 or more columns from the rows between start (inclusive) and
// end (exclusive). If columns is empty all of the columns for the table are
// retrieved.
func (i *Index) Scan(txn *Txn, start, end Row, maxRows int, columns ...string) ([]Row, error) {
	return nil, nil
}

// Key constructs an IndexKey from the specified column values.
func (i *Index) Key(values ...interface{}) IndexKey {
	// TODO(pmattis): Check that "values" contains the same number of elements as
	// the index.
	return IndexKey(values)
}

// Table ...
type Table struct {
	// Schema ...
	Columns []string
	PK      *Index
	indexes map[string]*Index
}

// OpenTable ...
func (db *DB) OpenTable(name string) (*Table, error) {
	return nil, nil
}

// CreateTable ...
func (db *DB) CreateTable(name string, schema proto.TableSchema) (*Table, error) {
	return nil, nil
}

// Index returns the specified index.
func (t *Table) Index(name string) (*Index, error) {
	i, ok := t.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index \"%s\" not found", name)
	}
	return i, nil
}

// Get retrieves 1 or more columns from the specified row. If columns is empty
// all of the columns defined for the table are retrieved for the specified
// row. The returned Row will contain the requested number of columns (either
// len(columns) or len(t.Columns)).
//
//   r, err := t.Get(nil, t.PK.Key("user1"), "name")
func (t *Table) Get(txn *Txn, primaryKey IndexKey, columns ...string) (Row, error) {
	return nil, nil
}

// Put sets the value for 1 or more columns for the specified row. Returns a
// builder on which the values for the put must be specified:
//
//   t.Put(nil, t.PK.Key("user1"), "name").Values("Spencer Kimball")
func (t *Table) Put(txn *Txn, primaryKey IndexKey, columns ...string) putBuilder {
	return putBuilder{}
}

// Del deletes 1 or more columns from the specified row. If columns is empty
// all of the columns defined for the table are deleted from the specified row:
//
//   t.Del(nil, t.PK.Key("user1"), "name")  // Delete the "name" column
//   t.Del(nil, t.PK.Key("user2"))          // Delete the entire row
func (t *Table) Del(txn *Txn, primaryKey IndexKey, columns ...string) error {
	return nil
}

// DelRange deletes 1 or more columns from specified range of rows. If columns
// is empty all of the columns defined for the table are deleted from the
// specified range of rows.
func (t *Table) DelRange(txn *Txn, start, end Row, columns ...string) error {
	return nil
}

// putBuilder ...
type putBuilder struct {
}

// Values specifies the values for a Put operation.
func (b *putBuilder) Values(values ...interface{}) error {
	return nil
}
