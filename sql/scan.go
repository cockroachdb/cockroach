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

package sql

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	db         *client.DB
	desc       *structured.TableDescriptor
	columns    []string
	err        error
	primaryKey []byte            // the primary key of the current row
	kvs        []client.KeyValue // the raw key/value pairs
	kvIndex    int               // current index into the key/value pairs
	vals       valMap            // the values in the current row
	row        parser.DTuple     // the rendered row
	filter     parser.Expr       // filtering expression for rows
	render     []parser.Expr     // rendering expressions for rows
}

func (n *scanNode) Columns() []string {
	return n.columns
}

func (n *scanNode) Values() parser.DTuple {
	return n.row
}

func (n *scanNode) Next() bool {
	if n.err != nil {
		return false
	}

	if n.kvs == nil {
		// Initialize our key/values.
		if n.desc == nil {
			// No table to read from, pretend there is a single empty row.
			n.kvs = []client.KeyValue{}
			n.primaryKey = []byte{}
		} else {
			// Retrieve all of the keys that start with our index key prefix.
			startKey := proto.Key(encodeIndexKeyPrefix(n.desc.ID, n.desc.Indexes[0].ID))
			endKey := startKey.PrefixEnd()
			// TODO(pmattis): Currently we retrieve all of the key/value pairs for
			// the table. We could enhance this code so that it retrieves the
			// key/value pairs in chunks.
			n.kvs, n.err = n.db.Scan(startKey, endKey, 0)
			if n.err != nil {
				return false
			}
		}
	}

	// All of the columns for a particular row will be grouped together. We loop
	// over the key/value pairs and decode the key to extract the columns encoded
	// within the key and the column ID. We use the column ID to lookup the
	// column and decode the value. All of these values go into a map keyed by
	// column name. When the index key changes we output a row containing the
	// current values.
	for {
		var kv client.KeyValue
		if n.kvIndex < len(n.kvs) {
			kv = n.kvs[n.kvIndex]
		}

		if n.primaryKey != nil &&
			(n.kvIndex == len(n.kvs) || !bytes.HasPrefix(kv.Key, n.primaryKey)) {
			// The current key belongs to a new row. Output the current row.
			n.primaryKey = nil
			var output bool
			output, n.err = n.filterRow()
			if n.err != nil {
				return false
			}
			if output {
				if n.err = n.renderRow(); n.err != nil {
					return false
				}
				return true
			}
		}

		if n.kvIndex == len(n.kvs) {
			return false
		}

		if n.primaryKey == nil {
			// This is the first key for the row, reset our vals map.
			n.vals = valMap{}
		}

		var remaining []byte
		remaining, n.err = decodeIndexKey(n.desc, n.desc.Indexes[0], n.vals, kv.Key)
		if n.err != nil {
			return false
		}
		n.primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		// TODO(pmattis): We should avoid looking up the column name by column ID
		// on every key. One possibility is that we could rewrite col-name
		// references in expressions to refer to <table-id, col-id> tuples.
		_, colID := encoding.DecodeUvarint(remaining)
		var col *structured.ColumnDescriptor
		col, n.err = n.desc.FindColumnByID(uint32(colID))
		if n.err != nil {
			return false
		}
		n.vals[col.Name] = unmarshalValue(*col, kv)

		if log.V(2) {
			log.Infof("Scan %q -> %v", kv.Key, n.vals[col.Name])
		}

		n.kvIndex++
	}
}

func (n *scanNode) Err() error {
	return n.err
}

func (n *scanNode) filterRow() (bool, error) {
	if n.filter == nil {
		return true, nil
	}
	d, err := parser.EvalExpr(n.filter, n.vals)
	if err != nil {
		return false, err
	}
	v, ok := d.(parser.DBool)
	if !ok {
		return false, fmt.Errorf("WHERE clause did not evaluate to a boolean")
	}
	return bool(v), nil
}

func (n *scanNode) renderRow() error {
	if n.row == nil {
		n.row = make([]parser.Datum, len(n.render))
	}
	for i, e := range n.render {
		var err error
		n.row[i], err = parser.EvalExpr(e, n.vals)
		if err != nil {
			return err
		}
	}
	return nil
}

func unmarshalValue(col structured.ColumnDescriptor, kv client.KeyValue) parser.Datum {
	if kv.Exists() {
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			return parser.DInt(kv.ValueInt())
		case structured.ColumnType_FLOAT:
			return parser.DFloat(math.Float64frombits(uint64(kv.ValueInt())))
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			return parser.DString(kv.ValueBytes())
		}
	}
	return parser.DNull{}
}

type valMap map[string]parser.Datum

func (m valMap) Get(name string) (parser.Datum, bool) {
	d, ok := m[name]
	return d, ok
}
