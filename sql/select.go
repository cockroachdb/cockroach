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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// Select selects rows from a single table.
func (p *planner) Select(n *parser.Select) (planNode, error) {
	var desc *structured.TableDescriptor

	switch len(n.From) {
	case 0:
		// desc remains nil.

	case 1:
		ate, ok := n.From[0].(*parser.AliasedTableExpr)
		if !ok {
			return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
		}
		table, ok := ate.Expr.(parser.QualifiedName)
		if !ok {
			return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
		}
		var err error
		desc, err = p.getTableDesc(table)
		if err != nil {
			return nil, err
		}

	default:
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
	}

	v := &valuesNode{
		columns: make([]string, 0, len(n.Exprs)),
	}

	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	exprs := make([]parser.Expr, 0, len(n.Exprs))
	for _, e := range n.Exprs {
		switch t := e.(type) {
		case *parser.StarExpr:
			if desc == nil {
				return nil, fmt.Errorf("* with no tables specified is not valid")
			}
			for _, col := range desc.Columns {
				v.columns = append(v.columns, col.Name)
				exprs = append(exprs, parser.QualifiedName{col.Name})
			}
		case *parser.NonStarExpr:
			exprs = append(exprs, t.Expr)
			if t.As != "" {
				v.columns = append(v.columns, t.As)
			} else {
				v.columns = append(v.columns, t.Expr.String())
			}
		}
	}

	if desc == nil {
		// No data to read, pretend there is a single empty row.
		vals := valMap{}
		if output, err := shouldOutputRow(n.Where, vals); err != nil {
			return nil, err
		} else if output {
			row, err := outputRow(exprs, vals)
			if err != nil {
				return nil, err
			}
			v.rows = append(v.rows, row)
		}
		return v, nil
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID))
	endKey := startKey.PrefixEnd()
	sr, err := p.db.Scan(startKey, endKey, 0)
	if err != nil {
		return nil, err
	}

	// All of the columns for a particular row will be grouped together. We loop
	// over the returned key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column ID to
	// lookup the column and decode the value. All of these values go into a map
	// keyed by column name. When the index key changes we output a row
	// containing the current values.
	//
	// The TODOs here are too numerous to list. This is only performing a full
	// table scan using the primary key.

	// TODO(pmattis): Use a scanNode instead of a valuesNode here.
	var primaryKey []byte
	vals := valMap{}
	l := len(sr)
	// Iterate through the scan result set. We decide at the beginning of each
	// new row whether the previous row is to be output. To deal with the very
	// last one, the loop below goes an extra iteration (i==l).
	for i := 0; ; i++ {
		var kv client.KeyValue
		if i < l {
			kv = sr[i]
		}
		if primaryKey != nil && (i == l || !bytes.HasPrefix(kv.Key, primaryKey)) {
			// The current key belongs to a new row. Decide whether the last
			// row is to be output.
			if output, err := shouldOutputRow(n.Where, vals); err != nil {
				return nil, err
			} else if output {
				row, err := outputRow(exprs, vals)
				if err != nil {
					return nil, err
				}
				v.rows = append(v.rows, row)
			}
			vals = valMap{}
		}

		if i >= l {
			break
		}

		remaining, err := decodeIndexKey(desc, desc.Indexes[0], vals, kv.Key)
		if err != nil {
			return nil, err
		}
		primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		_, colID := encoding.DecodeUvarint(remaining)
		if err != nil {
			return nil, err
		}
		col, err := desc.FindColumnByID(uint32(colID))
		if err != nil {
			return nil, err
		}
		vals[col.Name] = unmarshalValue(*col, kv)

		if log.V(2) {
			log.Infof("Scan %q -> %v", kv.Key, vals[col.Name])
		}
	}

	return v, nil
}

func outputRow(exprs []parser.Expr, vals valMap) ([]parser.Datum, error) {
	row := make([]parser.Datum, len(exprs))
	for i, e := range exprs {
		var err error
		row[i], err = parser.EvalExpr(e, vals)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func shouldOutputRow(where *parser.Where, vals valMap) (bool, error) {
	if where == nil {
		return true, nil
	}
	d, err := parser.EvalExpr(where.Expr, vals)
	if err != nil {
		return false, err
	}
	v, ok := d.(parser.DBool)
	if !ok {
		return false, fmt.Errorf("WHERE clause did not evaluate to a boolean")
	}
	return bool(v), nil
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
