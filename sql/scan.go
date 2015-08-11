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
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

type valMap map[structured.ColumnID]parser.Datum
type qvalMap map[structured.ColumnID]*parser.ParenExpr

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
	qvals      qvalMap           // the values in the current row
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
		if !n.init() {
			return false
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
			if !n.prepareVals() {
				return false
			}

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

		var vals valMap
		if n.primaryKey == nil {
			// This is the first key for the row, create the vals map in order to
			// decode the primary key columns.
			vals = make(valMap, len(n.desc.PrimaryIndex.ColumnIDs))
			// Reset the qvals map expressions to nil. The expresssion will get
			// filled in with the column values as we decode the key-value pairs for
			// the row.
			for _, e := range n.qvals {
				e.Expr = nil
			}
		}

		var remaining []byte
		remaining, n.err = decodeIndexKey(n.desc, n.desc.PrimaryIndex, vals, kv.Key)
		if n.err != nil {
			return false
		}

		if n.primaryKey == nil {
			n.primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

			// This is the first key for the row, initialize the column values that
			// are part of the primary key.
			for id, val := range vals {
				if qval := n.qvals[id]; qval != nil {
					qval.Expr = val
				}
			}
		}

		_, colID := encoding.DecodeUvarint(remaining)
		var col *structured.ColumnDescriptor
		col, n.err = n.desc.FindColumnByID(structured.ColumnID(colID))
		if n.err != nil {
			return false
		}
		if v, ok := n.qvals[col.ID]; ok && v.Expr == nil {
			v.Expr = unmarshalValue(*col, kv)
			if log.V(2) {
				log.Infof("Scan %q -> %v", kv.Key, n.qvals[col.ID].Expr)
			}
		} else {
			// No need to unmarshal the column value. Either the column was part of
			// the index key or it isn't needed by any of the render or filter
			// expressions.
			if log.V(2) {
				log.Infof("Scan %q -> [%d] (skipped)", kv.Key, col.ID)
			}
		}

		n.kvIndex++
	}
}

func (n *scanNode) Err() error {
	return n.err
}

func (n *scanNode) init() bool {
	return n.initScan() && n.initExprs()
}

// initExprs initializes the render and filter expressions for the
// scan. Initialization consists of replacing QualifiedName nodes with
// ParenExpr nodes for which the wrapped expression can be changed for each
// row.
func (n *scanNode) initExprs() bool {
	n.qvals = make(qvalMap)
	for i := range n.render {
		n.render[i], n.err = extractQVals(n.desc, n.qvals, n.render[i])
		if n.err != nil {
			return false
		}
	}
	n.filter, n.err = extractQVals(n.desc, n.qvals, n.filter)
	return n.err == nil
}

// initScan initializes (and performs) the key-value scan.
//
// TODO(pmattis): The key-value scan currently reads all of the key-value
// pairs, but they could just as easily be read in chunks. Probably worthwhile
// to separate out the retrieval of the key-value pairs into a separate
// structure.
func (n *scanNode) initScan() bool {
	// Initialize our key/values.
	if n.desc == nil {
		// No table to read from, pretend there is a single empty row.
		n.kvs = []client.KeyValue{}
		n.primaryKey = []byte{}
		return true
	}
	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(structured.MakeIndexKeyPrefix(n.desc.ID, n.desc.PrimaryIndex.ID))
	endKey := startKey.PrefixEnd()
	n.kvs, n.err = n.db.Scan(startKey, endKey, 0)
	if n.err != nil {
		return false
	}
	return true
}

func (n *scanNode) prepareVals() bool {
	if n.desc != nil {
		for _, col := range n.desc.Columns {
			if !col.Nullable {
				break
			}
			if v, ok := n.qvals[col.ID]; ok && v.Expr == nil {
				v.Expr = parser.DNull
				continue
			}
		}
	}
	return true
}

func (n *scanNode) filterRow() (bool, error) {
	if n.filter == nil {
		return true, nil
	}
	d, err := parser.EvalExpr(n.filter)
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
		n.row[i], err = parser.EvalExpr(e)
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
		case structured.ColumnType_BOOL:
			return parser.DBool(kv.ValueInt() != 0)
		case structured.ColumnType_FLOAT:
			return parser.DFloat(math.Float64frombits(uint64(kv.ValueInt())))
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			return parser.DString(kv.ValueBytes())
		}
	}
	return parser.DNull
}

type qnameVisitor struct {
	desc  *structured.TableDescriptor
	qvals qvalMap
	err   error
}

var _ parser.Visitor = &qnameVisitor{}

func (v *qnameVisitor) Visit(expr parser.Expr) parser.Expr {
	if v.err != nil {
		return expr
	}
	qname, ok := expr.(*parser.QualifiedName)
	if !ok {
		return expr
	}

	desc := v.getDesc(qname)
	if desc != nil {
		name := qname.Column()
		for _, col := range v.desc.Columns {
			if !strings.EqualFold(name, col.Name) {
				continue
			}
			paren := v.qvals[col.ID]
			if paren == nil {
				paren = &parser.ParenExpr{Expr: parser.DNull}
				v.qvals[col.ID] = paren
			}
			return paren
		}
	}

	v.err = fmt.Errorf("qualified name \"%s\" not found", qname)
	return expr
}

func (v *qnameVisitor) getDesc(qname *parser.QualifiedName) *structured.TableDescriptor {
	// TODO(pmattis): This logic isn't complete. A QualifiedName like "a.b.c"
	// currently refers to table "a", while I think it should refer to table "b"
	// in database "a". Need to track down what the appropriate behavior is and
	// implement it. This could also doesn't account for ArrayIndirection, so
	// "a[b]" is currently being interpreted as table "a" while in reality it is
	// column "a", element "b".
	if v.desc == nil {
		return nil
	}
	if len(qname.Indirect) == 0 {
		return v.desc
	}
	if strings.EqualFold(v.desc.Name, string(qname.Base)) {
		return v.desc
	}
	return nil
}

func extractQVals(desc *structured.TableDescriptor,
	qvals qvalMap, expr parser.Expr) (parser.Expr, error) {
	if expr == nil {
		return expr, nil
	}
	v := qnameVisitor{desc: desc, qvals: qvals}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.err
}
