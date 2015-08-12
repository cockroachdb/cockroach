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

type qvalMap map[structured.ColumnID]*parser.ParenExpr
type colKindMap map[structured.ColumnID]structured.ColumnType_Kind

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	txn              *client.Txn
	desc             *structured.TableDescriptor
	index            *structured.IndexDescriptor
	visibleCols      []structured.ColumnDescriptor
	isSecondaryIndex bool
	columns          []string
	err              error
	indexKey         []byte              // the index key of the current row
	kvs              []client.KeyValue   // the raw key/value pairs
	kvIndex          int                 // current index into the key/value pairs
	rowIndex         int                 // the index of the current row
	colID            structured.ColumnID // column ID of the current key
	vals             []parser.Datum      // the index key values for the current row
	qvals            qvalMap             // the values in the current row
	colKind          colKindMap          // map of column kinds for decoding column values
	row              parser.DTuple       // the rendered row
	filter           parser.Expr         // filtering expression for rows
	render           []parser.Expr       // rendering expressions for rows
	explain          explainMode
	explainValue     parser.Datum
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
		if n.maybeOutputRow() {
			return n.err == nil
		}
		if n.kvIndex == len(n.kvs) {
			return false
		}
		if !n.processKV(n.kvs[n.kvIndex]) {
			return false
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
		n.render[i], n.err = n.extractQVals(n.render[i])
		if n.err != nil {
			return false
		}
	}
	n.filter, n.err = n.extractQVals(n.filter)
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
		n.indexKey = []byte{}
		return true
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(structured.MakeIndexKeyPrefix(n.desc.ID, n.index.ID))
	endKey := startKey.PrefixEnd()
	n.kvs, n.err = n.txn.Scan(startKey, endKey, 0)
	if n.err != nil {
		return false
	}

	// Prepare our index key vals slice.
	n.vals, n.err = makeIndexKeyVals(n.desc, *n.index)
	if n.err != nil {
		return false
	}

	// Prepare a map from column ID to column kind used for unmarshalling values.
	n.colKind = make(colKindMap, len(n.desc.Columns))
	for _, col := range n.desc.Columns {
		n.colKind[col.ID] = col.Type.Kind
	}
	return true
}

func (n *scanNode) processKV(kv client.KeyValue) bool {
	if n.indexKey == nil {
		// Reset the qvals map expressions to nil. The expresssions will get filled
		// in with the column values as we decode the key-value pairs for the row.
		for _, e := range n.qvals {
			e.Expr = nil
		}
	}

	var remaining []byte
	remaining, n.err = decodeIndexKey(n.desc, *n.index, n.vals, kv.Key)
	if n.err != nil {
		return false
	}

	if n.indexKey == nil {
		n.indexKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		// This is the first key for the row, initialize the column values that are
		// part of the index key.
		for i, id := range n.index.ColumnIDs {
			if qval := n.qvals[id]; qval != nil {
				qval.Expr = n.vals[i]
			}
		}
	}

	var value parser.Datum
	n.colID = 0

	if !n.isSecondaryIndex && len(remaining) > 0 {
		_, v := encoding.DecodeUvarint(remaining)
		n.colID = structured.ColumnID(v)
		if v, ok := n.qvals[n.colID]; ok && v.Expr == nil {
			value, ok = n.unmarshalValue(kv)
			if !ok {
				return false
			}
			v.Expr = value
			if log.V(2) {
				log.Infof("Scan %q -> %v", kv.Key, value)
			}
		} else {
			// No need to unmarshal the column value. Either the column was part of
			// the index key or it isn't needed by any of the render or filter
			// expressions.
			if log.V(2) {
				log.Infof("Scan %q -> [%d] (skipped)", kv.Key, n.colID)
			}
		}
	} else {
		if log.V(2) {
			log.Infof("Scan %q", kv.Key)
		}
	}

	if n.explain == explainDebug {
		if value == nil {
			if n.colID > 0 {
				var ok bool
				value, ok = n.unmarshalValue(kv)
				if !ok {
					return false
				}
			} else {
				value = parser.DNull
			}
		}
		n.explainValue = value
	}
	return true
}

// maybeOutputRow checks to see if the current key belongs to a new row and if
// it does it outputs the last row. The return value indicates whether a row
// was output or an error occurred. In either case, iteration should terminate.
func (n *scanNode) maybeOutputRow() bool {
	if n.indexKey != nil &&
		(n.kvIndex == len(n.kvs) || !bytes.HasPrefix(n.kvs[n.kvIndex].Key, n.indexKey)) {
		// The current key belongs to a new row. Output the current row.
		n.indexKey = nil
		output := n.filterRow()
		if n.err != nil {
			return true
		}
		if output {
			n.renderRow()
			return true
		} else if n.explainValue != nil {
			n.explainDebug(true, false)
			return true
		}
	} else if n.explainValue != nil {
		n.explainDebug(false, false)
		return true
	}
	return false
}

// filterRow checks to see if the current row matches the filter (i.e. the
// where-clause). May set n.err if an error occurs during expression
// evaluation.
func (n *scanNode) filterRow() bool {
	if n.desc != nil {
		for _, col := range n.visibleCols {
			if !col.Nullable {
				break
			}
			if v, ok := n.qvals[col.ID]; ok && v.Expr == nil {
				v.Expr = parser.DNull
				continue
			}
		}
	}

	if n.filter == nil {
		return true
	}

	var d parser.Datum
	d, n.err = parser.EvalExpr(n.filter)
	if n.err != nil {
		return false
	}

	v, ok := d.(parser.DBool)
	if !ok {
		n.err = fmt.Errorf("WHERE clause did not evaluate to a boolean")
		return false
	}
	return bool(v)
}

// renderRow renders the row by evaluating the render expressions. May set
// n.err if an error occurs during expression evaluation.
func (n *scanNode) renderRow() {
	if n.explain == explainDebug {
		n.explainDebug(true, true)
		return
	}

	if n.row == nil {
		n.row = make([]parser.Datum, len(n.render))
	}
	for i, e := range n.render {
		n.row[i], n.err = parser.EvalExpr(e)
		if n.err != nil {
			return
		}
	}
	n.rowIndex++
}

func (n *scanNode) explainDebug(endOfRow, outputRow bool) {
	if n.row == nil {
		n.row = make([]parser.Datum, len(n.columns))
	}
	n.row[0] = parser.DInt(n.rowIndex)
	n.row[1] = parser.DString(n.prettyKey())
	n.row[2] = parser.DString(n.explainValue.String())
	if endOfRow {
		n.row[3] = parser.DBool(outputRow)
		n.rowIndex++
	} else {
		n.row[3] = parser.DNull
	}
	n.explainValue = nil
}

func (n *scanNode) prettyKey() string {
	if n.desc == nil {
		return ""
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "/%s/%s", n.desc.Name, n.index.Name)
	for _, v := range n.vals {
		if v == parser.DNull {
			fmt.Fprintf(&buf, "/NULL")
			continue
		}
		switch t := v.(type) {
		case parser.DBool:
			fmt.Fprintf(&buf, "/%v", t)
		case parser.DInt:
			fmt.Fprintf(&buf, "/%v", t)
		case parser.DFloat:
			fmt.Fprintf(&buf, "/%v", t)
		case parser.DString:
			fmt.Fprintf(&buf, "/%v", t)
		}
	}
	if n.colID > 0 {
		// TODO(pmattis): This is inefficient, but does it matter?
		col, _ := n.desc.FindColumnByID(n.colID)
		fmt.Fprintf(&buf, "/%s", col.Name)
	}
	return buf.String()
}

func (n *scanNode) unmarshalValue(kv client.KeyValue) (parser.Datum, bool) {
	kind, ok := n.colKind[n.colID]
	if !ok {
		n.err = fmt.Errorf("column-id \"%d\" does not exist", n.colID)
		return nil, false
	}
	if kv.Exists() {
		switch kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			return parser.DInt(kv.ValueInt()), true
		case structured.ColumnType_BOOL:
			return parser.DBool(kv.ValueInt() != 0), true
		case structured.ColumnType_FLOAT:
			return parser.DFloat(math.Float64frombits(uint64(kv.ValueInt()))), true
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			return parser.DString(kv.ValueBytes()), true
		}
	}
	return parser.DNull, true
}

type qnameVisitor struct {
	*scanNode
	err error
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

	v.err = qname.NormalizeColumnName()
	if v.err != nil {
		return expr
	}

	desc := v.getDesc(qname)
	if desc != nil {
		name := qname.Column()
		for _, col := range v.visibleCols {
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
	if v.desc == nil {
		return nil
	}
	if qname.Base == "" {
		qname.Base = parser.Name(v.desc.Alias)
		return v.desc
	}
	if strings.EqualFold(v.desc.Alias, string(qname.Base)) {
		return v.desc
	}
	return nil
}

func (n *scanNode) extractQVals(expr parser.Expr) (parser.Expr, error) {
	if expr == nil {
		return expr, nil
	}
	v := qnameVisitor{scanNode: n}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.err
}
