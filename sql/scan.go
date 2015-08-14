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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

type qvalue struct {
	// TODO(pmattis): Add a QualifiedValue expression (or IndirectExpr or
	// DReference) which contains a Datum but also a pointer to the column. This
	// is needed so that select planning can operation on resolved expressions
	// and map qualified values back to columns.
	parser.ParenExpr
	col structured.ColumnDescriptor
}

type qvalMap map[structured.ColumnID]*qvalue
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
		if !n.initScan() {
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

func (n *scanNode) initFrom(p *planner, from parser.TableExprs) error {
	switch len(from) {
	case 0:
		// n.desc remains nil.
		return nil

	case 1:
		if n.desc, n.err = p.getAliasedTableDesc(from[0]); n.err != nil {
			return n.err
		}

		if !n.desc.HasPrivilege(p.user, parser.PrivilegeRead) {
			n.err = fmt.Errorf("user %s does not have %s privilege on table %s",
				p.user, parser.PrivilegeRead, n.desc.Name)
			return n.err
		}

		// This is only kosher because we know that getAliasedDesc() succeeded.
		qname := from[0].(*parser.AliasedTableExpr).Expr.(*parser.QualifiedName)
		indexName := qname.Index()
		if indexName != "" && !strings.EqualFold(n.desc.PrimaryIndex.Name, indexName) {
			for i := range n.desc.Indexes {
				if strings.EqualFold(n.desc.Indexes[i].Name, indexName) {
					// Remove all but the matching index from the descriptor.
					n.desc.Indexes = n.desc.Indexes[i : i+1]
					n.index = &n.desc.Indexes[0]
					break
				}
			}
			if n.index == nil {
				n.err = fmt.Errorf("index \"%s\" not found", indexName)
				return n.err
			}
			// If the table was not aliased, use the index name instead of the table
			// name for fully-qualified columns in the expression.
			if from[0].(*parser.AliasedTableExpr).As == "" {
				n.desc.Alias = n.index.Name
			}
			// Strip out any columns from the table that are not present in the
			// index.
			indexColIDs := map[structured.ColumnID]struct{}{}
			for _, colID := range n.index.ColumnIDs {
				indexColIDs[colID] = struct{}{}
			}
			for _, col := range n.desc.Columns {
				if _, ok := indexColIDs[col.ID]; !ok {
					continue
				}
				n.visibleCols = append(n.visibleCols, col)
			}
			n.isSecondaryIndex = true
		} else {
			n.index = &n.desc.PrimaryIndex
			n.visibleCols = n.desc.Columns
		}

		return nil

	default:
		n.err = util.Errorf("TODO(pmattis): unsupported FROM: %s", from)
		return n.err
	}
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

func (n *scanNode) initWhere(where *parser.Where) error {
	if where == nil {
		return nil
	}
	n.filter, n.err = n.resolveQNames(where.Expr)
	if n.err == nil {
		// Evaluate the expression once to memoize operators and functions.
		_, n.err = parser.EvalExpr(n.filter)
	}
	return n.err
}

func (n *scanNode) initTargets(targets parser.SelectExprs) error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for _, target := range targets {
		if n.err = n.addRender(target); n.err != nil {
			return n.err
		}
	}
	return nil
}

func (n *scanNode) addRender(target parser.SelectExpr) error {
	// If a QualifiedName has a StarIndirection suffix we need to match the
	// prefix of the qualified name to one of the tables in the query and
	// then expand the "*" into a list of columns.
	if qname, ok := target.Expr.(*parser.QualifiedName); ok {
		if n.err = qname.NormalizeColumnName(); n.err != nil {
			return n.err
		}
		if qname.IsStar() {
			if n.desc == nil {
				return fmt.Errorf("\"%s\" with no tables specified is not valid", qname)
			}
			if target.As != "" {
				return fmt.Errorf("\"%s\" cannot be aliased", qname)
			}
			tableName := qname.Table()
			if tableName != "" && !strings.EqualFold(n.desc.Alias, tableName) {
				return fmt.Errorf("table \"%s\" not found", tableName)
			}

			if n.isSecondaryIndex {
				for i, col := range n.index.ColumnNames {
					n.columns = append(n.columns, col)
					var col *structured.ColumnDescriptor
					if col, n.err = n.desc.FindColumnByID(n.index.ColumnIDs[i]); n.err != nil {
						return n.err
					}
					n.render = append(n.render, n.getQVal(*col))
				}
			} else {
				for _, col := range n.desc.Columns {
					n.columns = append(n.columns, col.Name)
					n.render = append(n.render, n.getQVal(col))
				}
			}
			return nil
		}
	}

	// Resolve qualified names. This has the side-effect of normalizing any
	// qualified name found.
	var resolved parser.Expr
	if resolved, n.err = n.resolveQNames(target.Expr); n.err != nil {
		return n.err
	}
	// Evaluate the expression once to memoize operators and functions.
	if _, n.err = parser.EvalExpr(resolved); n.err != nil {
		return n.err
	}
	n.render = append(n.render, resolved)

	if target.As != "" {
		n.columns = append(n.columns, string(target.As))
		return nil
	}

	switch t := target.Expr.(type) {
	case *parser.QualifiedName:
		// If the expression is a qualified name, use the column name, not the
		// full qualification as the column name to return.
		n.columns = append(n.columns, t.Column())
	default:
		n.columns = append(n.columns, target.Expr.String())
	}
	return nil
}

func (n *scanNode) processKV(kv client.KeyValue) bool {
	if n.indexKey == nil {
		// Reset the qvals map expressions to nil. The expresssions will get filled
		// in with the column values as we decode the key-value pairs for the row.
		for _, qval := range n.qvals {
			qval.Expr = nil
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
		if qval, ok := n.qvals[n.colID]; ok && qval.Expr == nil {
			value, ok = n.unmarshalValue(kv)
			if !ok {
				return false
			}
			qval.Expr = value
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
			if qval, ok := n.qvals[col.ID]; ok && qval.Expr == nil {
				qval.Expr = parser.DNull
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

func (n *scanNode) getQVal(col structured.ColumnDescriptor) parser.Expr {
	if n.qvals == nil {
		n.qvals = make(qvalMap)
	}
	qval := n.qvals[col.ID]
	if qval == nil {
		qval = &qvalue{col: col}
		// We initialize the qvalue expression to a datum of the type matching the
		// column. This allows type analysis to be performed on the expression
		// before we start retrieving rows.
		//
		// TODO(pmattis): Nullable columns can have NULL values. The type analysis
		// needs to take that into consideration, but how to surface that info?
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			qval.Expr = parser.DInt(0)
		case structured.ColumnType_BOOL:
			qval.Expr = parser.DBool(true)
		case structured.ColumnType_FLOAT:
			qval.Expr = parser.DFloat(0)
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			qval.Expr = parser.DString("")
		default:
			panic(fmt.Sprintf("unsupported column type: %s", col.Type.Kind))
		}
		n.qvals[col.ID] = qval
	}
	return &qval.ParenExpr
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
	if qname.IsStar() {
		v.err = fmt.Errorf("qualified name \"%s\" not found", qname)
		return expr
	}

	desc := v.getDesc(qname)
	if desc != nil {
		name := qname.Column()
		for _, col := range v.visibleCols {
			if !strings.EqualFold(name, col.Name) {
				continue
			}
			return v.getQVal(col)
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

func (n *scanNode) resolveQNames(expr parser.Expr) (parser.Expr, error) {
	if expr == nil {
		return expr, nil
	}
	v := qnameVisitor{scanNode: n}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.err
}
