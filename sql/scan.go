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
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// qvalue implements the parser.DRereference interface and is used as a
// replacement node for QualifiedNames in expressions that can change their
// values for each row. Since it is a reference, expression walking can
// discover the qvalues and the columns they refer to.
type qvalue struct {
	datum parser.Datum
	col   ColumnDescriptor

	// Tricky: we embed a parser.Expr so that qvalue implements parser.expr()!
	// Note that we can't just have qvalue.expr() as that method is defined in
	// the wrong package.
	parser.Expr
}

var _ parser.DReference = &qvalue{}

func (q *qvalue) Datum() parser.Datum {
	return q.datum
}

func (q *qvalue) String() string {
	return q.col.Name
}

type qvalMap map[ColumnID]*qvalue
type colKindMap map[ColumnID]ColumnType_Kind

type span struct {
	start proto.Key
	end   proto.Key
}

func prettySpans(spans []span, desc *TableDescriptor, index *IndexDescriptor) string {
	valTypes, err := makeKeyVals(desc, index.ColumnIDs)
	if err != nil {
		return err.Error()
	}
	vals := make([]parser.Datum, len(valTypes))
	prefix := proto.Key(MakeIndexKeyPrefix(desc.ID, index.ID))

	var buf bytes.Buffer
	for i, span := range spans {
		if i > 0 {
			_, _ = buf.WriteString(" ")
		}
		for j, key := range []proto.Key{span.start, span.end} {
			if j == 1 {
				_, _ = buf.WriteString("-")
			}
			if !bytes.HasPrefix(key, prefix) {
				if j == 1 {
					continue
				}
				return fmt.Sprintf("index key missing index prefix: %q vs %q", prefix, key)
			}
			key = key[len(prefix):]
			k := 0
			for ; k < len(valTypes) && len(key) > 0; k++ {
				vals[k], key, err = decodeTableKey(valTypes[k], key)
				if err != nil {
					break
				}
			}
			_, _ = buf.WriteString(prettyKeyVals(vals[:k]))
		}
	}
	return buf.String()
}

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	txn              *client.Txn
	desc             *TableDescriptor
	index            *IndexDescriptor
	spans            []span
	visibleCols      []ColumnDescriptor
	isSecondaryIndex bool
	reverse          bool
	columns          []string
	columnIDs        []ColumnID
	ordering         []int
	err              error
	indexKey         []byte            // the index key of the current row
	kvs              []client.KeyValue // the raw key/value pairs
	kvIndex          int               // current index into the key/value pairs
	rowIndex         int               // the index of the current row
	colID            ColumnID          // column ID of the current key
	valTypes         []parser.Datum    // the index key value types for the current row
	vals             []parser.Datum    // the index key values for the current row
	implicitValTypes []parser.Datum    // the implicit value types for unique indexes
	implicitVals     []parser.Datum    // the implicit values for unique indexes
	qvals            qvalMap           // the values in the current row
	colKind          colKindMap        // map of column kinds for decoding column values
	row              parser.DTuple     // the rendered row
	filter           parser.Expr       // filtering expression for rows
	render           []parser.Expr     // rendering expressions for rows
	explain          explainMode
	explainValue     parser.Datum
}

func (n *scanNode) Columns() []string {
	return n.columns
}

func (n *scanNode) Ordering() []int {
	return n.ordering
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

func (n *scanNode) ExplainPlan() (name, description string, children []planNode) {
	if n.reverse {
		name = "revscan"
	} else {
		name = "scan"
	}
	if n.desc == nil {
		description = "-"
	} else {
		description = fmt.Sprintf("%s@%s %s", n.desc.Name, n.index.Name,
			prettySpans(n.spans, n.desc, n.index))
	}
	return name, description, nil
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

		if err := p.checkPrivilege(n.desc, privilege.SELECT); err != nil {
			return err
		}

		// This is only kosher because we know that getAliasedDesc() succeeded.
		qname := from[0].(*parser.AliasedTableExpr).Expr.(*parser.QualifiedName)
		indexName := qname.Index()
		if indexName != "" && !equalName(n.desc.PrimaryIndex.Name, indexName) {
			for i := range n.desc.Indexes {
				if equalName(n.desc.Indexes[i].Name, indexName) {
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
			indexColIDs := map[ColumnID]struct{}{}
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

	if len(n.spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix.
		start := proto.Key(MakeIndexKeyPrefix(n.desc.ID, n.index.ID))
		n.spans = append(n.spans, span{
			start: start,
			end:   start.PrefixEnd(),
		})
	}

	// Retrieve all the spans.
	b := &client.Batch{}
	if n.reverse {
		for i := len(n.spans) - 1; i >= 0; i-- {
			b.ReverseScan(n.spans[i].start, n.spans[i].end, 0)
		}
	} else {
		for i := 0; i < len(n.spans); i++ {
			b.Scan(n.spans[i].start, n.spans[i].end, 0)
		}
	}
	if n.err = n.txn.Run(b); n.err != nil {
		return false
	}

	for _, result := range b.Results {
		if n.kvs == nil {
			n.kvs = result.Rows
		} else {
			n.kvs = append(n.kvs, result.Rows...)
		}
	}

	// Prepare our index key vals slice.
	if n.valTypes, n.err = makeKeyVals(n.desc, n.columnIDs); n.err != nil {
		return false
	}
	n.vals = make([]parser.Datum, len(n.valTypes))

	if n.isSecondaryIndex && n.index.Unique {
		// Unique secondary indexes have a value that is the primary index
		// key. Prepare implicitVals for use in decoding this value.
		if n.implicitValTypes, n.err = makeKeyVals(n.desc, n.index.ImplicitColumnIDs); n.err != nil {
			return false
		}
		n.implicitVals = make([]parser.Datum, len(n.implicitValTypes))
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
		// Normalize the expression (this will also evaluate any branches that are
		// constant).
		n.filter, n.err = parser.NormalizeExpr(n.filter)
	}
	if n.err == nil {
		var whereType parser.Datum
		whereType, n.err = parser.TypeCheckExpr(n.filter)
		if n.err == nil {
			if !(whereType == parser.DummyBool || whereType == parser.DNull) {
				n.err = fmt.Errorf("argument of WHERE must be type %s, not type %s", parser.DummyBool.Type(), whereType.Type())
			}
		}
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

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering() {
	if n.index == nil {
		return
	}
	n.columnIDs = n.index.fullColumnIDs()
	n.ordering = n.computeOrdering(n.columnIDs)
	if n.reverse {
		for i := range n.ordering {
			n.ordering[i] = -n.ordering[i]
		}
	}
}

// computeOrdering computes the ordering information for the specified set of
// columns.
func (n *scanNode) computeOrdering(columnIDs []ColumnID) []int {
	// Loop over the column IDs and determine if they are used for any of the
	// render targets.
	var ordering []int
	for _, colID := range columnIDs {
		found := false
		for i, r := range n.render {
			if qval, ok := r.(*qvalue); ok && qval.col.ID == colID {
				found = true
				ordering = append(ordering, i+1)
				break
			}
		}
		if !found {
			// Exit if the column ID in the index is not one of the output
			// expressions.
			break
		}
	}
	return ordering
}

func (n *scanNode) addRender(target parser.SelectExpr) error {
	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression (such as star expansion).
	var outputName string
	if target.As != "" {
		outputName = string(target.As)
	} else {
		outputName = target.Expr.String()
	}

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
			if tableName != "" && !equalName(n.desc.Alias, tableName) {
				return fmt.Errorf("table \"%s\" not found", tableName)
			}

			if n.isSecondaryIndex {
				for i, col := range n.index.ColumnNames {
					n.columns = append(n.columns, col)
					var col *ColumnDescriptor
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
	// Type check the expression to memoize operators and functions.
	var normalized parser.Expr
	if normalized, n.err = parser.NormalizeAndTypeCheckExpr(resolved); n.err != nil {
		return n.err
	}
	n.render = append(n.render, normalized)

	if target.As == "" {
		switch t := target.Expr.(type) {
		case *parser.QualifiedName:
			// If the expression is a qualified name, use the column name, not the
			// full qualification as the column name to return.
			outputName = t.Column()
		}
	}
	n.columns = append(n.columns, outputName)
	return nil
}

func (n *scanNode) processKV(kv client.KeyValue) bool {
	if n.indexKey == nil {
		// Reset the qvals map expressions to nil. The expresssions will get filled
		// in with the column values as we decode the key-value pairs for the row.
		for _, qval := range n.qvals {
			qval.datum = nil
		}
	}

	var remaining []byte
	remaining, n.err = decodeIndexKey(n.desc, *n.index, n.valTypes, n.vals, kv.Key)
	if n.err != nil {
		return false
	}

	if n.indexKey == nil {
		n.indexKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		// This is the first key for the row, initialize the column values that are
		// part of the index key.
		for i, id := range n.columnIDs {
			if qval, ok := n.qvals[id]; ok {
				qval.datum = n.vals[i]
			}
		}
	}

	var value parser.Datum
	n.colID = 0

	if !n.isSecondaryIndex && len(remaining) > 0 {
		_, v := encoding.DecodeUvarint(remaining)
		n.colID = ColumnID(v)
		if qval, ok := n.qvals[n.colID]; ok && qval.datum == nil {
			value, ok = n.unmarshalValue(kv)
			if !ok {
				return false
			}
			qval.datum = value
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
		if n.implicitVals != nil {
			if _, n.err = decodeKeyVals(n.implicitValTypes, n.implicitVals, kv.ValueBytes()); n.err != nil {
				return false
			}
			for i, id := range n.index.ImplicitColumnIDs {
				if qval, ok := n.qvals[id]; ok {
					qval.datum = n.implicitVals[i]
				}
			}
		}

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
			if qval, ok := n.qvals[col.ID]; ok && qval.datum == nil {
				qval.datum = parser.DNull
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

	return d != parser.DNull && bool(d.(parser.DBool))
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
	if n.implicitVals != nil {
		n.row[2] = parser.DString(prettyKeyVals(n.implicitVals))
	} else {
		n.row[2] = parser.DString(n.explainValue.String())
	}
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
	fmt.Fprintf(&buf, "/%s/%s%s", n.desc.Name, n.index.Name, prettyKeyVals(n.vals))
	if n.colID > 0 {
		// TODO(pmattis): This is inefficient, but does it matter?
		col, err := n.desc.FindColumnByID(n.colID)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(&buf, "/%s", col.Name)
	}
	return buf.String()
}

func prettyKeyVals(vals []parser.Datum) string {
	var buf bytes.Buffer
	for _, v := range vals {
		fmt.Fprintf(&buf, "/%v", v)
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
		case ColumnType_INT:
			return parser.DInt(kv.ValueInt()), true
		case ColumnType_BOOL:
			return parser.DBool(kv.ValueInt() != 0), true
		case ColumnType_FLOAT:
			return parser.DFloat(math.Float64frombits(uint64(kv.ValueInt()))), true
		case ColumnType_STRING, ColumnType_BYTES:
			return parser.DString(kv.ValueBytes()), true
		case ColumnType_DATE:
			var t time.Time
			if err := t.UnmarshalBinary(kv.ValueBytes()); err != nil {
				return nil, false
			}
			return parser.DDate{Time: t}, true
		case ColumnType_TIMESTAMP:
			var t time.Time
			if err := t.UnmarshalBinary(kv.ValueBytes()); err != nil {
				return nil, false
			}
			return parser.DTimestamp{Time: t}, true
		case ColumnType_INTERVAL:
			return parser.DInterval{Duration: time.Duration(kv.ValueInt())}, true
		}
	}
	return parser.DNull, true
}

func (n *scanNode) getQVal(col ColumnDescriptor) *qvalue {
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
		case ColumnType_INT:
			qval.datum = parser.DummyInt
		case ColumnType_BOOL:
			qval.datum = parser.DummyBool
		case ColumnType_FLOAT:
			qval.datum = parser.DummyFloat
		case ColumnType_STRING, ColumnType_BYTES:
			qval.datum = parser.DummyString
		case ColumnType_DATE:
			qval.datum = parser.DummyDate
		case ColumnType_TIMESTAMP:
			qval.datum = parser.DummyTimestamp
		case ColumnType_INTERVAL:
			qval.datum = parser.DummyInterval
		default:
			panic(fmt.Sprintf("unsupported column type: %s", col.Type.Kind))
		}
		n.qvals[col.ID] = qval
	}
	return qval
}

type qnameVisitor struct {
	*scanNode
	err error
}

var _ parser.Visitor = &qnameVisitor{}

func (v *qnameVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.err != nil {
		return nil, expr
	}

	switch t := expr.(type) {
	case *qvalue:
		// We will encounter a qvalue in the expression during retry of an
		// auto-transaction. When that happens, we've already gone through
		// qualified name normalization and lookup, we just need to hook the qvalue
		// up to the scanNode.
		//
		// TODO(pmattis): Should we be more careful about ensuring that the various
		// statement implementations do not modify the AST nodes they are passed?
		return v, v.getQVal(t.col)

	case *parser.QualifiedName:
		qname := t

		v.err = qname.NormalizeColumnName()
		if v.err != nil {
			return nil, expr
		}
		if qname.IsStar() {
			v.err = fmt.Errorf("qualified name \"%s\" not found", qname)
			return nil, expr
		}

		desc := v.getDesc(qname)
		if desc != nil {
			name := qname.Column()
			for _, col := range v.visibleCols {
				if !equalName(name, col.Name) {
					continue
				}
				return v, v.getQVal(col)
			}
		}

		v.err = fmt.Errorf("qualified name \"%s\" not found", qname)
		return nil, expr

	case *parser.FuncExpr:
		// Special case handling for COUNT(*) and COUNT(foo.*), expanding the star
		// into a tuple containing the columns in the primary key of the referenced
		// table.
		if len(t.Name.Indirect) > 0 || !strings.EqualFold(string(t.Name.Base), "count") {
			break
		}
		// The COUNT function takes a single argument. Exit out if this isn't true
		// as this will be detected during expression evaluation.
		if len(t.Exprs) != 1 {
			break
		}
		qname, ok := t.Exprs[0].(*parser.QualifiedName)
		if !ok {
			break
		}
		v.err = qname.NormalizeColumnName()
		if v.err != nil {
			return nil, expr
		}
		if !qname.IsStar() {
			// This will cause us to recurse into the arguments of the function which
			// will perform normal qualified name resolution.
			break
		}
		// We've got either COUNT(*) or COUNT(foo.*). Retrieve the descriptor.
		desc := v.getDesc(qname)
		if desc == nil {
			break
		}
		// Replace the function argument with a non-NULL column. We currently use
		// the first column of the primary index since that column will be a
		// candidate for index selection, but any non-NULL column would do for
		// correctness of the aggregate.
		var col *ColumnDescriptor
		if col, v.err = desc.FindColumnByID(desc.PrimaryIndex.ColumnIDs[0]); v.err != nil {
			return nil, expr
		}
		t.Exprs[0] = v.getQVal(*col)
		return v, expr
	}

	return v, expr
}

func (v *qnameVisitor) getDesc(qname *parser.QualifiedName) *TableDescriptor {
	if v.desc == nil {
		return nil
	}
	if qname.Base == "" {
		qname.Base = parser.Name(v.desc.Alias)
		return v.desc
	}
	if equalName(v.desc.Alias, string(qname.Base)) {
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
