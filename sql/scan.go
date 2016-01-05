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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
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
}

var _ parser.VariableExpr = &qvalue{}

func (*qvalue) Variable() {}

func (q *qvalue) String() string {
	return q.col.Name
}

func (q *qvalue) Walk(v parser.Visitor) {
	q.datum = parser.WalkExpr(v, q.datum).(parser.Datum)
}

func (q *qvalue) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return q.datum.TypeCheck(args)
}

func (q *qvalue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return q.datum.Eval(ctx)
}

type qvalMap map[ColumnID]*qvalue
type colKindMap map[ColumnID]ColumnType_Kind

type span struct {
	start roachpb.Key
	end   roachpb.Key
	count int64
}

// prettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//   /Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func prettyKey(key roachpb.Key, skip int) string {
	p := key.String()
	for i := 0; i <= skip; i++ {
		n := strings.IndexByte(p[1:], '/')
		if n == -1 {
			return ""
		}
		p = p[n+1:]
	}
	return p
}

func prettyDatums(vals []parser.Datum) string {
	var buf bytes.Buffer
	for _, v := range vals {
		fmt.Fprintf(&buf, "/%v", v)
	}
	return buf.String()
}

func prettySpan(span span, skip int) string {
	var buf bytes.Buffer
	if span.count != 0 {
		fmt.Fprintf(&buf, "%d:", span.count)
	}
	fmt.Fprintf(&buf, "%s-%s", prettyKey(span.start, skip), prettyKey(span.end, skip))
	return buf.String()
}

func prettySpans(spans []span, skip int) string {
	var buf bytes.Buffer
	for i, span := range spans {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prettySpan(span, skip))
	}
	return buf.String()
}

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	planner          *planner
	txn              *client.Txn
	desc             *TableDescriptor
	index            *IndexDescriptor
	spans            []span
	visibleCols      []ColumnDescriptor
	isSecondaryIndex bool
	reverse          bool
	columns          []column
	originalCols     []column // copy of `columns` before additions (e.g. by sort or group)
	columnIDs        []ColumnID
	ordering         []int
	exactPrefix      int
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

func (n *scanNode) Columns() []column {
	return n.columns
}

func (n *scanNode) Ordering() ([]int, int) {
	return n.ordering, n.exactPrefix
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
			prettySpans(n.spans, 2))
	}
	return name, description, nil
}

func (n *scanNode) initFrom(p *planner, from parser.TableExprs) error {
	switch len(from) {
	case 0:
		// n.desc is nil, but can be set externally.
		return nil

	case 1:
		if n.desc, n.err = p.getAliasedTableLease(from[0]); n.err != nil {
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
			for _, colID := range n.index.ImplicitColumnIDs {
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
			n.initDescDefaults()
		}

		return nil

	default:
		n.err = util.Errorf("TODO(pmattis): unsupported FROM: %s", from)
		return n.err
	}
}

func (n *scanNode) initDescDefaults() {
	n.index = &n.desc.PrimaryIndex
	n.visibleCols = n.desc.Columns
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
		start := roachpb.Key(MakeIndexKeyPrefix(n.desc.ID, n.index.ID))
		n.spans = append(n.spans, span{
			start: start,
			end:   start.PrefixEnd(),
		})
	}

	// Retrieve all the spans.
	b := &client.Batch{}
	if n.reverse {
		for i := len(n.spans) - 1; i >= 0; i-- {
			b.ReverseScan(n.spans[i].start, n.spans[i].end, n.spans[i].count)
		}
	} else {
		for i := 0; i < len(n.spans); i++ {
			b.Scan(n.spans[i].start, n.spans[i].end, n.spans[i].count)
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

	if n.valTypes == nil {
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
	}
	return true
}

func (n *scanNode) initWhere(where *parser.Where) error {
	if where == nil {
		return nil
	}
	n.filter, n.err = n.resolveQNames(where.Expr)
	if n.err == nil {
		var whereType parser.Datum
		whereType, n.err = n.filter.TypeCheck(n.planner.evalCtx.Args)
		if n.err == nil {
			if !(whereType == parser.DummyBool || whereType == parser.DNull) {
				n.err = fmt.Errorf("argument of WHERE must be type %s, not type %s", parser.DummyBool.Type(), whereType.Type())
			}
		}
	}
	if n.err == nil {
		// Normalize the expression (this will also evaluate any branches that are
		// constant).
		n.filter, n.err = n.planner.parser.NormalizeExpr(n.planner.evalCtx, n.filter)
	}
	if n.err == nil {
		n.filter, n.err = n.planner.expandSubqueries(n.filter, 1)
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
	// `groupBy` or `orderBy` may internally add additional columns which we
	// do not want to include in validation of e.g. `GROUP BY 2`.
	n.originalCols = n.columns
	return nil
}

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering(exactPrefix int) {
	if n.index == nil {
		return
	}
	n.exactPrefix = exactPrefix
	n.columnIDs = n.index.fullColumnIDs()
	n.ordering = n.computeOrdering(n.columnIDs)
	if n.reverse {
		for i := range n.ordering {
			n.ordering[i] = -n.ordering[i]
		}
	}
}

// computeOrdering computes the ordering information for the specified set of
// columns. The returned slice will be the same length as the input slice. If a
// column is not part of the output 0 will be returned in the corresponding
// field of the output. Consider the query:
//
//   SELECT v FROM t WHERE k = 1
//
// If there is an index on (k, v) and we're asking for the ordering for those
// columns, computeOrdering will return (0, 1). This indicates that column k is
// not part of the output column set and column v is in ascending order.
func (n *scanNode) computeOrdering(columnIDs []ColumnID) []int {
	// Loop over the column IDs and determine if they are used for any of the
	// render targets.
	ordering := make([]int, len(columnIDs))
	for j, colID := range columnIDs {
		for i, r := range n.render {
			if qval, ok := r.(*qvalue); ok && qval.col.ID == colID {
				ordering[j] = i + 1
				break
			}
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
				for _, id := range n.index.ColumnIDs {
					var col *ColumnDescriptor
					if col, n.err = n.desc.FindColumnByID(id); n.err != nil {
						return n.err
					}
					qval := n.getQVal(*col)
					n.columns = append(n.columns, column{name: col.Name, typ: qval.datum})
					n.render = append(n.render, qval)
				}
			} else {
				for _, col := range n.desc.VisibleColumns() {
					qval := n.getQVal(col)
					n.columns = append(n.columns, column{name: col.Name, typ: qval.datum})
					n.render = append(n.render, qval)
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
	if resolved, n.err = n.planner.expandSubqueries(resolved, 1); n.err != nil {
		return n.err
	}
	var typ parser.Datum
	if typ, n.err = resolved.TypeCheck(n.planner.evalCtx.Args); n.err != nil {
		return n.err
	}
	var normalized parser.Expr
	normalized, n.err = n.planner.parser.NormalizeExpr(n.planner.evalCtx, resolved)
	if n.err != nil {
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
	n.columns = append(n.columns, column{name: outputName, typ: typ})
	return nil
}

func (n *scanNode) colIndex(expr parser.Expr) (int, error) {
	switch i := expr.(type) {
	case parser.DInt:
		index := int(i)
		if numCols := len(n.originalCols); index < 1 || index > numCols {
			return -1, fmt.Errorf("invalid column index: %d not in range [1, %d]", index, numCols)
		}
		return index - 1, nil

	case parser.Datum:
		return -1, fmt.Errorf("non-integer constant column index: %s", expr)

	default:
		// expr doesn't look like a col index (i.e. not a constant).
		return -1, nil
	}
}

func (n *scanNode) processKV(kv client.KeyValue) bool {
	if n.indexKey == nil {
		// Reset the qvals map expressions to nil. The expressions will get filled
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
		var v uint64
		_, v, n.err = encoding.DecodeUvarint(remaining)
		if n.err != nil {
			return false
		}
		n.colID = ColumnID(v)
		if qval, ok := n.qvals[n.colID]; ok && qval.datum == nil {
			value, ok = n.unmarshalValue(kv)
			if !ok {
				return false
			}
			qval.datum = value
			if log.V(2) {
				log.Infof("Scan %s -> %v", kv.Key, value)
			}
		} else {
			// No need to unmarshal the column value. Either the column was part of
			// the index key or it isn't needed by any of the render or filter
			// expressions.
			if log.V(2) {
				log.Infof("Scan %s -> [%d] (skipped)", kv.Key, n.colID)
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
			if n.implicitVals != nil {
				log.Infof("Scan %s -> %s", kv.Key, prettyDatums(n.implicitVals))
			} else {
				log.Infof("Scan %s", kv.Key)
			}
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
	// For unique secondary indexes, the index-key does not distinguish one row
	// from the next if both rows contain identical values along with a
	// NULL. Consider the keys:
	//
	//   /test/unique_idx/NULL/0
	//   /test/unique_idx/NULL/1
	//
	// The index-key extracted from the above keys is /test/unique_idx/NULL. The
	// trailing /0 and /1 are the primary key used to unique-ify the keys when a
	// NULL is present. Currently we don't detect NULLs on decoding. If we did we
	// could detect this case and enlarge the index-key. A simpler fix for this
	// problem is to simply always output a row for each key scanned from a
	// secondary index as secondary indexes have only one key per row.

	if n.indexKey != nil &&
		(n.isSecondaryIndex || n.kvIndex == len(n.kvs) ||
			!bytes.HasPrefix(n.kvs[n.kvIndex].Key, n.indexKey)) {
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
				continue
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
	d, n.err = n.filter.Eval(n.planner.evalCtx)
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
		n.row[i], n.err = e.Eval(n.planner.evalCtx)
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
		n.row[2] = parser.DString(prettyDatums(n.implicitVals))
	} else {
		// This conversion to DString is odd. `n.explainValue` is already a
		// `Datum`, but logic_test currently expects EXPLAIN DEBUG output
		// to come out formatted using `encodeSQLString`. This is not
		// consistent across all printing of strings in logic_test, though.
		// TODO(tamird/pmattis): figure out a consistent story for string
		// printing in logic_test.
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
	fmt.Fprintf(&buf, "/%s/%s%s", n.desc.Name, n.index.Name, prettyDatums(n.vals))
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

func (n *scanNode) unmarshalValue(kv client.KeyValue) (parser.Datum, bool) {
	kind, ok := n.colKind[n.colID]
	if !ok {
		n.err = fmt.Errorf("column-id \"%d\" does not exist", n.colID)
		return nil, false
	}
	var d parser.Datum
	d, n.err = unmarshalColumnValue(kind, kv.Value)
	return d, n.err == nil
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
		case ColumnType_STRING:
			qval.datum = parser.DummyString
		case ColumnType_BYTES:
			qval.datum = parser.DummyBytes
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

		if desc := v.getDesc(qname); desc != nil {
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

	case *parser.Subquery:
		// Do not recurse into subqueries.
		return nil, expr
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
