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
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracer"
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
	start roachpb.Key // inclusive key
	end   roachpb.Key // exclusive key
	count int64
}

type spans []span

// implement Sort.Interface
func (a spans) Len() int           { return len(a) }
func (a spans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a spans) Less(i, j int) bool { return a[i].start.Compare(a[j].start) < 0 }

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
	columns          []ResultColumn
	originalCols     []ResultColumn // copy of `columns` before additions (e.g. by sort or group)
	columnIDs        []ColumnID
	// The direction with which the corresponding column was encoded.
	columnDirs       []encoding.Direction
	ordering         orderingInfo
	pErr             *roachpb.Error
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

func (n *scanNode) Columns() []ResultColumn {
	return n.columns
}

func (n *scanNode) Ordering() orderingInfo {
	return n.ordering
}

func (n *scanNode) Values() parser.DTuple {
	return n.row
}

func (n *scanNode) Next() bool {
	tracer.AnnotateTrace()

	if n.pErr != nil {
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
			return n.pErr == nil
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

func (n *scanNode) PErr() *roachpb.Error {
	return n.pErr
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

// Initializes a scanNode from an AliasedTableExpr
func (n *scanNode) initTableExpr(p *planner, ate *parser.AliasedTableExpr) *roachpb.Error {
	if n.desc, n.pErr = p.getAliasedTableLease(ate); n.pErr != nil {
		return n.pErr
	}

	if pErr := p.checkPrivilege(n.desc, privilege.SELECT); pErr != nil {
		return pErr
	}

	qname := ate.Expr.(*parser.QualifiedName)
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
			n.pErr = roachpb.NewUErrorf("index \"%s\" not found", indexName)
			return n.pErr
		}
		// If the table was not aliased, use the index name instead of the table
		// name for fully-qualified columns in the expression.
		if ate.As == "" {
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
	if n.pErr = n.txn.Run(b); n.pErr != nil {
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
		if n.valTypes, n.pErr = makeKeyVals(n.desc, n.columnIDs); n.pErr != nil {
			return false
		}
		n.vals = make([]parser.Datum, len(n.valTypes))

		if n.isSecondaryIndex && n.index.Unique {
			// Unique secondary indexes have a value that is the primary index
			// key. Prepare implicitVals for use in decoding this value.
			// Primary indexes only contain ascendingly-encoded values. If this
			// ever changes, we'll probably have to figure out the directions here too.
			if n.implicitValTypes, n.pErr = makeKeyVals(n.desc, n.index.ImplicitColumnIDs); n.pErr != nil {
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

func (n *scanNode) init(sel *parser.Select) *roachpb.Error {
	// TODO(radu): the where/targets logic will move into selectNode
	if pErr := n.initWhere(sel.Where); pErr != nil {
		return pErr
	}
	return n.initTargets(sel.Exprs)
}

func (n *scanNode) initWhere(where *parser.Where) *roachpb.Error {
	if where == nil {
		return nil
	}
	n.filter, n.pErr = n.resolveQNames(where.Expr)
	if n.pErr == nil {
		var whereType parser.Datum
		var err error
		whereType, err = n.filter.TypeCheck(n.planner.evalCtx.Args)
		n.pErr = roachpb.NewError(err)
		if n.pErr == nil {
			if !(whereType == parser.DummyBool || whereType == parser.DNull) {
				n.pErr = roachpb.NewUErrorf("argument of WHERE must be type %s, not type %s", parser.DummyBool.Type(), whereType.Type())
			}
		}
	}
	if n.pErr == nil {
		// Normalize the expression (this will also evaluate any branches that are
		// constant).
		var err error
		n.filter, err = n.planner.parser.NormalizeExpr(n.planner.evalCtx, n.filter)
		n.pErr = roachpb.NewError(err)
	}
	if n.pErr == nil {
		n.filter, n.pErr = n.planner.expandSubqueries(n.filter, 1)
	}
	return n.pErr
}

func (n *scanNode) initTargets(targets parser.SelectExprs) *roachpb.Error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for _, target := range targets {
		if n.pErr = n.addRender(target); n.pErr != nil {
			return n.pErr
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
	n.columnIDs, n.columnDirs = n.index.fullColumnIDs()
	n.ordering = computeOrdering(n.render, n.index, exactPrefix, n.reverse)
}

// Searches for a render target for the value of the given column.
func findRenderIndexForCol(render []parser.Expr, colID ColumnID) (idx int, ok bool) {
	for i, r := range render {
		if qval, ok := r.(*qvalue); ok && qval.col.ID == colID {
			return i, true
		}
	}
	return -1, false
}

// computeOrdering calculates ordering information for render target columns assuming that:
//    - we scan a given index (potentially in reverse order), and
//    - the first `exactPrefix` columns of the index each have an exact (single value) match
//      (see orderingInfo).
//
// Some examples:
//
//    SELECT a, b FROM t@abc ...
//    	the ordering is: first by column 0 (a), then by column 1 (b)
//
//    SELECT a, b FROM t@abc WHERE a = 1 ...
//    	the ordering is: exact match column (a), ordered by column 1 (b)
//
//    SELECT b, a FROM t@abc ...
//      the ordering is: first by column 1 (a), then by column 0 (a)
//
//    SELECT a, c FROM t@abc ...
//      the ordering is: just by column 0 (a). Here we don't have b as a render target so we
//      cannot possibly use (or even express) the second-rank order by b (which makes any lower
//      ranks unusable as well).
//
//      Note that for queries like
//         SELECT a, c FROM t@abc ORDER by a,b,c
//      we internally add b as a render target. The same holds for any targets required for
//      grouping.
func computeOrdering(
	render []parser.Expr, index *IndexDescriptor, exactPrefix int, reverse bool) orderingInfo {
	var ordering orderingInfo

	columnIDs, dirs := index.fullColumnIDs()

	for i, colID := range columnIDs {
		renderIdx, ok := findRenderIndexForCol(render, colID)
		if ok {
			if i < exactPrefix {
				ordering.addExactMatchColumn(renderIdx)
			} else {
				dir := dirs[i]
				if reverse {
					dir = dir.Reverse()
				}
				ordering.addColumn(renderIdx, dir)
			}
			continue
		}
		// We have a column that isn't part of the output.
		if i < exactPrefix {
			// Fortunately this is an "exact match" column, so we can safely ignore it.
			//
			// For example, assume we are using an ascending index on (k, v) with the query:
			//
			//   SELECT v FROM t WHERE k = 1
			//
			// The rows from the index are ordered by k then by v, but since k is an exact match
			// column the results are also ordered just by v.
			continue
		} else {
			// Once we find a column that is not part of the output, the rest of the ordered
			// columns aren't useful.
			//
			// For example, assume we are using an ascending index on (k, v) with the query:
			//
			//   SELECT v FROM t WHERE k > 1
			//
			// The rows from the index are ordered by k then by v. We cannot make any use of this
			// ordering as an ordering on v.
			break
		}
	}
	return ordering
}

func (n *scanNode) addRender(target parser.SelectExpr) *roachpb.Error {
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
		if n.pErr = roachpb.NewError(qname.NormalizeColumnName()); n.pErr != nil {
			return n.pErr
		}
		if qname.IsStar() {
			if n.desc == nil {
				return roachpb.NewUErrorf("\"%s\" with no tables specified is not valid", qname)
			}
			if target.As != "" {
				return roachpb.NewUErrorf("\"%s\" cannot be aliased", qname)
			}
			tableName := qname.Table()
			if tableName != "" && !equalName(n.desc.Alias, tableName) {
				return roachpb.NewUErrorf("table \"%s\" not found", tableName)
			}

			if n.isSecondaryIndex {
				for _, id := range n.index.ColumnIDs {
					var col *ColumnDescriptor
					if col, n.pErr = n.desc.FindColumnByID(id); n.pErr != nil {
						return n.pErr
					}
					qval := n.getQVal(*col)
					n.columns = append(n.columns, ResultColumn{Name: col.Name, Typ: qval.datum})
					n.render = append(n.render, qval)
				}
			} else {
				for _, col := range n.desc.VisibleColumns() {
					qval := n.getQVal(col)
					n.columns = append(n.columns, ResultColumn{Name: col.Name, Typ: qval.datum})
					n.render = append(n.render, qval)
				}
			}
			return nil
		}
	}

	// Resolve qualified names. This has the side-effect of normalizing any
	// qualified name found.
	var resolved parser.Expr
	if resolved, n.pErr = n.resolveQNames(target.Expr); n.pErr != nil {
		return n.pErr
	}
	if resolved, n.pErr = n.planner.expandSubqueries(resolved, 1); n.pErr != nil {
		return n.pErr
	}
	var typ parser.Datum
	var err error
	typ, err = resolved.TypeCheck(n.planner.evalCtx.Args)
	n.pErr = roachpb.NewError(err)
	if n.pErr != nil {
		return n.pErr
	}
	var normalized parser.Expr
	normalized, err = n.planner.parser.NormalizeExpr(n.planner.evalCtx, resolved)
	n.pErr = roachpb.NewError(err)
	if n.pErr != nil {
		return n.pErr
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
	n.columns = append(n.columns, ResultColumn{Name: outputName, Typ: typ})
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
	remaining, n.pErr = decodeIndexKey(n.desc, n.index.ID, n.valTypes, n.vals, n.columnDirs, kv.Key)
	if n.pErr != nil {
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
		var err error
		_, v, err = encoding.DecodeUvarintAscending(remaining)
		n.pErr = roachpb.NewError(err)
		if n.pErr != nil {
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
			implicitDirs := make([]encoding.Direction, 0, len(n.index.ImplicitColumnIDs))
			for range n.index.ImplicitColumnIDs {
				implicitDirs = append(implicitDirs, encoding.Ascending)
			}
			if _, n.pErr = decodeKeyVals(
				n.implicitValTypes, n.implicitVals, implicitDirs, kv.ValueBytes()); n.pErr != nil {
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
		if n.pErr != nil {
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
// where-clause). May set n.pErr if an error occurs during expression
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
	var err error
	d, err = n.filter.Eval(n.planner.evalCtx)
	n.pErr = roachpb.NewError(err)
	if n.pErr != nil {
		return false
	}

	return d != parser.DNull && bool(d.(parser.DBool))
}

// renderRow renders the row by evaluating the render expressions. May set
// n.pErr if an error occurs during expression evaluation.
func (n *scanNode) renderRow() {
	if n.explain == explainDebug {
		n.explainDebug(true, true)
		return
	}

	if n.row == nil {
		n.row = make([]parser.Datum, len(n.render))
	}
	for i, e := range n.render {
		var err error
		n.row[i], err = e.Eval(n.planner.evalCtx)
		n.pErr = roachpb.NewError(err)
		if n.pErr != nil {
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
		n.pErr = roachpb.NewUErrorf("column-id \"%d\" does not exist", n.colID)
		return nil, false
	}
	var d parser.Datum
	d, n.pErr = unmarshalColumnValue(kind, kv.Value)
	return d, n.pErr == nil
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
	pErr *roachpb.Error
}

var _ parser.Visitor = &qnameVisitor{}

func (v *qnameVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if !pre || v.pErr != nil {
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

		v.pErr = roachpb.NewError(qname.NormalizeColumnName())
		if v.pErr != nil {
			return nil, expr
		}
		if qname.IsStar() {
			v.pErr = roachpb.NewUErrorf("qualified name \"%s\" not found", qname)
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

		v.pErr = roachpb.NewUErrorf("qualified name \"%s\" not found", qname)
		return nil, expr

	case *parser.FuncExpr:
		// Special case handling for COUNT(*). This is a special construct to
		// count the number of rows; in this case * does NOT refer to a set of
		// columns.
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
		v.pErr = roachpb.NewError(qname.NormalizeColumnName())
		if v.pErr != nil {
			return nil, expr
		}
		if !qname.IsStar() {
			// This will cause us to recurse into the arguments of the function which
			// will perform normal qualified name resolution.
			break
		}
		// Replace the function argument with a special non-NULL VariableExpr.
		t.Exprs[0] = starDatumInstance
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

func (n *scanNode) resolveQNames(expr parser.Expr) (parser.Expr, *roachpb.Error) {
	if expr == nil {
		return expr, nil
	}
	v := qnameVisitor{scanNode: n}
	expr = parser.WalkExpr(&v, expr)
	return expr, v.pErr
}

// A VariableExpr used as a dummy argument for the special case COUNT(*).  This
// ends up being processed correctly by the count aggregator since it is not
// parser.DNull.
//
// We need to implement enough functionality to satisfy the type checker and to
// allow the the intermediate rendering of the row (before the group
// aggregation).
type starDatum struct{}

var starDatumInstance = &starDatum{}
var _ parser.VariableExpr = starDatumInstance

func (*starDatum) Variable() {}

func (*starDatum) String() string {
	return "*"
}

func (*starDatum) Walk(v parser.Visitor) {}

func (*starDatum) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return parser.DummyInt.TypeCheck(args)
}

func (*starDatum) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return parser.DummyInt.Eval(ctx)
}
