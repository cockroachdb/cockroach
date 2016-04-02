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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// A scanNode handles scanning over the key/value pairs for a table and
// reconstructing them into rows.
type scanNode struct {
	planner *planner
	txn     *client.Txn
	desc    TableDescriptor
	index   *IndexDescriptor

	// Set if an index was explicitly specified.
	specifiedIndex *IndexDescriptor
	// Set if the NO_INDEX_JOIN hint was given.
	noIndexJoin bool

	// There is a 1-1 correspondence between desc.Column sand resultColumns.
	resultColumns []ResultColumn
	// row contains values for the current row. There is a 1-1 correspondence
	// between resultColumns and vals.
	row parser.DTuple
	// for each column in resultColumns, indicates if the value is needed (used
	// as an optimization when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in desc.Columns.
	colIdxMap map[ColumnID]int

	spans            []span
	isSecondaryIndex bool
	reverse          bool
	columnIDs        []ColumnID
	// The direction with which the corresponding column was encoded.
	columnDirs       []encoding.Direction
	ordering         orderingInfo
	pErr             *roachpb.Error
	indexKey         []byte         // the index key of the current row
	rowIndex         int            // the index of the current row
	colID            ColumnID       // column ID of the current key
	valTypes         []parser.Datum // the index key value types for the current row
	vals             []parser.Datum // the index key values for the current row
	implicitValTypes []parser.Datum // the implicit value types for unique indexes
	implicitVals     []parser.Datum // the implicit values for unique indexes
	explain          explainMode
	explainValue     parser.Datum
	debugVals        debugValues

	// filter that can be evaluated using only this table/index; it contains scanQValues.
	filter parser.Expr
	// qvalues (one per column) which can be part of the filter expression.
	qvals []scanQValue

	scanInitialized bool
	fetcher         kvFetcher
	// The current key/value, unless kvEnd is true.
	kv        client.KeyValue
	kvEnd     bool
	limitHint int64
}

func (n *scanNode) Columns() []ResultColumn {
	return n.resultColumns
}

func (n *scanNode) Ordering() orderingInfo {
	return n.ordering
}

func (n *scanNode) Values() parser.DTuple {
	return n.row
}

func (n *scanNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
}

func (n *scanNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *scanNode) SetLimitHint(numRows int64, soft bool) {
	n.limitHint = numRows
	if soft || n.filter != nil {
		// Read a multiple of the limit if the limit is "soft".
		n.limitHint *= 2
	}
}

// nextKey gets the next key and sets kv and kvEnd. Returns false on errors.
func (n *scanNode) nextKey() bool {
	var ok bool
	ok, n.kv, n.pErr = n.fetcher.nextKV()
	if n.pErr != nil {
		return false
	}
	n.kvEnd = !ok
	return true
}

func (n *scanNode) Next() bool {
	tracing.AnnotateTrace()

	if n.pErr != nil {
		return false
	}

	if !n.scanInitialized {
		if !n.initScan() {
			// Hit error.
			return false
		}
		if !n.nextKey() {
			// Hit error.
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
		if n.kvEnd {
			// End of scan.
			return false
		}
		if !n.processKV(n.kv) {
			// Hit error.
			return false
		}
		if !n.nextKey() {
			// Hit error.
			return false
		}
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
	description = fmt.Sprintf("%s@%s %s", n.desc.Name, n.index.Name, prettySpans(n.spans, 2))
	return name, description, nil
}

// Initializes a scanNode with a tableName. Returns the table or index name that can be used for
// fully-qualified columns if an alias is not specified.
func (n *scanNode) initTable(
	p *planner, tableName *parser.QualifiedName, indexHints *parser.IndexHints,
) (string, *roachpb.Error) {
	if n.desc, n.pErr = p.getTableLease(tableName); n.pErr != nil {
		return "", n.pErr
	}

	if err := p.checkPrivilege(&n.desc, privilege.SELECT); err != nil {
		return "", roachpb.NewError(err)
	}

	alias := n.desc.Name

	if indexHints != nil && indexHints.Index != "" {
		indexName := NormalizeName(string(indexHints.Index))
		if indexName == NormalizeName(n.desc.PrimaryIndex.Name) {
			n.specifiedIndex = &n.desc.PrimaryIndex
		} else {
			for i := range n.desc.Indexes {
				if indexName == NormalizeName(n.desc.Indexes[i].Name) {
					n.specifiedIndex = &n.desc.Indexes[i]
					break
				}
			}
			if n.specifiedIndex == nil {
				n.pErr = roachpb.NewUErrorf("index \"%s\" not found", indexName)
				return "", n.pErr
			}
		}
	}
	n.noIndexJoin = (indexHints != nil && indexHints.NoIndexJoin)
	n.initDescDefaults()
	return alias, nil
}

// makeResultColumns converts ColumnDescriptors to ResultColumns.
func makeResultColumns(colDescs []ColumnDescriptor) []ResultColumn {
	cols := make([]ResultColumn, 0, len(colDescs))
	for _, colDesc := range colDescs {
		// Convert the ColumnDescriptor to ResultColumn.
		var typ parser.Datum

		switch colDesc.Type.Kind {
		case ColumnType_INT:
			typ = parser.DummyInt
		case ColumnType_BOOL:
			typ = parser.DummyBool
		case ColumnType_FLOAT:
			typ = parser.DummyFloat
		case ColumnType_DECIMAL:
			typ = parser.DummyDecimal
		case ColumnType_STRING:
			typ = parser.DummyString
		case ColumnType_BYTES:
			typ = parser.DummyBytes
		case ColumnType_DATE:
			typ = parser.DummyDate
		case ColumnType_TIMESTAMP:
			typ = parser.DummyTimestamp
		case ColumnType_INTERVAL:
			typ = parser.DummyInterval
		default:
			panic(fmt.Sprintf("unsupported column type: %s", colDesc.Type.Kind))
		}
		hidden := colDesc.Hidden
		cols = append(cols, ResultColumn{Name: colDesc.Name, Typ: typ, hidden: hidden})
	}
	return cols
}

// setNeededColumns sets the flags indicating which columns are needed by the upper layer.
func (n *scanNode) setNeededColumns(needed []bool) {
	if len(needed) != len(n.valNeededForCol) {
		panic(fmt.Sprintf("invalid setNeededColumns argument (len %d instead of %d): %v",
			len(needed), len(n.valNeededForCol), needed))
	}
	copy(n.valNeededForCol, needed)
}

// Initializes the column structures.
func (n *scanNode) initDescDefaults() {
	n.index = &n.desc.PrimaryIndex
	cols := n.desc.Columns
	n.resultColumns = makeResultColumns(cols)
	n.colIdxMap = make(map[ColumnID]int, len(cols))
	for i, c := range cols {
		n.colIdxMap[c.ID] = i
	}
	n.valNeededForCol = make([]bool, len(cols))
	for i := range cols {
		n.valNeededForCol[i] = true
	}
	n.row = make([]parser.Datum, len(cols))
	n.qvals = make([]scanQValue, len(cols))
	for i := range n.qvals {
		n.qvals[i] = n.makeQValue(i)
	}
}

// initScan initializes but does not perform the key-value scan.
func (n *scanNode) initScan() bool {
	// Initialize our key/values.
	if len(n.spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix.
		start := roachpb.Key(MakeIndexKeyPrefix(n.desc.ID, n.index.ID))
		n.spans = append(n.spans, span{
			start: start,
			end:   start.PrefixEnd(),
		})
	}

	if n.valTypes == nil {
		// Prepare our index key vals slice.
		var err error
		n.valTypes, err = makeKeyVals(&n.desc, n.columnIDs)
		n.pErr = roachpb.NewError(err)
		if n.pErr != nil {
			return false
		}
		n.vals = make([]parser.Datum, len(n.valTypes))

		if n.isSecondaryIndex && n.index.Unique {
			// Unique secondary indexes have a value that is the primary index
			// key. Prepare implicitVals for use in decoding this value.
			// Primary indexes only contain ascendingly-encoded values. If this
			// ever changes, we'll probably have to figure out the directions here too.
			var err error
			n.implicitValTypes, err = makeKeyVals(&n.desc, n.index.ImplicitColumnIDs)
			n.pErr = roachpb.NewError(err)
			if n.pErr != nil {
				return false
			}
			n.implicitVals = make([]parser.Datum, len(n.implicitValTypes))
		}
	}

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := n.limitHint
	if firstBatchLimit != 0 {
		// For a secondary index, we have one key per row.
		if !n.isSecondaryIndex {
			// We have a sentinel key per row plus at most one key per non-PK column. Of course, we
			// may have other keys due to a schema change, but this is only a hint.
			firstBatchLimit *= int64(1 + len(n.desc.Columns) - len(n.index.ColumnIDs))
		}
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	n.fetcher = makeKVFetcher(n.txn, n.spans, n.reverse, firstBatchLimit)
	n.scanInitialized = true
	return true
}

// initOrdering initializes the ordering info using the selected index. This
// must be called after index selection is performed.
func (n *scanNode) initOrdering(exactPrefix int) {
	if n.index == nil {
		return
	}
	n.columnIDs, n.columnDirs = n.index.fullColumnIDs()
	n.ordering = n.computeOrdering(n.index, exactPrefix, n.reverse)
}

// computeOrdering calculates ordering information for table columns assuming that:
//    - we scan a given index (potentially in reverse order), and
//    - the first `exactPrefix` columns of the index each have an exact (single value) match
//      (see orderingInfo).
func (n *scanNode) computeOrdering(index *IndexDescriptor, exactPrefix int, reverse bool) orderingInfo {
	var ordering orderingInfo

	columnIDs, dirs := index.fullColumnIDs()

	for i, colID := range columnIDs {
		idx, ok := n.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		if i < exactPrefix {
			ordering.addExactMatchColumn(idx)
		} else {
			dir := dirs[i]
			if reverse {
				dir = dir.Reverse()
			}
			ordering.addColumn(idx, dir)
		}
	}
	// We included any implicit columns, so the results are unique.
	ordering.unique = true
	return ordering
}

func (n *scanNode) readIndexKey(k roachpb.Key) ([]byte, error) {
	return decodeIndexKey(&n.desc, n.index.ID, n.valTypes, n.vals, n.columnDirs, k)
}

func (n *scanNode) processKV(kv client.KeyValue) bool {
	if n.indexKey == nil {
		// Reset the row to nil; it will get filled in in with the column
		// values as we decode the key-value pairs for the row.
		for i := range n.row {
			n.row[i] = nil
		}
	}

	var remaining []byte
	var err error
	remaining, err = n.readIndexKey(kv.Key)
	n.pErr = roachpb.NewError(err)
	if n.pErr != nil {
		return false
	}

	if n.indexKey == nil {
		n.indexKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		// This is the first key for the row, initialize the column values that are
		// part of the index key.
		for i, id := range n.columnIDs {
			if idx, ok := n.colIdxMap[id]; ok {
				n.row[idx] = n.vals[i]
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
		if idx, ok := n.colIdxMap[n.colID]; ok && n.valNeededForCol[idx] {
			value, ok = n.unmarshalValue(kv)
			if !ok {
				return false
			}
			if n.row[idx] != nil {
				panic(fmt.Sprintf("duplicate value for column %d", idx))
			}
			n.row[idx] = value
			if log.V(2) {
				log.Infof("Scan %s -> %v", kv.Key, value)
			}
		} else {
			// No need to unmarshal the column value. Either the column was part of
			// the index key or it isn't needed.
			if log.V(2) {
				log.Infof("Scan %s -> [%d] (skipped)", kv.Key, n.colID)
			}
		}
	} else {
		if n.implicitVals != nil {
			var err error
			_, err = decodeKeyVals(n.implicitValTypes, n.implicitVals, nil, kv.ValueBytes())
			n.pErr = roachpb.NewError(err)
			if n.pErr != nil {
				return false
			}
			for i, id := range n.index.ImplicitColumnIDs {
				if idx, ok := n.colIdxMap[id]; ok && n.valNeededForCol[idx] {
					n.row[idx] = n.implicitVals[i]
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
		(n.isSecondaryIndex || n.kvEnd ||
			!bytes.HasPrefix(n.kv.Key, n.indexKey)) {
		// The current key belongs to a new row. Output the current row.
		n.indexKey = nil

		// Fill in any missing values with NULLs
		for i, col := range n.desc.Columns {
			if n.valNeededForCol[i] && n.row[i] == nil {
				if !col.Nullable {
					panic("Non-nullable column with no value!")
				}
				n.row[i] = parser.DNull
			}
		}

		// Run the filter.
		passesFilter, err := runFilter(n.filter, n.planner.evalCtx)
		if err != nil {
			n.pErr = roachpb.NewError(err)
			return true
		}
		if n.explainValue != nil {
			n.explainDebug(true, passesFilter)
			return true
		}
		return passesFilter
	} else if n.explainValue != nil {
		n.explainDebug(false, false)
		return true
	}
	return false
}

// explainDebug fills in n.debugVals.
func (n *scanNode) explainDebug(endOfRow bool, passesFilter bool) {
	n.debugVals.rowIdx = n.rowIndex
	n.debugVals.key = n.prettyKey()

	if n.implicitVals != nil {
		n.debugVals.value = prettyDatums(n.implicitVals)
	} else {
		// We convert any datum to string, which will eventually be returned as a DString.
		// This conversion to DString is odd. `n.explainValue` is already a `Datum`, but logic_test
		// currently expects EXPLAIN DEBUG output to come out formatted using `encodeSQLString`.
		// This is not consistent across all printing of strings in logic_test, though.
		// TODO(tamird/pmattis): figure out a consistent story for string
		// printing in logic_test.
		n.debugVals.value = n.explainValue.String()
	}
	if endOfRow {
		if passesFilter {
			n.debugVals.output = debugValueRow
		} else {
			n.debugVals.output = debugValueFiltered
		}
		n.rowIndex++
	} else {
		n.debugVals.output = debugValuePartial
	}
	n.explainValue = nil
}

func (n *scanNode) prettyKey() string {
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
	idx, ok := n.colIdxMap[n.colID]
	if !ok {
		n.pErr = roachpb.NewUErrorf("column-id \"%d\" does not exist", n.colID)
		return nil, false
	}
	kind := n.desc.Columns[idx].Type.Kind
	d, err := unmarshalColumnValue(kind, kv.Value)
	n.pErr = roachpb.NewError(err)
	return d, n.pErr == nil
}

// scanQValue implements the parser.VariableExpr interface and is used as a replacement node for
// QualifiedNames in expressions that can change their values for each row.
//
// It is analogous to qvalue but allows expressions to be evaluated in the context of a scanNode.
type scanQValue struct {
	scan   *scanNode
	colIdx int
}

var _ parser.VariableExpr = &scanQValue{}

func (*scanQValue) Variable() {}

func (q *scanQValue) String() string {
	return string(q.scan.resultColumns[q.colIdx].Name)
}

func (q *scanQValue) Walk(_ parser.Visitor) parser.Expr {
	panic("not implemented")
}

func (q *scanQValue) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return q.scan.resultColumns[q.colIdx].Typ.TypeCheck(args)
}

func (q *scanQValue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return q.scan.row[q.colIdx].Eval(ctx)
}

func (n *scanNode) makeQValue(colIdx int) scanQValue {
	if colIdx < 0 || colIdx >= len(n.row) {
		panic(fmt.Sprintf("invalid colIdx %d (columns: %d)", colIdx, len(n.row)))
	}
	return scanQValue{n, colIdx}
}

func (n *scanNode) getQValue(colIdx int) *scanQValue {
	return &n.qvals[colIdx]
}
