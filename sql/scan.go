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
	planner *planner
	txn     *client.Txn
	desc    *TableDescriptor
	index   *IndexDescriptor

	// visibleCols are normally a copy of desc.Columns (even when we are using an index). The only
	// exception is when we are selecting from a specific index (SELECT * from t@abc), in which case
	// it contains only the columns in the index.
	visibleCols []ColumnDescriptor
	// There is a 1-1 correspondence between visibleCols and resultColumns.
	resultColumns []ResultColumn
	// row contains values for the current row. There is a 1-1 correspondence between resultColumns and vals.
	row parser.DTuple
	// for each column in resultColumns, indicates if the value is needed (used as an optimization
	// when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in visibleCols.
	colIdxMap map[ColumnID]int

	spans            []span
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
	explain          explainMode
	explainValue     parser.Datum
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

// Initializes a scanNode with a tableName. Returns the table or index name that can be used for
// fully-qualified columns if an alias is not specified.
func (n *scanNode) initTable(p *planner, tableName *parser.QualifiedName) (string, *roachpb.Error) {
	if n.desc, n.pErr = p.getTableLease(tableName); n.pErr != nil {
		return "", n.pErr
	}

	if pErr := p.checkPrivilege(n.desc, privilege.SELECT); pErr != nil {
		return "", pErr
	}

	alias := n.desc.Name

	indexName := tableName.Index()
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
			return "", n.pErr
		}
		// Use the index name instead of the table name for fully-qualified columns in the
		// expression.
		alias = n.index.Name
		// Strip out any columns from the table that are not present in the
		// index.
		visibleCols := make([]ColumnDescriptor, 0, len(n.index.ColumnIDs)+len(n.index.ImplicitColumnIDs))
		for _, colID := range n.index.ColumnIDs {
			var col *ColumnDescriptor
			if col, n.pErr = n.desc.FindColumnByID(colID); n.pErr != nil {
				return "", n.pErr
			}
			visibleCols = append(visibleCols, *col)
		}
		for _, colID := range n.index.ImplicitColumnIDs {
			var col *ColumnDescriptor
			if col, n.pErr = n.desc.FindColumnByID(colID); n.pErr != nil {
				return "", n.pErr
			}
			visibleCols = append(visibleCols, *col)
		}
		n.isSecondaryIndex = true
		n.initVisibleCols(visibleCols)
	} else {
		n.initDescDefaults()
	}

	return alias, nil
}

func (n *scanNode) initDescDefaults() {
	n.index = &n.desc.PrimaryIndex
	n.initVisibleCols(n.desc.Columns)
}

// isColumnHidden returns true if the column with the given index shouldn't be be resolved by
// a star reference (e.g. SELECT * FROM t)
func (n *scanNode) isColumnHidden(idx int) bool {
	if idx < 0 || idx >= len(n.visibleCols) {
		panic(fmt.Sprintf("Invalid column index %d", idx))
	}
	if n.isSecondaryIndex {
		// When selecting from a specific index, we should hide the ImplicitColumnIDs
		return idx >= len(n.index.ColumnIDs)
	}
	return n.visibleCols[idx].Hidden
}

// makeResultColumns converts ColumnDescriptors to ResultColumns
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
		cols = append(cols, ResultColumn{Name: colDesc.Name, Typ: typ})
	}
	return cols
}

// Initializes the visibleCols and associated structures (resultColumns, colIdxMap).
func (n *scanNode) initVisibleCols(visibleCols []ColumnDescriptor) {
	n.visibleCols = visibleCols
	n.resultColumns = makeResultColumns(visibleCols)
	n.colIdxMap = make(map[ColumnID]int, len(visibleCols))
	for i, c := range visibleCols {
		n.colIdxMap[c.ID] = i
	}
	n.valNeededForCol = make([]bool, len(visibleCols))
	for i := range visibleCols {
		n.valNeededForCol[i] = true
	}
	n.row = make([]parser.Datum, len(visibleCols))
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
	}
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

// computeOrdering calculates ordering information for table columns (visibleCols) assuming that:
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
	return ordering
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
	remaining, n.pErr = decodeIndexKey(n.desc, n.index.ID, n.valTypes, n.vals, n.columnDirs, kv.Key)
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
			implicitDirs := make([]encoding.Direction, 0, len(n.index.ImplicitColumnIDs))
			for range n.index.ImplicitColumnIDs {
				implicitDirs = append(implicitDirs, encoding.Ascending)
			}
			if _, n.pErr = decodeKeyVals(
				n.implicitValTypes, n.implicitVals, implicitDirs, kv.ValueBytes()); n.pErr != nil {
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
		(n.isSecondaryIndex || n.kvIndex == len(n.kvs) ||
			!bytes.HasPrefix(n.kvs[n.kvIndex].Key, n.indexKey)) {
		// The current key belongs to a new row. Output the current row.
		n.indexKey = nil

		// Fill in any missing values with NULLs
		for i, col := range n.visibleCols {
			if n.valNeededForCol[i] && n.row[i] == nil {
				if !col.Nullable {
					panic("Non-nullable column with no value!")
				}
				n.row[i] = parser.DNull
			}
		}
		if n.explainValue != nil {
			n.explainDebug(true)
		}
		return true
	} else if n.explainValue != nil {
		n.explainDebug(false)
		return true
	}
	return false
}

// explainDebug fills in four extra debugging values in the current row:
//  - the row index,
//  - the key,
//  - a value string,
//  - a true bool if we are at the end of the row, or a NULL otherwise.
func (n *scanNode) explainDebug(endOfRow bool) {
	if len(n.row) == len(n.visibleCols) {
		n.row = append(n.row, nil, nil, nil, nil)
	}
	debugVals := n.row[len(n.row)-4:]

	debugVals[0] = parser.DInt(n.rowIndex)
	debugVals[1] = parser.DString(n.prettyKey())
	if n.implicitVals != nil {
		debugVals[2] = parser.DString(prettyDatums(n.implicitVals))
	} else {
		// This conversion to DString is odd. `n.explainValue` is already a
		// `Datum`, but logic_test currently expects EXPLAIN DEBUG output
		// to come out formatted using `encodeSQLString`. This is not
		// consistent across all printing of strings in logic_test, though.
		// TODO(tamird/pmattis): figure out a consistent story for string
		// printing in logic_test.
		debugVals[2] = parser.DString(n.explainValue.String())
	}
	if endOfRow {
		debugVals[3] = parser.DBool(true)
		n.rowIndex++
	} else {
		debugVals[3] = parser.DNull
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
	idx, ok := n.colIdxMap[n.colID]
	if !ok {
		n.pErr = roachpb.NewUErrorf("column-id \"%d\" does not exist", n.colID)
		return nil, false
	}
	kind := n.visibleCols[idx].Type.Kind
	var d parser.Datum
	d, n.pErr = unmarshalColumnValue(kind, kv.Value)
	return d, n.pErr == nil
}
