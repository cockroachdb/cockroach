// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// TableID uniquely identifies the usage of a table within the scope of a
// query. TableID 0 is reserved to mean "unknown table".
//
// Internally, the TableID consists of an index into the Metadata.tables slice,
// as well as the ColumnID of the first column in the table. Subsequent columns
// have sequential ids, relative to their ordinal position in the table.
//
// See the comment for Metadata for more details on identifiers.
type TableID uint64

const (
	tableIDMask = 0xffffffff
)

// ColumnID returns the metadata id of the column at the given ordinal position
// in the table.
//
// NOTE: This method cannot do bounds checking, so it's up to the caller to
//       ensure that a column really does exist at this ordinal position.
func (t TableID) ColumnID(ord int) ColumnID {
	return t.firstColID() + ColumnID(ord)
}

// ColumnOrdinal returns the ordinal position of the given column in its base
// table.
//
// NOTE: This method cannot do complete bounds checking, so it's up to the
//       caller to ensure that this column is really in the given base table.
func (t TableID) ColumnOrdinal(id ColumnID) int {
	if util.RaceEnabled {
		if id < t.firstColID() {
			panic(errors.AssertionFailedf("ordinal cannot be negative"))
		}
	}
	return int(id - t.firstColID())
}

// makeTableID constructs a new TableID from its component parts.
func makeTableID(index int, firstColID ColumnID) TableID {
	// Bias the table index by 1.
	return TableID((uint64(index+1) << 32) | uint64(firstColID))
}

// firstColID returns the ColumnID of the first column in the table.
func (t TableID) firstColID() ColumnID {
	return ColumnID(t & tableIDMask)
}

// index returns the index of the table in Metadata.tables. It's biased by 1, so
// that TableID 0 can be be reserved to mean "unknown table".
func (t TableID) index() int {
	return int((t>>32)&tableIDMask) - 1
}

// TableAnnID uniquely identifies an annotation on an instance of table
// metadata. A table annotation allows arbitrary values to be cached with table
// metadata, which can be used to avoid recalculating base table properties or
// other information each time it's needed.
//
// WARNING! When copying memo metadata (which happens when we use a cached
// memo), the annotations are cleared. Any code using a annotation must treat
// this as a best-effort cache and be prepared to repopulate the annotation as
// necessary.
//
// To create a TableAnnID, call NewTableAnnID during Go's program initialization
// phase. The returned TableAnnID never clashes with other annotations on the
// same table. Here is a usage example:
//
//   var myAnnID = NewTableAnnID()
//
//   md.SetTableAnnotation(TableID(1), myAnnID, "foo")
//   ann := md.TableAnnotation(TableID(1), myAnnID)
//
// Currently, the following annotations are in use:
//   - WeakKeys: weak keys derived from the base table
//   - Stats: statistics derived from the base table
//
// To add an additional annotation, increase the value of maxTableAnnIDCount and
// add a call to NewTableAnnID.
type TableAnnID int

// tableAnnIDCount counts the number of times NewTableAnnID is called.
var tableAnnIDCount TableAnnID

// maxTableAnnIDCount is the maximum number of times that NewTableAnnID can be
// called. Calling more than this number of times results in a panic. Having
// a maximum enables a static annotation array to be inlined into the metadata
// table struct.
const maxTableAnnIDCount = 2

// TableMeta stores information about one of the tables stored in the metadata.
type TableMeta struct {
	// MetaID is the identifier for this table that is unique within the query
	// metadata.
	MetaID TableID

	// Table is a reference to the table in the catalog.
	Table cat.Table

	// Alias stores the identifier used in the query to identify the table. This
	// might be explicitly qualified (e.g. <catalog>.<schema>.<table>), or not
	// (e.g. <table>). Or, it may be an alias used in the query, in which case it
	// is always an unqualified name.
	Alias tree.TableName

	// IgnoreForeignKeys is true if we should disable any rules that depend on the
	// consistency of outgoing foreign key references. Set by the
	// IGNORE_FOREIGN_KEYS table hint; useful for scrub queries meant to verify
	// the consistency of foreign keys.
	IgnoreForeignKeys bool

	// Constraints stores a *FiltersExpr containing filters that are known to
	// evaluate to true on the table data. This list is extracted from validated
	// check constraints; specifically, those check constraints that we can prove
	// never evaluate to NULL (as NULL is treated differently in check constraints
	// and filters).
	//
	// If nil, there are no check constraints.
	//
	// See comment above GenerateConstrainedScans for more detail.
	Constraints ScalarExpr

	// ComputedCols stores ScalarExprs for computed columns on the table, indexed
	// by ColumnID. These will be used as "known truths" about data when
	// constraining indexes. See comment above GenerateConstrainedScans for more
	// detail.
	//
	// Computed columns with non-immutable operators are omitted.
	ComputedCols map[ColumnID]ScalarExpr

	// PartialIndexPredicates is a map from an index ordinal on the table to
	// a ScalarExpr representing the predicate on the corresponding partial
	// index. If an index is not a partial index, it will not have an entry in
	// the map.
	PartialIndexPredicates map[cat.IndexOrdinal]ScalarExpr

	// anns annotates the table metadata with arbitrary data.
	anns [maxTableAnnIDCount]interface{}
}

// clearAnnotations resets all the table annotations; used when copying a
// Metadata.
func (tm *TableMeta) clearAnnotations() {
	for i := range tm.anns {
		tm.anns[i] = nil
	}
}

// IndexColumns returns the metadata IDs for the set of columns in the given
// index.
// TODO(justin): cache this value in the table metadata.
func (tm *TableMeta) IndexColumns(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.ColumnCount(); i < n; i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// IndexKeyColumns returns the metadata IDs for the set of strict key columns in
// the given index.
func (tm *TableMeta) IndexKeyColumns(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// SetConstraints sets the filters derived from check constraints; see
// TableMeta.Constraint. The argument must be a *FiltersExpr.
func (tm *TableMeta) SetConstraints(constraints ScalarExpr) {
	tm.Constraints = constraints
}

// AddComputedCol adds a computed column expression to the table's metadata.
func (tm *TableMeta) AddComputedCol(colID ColumnID, computedCol ScalarExpr) {
	if tm.ComputedCols == nil {
		tm.ComputedCols = make(map[ColumnID]ScalarExpr)
	}
	tm.ComputedCols[colID] = computedCol
}

// AddPartialIndexPredicate adds a partial index predicate to the table's
// metadata.
func (tm *TableMeta) AddPartialIndexPredicate(ord cat.IndexOrdinal, pred ScalarExpr) {
	if tm.PartialIndexPredicates == nil {
		tm.PartialIndexPredicates = make(map[cat.IndexOrdinal]ScalarExpr)
	}
	tm.PartialIndexPredicates[ord] = pred
}

// TableAnnotation returns the given annotation that is associated with the
// given table. If the table has no such annotation, TableAnnotation returns
// nil.
func (md *Metadata) TableAnnotation(tabID TableID, annID TableAnnID) interface{} {
	return md.tables[tabID.index()].anns[annID]
}

// SetTableAnnotation associates the given annotation with the given table. The
// annotation is associated by the given ID, which was allocated by calling
// NewTableAnnID. If an annotation with the ID already exists on the table, then
// it is overwritten.
//
// See the TableAnnID comment for more details and a usage example.
func (md *Metadata) SetTableAnnotation(tabID TableID, tabAnnID TableAnnID, ann interface{}) {
	md.tables[tabID.index()].anns[tabAnnID] = ann
}

// NewTableAnnID allocates a unique annotation identifier that is used to
// associate arbitrary data with table metadata. Only maxTableAnnIDCount total
// annotation ID's can exist in the system. Attempting to exceed the maximum
// results in a panic.
//
// This method is not thread-safe, and therefore should only be called during
// Go's program initialization phase (which uses a single goroutine to init
// variables).
//
// See the TableAnnID comment for more details and a usage example.
func NewTableAnnID() TableAnnID {
	if tableAnnIDCount == maxTableAnnIDCount {
		panic(errors.AssertionFailedf(
			"can't allocate table annotation id; increase maxTableAnnIDCount to allow"))
	}
	cnt := tableAnnIDCount
	tableAnnIDCount++
	return cnt
}
