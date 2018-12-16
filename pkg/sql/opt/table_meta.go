// Copyright 2018 The Cockroach Authors.
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

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util"
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
// in the table. This is equivalent to calling:
//
//   md.TableMeta(t).ColumnMeta(ord).MetaID
//
// NOTE: This method cannot do bounds checking, so it's up to the caller to
//       ensure that a column really does exist at this ordinal position.
func (t TableID) ColumnID(ord int) ColumnID {
	return t.firstColID() + ColumnID(ord)
}

// ColumnOrdinal returns the ordinal position of the given column in its base
// table. This is equivalent to calling:
//
//   md.ColumnMeta(id).Ordinal()
//
// NOTE: This method cannot do complete bounds checking, so it's up to the
//       caller to ensure that this column is really in the given base table.
func (t TableID) ColumnOrdinal(id ColumnID) int {
	if util.RaceEnabled {
		if id < t.firstColID() {
			panic("ordinal cannot be negative")
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

	// Alias is an alternate name for the base table to be used for formatting,
	// debugging, EXPLAIN output, etc. It is set to "" if no alias was specified.
	Alias string

	// anns annotates the table metadata with arbitrary data.
	anns [maxTableAnnIDCount]interface{}
}

// Name returns the table alias, if it was specified, or else the table's name
// as a string.
func (tm *TableMeta) Name() string {
	if tm.Alias != "" {
		return tm.Alias
	}
	return string(tm.Table.Name().TableName)
}

// IndexColumns returns the metadata IDs for the set of columns in the given
// index.
// TODO(justin): cache this value in the table metadata.
func (tm *TableMeta) IndexColumns(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(int(tm.MetaID.ColumnID(ord)))
	}
	return indexCols
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
		panic("can't allocate table annotation id; increase maxTableAnnIDCount to allow")
	}
	cnt := tableAnnIDCount
	tableAnnIDCount++
	return cnt
}
