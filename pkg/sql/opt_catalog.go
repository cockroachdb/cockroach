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

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// optCatalog implements the opt.Catalog interface over the SchemaResolver
// interface for the use of the new optimizer. The interfaces are simplified to
// only include what the optimizer needs, and certain common lookups are cached
// for faster performance.
type optCatalog struct {
	// resolver needs to be set via a call to init before calling other methods.
	resolver LogicalSchema

	statsCache *stats.TableStatisticsCache

	// dataSources is a cache of table and view objects that's used to satisfy
	// repeated calls for the same data source. The same underlying descriptor
	// will always return the same data source wrapper object.
	dataSources map[*sqlbase.ImmutableTableDescriptor]opt.DataSource
}

var _ opt.Catalog = &optCatalog{}

// init allows the caller to pre-allocate optCatalog.
func (oc *optCatalog) init(statsCache *stats.TableStatisticsCache, resolver LogicalSchema) {
	oc.resolver = resolver
	oc.statsCache = statsCache
	oc.dataSources = nil
}

// ResolveDataSource is part of the opt.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, name *tree.TableName,
) (opt.DataSource, error) {
	desc, err := ResolveExistingObject(ctx, oc.resolver, name, true /* required */, anyDescType)
	if err != nil {
		return nil, err
	}
	return oc.newDataSource(desc, name)
}

// ResolveDataSourceByID is part of the opt.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, dataSourceID int64,
) (opt.DataSource, error) {

	tableLookup, err := oc.resolver.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
			return nil, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: dataSourceID})
		}
		return nil, err
	}
	desc := tableLookup.Table

	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, oc.resolver.Txn(), desc.ParentID)
	if err != nil {
		return nil, err
	}

	name := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(desc.Name))
	return oc.newDataSource(desc, &name)
}

// CheckPrivilege is part of the opt.Catalog interface.
func (oc *optCatalog) CheckPrivilege(
	ctx context.Context, ds opt.DataSource, priv privilege.Kind,
) error {
	switch t := ds.(type) {
	case *optTable:
		return oc.resolver.CheckPrivilege(ctx, t.desc, priv)
	case *optView:
		return oc.resolver.CheckPrivilege(ctx, t.desc, priv)
	case *optSequence:
		return oc.resolver.CheckPrivilege(ctx, t.desc, priv)
	default:
		panic("invalid DataSource")
	}
}

// newDataSource returns a data source wrapper for the given table descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) newDataSource(
	desc *sqlbase.ImmutableTableDescriptor, name *tree.TableName,
) (opt.DataSource, error) {
	// Check to see if there's already a data source wrapper for this descriptor.
	if oc.dataSources == nil {
		oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]opt.DataSource)
	} else {
		if ds, ok := oc.dataSources[desc]; ok {
			return ds, nil
		}
	}

	// Create wrapper for the data source now.
	var ds opt.DataSource
	switch {
	case desc.IsTable():
		stats, err := oc.statsCache.GetTableStats(context.TODO(), desc.ID)
		if err != nil {
			// Ignore any error. We still want to be able to run queries even if we lose
			// access to the statistics table.
			// TODO(radu): at least log the error.
			stats = nil
		}
		ds = newOptTable(desc, name, stats)

	case desc.IsView():
		ds = newOptView(desc, name)

	case desc.IsSequence():
		ds = newOptSequence(desc, name)

	default:
		panic(fmt.Sprintf("unexpected table descriptor: %+v", desc))
	}

	oc.dataSources[desc] = ds
	return ds, nil
}

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements the
// opt.DataSource and opt.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of
	// the view.
	name tree.TableName
}

var _ opt.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor, name *tree.TableName) *optView {
	ov := &optView{desc: desc, name: *name}

	// The opt.View interface requires that view names be fully qualified.
	ov.name.ExplicitSchema = true
	ov.name.ExplicitCatalog = true

	return ov
}

// Fingerprint is part of the opt.DataSource interface.
func (ov *optView) Fingerprint() opt.Fingerprint {
	return opt.Fingerprint(ov.desc.ID)<<32 | opt.Fingerprint(ov.desc.Version)
}

// Name is part of the opt.View interface.
func (ov *optView) Name() *tree.TableName {
	return &ov.name
}

// Query is part of the opt.View interface.
func (ov *optView) Query() string {
	return ov.desc.ViewQuery
}

// ColumnNameCount is part of the opt.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.Columns)
}

// ColumnName is part of the opt.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return tree.Name(ov.desc.Columns[i].Name)
}

// optSequence is a wrapper around sqlbase.ImmutableTableDescriptor that implements the
// opt.DataSource interface.
//
// TODO(andyk): This should implement opt.Sequence once we have it.
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// sequence.
	name tree.TableName
}

var _ opt.DataSource = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor, name *tree.TableName) *optSequence {
	ot := &optSequence{desc: desc, name: *name}

	// The opt.Sequence interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	return ot
}

// Fingerprint is part of the opt.DataSource interface.
func (os *optSequence) Fingerprint() opt.Fingerprint {
	return opt.Fingerprint(os.desc.ID)<<32 | opt.Fingerprint(os.desc.Version)
}

// Name is part of the opt.DataSource interface.
func (os *optSequence) Name() *tree.TableName {
	return &os.name
}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches index
// wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// table.
	name tree.TableName

	// primary is the inlined wrapper for the table's primary index.
	primary optIndex

	// stats is nil until StatisticCount is called. After that it will not be nil,
	// even when there are no statistics.
	stats []optTableStat

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int

	// wrappers is a cache of index wrappers that's used to satisfy repeated
	// calls to the SecondaryIndex method for the same index.
	wrappers map[*sqlbase.IndexDescriptor]*optIndex
}

var _ opt.Table = &optTable{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor, name *tree.TableName, stats []*stats.TableStatistic,
) *optTable {
	ot := &optTable{desc: desc, name: *name}
	if stats != nil {
		ot.stats = make([]optTableStat, len(stats))
		n := 0
		for i := range stats {
			// We skip any stats that have columns that don't exist in the table anymore.
			if ot.stats[n].init(ot, stats[i]) {
				n++
			}
		}
		ot.stats = ot.stats[:n]
	}

	// The opt.Table interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	ot.primary.init(ot, &desc.PrimaryIndex)

	return ot
}

// Fingerprint is part of the opt.DataSource interface.
func (ot *optTable) Fingerprint() opt.Fingerprint {
	return opt.Fingerprint(ot.desc.ID)<<32 | opt.Fingerprint(ot.desc.Version)
}

// Name is part of the opt.DataSource interface.
func (ot *optTable) Name() *tree.TableName {
	return &ot.name
}

// InternalID is part of the opt.Table interface.
func (ot *optTable) InternalID() uint64 {
	return uint64(ot.desc.ID)
}

// IsVirtualTable is part of the opt.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// ColumnCount is part of the opt.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// Column is part of the opt.Table interface.
func (ot *optTable) Column(i int) opt.Column {
	return &ot.desc.Columns[i]
}

// IndexCount is part of the opt.Table interface.
func (ot *optTable) IndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// Index is part of the opt.Table interface.
func (ot *optTable) Index(i int) opt.Index {
	// Primary index is always 0th index.
	if i == opt.PrimaryIndex {
		return &ot.primary
	}

	// Bias i to account for lack of primary index in Indexes slice.
	desc := &ot.desc.Indexes[i-1]

	// Check to see if there's already a wrapper for this index descriptor.
	if ot.wrappers == nil {
		ot.wrappers = make(map[*sqlbase.IndexDescriptor]*optIndex, len(ot.desc.Indexes))
	}
	wrapper, ok := ot.wrappers[desc]
	if !ok {
		wrapper = newOptIndex(ot, desc)
		ot.wrappers[desc] = wrapper
	}
	return wrapper
}

// StatisticCount is part of the opt.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the opt.Table interface.
func (ot *optTable) Statistic(i int) opt.TableStatistic {
	return &ot.stats[i]
}

func (ot *optTable) ensureColMap() {
	if ot.colMap == nil {
		ot.colMap = make(map[sqlbase.ColumnID]int, len(ot.desc.Columns))
		for i := range ot.desc.Columns {
			ot.colMap[ot.desc.Columns[i].ID] = i
		}
	}
}

// LookupColumnOrdinal is part of the opt.Table interface
func (ot *optTable) LookupColumnOrdinal(colID uint32) (int, error) {
	// LookupColumnOrdinal exposes optTable.lookupColumnOrdinal.
	// In order to preserve the argument type information
	// on lookupColumnOrdinal, this wrapper function exists.
	// colID as the sqlbase.ColumnID type would result in a
	// circular dependency for catalog.Table - the interface this
	// implements.
	return ot.lookupColumnOrdinal(sqlbase.ColumnID(colID))
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
	ot.ensureColMap()
	col, ok := ot.colMap[colID]
	if ok {
		return col, nil
	}
	return col, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
		"column [%d] does not exist", colID)
}

// optIndex is a wrapper around sqlbase.IndexDescriptor that caches some
// commonly accessed information and keeps a reference to the table wrapper.
type optIndex struct {
	tab  *optTable
	desc *sqlbase.IndexDescriptor
	// storedCols is the set of non-PK columns if this is the primary index,
	// otherwise it is desc.StoreColumnIDs.
	storedCols []sqlbase.ColumnID

	numCols       int
	numKeyCols    int
	numLaxKeyCols int

	// foreignKey stores IDs of another table and one of its indexes,
	// if this index is part of an outbound foreign key relation.
	foreignKey opt.ForeignKeyReference
}

var _ opt.Index = &optIndex{}

func newOptIndex(tab *optTable, desc *sqlbase.IndexDescriptor) *optIndex {
	oi := &optIndex{}
	oi.init(tab, desc)
	return oi
}

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(tab *optTable, desc *sqlbase.IndexDescriptor) {
	oi.tab = tab
	oi.desc = desc
	if desc == &tab.desc.PrimaryIndex {
		oi.storedCols = make([]sqlbase.ColumnID, 0, len(tab.desc.Columns)-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i := range tab.desc.Columns {
			id := tab.desc.Columns[i].ID
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, id)
			}
		}
		oi.numCols = len(tab.desc.Columns)
	} else {
		oi.storedCols = desc.StoreColumnIDs
		oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)
	}

	if desc.Unique {
		notNull := true
		for _, id := range desc.ColumnIDs {
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.desc.Columns[ord].Nullable {
				notNull = false
				break
			}
		}

		if notNull {
			// Unique index with no null columns: columns from index are sufficient
			// to form a key without needing extra primary key columns. There is no
			// separate lax key.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols
		} else {
			// Unique index with at least one nullable column: extra primary key
			// columns will be added to the row key when one of the unique index
			// columns has a NULL value.
			oi.numLaxKeyCols = len(desc.ColumnIDs)
			oi.numKeyCols = oi.numLaxKeyCols + len(desc.ExtraColumnIDs)
		}
	} else {
		// Non-unique index: extra primary key columns are always added to the row
		// key. There is no separate lax key.
		oi.numLaxKeyCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs)
		oi.numKeyCols = oi.numLaxKeyCols
	}

	if desc.ForeignKey.IsSet() {
		oi.foreignKey.TableID = uint64(desc.ForeignKey.Table)
		oi.foreignKey.IndexID = uint64(desc.ForeignKey.Index)
		oi.foreignKey.PrefixLen = desc.ForeignKey.SharedPrefixLen
	}
}

// IdxName is part of the opt.Index interface.
func (oi *optIndex) IdxName() string {
	return oi.desc.Name
}

// InternalID is part of the opt.Index interface.
func (oi *optIndex) InternalID() uint64 {
	return uint64(oi.desc.ID)
}

// IsInverted is part of the opt.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.desc.Type == sqlbase.IndexDescriptor_INVERTED
}

// ColumnCount is part of the opt.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the opt.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the opt.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// Column is part of the opt.Index interface.
func (oi *optIndex) Column(i int) opt.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return opt.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return opt.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return opt.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// ForeignKey is part of the opt.Index interface.
func (oi *optIndex) ForeignKey() (opt.ForeignKeyReference, bool) {
	desc := oi.desc
	if desc.ForeignKey.IsSet() {
		oi.foreignKey.TableID = uint64(desc.ForeignKey.Table)
		oi.foreignKey.IndexID = uint64(desc.ForeignKey.Index)
		oi.foreignKey.PrefixLen = desc.ForeignKey.SharedPrefixLen
	}
	return oi.foreignKey, oi.desc.ForeignKey.IsSet()
}

// Table is part of the opt.Index interface.
func (oi *optIndex) Table() opt.Table {
	return oi.tab
}

type optTableStat struct {
	createdAt      time.Time
	columnOrdinals []int
	rowCount       uint64
	distinctCount  uint64
	nullCount      uint64
}

var _ opt.TableStatistic = &optTableStat{}

func (os *optTableStat) init(tab *optTable, stat *stats.TableStatistic) (ok bool) {
	os.createdAt = stat.CreatedAt
	os.rowCount = stat.RowCount
	os.distinctCount = stat.DistinctCount
	os.nullCount = stat.NullCount
	os.columnOrdinals = make([]int, len(stat.ColumnIDs))
	tab.ensureColMap()
	for i, c := range stat.ColumnIDs {
		var ok bool
		os.columnOrdinals[i], ok = tab.colMap[c]
		if !ok {
			// Column not in table (this is possible if the column was removed since
			// the statistic was calculated).
			return false
		}
	}
	return true
}

// CreatedAt is part of the opt.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.createdAt
}

// ColumnCount is part of the opt.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the opt.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the opt.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.rowCount
}

// DistinctCount is part of the opt.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.distinctCount
}

// NullCount is part of the opt.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.nullCount
}
