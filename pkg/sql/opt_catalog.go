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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// optCatalog implements the cat.Catalog interface over the SchemaResolver
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
	dataSources map[*sqlbase.ImmutableTableDescriptor]cat.DataSource

	// tn is a temporary name used during resolution to avoid heap allocation.
	tn tree.TableName
}

var _ cat.Catalog = &optCatalog{}

// init allows the caller to pre-allocate optCatalog.
func (oc *optCatalog) init(statsCache *stats.TableStatisticsCache, resolver LogicalSchema) {
	oc.resolver = resolver
	oc.statsCache = statsCache
	oc.dataSources = nil
}

// optSchema is a wrapper around sqlbase.DatabaseDescriptor that implements the
// cat.Object and cat.Schema interfaces.
type optSchema struct {
	desc *sqlbase.DatabaseDescriptor

	name cat.SchemaName
}

// ID is part of the cat.Object interface.
func (os *optSchema) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(ctx context.Context, name *cat.SchemaName) (cat.Schema, error) {
	p := oc.resolver.(*planner)
	defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
	p.avoidCachedDescriptors = true

	// ResolveTargetObject wraps ResolveTarget in order to raise "schema not
	// found" and "schema cannot be modified" errors. However, ResolveTargetObject
	// assumes that a data source object is being resolved, which is not the case
	// for ResolveSchema. Therefore, call ResolveTarget directly and produce a
	// more general error.
	oc.tn.TableNamePrefix = *name
	found, desc, err := oc.tn.ResolveTarget(
		ctx,
		oc.resolver,
		oc.resolver.CurrentDatabase(),
		oc.resolver.CurrentSearchPath(),
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidSchemaNameError,
			"target database or schema does not exist")
	}
	*name = oc.tn.TableNamePrefix
	return &optSchema{desc: desc.(*DatabaseDescriptor)}, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, name *cat.DataSourceName,
) (cat.DataSource, error) {
	desc, err := ResolveExistingObject(ctx, oc.resolver, name, true /* required */, anyDescType)
	if err != nil {
		return nil, err
	}
	return oc.newDataSource(desc, name)
}

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSourceByID(
	ctx context.Context, dataSourceID cat.StableID,
) (cat.DataSource, error) {
	tableLookup, err := oc.resolver.LookupTableByID(ctx, sqlbase.ID(dataSourceID))

	if err != nil || tableLookup.IsAdding {
		if err == sqlbase.ErrDescriptorNotFound || tableLookup.IsAdding {
			return nil, sqlbase.NewUndefinedRelationError(&tree.TableRef{TableID: int64(dataSourceID)})
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

// CheckPrivilege is part of the cat.Catalog interface.
func (oc *optCatalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	switch t := o.(type) {
	case *optSchema:
		return oc.resolver.CheckPrivilege(ctx, t.desc, priv)
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
	desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName,
) (cat.DataSource, error) {
	// Check to see if there's already a data source wrapper for this descriptor.
	if oc.dataSources == nil {
		oc.dataSources = make(map[*sqlbase.ImmutableTableDescriptor]cat.DataSource)
	} else {
		if ds, ok := oc.dataSources[desc]; ok {
			return ds, nil
		}
	}

	// Create wrapper for the data source now.
	var ds cat.DataSource
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

// optView is a wrapper around sqlbase.ImmutableTableDescriptor that implements
// the cat.Object, cat.DataSource, and cat.View interfaces.
type optView struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of
	// the view.
	name cat.DataSourceName
}

var _ cat.View = &optView{}

func newOptView(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optView {
	ov := &optView{desc: desc, name: *name}

	// The cat.View interface requires that view names be fully qualified.
	ov.name.ExplicitSchema = true
	ov.name.ExplicitCatalog = true

	return ov
}

// ID is part of the cat.Object interface.
func (ov *optView) ID() cat.StableID {
	return cat.StableID(ov.desc.ID)
}

// Version is part of the cat.DataSource interface.
func (ov *optView) Version() cat.Version {
	return cat.Version(ov.desc.Version)
}

// Name is part of the cat.View interface.
func (ov *optView) Name() *cat.DataSourceName {
	return &ov.name
}

// Query is part of the cat.View interface.
func (ov *optView) Query() string {
	return ov.desc.ViewQuery
}

// ColumnNameCount is part of the cat.View interface.
func (ov *optView) ColumnNameCount() int {
	return len(ov.desc.Columns)
}

// ColumnName is part of the cat.View interface.
func (ov *optView) ColumnName(i int) tree.Name {
	return tree.Name(ov.desc.Columns[i].Name)
}

// optSequence is a wrapper around sqlbase.ImmutableTableDescriptor that
// implements the cat.Object and cat.DataSource interfaces.
//
// TODO(andyk): This should implement cat.Sequence once we have it.
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// sequence.
	name cat.DataSourceName
}

var _ cat.DataSource = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optSequence {
	ot := &optSequence{desc: desc, name: *name}

	// The cat.Sequence interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	return ot
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Version is part of the cat.DataSource interface.
func (os *optSequence) Version() cat.Version {
	return cat.Version(os.desc.Version)
}

// Name is part of the cat.DataSource interface.
func (os *optSequence) Name() *cat.DataSourceName {
	return &os.name
}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// table.
	name cat.DataSourceName

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

	// mutations is a list of mutation columns associated with this table. These
	// are present when the table is undergoing an online schema change where one
	// or more columns are being added or dropped.
	mutations []cat.MutationColumn
}

var _ cat.Table = &optTable{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName, stats []*stats.TableStatistic,
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

	ot.prepareMutationColumns(desc)

	// The cat.Table interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	ot.primary.init(ot, &desc.PrimaryIndex)

	return ot
}

func (ot *optTable) prepareMutationColumns(desc *sqlbase.ImmutableTableDescriptor) {
	if len(desc.MutationColumns()) != 0 {
		ot.mutations = make([]cat.MutationColumn, 0, len(ot.desc.MutationColumns()))
		writeCols := ot.desc.WriteOnlyColumns()
		delCols := ot.desc.DeleteOnlyColumns()
		for i := range writeCols {
			ot.mutations = append(ot.mutations, cat.MutationColumn{
				Column:       &writeCols[i],
				IsDeleteOnly: false,
			})
		}
		for i := range delCols {
			ot.mutations = append(ot.mutations, cat.MutationColumn{
				Column:       &delCols[i],
				IsDeleteOnly: true,
			})
		}
	}
}

// ID is part of the cat.Object interface.
func (ot *optTable) ID() cat.StableID {
	return cat.StableID(ot.desc.ID)
}

// Version is part of the cat.DataSource interface.
func (ot *optTable) Version() cat.Version {
	return cat.Version(ot.desc.Version)
}

// Name is part of the cat.DataSource interface.
func (ot *optTable) Name() *cat.DataSourceName {
	return &ot.name
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns) + len(ot.mutations)
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) cat.Column {
	if i < len(ot.desc.Columns) {
		return &ot.desc.Columns[i]
	}
	return &ot.mutations[i-len(ot.desc.Columns)]
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i int) cat.Index {
	// Primary index is always 0th index.
	if i == cat.PrimaryIndex {
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

// StatisticCount is part of the cat.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the cat.Table interface.
func (ot *optTable) Statistic(i int) cat.TableStatistic {
	return &ot.stats[i]
}

func (ot *optTable) ensureColMap() {
	if ot.colMap == nil {
		ot.colMap = make(map[sqlbase.ColumnID]int, ot.ColumnCount())
		for i, n := 0, ot.ColumnCount(); i < n; i++ {
			ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
		}
	}
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
	foreignKey cat.ForeignKeyReference
}

var _ cat.Index = &optIndex{}

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
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]sqlbase.ColumnID, 0, tab.ColumnCount()-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i, n := 0, tab.ColumnCount(); i < n; i++ {
			id := tab.Column(i).ColID()
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, sqlbase.ColumnID(id))
			}
		}
		oi.numCols = tab.ColumnCount()
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
		oi.foreignKey.TableID = cat.StableID(desc.ForeignKey.Table)
		oi.foreignKey.IndexID = cat.StableID(desc.ForeignKey.Index)
		oi.foreignKey.PrefixLen = desc.ForeignKey.SharedPrefixLen
	}
}

// ID is part of the cat.Index interface.
func (oi *optIndex) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Index interface.
func (oi *optIndex) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// IsUnique is part of the cat.Index interface.
func (oi *optIndex) IsUnique() bool {
	return oi.desc.Unique
}

// IsInverted is part of the cat.Index interface.
func (oi *optIndex) IsInverted() bool {
	return oi.desc.Type == sqlbase.IndexDescriptor_INVERTED
}

// ColumnCount is part of the cat.Index interface.
func (oi *optIndex) ColumnCount() int {
	return oi.numCols
}

// KeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) KeyColumnCount() int {
	return oi.numKeyCols
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (oi *optIndex) LaxKeyColumnCount() int {
	return oi.numLaxKeyCols
}

// Column is part of the cat.Index interface.
func (oi *optIndex) Column(i int) cat.IndexColumn {
	length := len(oi.desc.ColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
		return cat.IndexColumn{
			Column:     oi.tab.Column(ord),
			Ordinal:    ord,
			Descending: oi.desc.ColumnDirections[i] == sqlbase.IndexDescriptor_DESC,
		}
	}

	i -= length
	length = len(oi.desc.ExtraColumnIDs)
	if i < length {
		ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ExtraColumnIDs[i])
		return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
	}

	i -= length
	ord, _ := oi.tab.lookupColumnOrdinal(oi.storedCols[i])
	return cat.IndexColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// ForeignKey is part of the cat.Index interface.
func (oi *optIndex) ForeignKey() (cat.ForeignKeyReference, bool) {
	desc := oi.desc
	if desc.ForeignKey.IsSet() {
		oi.foreignKey.TableID = cat.StableID(desc.ForeignKey.Table)
		oi.foreignKey.IndexID = cat.StableID(desc.ForeignKey.Index)
		oi.foreignKey.PrefixLen = desc.ForeignKey.SharedPrefixLen
		oi.foreignKey.Match = sqlbase.ForeignKeyReferenceMatchValue[desc.ForeignKey.Match]
	}
	return oi.foreignKey, oi.desc.ForeignKey.IsSet()
}

// Table is part of the cat.Index interface.
func (oi *optIndex) Table() cat.Table {
	return oi.tab
}

type optTableStat struct {
	createdAt      time.Time
	columnOrdinals []int
	rowCount       uint64
	distinctCount  uint64
	nullCount      uint64
}

var _ cat.TableStatistic = &optTableStat{}

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

// CreatedAt is part of the cat.TableStatistic interface.
func (os *optTableStat) CreatedAt() time.Time {
	return os.createdAt
}

// ColumnCount is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnCount() int {
	return len(os.columnOrdinals)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (os *optTableStat) ColumnOrdinal(i int) int {
	return os.columnOrdinals[i]
}

// RowCount is part of the cat.TableStatistic interface.
func (os *optTableStat) RowCount() uint64 {
	return os.rowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (os *optTableStat) DistinctCount() uint64 {
	return os.distinctCount
}

// NullCount is part of the cat.TableStatistic interface.
func (os *optTableStat) NullCount() uint64 {
	return os.nullCount
}
