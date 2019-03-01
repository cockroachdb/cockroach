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
	"math"
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

// Equals is part of the cat.Object interface.
func (os *optSchema) Equals(other cat.Object) bool {
	otherSchema, ok := other.(*optSchema)
	return ok && os.desc.ID == otherSchema.desc.ID
}

// Name is part of the cat.Schema interface.
func (os *optSchema) Name() *cat.SchemaName {
	return &os.name
}

// ResolveSchema is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveSchema(
	ctx context.Context, name *cat.SchemaName,
) (cat.Schema, cat.SchemaName, error) {
	p := oc.resolver.(*planner)
	defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
	p.avoidCachedDescriptors = true

	// ResolveTargetObject wraps ResolveTarget in order to raise "schema not
	// found" and "schema cannot be modified" errors. However, ResolveTargetObject
	// assumes that a data source object is being resolved, which is not the case
	// for ResolveSchema. Therefore, call ResolveTarget directly and produce a
	// more general error.
	oc.tn.TableName = ""
	oc.tn.TableNamePrefix = *name
	found, desc, err := oc.tn.ResolveTarget(
		ctx,
		oc.resolver,
		oc.resolver.CurrentDatabase(),
		oc.resolver.CurrentSearchPath(),
	)
	if err != nil {
		return nil, cat.SchemaName{}, err
	}
	if !found {
		return nil, cat.SchemaName{}, pgerror.NewErrorf(pgerror.CodeInvalidSchemaNameError,
			"target database or schema does not exist")
	}
	return &optSchema{desc: desc.(*DatabaseDescriptor)}, oc.tn.TableNamePrefix, nil
}

// ResolveDataSource is part of the cat.Catalog interface.
func (oc *optCatalog) ResolveDataSource(
	ctx context.Context, name *cat.DataSourceName,
) (cat.DataSource, cat.DataSourceName, error) {
	oc.tn = *name
	desc, err := ResolveExistingObject(ctx, oc.resolver, &oc.tn, true /* required */, anyDescType)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	ds, err := oc.newDataSource(ctx, desc, &oc.tn)
	if err != nil {
		return nil, cat.DataSourceName{}, err
	}
	return ds, oc.tn, nil
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
	desc := tableLookup.Desc

	dbDesc, err := sqlbase.GetDatabaseDescFromID(ctx, oc.resolver.Txn(), desc.ParentID)
	if err != nil {
		return nil, err
	}

	name := tree.MakeTableName(tree.Name(dbDesc.Name), tree.Name(desc.Name))
	return oc.newDataSource(ctx, desc, &name)
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
		return pgerror.NewAssertionErrorf("invalid object type: %T", o)
	}
}

// newDataSource returns a data source wrapper for the given table descriptor.
// The wrapper might come from the cache, or it may be created now.
func (oc *optCatalog) newDataSource(
	ctx context.Context, desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName,
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
		id := cat.StableID(desc.ID)
		if desc.IsVirtualTable() {
			// A virtual table can effectively have multiple instances, with different
			// contents. For example `db1.pg_catalog.pg_sequence` contains info about
			// sequences in db1, whereas `db2.pg_catalog.pg_sequence` contains info
			// about sequences in db2.
			//
			// These instances should have different stable IDs. To achieve this, we
			// prepend the database ID.
			//
			// Note that some virtual tables have a special instance with empty catalog,
			// for example "".information_schema.tables contains info about tables in
			// all databases. We treat the empty catalog as having database ID 0.
			if name.Catalog() != "" {
				// TODO(radu): it's unfortunate that we have to lookup the schema again.
				_, dbDesc, err := oc.resolver.LookupSchema(ctx, name.Catalog(), name.Schema())
				if err != nil {
					return nil, err
				}
				if dbDesc == nil {
					// The database was not found. This can happen e.g. when
					// accessing a virtual schema over a non-existent
					// database. This is a common scenario when the current db
					// in the session points to a database that was not created
					// yet.
					//
					// In that case we use an invalid database ID. We
					// distinguish this from the empty database case because the
					// virtual tables do not "contain" the same information in
					// both cases.
					id |= cat.StableID(math.MaxUint32) << 32
				} else {
					id |= cat.StableID(dbDesc.(*DatabaseDescriptor).ID) << 32
				}
			}
		}

		stats, err := oc.statsCache.GetTableStats(context.TODO(), desc.ID)
		if err != nil {
			// Ignore any error. We still want to be able to run queries even if we lose
			// access to the statistics table.
			// TODO(radu): at least log the error.
			stats = nil
		}
		ds = newOptTable(desc, id, name, stats)

	case desc.IsView():
		ds = newOptView(desc, name)

	case desc.IsSequence():
		ds = newOptSequence(desc, name)

	default:
		return nil, pgerror.NewAssertionErrorf("unexpected table descriptor: %+v", desc)
	}

	if !desc.IsVirtualTable() {
		// Virtual tables can have multiple effective instances that utilize the
		// same descriptor (see above).
		oc.dataSources[desc] = ds
	}
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

// Equals is part of the cat.Object interface.
func (ov *optView) Equals(other cat.Object) bool {
	otherView, ok := other.(*optView)
	if !ok {
		return false
	}
	return ov.desc.ID == otherView.desc.ID && ov.desc.Version == otherView.desc.Version
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
type optSequence struct {
	desc *sqlbase.ImmutableTableDescriptor

	// name is the fully qualified, fully resolved, fully normalized name of the
	// sequence.
	name cat.DataSourceName
}

var _ cat.DataSource = &optSequence{}
var _ cat.Sequence = &optSequence{}

func newOptSequence(desc *sqlbase.ImmutableTableDescriptor, name *cat.DataSourceName) *optSequence {
	os := &optSequence{desc: desc, name: *name}

	// The cat.Sequence interface requires that table names be fully qualified.
	os.name.ExplicitSchema = true
	os.name.ExplicitCatalog = true

	return os
}

// ID is part of the cat.Object interface.
func (os *optSequence) ID() cat.StableID {
	return cat.StableID(os.desc.ID)
}

// Equals is part of the cat.Object interface.
func (os *optSequence) Equals(other cat.Object) bool {
	otherSeq, ok := other.(*optSequence)
	if !ok {
		return false
	}
	return os.desc.ID == otherSeq.desc.ID && os.desc.Version == otherSeq.desc.Version
}

// Name is part of the cat.DataSource interface.
func (os *optSequence) Name() *cat.DataSourceName {
	return &os.name
}

// SequenceName is part of the cat.Sequence interface.
func (os *optSequence) SequenceName() *tree.TableName {
	return os.Name()
}

// optTable is a wrapper around sqlbase.ImmutableTableDescriptor that caches
// index wrappers and maintains a ColumnID => Column mapping for fast lookup.
type optTable struct {
	desc *sqlbase.ImmutableTableDescriptor

	// This is the descriptor ID, except for virtual tables.
	id cat.StableID

	// name is the fully qualified, fully resolved, fully normalized name of the
	// table.
	name cat.DataSourceName

	// indexes are the inlined wrappers for the table's primary and secondary
	// indexes.
	indexes []optIndex

	// stats are the inlined wrappers for table statistics.
	stats []optTableStat

	// family is the inlined wrapper for the table's primary family. The primary
	// family is the first family explicitly specified by the user. If no families
	// were explicitly specified, then the primary family is synthesized.
	primaryFamily optFamily

	// families are the inlined wrappers for the table's non-primary families,
	// which are all the families specified by the user after the first. The
	// primary family is kept separate since the common case is that there's just
	// one family.
	families []optFamily

	// colMap is a mapping from unique ColumnID to column ordinal within the
	// table. This is a common lookup that needs to be fast.
	colMap map[sqlbase.ColumnID]int
}

var _ cat.Table = &optTable{}

func newOptTable(
	desc *sqlbase.ImmutableTableDescriptor,
	id cat.StableID,
	name *cat.DataSourceName,
	stats []*stats.TableStatistic,
) *optTable {
	ot := &optTable{desc: desc, id: id, name: *name}

	// The cat.Table interface requires that table names be fully qualified.
	ot.name.ExplicitSchema = true
	ot.name.ExplicitCatalog = true

	// Create the table's column mapping from sqlbase.ColumnID to column ordinal.
	ot.colMap = make(map[sqlbase.ColumnID]int, ot.DeletableColumnCount())
	for i, n := 0, ot.DeletableColumnCount(); i < n; i++ {
		ot.colMap[sqlbase.ColumnID(ot.Column(i).ColID())] = i
	}

	if !ot.desc.IsVirtualTable() {
		// Build the indexes (add 1 to account for lack of primary index in
		// DeletableIndexes slice).
		ot.indexes = make([]optIndex, 1+len(ot.desc.DeletableIndexes()))

		for i := range ot.indexes {
			var idxDesc *sqlbase.IndexDescriptor
			if i == 0 {
				idxDesc = &desc.PrimaryIndex
			} else {
				idxDesc = &ot.desc.DeletableIndexes()[i-1]
			}
			ot.indexes[i].init(ot, idxDesc)
		}
	}

	if len(desc.Families) == 0 {
		// This must be a virtual table, so synthesize a primary family. Only
		// column ids are needed by the family wrapper.
		family := &sqlbase.ColumnFamilyDescriptor{Name: "primary", ID: 0}
		family.ColumnIDs = make([]sqlbase.ColumnID, len(desc.Columns))
		for i := range family.ColumnIDs {
			family.ColumnIDs[i] = desc.Columns[i].ID
		}
		ot.primaryFamily.init(ot, family)
	} else {
		ot.primaryFamily.init(ot, &desc.Families[0])
		ot.families = make([]optFamily, len(desc.Families)-1)
		for i := range ot.families {
			ot.families[i].init(ot, &desc.Families[i+1])
		}
	}

	// Add stats last, now that other metadata is initialized.
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

	return ot
}

// ID is part of the cat.Object interface.
func (ot *optTable) ID() cat.StableID {
	return ot.id
}

// Equals is part of the cat.Object interface.
func (ot *optTable) Equals(other cat.Object) bool {
	otherTable, ok := other.(*optTable)
	if !ok {
		return false
	}
	if ot.id != otherTable.id || ot.desc.Version != otherTable.desc.Version {
		return false
	}
	// Verify the stats are identical.
	if len(ot.stats) != len(otherTable.stats) {
		return false
	}
	for i := range ot.stats {
		if !ot.stats[i].equals(&otherTable.stats[i]) {
			return false
		}
	}
	return true
}

// Name is part of the cat.DataSource interface.
func (ot *optTable) Name() *cat.DataSourceName {
	return &ot.name
}

// IsVirtualTable is part of the cat.Table interface.
func (ot *optTable) IsVirtualTable() bool {
	return ot.desc.IsVirtualTable()
}

// IsInterleaved is part of the cat.Table interface.
func (ot *optTable) IsInterleaved() bool {
	return ot.desc.IsInterleaved()
}

// IsReferenced is part of the cat.Table interface.
func (ot *optTable) IsReferenced() bool {
	for i, n := 0, ot.DeletableIndexCount(); i < n; i++ {
		if len(ot.Index(i).(*optIndex).desc.ReferencedBy) != 0 {
			return true
		}
	}
	return false
}

// ColumnCount is part of the cat.Table interface.
func (ot *optTable) ColumnCount() int {
	return len(ot.desc.Columns)
}

// WritableColumnCount is part of the cat.Table interface.
func (ot *optTable) WritableColumnCount() int {
	return len(ot.desc.WritableColumns())
}

// DeletableColumnCount is part of the cat.Table interface.
func (ot *optTable) DeletableColumnCount() int {
	return len(ot.desc.DeletableColumns())
}

// Column is part of the cat.Table interface.
func (ot *optTable) Column(i int) cat.Column {
	return &ot.desc.DeletableColumns()[i]
}

// IndexCount is part of the cat.Table interface.
func (ot *optTable) IndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.Indexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ot *optTable) WritableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.WritableIndexes())
}

// DeletableIndexCount is part of the cat.Table interface.
func (ot *optTable) DeletableIndexCount() int {
	if ot.desc.IsVirtualTable() {
		return 0
	}
	// Primary index is always present, so count is always >= 1.
	return 1 + len(ot.desc.DeletableIndexes())
}

// Index is part of the cat.Table interface.
func (ot *optTable) Index(i int) cat.Index {
	return &ot.indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (ot *optTable) StatisticCount() int {
	return len(ot.stats)
}

// Statistic is part of the cat.Table interface.
func (ot *optTable) Statistic(i int) cat.TableStatistic {
	return &ot.stats[i]
}

// CheckCount is part of the cat.Table interface.
func (ot *optTable) CheckCount() int {
	return len(ot.desc.AllChecks())
}

// Check is part of the cat.Table interface.
func (ot *optTable) Check(i int) cat.CheckConstraint {
	check := ot.desc.AllChecks()[i]
	return cat.CheckConstraint(check.Expr)
}

// FamilyCount is part of the cat.Table interface.
func (ot *optTable) FamilyCount() int {
	return 1 + len(ot.families)
}

// Family is part of the cat.Table interface.
func (ot *optTable) Family(i int) cat.Family {
	if i == 0 {
		return &ot.primaryFamily
	}
	return &ot.families[i-1]
}

// lookupColumnOrdinal returns the ordinal of the column with the given ID. A
// cache makes the lookup O(1).
func (ot *optTable) lookupColumnOrdinal(colID sqlbase.ColumnID) (int, error) {
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

// init can be used instead of newOptIndex when we have a pre-allocated instance
// (e.g. as part of a bigger struct).
func (oi *optIndex) init(tab *optTable, desc *sqlbase.IndexDescriptor) {
	oi.tab = tab
	oi.desc = desc
	if desc == &tab.desc.PrimaryIndex {
		// Although the primary index contains all columns in the table, the index
		// descriptor does not contain columns that are not explicitly part of the
		// primary key. Retrieve those columns from the table descriptor.
		oi.storedCols = make([]sqlbase.ColumnID, 0, tab.DeletableColumnCount()-len(desc.ColumnIDs))
		var pkCols util.FastIntSet
		for i := range desc.ColumnIDs {
			pkCols.Add(int(desc.ColumnIDs[i]))
		}
		for i, n := 0, tab.DeletableColumnCount(); i < n; i++ {
			id := tab.Column(i).ColID()
			if !pkCols.Contains(int(id)) {
				oi.storedCols = append(oi.storedCols, sqlbase.ColumnID(id))
			}
		}
		oi.numCols = tab.DeletableColumnCount()
	} else {
		oi.storedCols = desc.StoreColumnIDs
		oi.numCols = len(desc.ColumnIDs) + len(desc.ExtraColumnIDs) + len(desc.StoreColumnIDs)
	}

	if desc.Unique {
		notNull := true
		for _, id := range desc.ColumnIDs {
			ord, _ := tab.lookupColumnOrdinal(id)
			if tab.desc.DeletableColumns()[ord].Nullable {
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

func (os *optTableStat) equals(other *optTableStat) bool {
	// Two table statistics are considered equal if they have been created at the
	// same time, on the same set of columns.
	if os.createdAt != other.createdAt || len(os.columnOrdinals) != len(other.columnOrdinals) {
		return false
	}
	for i, c := range os.columnOrdinals {
		if c != other.columnOrdinals[i] {
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

// optFamily is a wrapper around sqlbase.ColumnFamilyDescriptor that keeps a
// reference to the table wrapper.
type optFamily struct {
	tab  *optTable
	desc *sqlbase.ColumnFamilyDescriptor
}

var _ cat.Family = &optFamily{}

// init can be used instead of newOptFamily when we have a pre-allocated
// instance (e.g. as part of a bigger struct).
func (oi *optFamily) init(tab *optTable, desc *sqlbase.ColumnFamilyDescriptor) {
	oi.tab = tab
	oi.desc = desc
}

// ID is part of the cat.Family interface.
func (oi *optFamily) ID() cat.StableID {
	return cat.StableID(oi.desc.ID)
}

// Name is part of the cat.Family interface.
func (oi *optFamily) Name() tree.Name {
	return tree.Name(oi.desc.Name)
}

// ColumnCount is part of the cat.Family interface.
func (oi *optFamily) ColumnCount() int {
	return len(oi.desc.ColumnIDs)
}

// Column is part of the cat.Family interface.
func (oi *optFamily) Column(i int) cat.FamilyColumn {
	ord, _ := oi.tab.lookupColumnOrdinal(oi.desc.ColumnIDs[i])
	return cat.FamilyColumn{Column: oi.tab.Column(ord), Ordinal: ord}
}

// Table is part of the cat.Family interface.
func (oi *optFamily) Table() cat.Table {
	return oi.tab
}
