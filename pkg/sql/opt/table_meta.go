// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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
//
//	ensure that a column really does exist at this ordinal position.
func (t TableID) ColumnID(ord int) ColumnID {
	return t.firstColID() + ColumnID(ord)
}

// IndexColumnID returns the metadata id of the idxOrd-th index column in the
// given index.
func (t TableID) IndexColumnID(idx cat.Index, idxOrd int) ColumnID {
	return t.ColumnID(idx.Column(idxOrd).Ordinal())
}

// ColumnOrdinal returns the ordinal position of the given column in its base
// table.
//
// NOTE: This method cannot do complete bounds checking, so it's up to the
//
//	caller to ensure that this column is really in the given base table.
func (t TableID) ColumnOrdinal(id ColumnID) int {
	if buildutil.CrdbTestBuild && id < t.firstColID() {
		panic(errors.AssertionFailedf("ordinal cannot be negative"))
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
//	var myAnnID = NewTableAnnID()
//
//	md.SetTableAnnotation(TableID(1), myAnnID, "foo")
//	ann := md.TableAnnotation(TableID(1), myAnnID)
//
// Currently, the following annotations are in use:
//   - FuncDeps: functional dependencies derived from the base table
//   - Stats: statistics derived from the base table
//   - NotNullCols: not null columns derived from the base table
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
const maxTableAnnIDCount = 4

// NotNullAnnID is the annotation ID for table not null columns.
var NotNullAnnID = NewTableAnnID()

// regionConfigAnnID is the annotation ID for multiregion table config.
var regionConfigAnnID = NewTableAnnID()

// TableMeta stores information about one of the tables stored in the metadata.
//
// NOTE: Metadata.DuplicateTable and TableMeta.copyFrom must be kept in sync
// with changes to this struct.
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

	// IgnoreUniqueWithoutIndexKeys is true if we should disable any rules that
	// depend on the consistency of unique without index constraints.
	IgnoreUniqueWithoutIndexKeys bool

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

	// ColsInComputedColsExpressions is the set of all columns referenced in the
	// expressions used to build the column data of computed columns.
	ColsInComputedColsExpressions ColSet

	// partialIndexPredicates is a map from index ordinals on the table to
	// *FiltersExprs representing the predicate on the corresponding partial
	// index. If an index is not a partial index, it will not have an entry in
	// the map.
	partialIndexPredicates map[cat.IndexOrdinal]ScalarExpr

	// indexPartitionLocalities is a slice containing PrefixSorters for each
	// index that has local and remote partitions. The i-th PrefixSorter in the
	// slice corresponds to the i-th index in the table.
	//
	// The PrefixSorters represent the PARTITION BY LIST values of the index and
	// whether each of those partitions is region-local with respect to the
	// query being run. If an index is partitioned BY LIST, and has both local
	// and remote partitions, it will have an entry in the map. Local partitions
	// are those where all row ranges they own have a preferred region for
	// leaseholder nodes the same as the gateway region of the current
	// connection that is running the query. Remote partitions have at least one
	// row range with a leaseholder preferred region which is different from the
	// gateway region.
	indexPartitionLocalities []partition.PrefixSorter

	// checkConstraintsStats is a map from the current ColumnID statistics based
	// on CHECK constraint values is based on back to the original ColumnStatistic
	// entry that was built for a copy of this table. This is used to look up and
	// reuse histogram data when a Scan is duplicated so this expensive processing
	// is done at most once per table reference in a query.
	checkConstraintsStats map[ColumnID]interface{}

	// anns annotates the table metadata with arbitrary data.
	anns [maxTableAnnIDCount]interface{}

	// indexVisibility caches the ordinals of indexes which are not-visible. This
	// avoids re-computation and ensures a consistent answer within the query for
	// indexes with fractional visibility. The maps are stored as pointers to
	// ensure that copying the TableMeta doesn't cause the maps to diverge.
	indexVisibility struct {
		cached     *intsets.Fast
		notVisible *intsets.Fast
	}
}

// IsIndexNotVisible returns true if the given index is not visible, and false
// if it is fully visible. If the index is partially visible (i.e., it has a
// value for invisibility in the range (0.0, 1.0)), IsIndexNotVisible randomly
// chooses to make the index fully not visible (to this query) with probability
// proportional to the invisibility setting for the index. Otherwise, the index
// is fully visible (to this query). IsIndexNotVisible caches the result so that
// it always returns the same value for a given index.
func (tm *TableMeta) IsIndexNotVisible(indexOrd cat.IndexOrdinal, rng *rand.Rand) bool {
	if tm.indexVisibility.cached == nil {
		tm.indexVisibility.cached = &intsets.Fast{}
		tm.indexVisibility.notVisible = &intsets.Fast{}
	}
	if tm.indexVisibility.cached.Contains(indexOrd) {
		return tm.indexVisibility.notVisible.Contains(indexOrd)
	}
	// Otherwise, roll the dice to assign index visibility.
	indexInvisibility := tm.Table.Index(indexOrd).GetInvisibility()
	// If we are making an index recommendation, we do not want to use partially
	// visible indexes.
	if tm.Table.IsHypothetical() && indexInvisibility != 0 {
		indexInvisibility = 1
	}

	// If the index invisibility is 40%, we want to make this index invisible 40%
	// of the time (invisible to 40% of the queries).
	isNotVisible := false
	if indexInvisibility == 1 {
		isNotVisible = true
	} else if indexInvisibility != 0 {
		var r float64
		if rng == nil {
			r = rand.Float64()
		} else {
			r = rng.Float64()
		}
		if r <= indexInvisibility {
			isNotVisible = true
		}
	}
	tm.indexVisibility.cached.Add(indexOrd)
	if isNotVisible {
		tm.indexVisibility.notVisible.Add(indexOrd)
	}
	return isNotVisible
}

// TableAnnotation returns the given annotation that is associated with the
// given table. If the table has no such annotation, TableAnnotation returns
// nil.
func (tm *TableMeta) TableAnnotation(annID TableAnnID) interface{} {
	return tm.anns[annID]
}

// SetTableAnnotation associates the given annotation with the given table. The
// annotation is associated by the given ID, which was allocated by calling
// NewTableAnnID. If an annotation with the ID already exists on the table, then
// it is overwritten.
//
// See the TableAnnID comment for more details and a usage example.
func (tm *TableMeta) SetTableAnnotation(tabAnnID TableAnnID, ann interface{}) {
	tm.anns[tabAnnID] = ann
}

// copyFrom initializes the receiver with a copy of the given TableMeta, which
// is considered immutable.
//
// Annotations are not copied because they can be mutable.
//
// Scalar expressions are reconstructed using copyScalarFn, which returns a copy
// of the given scalar expression.
func (tm *TableMeta) copyFrom(from *TableMeta, copyScalarFn func(Expr) Expr) {
	*tm = TableMeta{
		MetaID:                       from.MetaID,
		Table:                        from.Table,
		Alias:                        from.Alias,
		IgnoreForeignKeys:            from.IgnoreForeignKeys,
		IgnoreUniqueWithoutIndexKeys: from.IgnoreUniqueWithoutIndexKeys,
		// Annotations are not copied.
	}

	if from.Constraints != nil {
		tm.Constraints = copyScalarFn(from.Constraints).(ScalarExpr)
	}

	if from.ComputedCols != nil {
		tm.ComputedCols = make(map[ColumnID]ScalarExpr, len(from.ComputedCols))
		for col, e := range from.ComputedCols {
			tm.ComputedCols[col] = copyScalarFn(e).(ScalarExpr)
		}
		tm.ColsInComputedColsExpressions = from.ColsInComputedColsExpressions
	}

	if from.partialIndexPredicates != nil {
		tm.partialIndexPredicates = make(map[cat.IndexOrdinal]ScalarExpr, len(from.partialIndexPredicates))
		for idx, e := range from.partialIndexPredicates {
			tm.partialIndexPredicates[idx] = copyScalarFn(e).(ScalarExpr)
		}
	}

	if from.checkConstraintsStats != nil {
		tm.checkConstraintsStats = make(map[ColumnID]interface{}, len(from.checkConstraintsStats))
		for i := range from.checkConstraintsStats {
			tm.checkConstraintsStats[i] = from.checkConstraintsStats[i]
		}
	}

	// This map has no ColumnID or TableID specific information in it, so it can
	// be shared.
	tm.indexPartitionLocalities = from.indexPartitionLocalities
}

// IndexColumns returns the set of table columns in the given index.
// TODO(justin): cache this value in the table metadata.
func (tm *TableMeta) IndexColumns(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.ColumnCount(); i < n; i++ {
		ord := index.Column(i).Ordinal()
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// IndexColumnsMapInverted returns the set of table columns in the given index.
// Inverted index columns are mapped to their source column.
func (tm *TableMeta) IndexColumnsMapInverted(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.ColumnCount(); i < n; i++ {
		col := index.Column(i)
		ord := col.Ordinal()
		if col.Kind() == cat.Inverted {
			ord = col.InvertedSourceColumnOrdinal()
		}
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// IndexKeyColumns returns the set of strict key columns in the given index.
func (tm *TableMeta) IndexKeyColumns(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		ord := index.Column(i).Ordinal()
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// IndexKeyColumnsMapInverted returns the set of strict key columns in the given
// index. Inverted index columns are mapped to their source column.
func (tm *TableMeta) IndexKeyColumnsMapInverted(indexOrd int) ColSet {
	index := tm.Table.Index(indexOrd)

	var indexCols ColSet
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		col := index.Column(i)
		ord := col.Ordinal()
		if col.Kind() == cat.Inverted {
			ord = col.InvertedSourceColumnOrdinal()
		}
		indexCols.Add(tm.MetaID.ColumnID(ord))
	}
	return indexCols
}

// SetConstraints sets the filters derived from check constraints; see
// TableMeta.Constraint. The argument must be a *FiltersExpr.
func (tm *TableMeta) SetConstraints(constraints ScalarExpr) {
	tm.Constraints = constraints
}

// AddComputedCol adds a computed column expression to the table's metadata and
// also adds any referenced columns in the `computedCol` expression to the
// table's metadata.
func (tm *TableMeta) AddComputedCol(colID ColumnID, computedCol ScalarExpr, outerCols ColSet) {
	if tm.ComputedCols == nil {
		tm.ComputedCols = make(map[ColumnID]ScalarExpr)
	}
	tm.ComputedCols[colID] = computedCol
	tm.ColsInComputedColsExpressions.UnionWith(outerCols)
}

// ComputedColExpr returns the computed expression for the given column, if it
// is a computed column. If it is not a computed column, ok=false is returned.
func (tm *TableMeta) ComputedColExpr(id ColumnID) (_ ScalarExpr, ok bool) {
	if tm.ComputedCols == nil {
		return nil, false
	}
	e, ok := tm.ComputedCols[id]
	return e, ok
}

// AddCheckConstraintsStats adds a column, ColumnStatistic pair to the
// checkConstraintsStats map. When the table is duplicated, the mapping from the
// new check constraint ColumnID back to the original ColumnStatistic is
// recorded so it can be reused.
func (tm *TableMeta) AddCheckConstraintsStats(colID ColumnID, colStats interface{}) {
	if tm.checkConstraintsStats == nil {
		tm.checkConstraintsStats = make(map[ColumnID]interface{})
	}
	tm.checkConstraintsStats[colID] = colStats
}

// OrigCheckConstraintsStats looks up if statistics were ever created
// based on a CHECK constraint on colID, and if so, returns the original
// ColumnStatistic.
func (tm *TableMeta) OrigCheckConstraintsStats(
	colID ColumnID,
) (origColumnStatistic interface{}, ok bool) {
	if tm.checkConstraintsStats != nil {
		if origColumnStatistic, ok = tm.checkConstraintsStats[colID]; ok {
			return origColumnStatistic, true
		}
	}
	return nil, false
}

// CacheIndexPartitionLocalities caches locality prefix sorters in the table
// metadata for indexes that have a mix of local and remote partitions. It can
// be called multiple times if necessary to update with new indexes.
func (tm *TableMeta) CacheIndexPartitionLocalities(ctx context.Context, evalCtx *eval.Context) {
	tab := tm.Table
	if cap(tm.indexPartitionLocalities) < tab.IndexCount() {
		tm.indexPartitionLocalities = make([]partition.PrefixSorter, tab.IndexCount())
	}
	tm.indexPartitionLocalities = tm.indexPartitionLocalities[:tab.IndexCount()]
	for indexOrd, n := 0, tab.IndexCount(); indexOrd < n; indexOrd++ {
		index := tab.Index(indexOrd)
		if localPartitions, ok := partition.HasMixOfLocalAndRemotePartitions(evalCtx, index); ok {
			ps := partition.GetSortedPrefixes(ctx, index, localPartitions, evalCtx)
			tm.indexPartitionLocalities[indexOrd] = ps
		}
	}
}

// IndexPartitionLocality returns the given index's PrefixSorter. An empty
// PrefixSorter is returned if the index does not have a mix of local and remote
// partitions.
func (tm *TableMeta) IndexPartitionLocality(ord cat.IndexOrdinal) (ps partition.PrefixSorter) {
	if tm.indexPartitionLocalities != nil {
		if ord >= len(tm.indexPartitionLocalities) {
			panic(errors.AssertionFailedf(
				"index ordinal %d greater than length of indexPartitionLocalities", ord,
			))
		}
		ps := tm.indexPartitionLocalities[ord]
		return ps
	}
	return partition.PrefixSorter{}
}

// AddPartialIndexPredicate adds a partial index predicate to the table's
// metadata.
func (tm *TableMeta) AddPartialIndexPredicate(ord cat.IndexOrdinal, pred ScalarExpr) {
	if tm.partialIndexPredicates == nil {
		tm.partialIndexPredicates = make(map[cat.IndexOrdinal]ScalarExpr)
	}
	tm.partialIndexPredicates[ord] = pred
}

// PartialIndexPredicate returns the given index's predicate scalar expression,
// if the index is a partial index. Returns ok=false if the index is not a
// partial index. Panics if the index is a partial index according to the
// catalog, but a predicate scalar expression does not exist in the table
// metadata.
func (tm *TableMeta) PartialIndexPredicate(ord cat.IndexOrdinal) (pred ScalarExpr, ok bool) {
	if _, isPartialIndex := tm.Table.Index(ord).Predicate(); !isPartialIndex {
		return nil, false
	}
	pred, ok = tm.partialIndexPredicates[ord]
	if !ok {
		panic(errors.AssertionFailedf("partial index predicate does not exist in table metadata"))
	}
	return pred, true
}

// PartialIndexPredicatesUnsafe returns the partialIndexPredicates map.
//
// WARNING: The returned map is NOT a source-of-truth for determining if an
// index is a partial index. It does not verify that all partial indexes in the
// catalog are included in the returned map. This function should only be used
// in tests or for formatting optimizer expressions. Use PartialIndexPredicate
// in all other cases.
func (tm *TableMeta) PartialIndexPredicatesUnsafe() map[cat.IndexOrdinal]ScalarExpr {
	return tm.partialIndexPredicates
}

// VirtualComputedColumns returns the set of virtual computed table columns.
func (tm *TableMeta) VirtualComputedColumns() ColSet {
	var virtualCols ColSet
	for col := range tm.ComputedCols {
		ord := tm.MetaID.ColumnOrdinal(col)
		if tm.Table.Column(ord).IsVirtualComputed() {
			virtualCols.Add(col)
		}
	}
	return virtualCols
}

// GetRegionsInDatabase finds the full set of regions in the multiregion
// database owning the table described by `tm`, or returns hasRegionName=false
// if not multiregion. The result is cached in TableMeta.
func (tm *TableMeta) GetRegionsInDatabase(
	ctx context.Context, planner eval.Planner,
) (regionNames catpb.RegionNames, hasRegionNames bool) {
	multiregionConfig, ok := tm.TableAnnotation(regionConfigAnnID).(*multiregion.RegionConfig)
	if ok {
		if multiregionConfig == nil {
			return nil /* regionNames */, false
		}
		return multiregionConfig.Regions(), true
	}
	dbID := tm.Table.GetDatabaseID()
	defer func() {
		if !hasRegionNames {
			tm.SetTableAnnotation(
				regionConfigAnnID,
				// Use a nil pointer to a RegionConfig, which is distinct from the
				// untyped nil and will be detected in the type assertion above.
				(*multiregion.RegionConfig)(nil),
			)
		}
	}()
	if dbID == 0 || !tm.Table.IsMultiregion() {
		return nil /* regionNames */, false
	}
	regionConfig, ok := planner.GetMultiregionConfig(ctx, dbID)
	if !ok {
		return nil /* regionNames */, false
	}
	multiregionConfig, _ = regionConfig.(*multiregion.RegionConfig)
	tm.SetTableAnnotation(regionConfigAnnID, multiregionConfig)
	return multiregionConfig.Regions(), true
}

// GetDatabaseSurvivalGoal finds the survival goal of the multiregion database
// owning the table described by `tm`, or returns ok=false if not multiregion.
// The result is cached in TableMeta.
func (tm *TableMeta) GetDatabaseSurvivalGoal(
	ctx context.Context, planner eval.Planner,
) (survivalGoal descpb.SurvivalGoal, ok bool) {
	// If planner is nil, we could be running an internal query or something else
	// which is not a user query, so make sure we don't error out this case.
	if planner == nil {
		return descpb.SurvivalGoal_ZONE_FAILURE /* survivalGoal */, true
	}
	multiregionConfig, ok := tm.TableAnnotation(regionConfigAnnID).(*multiregion.RegionConfig)
	if ok {
		if multiregionConfig == nil {
			return descpb.SurvivalGoal_ZONE_FAILURE /* survivalGoal */, false
		}
		return multiregionConfig.SurvivalGoal(), true
	}
	dbID := tm.Table.GetDatabaseID()
	defer func() {
		if !ok {
			tm.SetTableAnnotation(
				regionConfigAnnID,
				// Use a nil pointer to a RegionConfig, which is distinct from the
				// untyped nil and will be detected in the type assertion above.
				(*multiregion.RegionConfig)(nil),
			)
		}
	}()
	if dbID == 0 || !tm.Table.IsMultiregion() {
		return descpb.SurvivalGoal_ZONE_FAILURE /* regionNames */, false
	}
	regionConfig, ok := planner.GetMultiregionConfig(ctx, dbID)
	if !ok {
		return descpb.SurvivalGoal_ZONE_FAILURE /* survivalGoal */, false
	}
	multiregionConfig, _ = regionConfig.(*multiregion.RegionConfig)
	tm.SetTableAnnotation(regionConfigAnnID, multiregionConfig)
	return multiregionConfig.SurvivalGoal(), true
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
