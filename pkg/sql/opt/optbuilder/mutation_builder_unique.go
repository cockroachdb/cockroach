// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// UniquenessChecksForGenRandomUUIDClusterMode controls the cluster setting for
// enabling uniqueness checks for UUID columns set to gen_random_uuid().
var UniquenessChecksForGenRandomUUIDClusterMode = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.optimizer.uniqueness_checks_for_gen_random_uuid.enabled",
	"if enabled, uniqueness checks may be planned for mutations of UUID columns updated with"+
		" gen_random_uuid(); otherwise, uniqueness is assumed due to near-zero collision probability",
	false,
	settings.WithPublic)

// buildUniqueChecksForInsert builds uniqueness check queries for an insert.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForInsert() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	h := &mb.uniqueCheckHelper

	buildFastPathCheck := true
	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		u := mb.tab.Unique(i)
		if !u.WithoutIndex() || u.UniquenessGuaranteedByAnotherIndex() {
			continue
		}

		// For non-serializable transactions, we guarantee uniqueness by writing tombstones in all
		// partitions of a unique index with implicit partitioning columns.
		if mb.b.evalCtx.TxnIsoLevel != isolation.Serializable {
			if indexOrdinal, ok := u.TombstoneIndexOrdinal(); ok {
				mb.uniqueWithTombstoneIndexes.Add(indexOrdinal)
				continue
			}
			panic(unimplemented.NewWithIssue(126592,
				"unique without index constraint under non-serializable isolation levels"))
		}

		// If this constraint is an arbiter of an INSERT ... ON CONFLICT ... DO
		// NOTHING clause, we don't need to plan a check (ON CONFLICT ... DO UPDATE
		// does not go through this code path; that's handled by
		// buildUniqueChecksForUpsert).
		if mb.uniqueConstraintIsArbiter(i) {
			continue
		}

		if h.init(mb, i) {
			uniqueChecksItem, fastPathUniqueChecksItem := h.buildInsertionCheck(buildFastPathCheck)
			if fastPathUniqueChecksItem == nil {
				// If we can't build one fast path check, don't build any of them into
				// the expression tree.
				buildFastPathCheck = false
				mb.fastPathUniqueChecks = nil
			} else {
				mb.fastPathUniqueChecks = append(mb.fastPathUniqueChecks, *fastPathUniqueChecksItem)
			}
			mb.uniqueChecks = append(mb.uniqueChecks, uniqueChecksItem)
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// buildUniqueChecksForUpdate builds uniqueness check queries for an update.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForUpdate() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	mb.ensureWithID()
	h := &mb.uniqueCheckHelper

	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		u := mb.tab.Unique(i)
		if !u.WithoutIndex() || u.UniquenessGuaranteedByAnotherIndex() {
			continue
		}
		// If this constraint doesn't include the updated columns we don't need to
		// plan a check.
		if !mb.uniqueColsUpdated(i) {
			continue
		}
		// For non-serializable transactions, we guarantee uniqueness by writing tombstones in all
		// partitions of a unique index with implicit partitioning columns.
		if mb.b.evalCtx.TxnIsoLevel != isolation.Serializable {
			if indexOrdinal, ok := u.TombstoneIndexOrdinal(); ok {
				mb.uniqueWithTombstoneIndexes.Add(indexOrdinal)
				continue
			}
			panic(unimplemented.NewWithIssue(126592,
				"unique without index constraint under non-serializable isolation levels"))
		}

		if h.init(mb, i) {
			// The insertion check works for updates too since it simply checks that
			// the unique columns in the newly inserted or updated rows do not match
			// any existing rows. The check prevents rows from matching themselves by
			// adding a filter based on the primary key.
			uniqueCheckItems, _ := h.buildInsertionCheck(false /* buildFastPathCheck */)
			mb.uniqueChecks = append(mb.uniqueChecks, uniqueCheckItems)
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// buildUniqueChecksForUpsert builds uniqueness check queries for an upsert.
// These check queries are used to enforce UNIQUE WITHOUT INDEX constraints.
func (mb *mutationBuilder) buildUniqueChecksForUpsert() {
	// We only need to build unique checks if there is at least one unique
	// constraint without an index.
	if !mb.hasUniqueWithoutIndexConstraints() {
		return
	}

	mb.ensureWithID()
	h := &mb.uniqueCheckHelper

	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		// If this constraint is already enforced by an index, we don't need to plan
		// a check.
		u := mb.tab.Unique(i)
		if !u.WithoutIndex() || u.UniquenessGuaranteedByAnotherIndex() {
			continue
		}
		// For non-serializable transactions, we guarantee uniqueness by writing tombstones in all
		// partitions of a unique index with implicit partitioning columns.
		if mb.b.evalCtx.TxnIsoLevel != isolation.Serializable {
			if indexOrdinal, ok := u.TombstoneIndexOrdinal(); ok {
				mb.uniqueWithTombstoneIndexes.Add(indexOrdinal)
				continue
			}
			panic(unimplemented.NewWithIssue(126592,
				"unique without index constraint under non-serializable isolation levels"))
		}

		// If this constraint is an arbiter of an INSERT ... ON CONFLICT ... DO
		// UPDATE clause and not updated by the DO UPDATE clause, we don't need to
		// plan a check (ON CONFLICT ... DO NOTHING does not go through this code
		// path; that's handled by buildUniqueChecksForInsert). Note that if
		// the constraint is partial and columns referenced in the predicate are
		// updated, we'll still plan the check (this is handled correctly by
		// mb.uniqueColsUpdated).
		if mb.uniqueConstraintIsArbiter(i) && !mb.uniqueColsUpdated(i) {
			continue
		}
		if h.init(mb, i) {
			// The insertion check works for upserts too since it simply checks that
			// the unique columns in the newly inserted or updated rows do not match
			// any existing rows. The check prevents rows from matching themselves by
			// adding a filter based on the primary key.
			uniqueCheckItems, _ := h.buildInsertionCheck(false /* buildFastPathCheck */)
			mb.uniqueChecks = append(mb.uniqueChecks, uniqueCheckItems)
		}
	}
	telemetry.Inc(sqltelemetry.UniqueChecksUseCounter)
}

// hasUniqueWithoutIndexConstraints returns true if there are any UNIQUE WITHOUT
// INDEX constraints on the table for which uniqueness is not guaranteed by
// another index. Currently, UNIQUE WITHOUT INDEX constraints that are
// synthesized for unique, hash-sharded indexes in the opt catalog are the only
// constraints that are guaranteed by another index, i.e., the physical unique,
// hash-sharded index they are synthesized from.
func (mb *mutationBuilder) hasUniqueWithoutIndexConstraints() bool {
	for i, n := 0, mb.tab.UniqueCount(); i < n; i++ {
		u := mb.tab.Unique(i)
		if u.WithoutIndex() && !u.UniquenessGuaranteedByAnotherIndex() {
			return true
		}
	}
	return false
}

// uniqueColsUpdated returns true if any of the columns for a unique
// constraint are being updated (according to updateColIDs). When the unique
// constraint has a partial predicate, it also returns true if the predicate
// references any of the columns being updated.
func (mb *mutationBuilder) uniqueColsUpdated(uniqueOrdinal cat.UniqueOrdinal) bool {
	uc := mb.tab.Unique(uniqueOrdinal)

	for i, n := 0, uc.ColumnCount(); i < n; i++ {
		if ord := uc.ColumnOrdinal(mb.tab, i); mb.updateColIDs[ord] != 0 {
			return true
		}
	}

	if _, isPartial := uc.Predicate(); isPartial {
		pred := mb.parseUniqueConstraintPredicateExpr(uniqueOrdinal)
		typedPred := mb.fetchScope.resolveAndRequireType(pred, types.Bool)

		var predCols opt.ColSet
		mb.b.buildScalar(typedPred, mb.fetchScope, nil, nil, &predCols)
		for colID, ok := predCols.Next(0); ok; colID, ok = predCols.Next(colID + 1) {
			ord := mb.md.ColumnMeta(colID).Table.ColumnOrdinal(colID)
			if mb.updateColIDs[ord] != 0 {
				return true
			}
		}
	}

	return false
}

// uniqueConstraintIsArbiter returns true if the given unique constraint is used
// as an arbiter to detect conflicts in an  INSERT ... ON CONFLICT statement.
func (mb *mutationBuilder) uniqueConstraintIsArbiter(uniqueOrdinal int) bool {
	return mb.arbiters.ContainsUniqueConstraint(uniqueOrdinal)
}

// uniqueCheckHelper is a type associated with a single unique constraint and
// is used to build the "leaves" of a unique check expression, namely the
// WithScan of the mutation input and the Scan of the table.
type uniqueCheckHelper struct {
	mb *mutationBuilder

	unique        cat.UniqueConstraint
	uniqueOrdinal int

	// uniqueOrdinals are the table ordinals of the unique columns in the table
	// that is being mutated. They correspond 1-to-1 to the columns in the
	// UniqueConstraint.
	uniqueOrdinals intsets.Fast

	// primaryKeyOrdinals includes the ordinals from any primary key columns
	// that are not included in uniqueOrdinals.
	primaryKeyOrdinals intsets.Fast

	// The scope and column ordinals of the scan that will serve as the right
	// side of the semi join for the uniqueness checks.
	scanScope    *scope
	scanOrdinals []int
}

// init initializes the helper with a unique constraint.
//
// Returns false if the constraint should be ignored (e.g. because the new
// values for the unique columns are known to be always NULL).
func (h *uniqueCheckHelper) init(mb *mutationBuilder, uniqueOrdinal int) bool {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*h = uniqueCheckHelper{
		mb:            mb,
		unique:        mb.tab.Unique(uniqueOrdinal),
		uniqueOrdinal: uniqueOrdinal,
	}

	var uniqueOrds intsets.Fast
	for i, n := 0, h.unique.ColumnCount(); i < n; i++ {
		uniqueOrds.Add(h.unique.ColumnOrdinal(mb.tab, i))
	}

	// Find the primary key columns that are not part of the unique constraint.
	// If there aren't any, we don't need a check.
	// TODO(mgartner): We also don't need a check if there exists a unique index
	// with columns that are a subset of the unique constraint columns.
	// Similarly, we don't need a check for a partial unique constraint if there
	// exists a non-partial unique constraint with columns that are a subset of
	// the partial unique constraint columns.
	primaryOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
	primaryOrds.DifferenceWith(uniqueOrds)
	if primaryOrds.Empty() {
		// The primary key columns are a subset of the unique columns; unique check
		// not needed.
		return false
	}

	h.uniqueOrdinals = uniqueOrds
	h.primaryKeyOrdinals = primaryOrds

	for tabOrd, ok := h.uniqueOrdinals.Next(0); ok; tabOrd, ok = h.uniqueOrdinals.Next(tabOrd + 1) {
		colID := mb.mapToReturnColID(tabOrd)
		// Check if we are setting NULL values for the unique columns, like when
		// this mutation is the result of a SET NULL cascade action. If at least one
		// unique column is getting a NULL value, unique check not needed.
		if memo.OutputColumnIsAlwaysNull(mb.outScope.expr, colID) {
			return false
		}

		// If one of the columns is a UUID (or UUID casted to STRING or BYTES) set
		// to gen_random_uuid() and we don't require uniqueness checks for
		// gen_random_uuid(), unique check not needed.
		switch mb.md.ColumnMeta(colID).Type.Family() {
		case types.UuidFamily, types.StringFamily, types.BytesFamily:
			if columnIsGenRandomUUID(mb.outScope.expr, colID) {
				requireCheck := UniquenessChecksForGenRandomUUIDClusterMode.Get(&mb.b.evalCtx.Settings.SV)
				if !requireCheck {
					return false
				}
			}
		}
	}

	// Build the scan that will serve as the right side of the semi join in the
	// uniqueness check. We need to build the scan now so that we can use its
	// FDs below.
	h.scanScope, h.scanOrdinals = h.buildTableScan()

	// Check that the columns in the unique constraint aren't already known to
	// form a lax key. This can happen if there is a unique index on a superset of
	// these columns, where all other columns are computed columns that depend
	// only on our columns. This is especially important for multi-region tables
	// when the region column is computed.
	//
	// For example:
	//
	//   CREATE TABLE tab (
	//     k INT PRIMARY KEY,
	//     region crdb_internal_region AS (
	//       CASE WHEN k < 10 THEN 'us-east1' ELSE 'us-west1' END
	//     ) STORED
	//   ) LOCALITY REGIONAL BY ROW AS region
	//
	// Because this is a REGIONAL BY ROW table, the region column is implicitly
	// added to the front of every index, including the primary index. As a
	// result, we would normally need to add a uniqueness check to all mutations
	// to ensure that the primary key column (k in this case) remains unique.
	// However, because the region column is computed and depends only on k, the
	// presence of the unique index on (region, k) (i.e., the primary index) is
	// sufficient to guarantee the uniqueness of k.
	var uniqueCols opt.ColSet
	h.uniqueOrdinals.ForEach(func(ord int) {
		colID := h.scanScope.cols[ord].id
		uniqueCols.Add(colID)
	})
	fds := &h.scanScope.expr.Relational().FuncDeps
	return !fds.ColsAreLaxKey(uniqueCols)
}

// buildFiltersForFastPathCheck builds ANDed equality filters between the
// columns in the uniqueness check defined by h.uniqueOrdinals and scalar
// expressions present in a single Values row being inserted. It is expected
// that buildCheckInputScan has been called and has set up in
// uniqueCheckExpr the columns corresponding with the scalars in the
// insert row. buildCheckInputScan has either inlined the insert row as a Values
// expression, or embedded it within a WithScanExpr, in which case `h.mb.inputForInsertExpr`
// holds the input to the WithScanExpr. In the latter case, for a
// given table column ordinal `i` in `h.uniqueOrdinals`, instead of finding the
// matching scalar in the Values row via uniqueCheckCols[i],
// withScanExpr.InCols[i] holds the column ID to match on. scanExpr is
// the scan on the insert target table used on the right side of the semijoins
// in the non-fast-path uniqueness checks, with column ids matching h.scanScope.cols.
//
// The purpose of this function is to build filters representing a uniqueness
// check on a given insert row, which can be applied as a Select from a Scan,
// and optimized during exploration when all placeholders have been filled in.
// The goal is to find a constrained Scan of an index, which consumes all
// filters (meaning it could also be safely executed via a KV lookup in a fast
// path uniqueness check).
func (h *uniqueCheckHelper) buildFiltersForFastPathCheck(
	uniqueCheckExpr memo.RelExpr, uniqueCheckCols []scopeColumn, scanExpr *memo.ScanExpr,
) (scanFilters memo.FiltersExpr) {
	f := h.mb.b.factory

	scanFilters = make(memo.FiltersExpr, 0, h.uniqueOrdinals.Len())
	// It is expected that `inputForInsertExpr` contains the result of the most
	// recent call to `buildInputForInsert`.
	insertInputValues := h.mb.inputForInsertExpr
	// Skip to the WithScan or Values.
	for skipProjectExpr, ok := insertInputValues.(*memo.ProjectExpr); ok; skipProjectExpr, ok = insertInputValues.(*memo.ProjectExpr) {
		insertInputValues = skipProjectExpr.Input
	}

	// If the source is a WithScan, we use InCols to find the matching value
	// in the Values tuple. If the source is a Values expression, use the scope's
	// column ID directly to find the desired scalar in the Values tuple.
	withScanExpr, isWithScan := uniqueCheckExpr.(*memo.WithScanExpr)
	valuesExpr, isValues := uniqueCheckExpr.(*memo.ValuesExpr)

	// Find a Values expression either inlined, or embedded in a WithScanExpr.
	if !isValues {
		valuesExpr, isValues = insertInputValues.(*memo.ValuesExpr)
	}
	// valuesExpr may be sourced from a values expression, or a values expression
	// nested in a WithScanExpr. The value of variable `isWithScan` determines
	// how to find the desired field in the valuesExpr below.
	// This currently only supports single-row insert. It may be possible to
	// support multi-row insert here by generating a tuple IN expression or an
	// ORed predicate, eg. (col1 = 1 AND col2 = 2) OR (col1 = 3 AND col2 = 4)...
	if !isValues || len(valuesExpr.Rows) != 1 || valuesExpr.Rows[0].Op() != opt.TupleOp {
		return nil
	}
	tupleExpr, _ := valuesExpr.Rows[0].(*memo.TupleExpr)
	// Match either unique constraint table column ordinal with the corresponding
	// scalar expression using either the columns defined in the uniqueCheckScope
	// or the input columns to the WithScanExpr.
	for i, ok := h.uniqueOrdinals.Next(0); ok; i, ok = h.uniqueOrdinals.Next(i + 1) {
		uniqueCheckColID := opt.ColumnID(0)
		if isWithScan {
			// Sanity check the index is in bounds.
			if i >= len(withScanExpr.InCols) {
				return nil
			}
			uniqueCheckColID = withScanExpr.InCols[i]
		} else {
			uniqueCheckColID = uniqueCheckCols[i].id
		}
		found := false
		var tupleScalarExpression opt.ScalarExpr
		for tupleIndex, valuesColID := range valuesExpr.Cols {
			if valuesColID == uniqueCheckColID {
				found = true
				tupleScalarExpression = tupleExpr.Elems[tupleIndex]
				break
			}
		}
		// If we can't build any part of the filters, need to give up on fast
		// path.
		if !found {
			return nil
		}
		if !scanExpr.Cols.Contains(h.scanScope.cols[i].id) {
			// Trying to build a predicate on a column added in the projection
			// on top of the scan. This may be from an expression index such as:
			// `UNIQUE INDEX ((col1 + 10))`
			// This is currently not supported.
			return nil
		}
		filtersItem := f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(h.scanScope.cols[i].id),
				tupleScalarExpression,
			),
		)
		// Volatile expressions may return a different value on each evaluation,
		// so must be disabled for this check as the actual insert value may differ.
		if filtersItem.ScalarProps().VolatilitySet.HasVolatile() {
			return nil
		}
		scanFilters = append(scanFilters, filtersItem)
	}
	return scanFilters
}

// buildInsertionCheck creates a unique check for rows which are added to a
// table. The input to the insertion check will be produced from the input to
// the mutation operator. If buildFastPathCheck is true, a fast-path unique
// check for the insert is also built. A `UniqueChecksItem` can always be built,
// but if it is not possible to build a `FastPathUniqueChecksItem`, the second
// return value is nil.
func (h *uniqueCheckHelper) buildInsertionCheck(
	buildFastPathCheck bool,
) (memo.UniqueChecksItem, *memo.FastPathUniqueChecksItem) {
	f := h.mb.b.factory

	// Build a self semi-join, with the new values on the left and the
	// existing values on the right.

	uniqueCheckScope, _ := h.mb.buildCheckInputScan(
		checkInputScanNewVals, h.scanOrdinals, false, /* isFK */
	)
	// Do NOT build any expressions using uniqueCheckScope in between the call to
	// buildCheckInputScan and the setting of uniqueCheckExpr and uniqueCheckCols.
	uniqueCheckExpr := uniqueCheckScope.expr
	uniqueCheckCols := uniqueCheckScope.cols

	// Build the join filters:
	//   (new_a = existing_a) AND (new_b = existing_b) AND ...
	//
	// Set the capacity to h.uniqueOrdinals.Len()+1 since we'll have an equality
	// condition for each column in the unique constraint, plus one additional
	// condition to prevent rows from matching themselves (see below). If the
	// constraint is partial, add 2 to account for filtering both the WithScan
	// and the Scan by the partial unique constraint predicate.
	numFilters := h.uniqueOrdinals.Len() + 1
	_, isPartial := h.unique.Predicate()
	if isPartial {
		numFilters += 2
	}
	semiJoinFilters := make(memo.FiltersExpr, 0, numFilters)
	for i, ok := h.uniqueOrdinals.Next(0); ok; i, ok = h.uniqueOrdinals.Next(i + 1) {
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(
			f.ConstructEq(
				f.ConstructVariable(uniqueCheckScope.cols[i].id),
				f.ConstructVariable(h.scanScope.cols[i].id),
			),
		))
	}
	// Find the ScanExpr which reads from the table this unique check applies to.
	var uniqueFastPathCheck memo.RelExpr
	var foundScan bool
	var scanExpr *memo.ScanExpr
	var scanFilters memo.FiltersExpr
	if buildFastPathCheck {
		possibleScan := h.scanScope.expr
		// Projections may have been added such as for computed column expressions.
		// Skip over these to find the Scan on the target table of the Insert.
		// Fast path uniqueness checks on computed columns aren't supported
		// currently. We just need to access the Scan defining regular columns.
		if skipProjectExpr, ok := possibleScan.(*memo.ProjectExpr); ok {
			possibleScan = skipProjectExpr.Input
		}
		scanExpr, foundScan = possibleScan.(*memo.ScanExpr)

		// Fast path is disabled if this check is for a UNIQUE WITHOUT INDEX with a
		// partial index predicate.
		if foundScan && !isPartial {
			scanFilters = h.buildFiltersForFastPathCheck(uniqueCheckExpr, uniqueCheckCols, scanExpr)
		}
	}

	// If the unique constraint is partial, we need to filter out inserted rows
	// that don't satisfy the predicate. We also need to make sure that rows do
	// not match existing rows in the table that do not satisfy the
	// predicate. So we add the predicate as a filter on both the WithScan
	// columns and the Scan columns.
	if isPartial {
		pred := h.mb.parseUniqueConstraintPredicateExpr(h.uniqueOrdinal)

		typedPred := uniqueCheckScope.resolveAndRequireType(pred, types.Bool)
		withScanPred := h.mb.b.buildScalar(typedPred, uniqueCheckScope, nil, nil, nil)
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(withScanPred))

		typedPred = h.scanScope.resolveAndRequireType(pred, types.Bool)
		scanPred := h.mb.b.buildScalar(typedPred, h.scanScope, nil, nil, nil)
		semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(scanPred))
	}

	// We need to prevent rows from matching themselves in the semi join. We can
	// do this by adding another filter that uses the primary keys to check if
	// two rows are identical:
	//    (new_pk1 != existing_pk1) OR (new_pk2 != existing_pk2) OR ...
	var pkFilter opt.ScalarExpr
	for i, ok := h.primaryKeyOrdinals.Next(0); ok; i, ok = h.primaryKeyOrdinals.Next(i + 1) {
		pkFilterLocal := f.ConstructNe(
			f.ConstructVariable(uniqueCheckScope.cols[i].id),
			f.ConstructVariable(h.scanScope.cols[i].id),
		)
		if pkFilter == nil {
			pkFilter = pkFilterLocal
		} else {
			pkFilter = f.ConstructOr(pkFilter, pkFilterLocal)
		}
	}
	semiJoinFilters = append(semiJoinFilters, f.ConstructFiltersItem(pkFilter))

	joinPrivate := memo.EmptyJoinPrivate
	// If we're using a weaker isolation level, the semi-joined scan needs to
	// obtain predicate locks. We must use a lookup semi-join for predicate locks
	// to work.
	if h.mb.b.evalCtx.TxnIsoLevel != isolation.Serializable {
		joinPrivate = &memo.JoinPrivate{
			Flags: memo.PreferLookupJoinIntoRight,
		}
	}

	semiJoin := f.ConstructSemiJoin(uniqueCheckScope.expr, h.scanScope.expr, semiJoinFilters, joinPrivate)

	// Collect the key columns that will be shown in the error message if there
	// is a duplicate key violation resulting from this uniqueness check.
	keyCols := make(opt.ColList, 0, h.uniqueOrdinals.Len())
	for i, ok := h.uniqueOrdinals.Next(0); ok; i, ok = h.uniqueOrdinals.Next(i + 1) {
		keyCols = append(keyCols, uniqueCheckScope.cols[i].id)
	}

	// Create a Project that passes-through only the key columns. This allows
	// normalization rules to prune any unnecessary columns from the expression.
	// The key columns are always needed in order to display the constraint
	// violation error.
	project := f.ConstructProject(semiJoin, nil /* projections */, keyCols.ToSet())

	uniqueChecks := f.ConstructUniqueChecksItem(project, &memo.UniqueChecksItemPrivate{
		Table:        h.mb.tabID,
		CheckOrdinal: h.uniqueOrdinal,
		KeyCols:      keyCols,
	})
	if !buildFastPathCheck {
		return uniqueChecks, nil
	}
	// Build a SelectExpr which can be optimized in the explore phase and used
	// to build information needed to perform the fast path uniqueness check.
	// The goal is for the Select to be rewritten into a constrained scan on
	// an index which applies all filters. If no such scans are found, insert
	// fast path cannot be applied.
	if foundScan && len(scanFilters) != 0 {
		newScanScope, _ := h.buildTableScan()
		newPossibleScan := newScanScope.expr
		// Hash-sharded REGIONAL BY ROW tables may include a projection which can
		// be skipped over to find the applicable Scan.
		if skipProjectExpr, ok := newPossibleScan.(*memo.ProjectExpr); ok {
			newPossibleScan = skipProjectExpr.Input
		}
		if newScanExpr, ok := newPossibleScan.(*memo.ScanExpr); ok {
			newScanPrivate := &newScanExpr.ScanPrivate
			newFilters := f.CustomFuncs().RemapScanColsInFilter(scanFilters, &scanExpr.ScanPrivate, newScanPrivate)
			uniqueFastPathCheck = f.ConstructSelect(newScanExpr, newFilters)
		} else {
			// Don't build a fast-path check if we failed to create a new ScanExpr.
			return uniqueChecks, nil
		}
	} else if buildFastPathCheck {
		// Don't build a fast-path check if we failed to build a ScanExpr with
		// filters on all unique check columns.
		return uniqueChecks, nil
	}
	fastPathChecks := f.ConstructFastPathUniqueChecksItem(uniqueFastPathCheck, &memo.FastPathUniqueChecksItemPrivate{ReferencedTableID: h.mb.tabID, CheckOrdinal: h.uniqueOrdinal})
	return uniqueChecks, &fastPathChecks
}

// buildTableScan builds a Scan of the table. The ordinals of the columns
// scanned are also returned.
func (h *uniqueCheckHelper) buildTableScan() (outScope *scope, ordinals []int) {
	tabMeta := h.mb.b.addTable(h.mb.tab, tree.NewUnqualifiedTableName(h.mb.tab.Name()))
	ordinals = tableOrdinals(tabMeta.Table, columnKinds{
		includeMutations: false,
		includeSystem:    false,
		includeInverted:  false,
	})
	locking := noRowLocking
	// If we're using a weaker isolation level, we lock the checked predicate(s)
	// to prevent concurrent inserts from other transactions from violating the
	// unique constraint.
	if h.mb.b.evalCtx.TxnIsoLevel != isolation.Serializable {
		locking = lockingSpec{
			&lockingItem{
				item: &tree.LockingItem{
					// TODO(michae2): Change this to ForKeyShare when it is supported.
					Strength:   tree.ForShare,
					Targets:    []tree.TableName{tree.MakeUnqualifiedTableName(h.mb.tab.Name())},
					WaitPolicy: tree.LockWaitBlock,
					// Unique checks must ensure the non-existence of certain rows, so we
					// use predicate locks instead of record locks to prevent insertion of
					// new rows into the locked span(s) by other concurrent transactions.
					Form: tree.LockPredicate,
				},
			},
		}
	}
	// After the update we can't guarantee that the constraints are unique
	// (which is why we need the uniqueness checks in the first place).
	indexFlags := &tree.IndexFlags{IgnoreUniqueWithoutIndexKeys: true}
	if h.mb.b.evalCtx.SessionData().AvoidFullTableScansInMutations {
		indexFlags.AvoidFullScan = true
	}
	// The scan is exempt from RLS to maintain data integrity.
	return h.mb.b.buildScan(
		tabMeta,
		ordinals,
		indexFlags,
		locking,
		h.mb.b.allocScope(),
		true, /* disableNotVisibleIndex */
		cat.PolicyScopeExempt,
	), ordinals
}

// columnIsGenRandomUUID returns true if the expression returns the function
// gen_random_uuid() for the given column.
func columnIsGenRandomUUID(e memo.RelExpr, col opt.ColumnID) bool {
	isGenRandomUUIDFunction := func(scalar opt.ScalarExpr) bool {
		if cast, ok := scalar.(*memo.CastExpr); ok &&
			(cast.Typ.Family() == types.StringFamily || cast.Typ.Family() == types.BytesFamily) &&
			cast.Typ.Width() == 0 {
			scalar = cast.Input
		} else if cast, ok := scalar.(*memo.AssignmentCastExpr); ok &&
			(cast.Typ.Family() == types.StringFamily || cast.Typ.Family() == types.BytesFamily) &&
			cast.Typ.Width() == 0 {
			scalar = cast.Input
		}
		if function, ok := scalar.(*memo.FunctionExpr); ok {
			if function.Name == "gen_random_uuid" {
				return true
			}
		}
		return false
	}

	switch e.Op() {
	case opt.ProjectOp:
		p := e.(*memo.ProjectExpr)
		if p.Passthrough.Contains(col) {
			return columnIsGenRandomUUID(p.Input, col)
		}
		for i := range p.Projections {
			if p.Projections[i].Col == col {
				return isGenRandomUUIDFunction(p.Projections[i].Element)
			}
		}

	case opt.ValuesOp:
		v := e.(*memo.ValuesExpr)
		colOrdinal, ok := v.Cols.Find(col)
		if !ok {
			return false
		}
		for i := range v.Rows {
			if !isGenRandomUUIDFunction(v.Rows[i].(*memo.TupleExpr).Elems[colOrdinal]) {
				return false
			}
		}
		return true
	}

	return false
}
