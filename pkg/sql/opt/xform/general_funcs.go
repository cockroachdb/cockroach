// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by the
// exploration rules. The unnamed norm.CustomFuncs allows CustomFuncs to provide
// a clean interface for calling functions from both the xform and norm packages
// using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e  *explorer
	im partialidx.Implicator
}

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = CustomFuncs{
		e: e,
	}
	c.CustomFuncs.Init(e.f)
	c.im.Init(e.f, e.mem.Metadata(), e.evalCtx)
}

// IsCanonicalScan returns true if the given ScanPrivate is an original
// unaltered primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(scan *memo.ScanPrivate) bool {
	return scan.IsCanonical()
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(scanPrivate *memo.ScanPrivate) bool {
	md := c.e.mem.Metadata()
	tab := md.Table(scanPrivate.Table)

	// Skip the primary index because it cannot be inverted.
	for i := 1; i < tab.IndexCount(); i++ {
		if tab.Index(i).IsInverted() {
			return true
		}
	}
	return false
}

// MapFilterCols returns a new FiltersExpr with all the src column IDs in
// the input expression replaced with column IDs in dst.
//
// NOTE: Every ColumnID in src must map to the a ColumnID in dst with the same
// relative position in the ColSets. For example, if src and dst are (1, 5, 6)
// and (7, 12, 15), then the following mapping would be applied:
//
//   1 => 7
//   5 => 12
//   6 => 15
func (c *CustomFuncs) MapFilterCols(
	filters memo.FiltersExpr, src, dst opt.ColSet,
) memo.FiltersExpr {
	newFilters := c.mapScalarExprCols(&filters, src, dst).(*memo.FiltersExpr)
	return *newFilters
}

func (c *CustomFuncs) mapScalarExprCols(scalar opt.ScalarExpr, src, dst opt.ColSet) opt.ScalarExpr {
	if src.Len() != dst.Len() {
		panic(errors.AssertionFailedf(
			"src and dst must have the same number of columns, src: %v, dst: %v",
			src,
			dst,
		))
	}

	// Map each column in src to a column in dst based on the relative position
	// of both the src and dst ColumnIDs in the ColSet.
	var colMap opt.ColMap
	dstCol, _ := dst.Next(0)
	for srcCol, ok := src.Next(0); ok; srcCol, ok = src.Next(srcCol + 1) {
		colMap.Set(int(srcCol), int(dstCol))
		dstCol, _ = dst.Next(dstCol + 1)
	}

	return c.RemapCols(scalar, colMap)
}

// checkConstraintFilters generates all filters that we can derive from the
// check constraints. These are constraints that have been validated and are
// non-nullable. We only use non-nullable check constraints because they
// behave differently from filters on NULL. Check constraints are satisfied
// when their expression evaluates to NULL, while filters are not.
//
// For example, the check constraint a > 1 is satisfied if a is NULL but the
// equivalent filter a > 1 is not.
//
// These filters do not really filter any rows, they are rather facts or
// guarantees about the data but treating them as filters may allow some
// indexes to be constrained and used. Consider the following example:
//
// CREATE TABLE abc (
// 	a INT PRIMARY KEY,
// 	b INT NOT NULL,
// 	c STRING NOT NULL,
// 	CHECK (a < 10 AND a > 1),
// 	CHECK (b < 10 AND b > 1),
// 	CHECK (c in ('first', 'second')),
// 	INDEX secondary (b, a),
// 	INDEX tertiary (c, b, a))
//
// Now consider the query: SELECT a, b WHERE a > 5
//
// Notice that the filter provided previously wouldn't let the optimizer use
// the secondary or tertiary indexes. However, given that we can use the
// constraints on a, b and c, we can actually use the secondary and tertiary
// indexes. In fact, for the above query we can do the following:
//
// select
//  ├── columns: a:1(int!null) b:2(int!null)
//  ├── scan abc@tertiary
//  │		├── columns: a:1(int!null) b:2(int!null)
//  │		└── constraint: /3/2/1: [/'first'/2/6 - /'first'/9/9] [/'second'/2/6 - /'second'/9/9]
//  └── filters
//        └── gt [type=bool]
//            ├── variable: a [type=int]
//            └── const: 5 [type=int]
//
// Similarly, the secondary index could also be used. All such index scans
// will be added to the memo group.
func (c *CustomFuncs) checkConstraintFilters(tabID opt.TableID) memo.FiltersExpr {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(tabID)
	if tabMeta.Constraints == nil {
		return memo.FiltersExpr{}
	}
	filters := *tabMeta.Constraints.(*memo.FiltersExpr)
	// Limit slice capacity to allow the caller to append if necessary.
	return filters[:len(filters):len(filters)]
}

func (c *CustomFuncs) initIdxConstraintForIndex(
	requiredFilters, optionalFilters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) (ic *idxconstraint.Instance) {
	ic = &idxconstraint.Instance{}

	// Fill out data structures needed to initialize the idxconstraint library.
	// Use LaxKeyColumnCount, since all columns <= LaxKeyColumnCount are
	// guaranteed to be part of each row's key (i.e. not stored in row's value,
	// which does not take part in an index scan). Note that the OrderingColumn
	// slice cannot be reused, as Instance.Init can use it in the constraint.
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(tabID)
	index := tabMeta.Table.Index(indexOrd)
	columns := make([]opt.OrderingColumn, index.LaxKeyColumnCount())
	var notNullCols opt.ColSet
	for i := range columns {
		col := index.Column(i)
		ordinal := col.Ordinal()
		nullable := col.IsNullable()
		colID := tabID.ColumnID(ordinal)
		columns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !nullable {
			notNullCols.Add(colID)
		}
	}

	// Generate index constraints.
	ic.Init(
		requiredFilters, optionalFilters,
		columns, notNullCols, tabMeta.ComputedCols,
		true /* consolidate */, c.e.evalCtx, c.e.f,
	)
	return ic
}

// computedColFilters generates all filters that can be derived from the list of
// computed column expressions from the given table. A computed column can be
// used as a filter when it has a constant value. That is true when:
//
//   1. All other columns it references are constant, because other filters in
//      the query constrain them to be so.
//   2. All functions in the computed column expression can be folded into
//      constants (i.e. they do not have problematic side effects).
//
// Note that computed columns can depend on other computed columns; in general
// the dependencies form an acyclic directed graph. computedColFilters will
// return filters for all constant computed columns, regardless of the order of
// their dependencies.
//
// As with checkConstraintFilters, computedColFilters do not really filter any
// rows, they are rather facts or guarantees about the data. Treating them as
// filters may allow some indexes to be constrained and used. Consider the
// following example:
//
//   CREATE TABLE t (
//     k INT NOT NULL,
//     hash INT AS (k % 4) STORED,
//     PRIMARY KEY (hash, k)
//   )
//
//   SELECT * FROM t WHERE k = 5
//
// Notice that the filter provided explicitly wouldn't allow the optimizer to
// seek using the primary index (it would have to fall back to a table scan).
// However, column "hash" can be proven to have the constant value of 1, since
// it's dependent on column "k", which has the constant value of 5. This enables
// usage of the primary index:
//
//     scan t
//      ├── columns: k:1(int!null) hash:2(int!null)
//      ├── constraint: /2/1: [/1/5 - /1/5]
//      ├── key: (2)
//      └── fd: ()-->(1)
//
// The values of both columns in that index are known, enabling a single value
// constraint to be generated.
func (c *CustomFuncs) computedColFilters(
	scanPrivate *memo.ScanPrivate, requiredFilters, optionalFilters memo.FiltersExpr,
) memo.FiltersExpr {
	tabMeta := c.e.mem.Metadata().TableMeta(scanPrivate.Table)
	if len(tabMeta.ComputedCols) == 0 {
		return nil
	}

	// Start with set of constant columns, as derived from the list of filter
	// conditions.
	constCols := make(map[opt.ColumnID]opt.ScalarExpr)
	c.findConstantFilterCols(constCols, scanPrivate, requiredFilters)
	c.findConstantFilterCols(constCols, scanPrivate, optionalFilters)
	if len(constCols) == 0 {
		// No constant values could be derived from filters, so assume that there
		// are also no constant computed columns.
		return nil
	}

	// Construct a new filter condition for each computed column that is
	// constant (i.e. all of its variables are in the constCols set).
	var computedColFilters memo.FiltersExpr
	for colID := range tabMeta.ComputedCols {
		if c.tryFoldComputedCol(tabMeta, colID, constCols) {
			constVal := constCols[colID]
			// Note: Eq is not correct here because of NULLs.
			eqOp := c.e.f.ConstructIs(c.e.f.ConstructVariable(colID), constVal)
			computedColFilters = append(computedColFilters, c.e.f.ConstructFiltersItem(eqOp))
		}
	}
	return computedColFilters
}

// findConstantFilterCols adds to constFilterCols mappings from table column ID
// to the constant value of that column. It does this by iterating over the
// given lists of filters and finding expressions that constrain columns to a
// single constant value. For example:
//
//   x = 5 AND y = 'foo'
//
// This would add a mapping from x => 5 and y => 'foo', which constants can
// then be used to prove that dependent computed columns are also constant.
func (c *CustomFuncs) findConstantFilterCols(
	constFilterCols map[opt.ColumnID]opt.ScalarExpr,
	scanPrivate *memo.ScanPrivate,
	filters memo.FiltersExpr,
) {
	tab := c.e.mem.Metadata().Table(scanPrivate.Table)
	for i := range filters {
		// If filter constraints are not tight, then no way to derive constant
		// values.
		props := filters[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}

		// Iterate over constraint conjuncts with a single column and single
		// span having a single key.
		for i, n := 0, props.Constraints.Length(); i < n; i++ {
			cons := props.Constraints.Constraint(i)
			if cons.Columns.Count() != 1 || cons.Spans.Count() != 1 {
				continue
			}

			// Skip columns that aren't in the scanned table.
			colID := cons.Columns.Get(0).ID()
			if !scanPrivate.Cols.Contains(colID) {
				continue
			}

			// Skip columns with a data type that uses a composite key encoding.
			// Each of these data types can have multiple distinct values that
			// compare equal. For example, 0 == -0 for the FLOAT data type. It's
			// not safe to treat these as constant inputs to computed columns,
			// since the computed expression may differentiate between the
			// different forms of the same value.
			colTyp := tab.Column(scanPrivate.Table.ColumnOrdinal(colID)).DatumType()
			if colinfo.HasCompositeKeyEncoding(colTyp) {
				continue
			}

			span := cons.Spans.Get(0)
			if !span.HasSingleKey(c.e.evalCtx) {
				continue
			}

			datum := span.StartKey().Value(0)
			if datum != tree.DNull {
				constFilterCols[colID] = c.e.f.ConstructConstVal(datum, colTyp)
			}
		}
	}
}

// isZoneLocal returns true if the given zone config indicates that the replicas
// it constrains will be primarily located in the localRegion.
func isZoneLocal(zone cat.Zone, localRegion string) bool {
	// First count the number of local and remote replica constraints. If all
	// are local or all are remote, we can return early.
	local, remote := 0, 0
	for i, n := 0, zone.ReplicaConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.ReplicaConstraints(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Next check the voter replica constraints. Once again, if all are local or
	// all are remote, we can return early.
	local, remote = 0, 0
	for i, n := 0, zone.VoterConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.VoterConstraint(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				if isLocal {
					local++
				} else {
					remote++
				}
			}
		}
	}
	if local > 0 && remote == 0 {
		return true
	}
	if remote > 0 && local == 0 {
		return false
	}

	// Use the lease preferences as a tie breaker. We only really care about the
	// first one, since subsequent lease preferences only apply in edge cases.
	if zone.LeasePreferenceCount() > 0 {
		leasePref := zone.LeasePreference(0)
		for i, n := 0, leasePref.ConstraintCount(); i < n; i++ {
			constraint := leasePref.Constraint(i)
			if isLocal, ok := isConstraintLocal(constraint, localRegion); ok {
				return isLocal
			}
		}
	}

	return false
}

// isConstraintLocal returns isLocal=true and ok=true if the given constraint is
// a required constraint matching the given localRegion. Returns isLocal=false
// and ok=true if the given constraint is a prohibited constraint matching the
// given local region or if it is a required constraint matching a different
// region. Any other scenario returns ok=false, since this constraint gives no
// information about whether the constrained replicas are local or remote.
func isConstraintLocal(constraint cat.Constraint, localRegion string) (isLocal bool, ok bool) {
	if constraint.GetKey() != regionKey {
		// We only care about constraints on the region.
		return false /* isLocal */, false /* ok */
	}
	if constraint.GetValue() == localRegion {
		if constraint.IsRequired() {
			// The local region is required.
			return true /* isLocal */, true /* ok */
		}
		// The local region is prohibited.
		return false /* isLocal */, true /* ok */
	}
	if constraint.IsRequired() {
		// A remote region is required.
		return false /* isLocal */, true /* ok */
	}
	// A remote region is prohibited, so this constraint gives no information
	// about whether the constrained replicas are local or remote.
	return false /* isLocal */, false /* ok */
}

// prefixIsLocal contains a PARTITION BY LIST prefix, and a boolean indicating
// whether the prefix is from a local partition.
type prefixIsLocal struct {
	prefix  tree.Datums
	isLocal bool
}

// prefixSorter sorts prefixes (which are wrapped in prefixIsLocal structs) so
// that longer prefixes are ordered first.
type prefixSorter []prefixIsLocal

var _ sort.Interface = &prefixSorter{}

// Len is part of sort.Interface.
func (ps prefixSorter) Len() int {
	return len(ps)
}

// Less is part of sort.Interface.
func (ps prefixSorter) Less(i, j int) bool {
	return len(ps[i].prefix) > len(ps[j].prefix)
}

// Swap is part of sort.Interface.
func (ps prefixSorter) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
}

// getSortedPrefixes collects all the prefixes from all the different partitions
// in the index (remembering which ones came from local partitions), and sorts
// them so that longer prefixes come before shorter prefixes.
func getSortedPrefixes(index cat.Index, localPartitions util.FastIntSet) []prefixIsLocal {
	allPrefixes := make(prefixSorter, 0, index.PartitionCount())
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)
		isLocal := localPartitions.Contains(i)
		partitionPrefixes := part.PartitionByListPrefixes()
		if len(partitionPrefixes) == 0 {
			// This can happen when the partition value is DEFAULT.
			allPrefixes = append(allPrefixes, prefixIsLocal{
				prefix:  nil,
				isLocal: isLocal,
			})
		}
		for j := range partitionPrefixes {
			allPrefixes = append(allPrefixes, prefixIsLocal{
				prefix:  partitionPrefixes[j],
				isLocal: isLocal,
			})
		}
	}
	sort.Sort(allPrefixes)
	return allPrefixes
}
