// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// GenerateIndexScans enumerates all non-inverted secondary indexes on the given
// Scan operator's table and generates an alternate Scan operator for each index
// that includes the set of needed columns specified in the ScanOpDef.
//
// This transformation can only consider partial indexes that are guaranteed to
// index every row in the table. Therefore, only partial indexes with predicates
// that always evaluate to true are considered. Such an index is pseudo-partial
// in that it behaves the exactly the same as a non-partial secondary index.
//
// NOTE: This does not generate index joins for non-covering indexes (except in
//       case of ForceIndex). Index joins are usually only introduced "one level
//       up", when the Scan operator is wrapped by an operator that constrains
//       or limits scan output in some way (e.g. Select, Limit, InnerJoin).
//       Index joins are only lower cost when their input does not include all
//       rows from the table. See ConstrainScans and LimitScans for cases where
//       index joins are introduced into the memo.
func (c *CustomFuncs) GenerateIndexScans(grp memo.RelExpr, scanPrivate *memo.ScanPrivate) {
	// Iterate over all non-inverted and non-partial secondary indexes.
	var iter scanIndexIter
	iter.Init(c.e.mem, &c.im, scanPrivate, nil /* filters */, rejectPrimaryIndex|rejectInvertedIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool) {
		// If the secondary index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if isCovering {
			scan := memo.ScanExpr{ScanPrivate: *scanPrivate}
			scan.Index = index.Ordinal()
			c.e.mem.AddScanToGroup(&scan, grp)
			return
		}

		// Otherwise, if the index must be forced, then construct an IndexJoin
		// operator that provides the columns missing from the index. Note that
		// if ForceIndex=true, scanIndexIter only returns the one index that is
		// being forced, so no need to check that here.
		if !scanPrivate.Flags.ForceIndex {
			return
		}

		var sb indexScanBuilder
		sb.init(c, scanPrivate.Table)

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		sb.addIndexJoin(scanPrivate.Cols)
		sb.build(grp)
	})
}
