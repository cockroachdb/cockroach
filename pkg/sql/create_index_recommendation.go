// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
)

// makeQueryIndexRecommendation builds/optimizes a statement and walks through
// it to find potential index candidates. It then builds and optimizes the
// statement with those indexes hypothetically added to the table. An index
// recommendation for the query is outputted based on which hypothetical indexes
// are helpful in the optimal plan.
func (opc *optPlanningCtx) makeQueryIndexRecommendation(
	f *norm.Factory, ctx context.Context,
) error {
	indexCandidates := opc.findIndexCandidates(f)
	oldTables, hypTables := indexrec.BuildHypotheticalTables(
		indexCandidates, getTableZones(indexCandidates),
	)

	err := opc.buildAndOptimizeWithTables(f, ctx, hypTables)
	if err != nil {
		return err
	}

	indexes := make(map[cat.StableID][]int)
	indexrec.WalkOptExprIndexesUsed(f.Memo().RootExpr(), f.Metadata(), indexes)
	// TODO(neha): Output index recommendation information in the EXPLAIN.

	// Revert to executable plan without hypothetical indexes.
	err = opc.buildAndOptimizeWithTables(f, ctx, oldTables)
	if err != nil {
		return err
	}

	return nil
}

// buildAndOptimizeWithTables builds and optimizes a query with each TableMeta
// updated to store the corresponding table in the tables map.
func (opc *optPlanningCtx) buildAndOptimizeWithTables(
	f *norm.Factory, ctx context.Context, tables map[cat.StableID]cat.Table,
) error {
	opc.reset()
	bld := optbuilder.New(ctx, &opc.p.semaCtx, opc.p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return err
	}

	opc.optimizer.Memo().Metadata().UpdateTableMeta(tables)
	if _, err := opc.optimizer.Optimize(); err != nil {
		return err
	}

	return nil
}

// findIndexCandidates returns a map storing potential indexes for each table
// referenced in a query.
//
// 	1. Add a single index on all columns in a Group By or Order By expression if
//	   the columns are from the same table. Otherwise, group expressions into
//	   indexes by table. For Order By, the first column of each index will be
//     ascending. If that is the opposite of the column's ordering, each
//     subsequent column will also be ordered opposite to its ordering (and vice
//     versa).
//  2. Add a single-column index on any Range expression or comparison
//     expression (=, <, >, <=, >=).
// 	3. Add a single-column index on any column that appears in a JOIN predicate.
//  4. If there exist multiple columns from the same table in a JOIN predicate,
//     create a single index on all such columns.
//  5. Construct three groups for each table: EQ, R, and J.
//     - EQ is a single index of all columns that appear in equal predicates.
//     - R is all indexes that come from rule 2.
//     - J is all indexes that come from rules 3 and 4.
//     From these groups,construct the following multi-column index
//     combinations: EQ + R, J + R, J + EQ, J + EQ + R.
func (opc *optPlanningCtx) findIndexCandidates(
	f *norm.Factory,
) (indexCandidates map[cat.Table][][]cat.IndexColumn) {
	equalCandidates := make(map[cat.Table][][]cat.IndexColumn)
	rangeCandidates := make(map[cat.Table][][]cat.IndexColumn)
	joinCandidates := make(map[cat.Table][][]cat.IndexColumn)
	indexCandidates = make(map[cat.Table][][]cat.IndexColumn)

	indexrec.WalkOptExprIndexCandidates(
		f.Memo().RootExpr(),
		f.Metadata(),
		&opc.catalog,
		equalCandidates,
		rangeCandidates,
		joinCandidates,
		indexCandidates,
	)

	// Copy indexes in each category to indexCandidates without duplicates.
	indexrec.CopyIndexes(equalCandidates, indexCandidates)
	indexrec.CopyIndexes(rangeCandidates, indexCandidates)
	indexrec.CopyIndexes(joinCandidates, indexCandidates)

	joinEqualCandidates := make(map[cat.Table][][]cat.IndexColumn)
	equalGroupedCandidates := make(map[cat.Table][][]cat.IndexColumn)

	// Create a single index for equal columns for each table. As described above,
	// construct EQ + R, J + R, J + EQ, J + EQ + R.
	indexrec.GroupIndexesByTable(equalCandidates, equalGroupedCandidates)
	indexrec.ConstructIndexCombinations(equalGroupedCandidates, rangeCandidates, indexCandidates)
	indexrec.ConstructIndexCombinations(joinCandidates, rangeCandidates, indexCandidates)
	indexrec.ConstructIndexCombinations(joinCandidates, equalGroupedCandidates, joinEqualCandidates)
	indexrec.CopyIndexes(joinEqualCandidates, indexCandidates)
	indexrec.ConstructIndexCombinations(joinEqualCandidates, rangeCandidates, indexCandidates)

	return indexCandidates
}

// getTableZones casts the table keys of an indexCandidates map to *optTable in
// order to map tables to their corresponding zones.
func getTableZones(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (tableZones map[cat.Table]*zonepb.ZoneConfig) {
	tableZones = make(map[cat.Table]*zonepb.ZoneConfig)
	for t := range indexCandidates {
		tableZones[t] = t.(*optTable).zone
	}
	return tableZones
}
