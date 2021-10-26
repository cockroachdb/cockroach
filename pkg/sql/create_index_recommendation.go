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
	ctx context.Context, f *norm.Factory,
) error {
	indexCandidates := indexrec.FindIndexCandidates(f, &opc.catalog)
	oldTables, hypTables := indexrec.BuildHypotheticalTables(
		indexCandidates, getTableZones(indexCandidates),
	)

	err := opc.buildAndOptimizeWithTables(ctx, f, hypTables)
	if err != nil {
		return err
	}

	indexes := make(map[cat.StableID][]int)
	indexrec.WalkOptExprIndexesUsed(f.Memo().RootExpr(), f.Metadata(), indexes)
	// TODO(neha): Output index recommendation information in the EXPLAIN.

	// Revert to executable plan without hypothetical indexes.
	err = opc.buildAndOptimizeWithTables(ctx, f, oldTables)
	if err != nil {
		return err
	}

	return nil
}

// buildAndOptimizeWithTables builds and optimizes a query with each TableMeta
// updated to store the corresponding table in the tables map.
func (opc *optPlanningCtx) buildAndOptimizeWithTables(
	ctx context.Context, f *norm.Factory, tables map[cat.StableID]cat.Table,
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
