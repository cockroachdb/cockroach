// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"
	gojson "encoding/json"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// AlterTable is a partial implementation of the ALTER TABLE statement.
//
// Supported commands:
//   - INJECT STATISTICS: imports table statistics from a JSON object.
//   - ADD CONSTRAINT FOREIGN KEY: add a foreign key reference.
//   - {ENABLE | DISABLE} ROW LEVEL SECURITY: enables or disables RLS policies for the table.
func (tc *Catalog) AlterTable(stmt *tree.AlterTable) {
	tn := stmt.Table.ToTableName()
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&tn)
	tab := tc.Table(&tn)

	for _, cmd := range stmt.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterTableInjectStats:
			injectTableStats(tab, t.Stats, tc)

		case *tree.AlterTableSetRLSMode:
			toggleRLSMode(tab, t.Mode)

		case *tree.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *tree.ForeignKeyConstraintTableDef:
				tc.resolveFK(tab, d)

			default:
				panic(errors.AssertionFailedf("unsupported constraint type %v", d))
			}

		default:
			panic(errors.AssertionFailedf("unsupported ALTER TABLE command %T", t))
		}
	}
}

// injectTableStats sets the table statistics as specified by a JSON object.
func injectTableStats(tt *Table, statsExpr tree.Expr, tc *Catalog) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	typedExpr, err := tree.TypeCheckAndRequire(ctx, statsExpr, &semaCtx, types.Jsonb, "INJECT STATISTICS")
	if err != nil {
		panic(err)
	}
	val, err := eval.Expr(ctx, &evalCtx, typedExpr)
	if err != nil {
		panic(err)
	}

	if val == tree.DNull {
		panic("statistics cannot be NULL")
	}
	jsonStr := val.(*tree.DJSON).JSON.String()
	var stats []stats.JSONStatistic
	if err := gojson.Unmarshal([]byte(jsonStr), &stats); err != nil {
		panic(err)
	}
	tt.Stats = make([]*TableStat, len(stats))
	for i := range stats {
		tt.Stats[i] = &TableStat{js: stats[i], tt: tt, evalCtx: &evalCtx, tc: tc}
	}
	// Call ColumnOrdinal on all possible columns to assert that
	// the column names are valid.
	for _, ts := range tt.Stats {
		for i := 0; i < ts.ColumnCount(); i++ {
			ts.ColumnOrdinal(i)
		}
	}

	// Finally, sort the stats with most recent first.
	sort.Sort(tt.Stats)
}

// toggleRLSMode will change the row-level security enabled field in the table.
func toggleRLSMode(tt *Table, mode tree.TableRLSMode) {
	switch mode {
	case tree.TableRLSEnable:
		tt.rlsEnabled = true
	case tree.TableRLSDisable:
		tt.rlsEnabled = false
	default:
		return
	}
}
