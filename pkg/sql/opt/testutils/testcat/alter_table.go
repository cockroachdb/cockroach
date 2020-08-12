// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"context"
	gojson "encoding/json"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// AlterTable is a partial implementation of the ALTER TABLE statement.
//
// Supported commands:
//  - INJECT STATISTICS: imports table statistics from a JSON object.
//  - ADD CONSTRAINT FOREIGN KEY: add a foreign key reference.
//
func (tc *Catalog) AlterTable(stmt *tree.AlterTable) {
	tn := stmt.Table.ToTableName()
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&tn)
	tab := tc.Table(&tn)

	for _, cmd := range stmt.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterTableInjectStats:
			injectTableStats(tab, t.Stats)

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
func injectTableStats(tt *Table, statsExpr tree.Expr) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	typedExpr, err := tree.TypeCheckAndRequire(
		ctx, statsExpr, &semaCtx, types.Jsonb, "INJECT STATISTICS",
	)
	if err != nil {
		panic(err)
	}
	val, err := typedExpr.Eval(&evalCtx)
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
		tt.Stats[i] = &TableStat{js: stats[i], tt: tt}
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
