// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestPlanToTreeAndPlanToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.ExecMultiple(t,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`,
		`
		CREATE DATABASE t;
		USE t;
	`)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "explain_tree"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec":
			r.Exec(t, d.Input)
			return ""

		case "plan-string":
			stmt, err := parser.ParseOne(d.Input)
			if err != nil {
				t.Fatal(err)
			}

			sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
			internalPlanner, cleanup := NewInternalPlanner(
				"test",
				kv.NewTxn(ctx, db, s.NodeID()),
				username.NodeUserName(),
				&MemoryMetrics{},
				&execCfg,
				sd,
			)
			defer cleanup()
			p := internalPlanner.(*planner)

			ih := &p.instrumentation
			ih.codec = execCfg.Codec
			ih.collectBundle = true

			p.stmt = makeStatement(stmt, clusterunique.ID{},
				tree.FmtFlags(queryFormattingForFingerprintsMask.Get(&execCfg.Settings.SV)))
			if err := p.makeOptimizerPlan(ctx); err != nil {
				t.Fatal(err)
			}
			defer p.curPlan.close(ctx)
			p.curPlan.savePlanInfo()
			ob := ih.emitExplainAnalyzePlanToOutputBuilder(
				ctx,
				explain.Flags{Verbose: true, ShowTypes: true},
				sessionphase.NewTimes(),
				&execstats.QueryLevelStats{},
			)
			return ob.BuildString()
		default:
			t.Fatalf("unsupported command %s", d.Cmd)
			return ""
		}
	})
}
