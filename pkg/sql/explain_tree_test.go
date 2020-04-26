// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	yaml "gopkg.in/yaml.v2"
)

func TestPlanToTreeAndPlanToString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, `
		SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;
		CREATE DATABASE t;
		USE t;
	`)

	datadriven.RunTest(t, "testdata/explain_tree", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec":
			r.Exec(t, d.Input)
			return ""

		case "plan-string", "plan-tree":
			stmt, err := parser.ParseOne(d.Input)
			if err != nil {
				t.Fatal(err)
			}

			internalPlanner, cleanup := NewInternalPlanner(
				"test",
				kv.NewTxn(ctx, db, s.NodeID()),
				security.RootUser,
				&MemoryMetrics{},
				&execCfg,
			)
			defer cleanup()
			p := internalPlanner.(*planner)

			p.stmt = &Statement{Statement: stmt}
			if err := p.makeOptimizerPlan(ctx); err != nil {
				t.Fatal(err)
			}
			if d.Cmd == "plan-string" {
				return planToString(ctx, &p.curPlan)
			}
			tree := planToTree(ctx, &p.curPlan)
			treeYaml, err := yaml.Marshal(tree)
			if err != nil {
				t.Fatal(err)
			}
			return string(treeYaml)

		default:
			t.Fatalf("unsupported command %s", d.Cmd)
			return ""
		}
	})
}
