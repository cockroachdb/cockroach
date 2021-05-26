// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestPlanAlterTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, filepath.Join("testdata"), func(t *testing.T, path string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		run := func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-view", "create-sequence", "create-table", "create-type":
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.Len(t, stmts, 1)
				tableName := ""
				switch node := stmts[0].AST.(type) {
				case *tree.CreateTable:
					tableName = node.Table.String()
				case *tree.CreateSequence:
					tableName = node.Name.String()
				case *tree.CreateView:
					tableName = node.Name.String()
				case *tree.CreateType:
					tableName = ""
				default:
					t.Fatal("not a CREATE TABLE/SEQUENCE/VIEW statement")
				}
				tdb.Exec(t, d.Input)

				if len(tableName) > 0 {
					var tableID descpb.ID
					tdb.QueryRow(t, `SELECT $1::regclass::int`, tableName).Scan(&tableID)
					if tableID == 0 {
						t.Fatalf("failed to read ID of new table %s", tableName)
					}
					t.Logf("created relation with id %d", tableID)
				}
				return ""
			case "ops", "deps":
				deps, cleanup := newTestingPlanDeps(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				var outputNodes []*scpb.Node
				for i := range stmts {
					outputNodes, err = scbuild.Build(ctx, stmts[i].AST, *deps, outputNodes)
					require.NoError(t, err)
				}

				plan, err := scplan.MakePlan(outputNodes,
					scplan.Params{
						ExecutionPhase:         scplan.PostCommitPhase,
						DisableOpRandomization: true,
					})
				require.NoError(t, err)

				if d.Cmd == "ops" {
					return marshalOps(t, &plan)
				}
				return marshalDeps(t, &plan)
			case "unimplemented":
				deps, cleanup := newTestingPlanDeps(s)
				defer cleanup()

				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.Len(t, stmts, 1)

				stmt := stmts[0]
				alter, ok := stmt.AST.(*tree.AlterTable)
				require.Truef(t, ok, "not an ALTER TABLE statement: %s", stmt.SQL)
				_, err = scbuild.Build(ctx, alter, *deps, nil)
				require.Truef(t, scbuild.HasNotImplemented(err), "expected unimplemented, got %v", err)
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		}
		datadriven.RunTest(t, path, run)
	})
}

// indentText indents text for formatting out marshaled data.
func indentText(input string, tab string) (final string) {
	split := strings.Split(input, "\n")
	for idx, line := range split {
		if len(line) == 0 {
			continue
		}
		final += tab + line
		if idx != len(split)-1 {
			final = final + "\n"
		}
	}
	return final
}

// marshalDeps marshals dependencies in scplan.Plan to a string.
func marshalDeps(t *testing.T, plan *scplan.Plan) string {
	var stages strings.Builder
	err := plan.Graph.ForEachNode(func(n *scpb.Node) error {
		return plan.Graph.ForEachDepEdgeFrom(n, func(de *scgraph.DepEdge) error {
			toAttr := de.To().Element().GetAttributes()
			fromAttr := de.From().Element().GetAttributes()
			fmt.Fprintf(&stages, "- from: [%s, %s]\n", fromAttr, de.From().State)
			fmt.Fprintf(&stages, "  to:   [%s, %s]\n", toAttr, de.To().State)
			return nil
		})
	})
	if err != nil {
		panic(errors.Wrap(err, "failed marshaling dependencies."))
	}
	return stages.String()
}

// marshalOps marshals operations in scplan.Plan to a string.
func marshalOps(t *testing.T, plan *scplan.Plan) string {
	stages := ""
	for stageIdx, stage := range plan.Stages {
		stages += fmt.Sprintf("Stage %d\n", stageIdx)
		stageOps := ""
		for _, op := range stage.Ops.Slice() {
			opMap, err := scgraphviz.ToMap(op)
			require.NoError(t, err)
			data, err := yaml.Marshal(opMap)
			require.NoError(t, err)
			stageOps += fmt.Sprintf("%T\n%s", op, indentText(string(data), "  "))
		}
		stages += indentText(stageOps, "  ")
	}
	return stages
}

func newTestingPlanDeps(s serverutils.TestServerInterface) (*scbuild.Dependencies, func()) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ip, cleanup := sql.NewInternalPlanner(
		"test",
		kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
		security.RootUserName(),
		&sql.MemoryMetrics{},
		&execCfg,
		// Setting the database on the session data to "defaultdb" in the obvious
		// way doesn't seem to do what we want.
		sessiondatapb.SessionData{},
	)
	planner := ip.(interface {
		resolver.SchemaResolver
		SemaCtx() *tree.SemaContext
		EvalContext() *tree.EvalContext
		Descriptors() *descs.Collection
		scbuild.AuthorizationAccessor
	})
	buildDeps := scbuild.Dependencies{
		Res:          planner,
		SemaCtx:      planner.SemaCtx(),
		EvalCtx:      planner.EvalContext(),
		Descs:        planner.Descriptors(),
		AuthAccessor: planner,
	}
	return &buildDeps, cleanup
}
