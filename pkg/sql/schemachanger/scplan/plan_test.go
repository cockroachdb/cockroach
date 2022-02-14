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
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestPlanDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()
	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		run := func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-view", "create-sequence", "create-table", "create-type", "create-database", "create-schema", "create-index":
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
				case *tree.CreateDatabase:
					tableName = ""
				case *tree.CreateSchema:
					tableName = ""
				case *tree.CreateIndex:
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
				var plan scplan.Plan
				sctestutils.WithBuilderDependenciesFromTestServer(s, func(deps scbuild.Dependencies) {
					stmts, err := parser.Parse(d.Input)
					require.NoError(t, err)
					var state scpb.CurrentState
					for i := range stmts {
						state, err = scbuild.Build(ctx, deps, state, stmts[i].AST)
						require.NoError(t, err)
					}

					plan = sctestutils.MakePlan(t, state, scop.EarliestPhase)
					sctestutils.TruncateJobOps(&plan)
					validatePlan(t, &plan)
				})

				if d.Cmd == "ops" {
					return marshalOps(t, plan.TargetState, plan.Stages)
				}
				return marshalDeps(t, &plan)
			case "unimplemented":
				sctestutils.WithBuilderDependenciesFromTestServer(s, func(deps scbuild.Dependencies) {
					stmts, err := parser.Parse(d.Input)
					require.NoError(t, err)
					require.Len(t, stmts, 1)

					stmt := stmts[0]
					alter, ok := stmt.AST.(*tree.AlterTable)
					require.Truef(t, ok, "not an ALTER TABLE statement: %s", stmt.SQL)
					_, err = scbuild.Build(ctx, deps, scpb.CurrentState{}, alter)
					require.Truef(t, scerrors.HasNotImplemented(err), "expected unimplemented, got %v", err)
				})
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		}
		datadriven.RunTest(t, path, run)
	})
}

// validatePlan takes an existing plan and re-plans using the starting state of
// an arbitrary stage in the existing plan: the results should be the same as in
// the original plan, minus the stages prior to the selected stage.
// This guarantees the idempotency of the planning scheme, which is a useful
// property to have. For instance it guarantees that the output of EXPLAIN (DDL)
// represents the plan that actually gets executed in the various execution
// phases.
func validatePlan(t *testing.T, plan *scplan.Plan) {
	stages := plan.Stages
	for i, stage := range stages {
		expected := make([]scstage.Stage, len(stages[i:]))
		for j, s := range stages[i:] {
			if s.Phase == stage.Phase {
				offset := stage.Ordinal - 1
				s.Ordinal = s.Ordinal - offset
				s.StagesInPhase = s.StagesInPhase - offset
			}
			expected[j] = s
		}
		e := marshalOps(t, plan.TargetState, expected)
		cs := scpb.CurrentState{
			TargetState: plan.TargetState,
			Current:     stage.Before,
		}
		truncatedPlan := sctestutils.MakePlan(t, cs, stage.Phase)
		sctestutils.TruncateJobOps(&truncatedPlan)
		a := marshalOps(t, plan.TargetState, truncatedPlan.Stages)
		require.Equalf(t, e, a, "plan mismatch when re-planning %d stage(s) later", i)
	}
}

// indentText indents text for formatting out marshaled data.
func indentText(input string, tab string) string {
	result := strings.Builder{}
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		result.WriteString(tab)
		result.WriteString(line)
		result.WriteString("\n")
	}
	return result.String()
}

// marshalDeps marshals dependencies in scplan.Plan to a string.
func marshalDeps(t *testing.T, plan *scplan.Plan) string {
	var sortedDeps []string
	err := plan.Graph.ForEachNode(func(n *screl.Node) error {
		return plan.Graph.ForEachDepEdgeFrom(n, func(de *scgraph.DepEdge) error {
			var deps strings.Builder
			fmt.Fprintf(&deps, "- from: [%s, %s]\n",
				screl.ElementString(de.From().Element()), de.From().CurrentStatus)
			fmt.Fprintf(&deps, "  to:   [%s, %s]\n",
				screl.ElementString(de.To().Element()), de.To().CurrentStatus)
			fmt.Fprintf(&deps, "  kind: %s\n", de.Kind())
			fmt.Fprintf(&deps, "  rule: %s\n", de.Name())
			sortedDeps = append(sortedDeps, deps.String())
			return nil
		})
	})
	if err != nil {
		panic(errors.Wrap(err, "failed marshaling dependencies."))
	}
	// Lexicographically sort the dependencies,
	// since the order is not fully deterministic.
	sort.Strings(sortedDeps)
	var stages strings.Builder
	for _, dep := range sortedDeps {
		fmt.Fprintf(&stages, "%s", dep)
	}
	return stages.String()
}

// marshalOps marshals operations in scplan.Plan to a string.
func marshalOps(t *testing.T, ts scpb.TargetState, stages []scstage.Stage) string {
	var sb strings.Builder
	for _, stage := range stages {
		sb.WriteString(stage.String())
		sb.WriteString("\n  transitions:\n")
		var transitionsBuf strings.Builder
		for i, before := range stage.Before {
			after := stage.After[i]
			if before == after {
				continue
			}
			n := &screl.Node{
				Target:        &ts.Targets[i],
				CurrentStatus: before,
			}
			_, _ = fmt.Fprintf(&transitionsBuf, "%s -> %s\n", screl.NodeString(n), after)
		}
		sb.WriteString(indentText(transitionsBuf.String(), "    "))
		ops := stage.Ops()
		if len(ops) == 0 {
			// Although unlikely, it's possible for a stage to have no operations.
			// This requires them to have been optimized out, and also requires that
			// the resulting empty stage could not be collapsed into another.
			sb.WriteString("  no ops\n")
			continue
		}
		sb.WriteString("  ops:\n")
		stageOps := ""
		for _, op := range ops {
			if setJobStateOp, ok := op.(*scop.SetJobStateOnDescriptor); ok {
				clone := *setJobStateOp
				clone.State = scpb.DescriptorState{}
				op = &clone
			}
			opMap, err := scgraphviz.ToMap(op)
			require.NoError(t, err)
			data, err := yaml.Marshal(opMap)
			require.NoError(t, err)
			stageOps += fmt.Sprintf("%T\n%s", op, indentText(string(data), "  "))
		}
		sb.WriteString(indentText(stageOps, "    "))
	}
	return sb.String()
}
