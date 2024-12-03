// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scplan_test

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestPlanDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()() // Allow procedures and PLpgSQL.
	ctx := context.Background()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		run := func(t *testing.T, d *datadriven.TestData) string {
			sqlutils.VerifyStatementPrettyRoundtrip(t, d.Input)
			switch d.Cmd {
			case "setup":
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				for _, stmt := range stmts {
					tableName := sctestutils.TableNameFromStmt(stmt)
					tdb.Exec(t, stmt.SQL)
					if len(tableName) > 0 {
						var tableID descpb.ID
						tdb.QueryRow(t, `SELECT $1::regclass::int`, tableName).Scan(&tableID)
						if tableID == 0 {
							t.Fatalf("failed to read ID of new table %s", tableName)
						}
						t.Logf("created relation with id %d", tableID)
					}
				}
				return ""
			case "ops", "deps":
				var plan scplan.Plan
				sctestutils.WithBuilderDependenciesFromTestServer(s.ApplicationLayer(), s.NodeID(), func(deps scbuild.Dependencies) {
					stmts, err := parser.Parse(d.Input)
					require.NoError(t, err)
					var state scpb.CurrentState
					var logSchemaChangesFn scbuild.LogSchemaChangerEventsFn
					for i := range stmts {
						state, logSchemaChangesFn, err = scbuild.Build(ctx, deps, state, stmts[i].AST, mon.NewStandaloneUnlimitedAccount())
						require.NoError(t, err)
						require.NoError(t, logSchemaChangesFn(ctx))
					}

					plan = sctestutils.MakePlan(t, state, scop.EarliestPhase, nil /* memAcc */)
					sctestutils.TruncateJobOps(&plan)
					validatePlan(t, &plan)
				})

				if d.Cmd == "ops" {
					return marshalOps(t, plan.TargetState, plan.Stages)
				}
				return marshalDeps(t, &plan)
			case "unimplemented":
				sctestutils.WithBuilderDependenciesFromTestServer(s.ApplicationLayer(), s.NodeID(), func(deps scbuild.Dependencies) {
					stmts, err := parser.Parse(d.Input)
					require.NoError(t, err)
					require.Len(t, stmts, 1)

					stmt := stmts[0]
					alter, ok := stmt.AST.(*tree.AlterTable)
					require.Truef(t, ok, "not an ALTER TABLE statement: %s", stmt.SQL)
					_, _, err = scbuild.Build(ctx, deps, scpb.CurrentState{}, alter, mon.NewStandaloneUnlimitedAccount())
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
// property to have. For instance, it guarantees that the output of EXPLAIN (DDL)
// represents the plan that actually gets executed in the various execution
// phases.
func validatePlan(t *testing.T, plan *scplan.Plan) {
	stages := plan.Stages
	for i, stage := range stages {
		if stage.Phase == scop.PreCommitPhase && stage.Ordinal == 2 {
			// Skip the pre-commit main stage. Otherwise, the re-planned plan
			// will have a reset stage and the assertions won't be verified.
			continue
		}
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
		cs := plan.CurrentState.WithCurrentStatuses(stage.Before)
		truncatedPlan := sctestutils.MakePlan(t, cs, stage.Phase, nil /* memAcc */)
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
			if rn := de.RuleNames(); len(rn) == 1 {
				fmt.Fprintf(&deps, "  rule: %s\n", rn)
			} else {
				fmt.Fprintf(&deps, "  rules: %s\n", rn)
			}
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

// TestExplainPlanIsMemoryMonitored tests that explaining a plan is properly
// memory monitored.
// Such monitoring is important to prevent OOM for explaining a large plan as
// it can take up a lot of memory to serialize the plan into a string.
func TestExplainPlanIsMemoryMonitored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "large test; uses a lot of memory")

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 10 << 20, /* 10MiB */
	})
	defer s.Stopper().Stop(context.Background())
	tt, nodeID := s.ApplicationLayer(), s.NodeID()
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, `use defaultdb;`)
	tdb.Exec(t, `select crdb_internal.generate_test_objects('test',  1000);`)
	tdb.Exec(t, `use system;`)

	var incumbent scpb.CurrentState
	var logSchemaChangesFn scbuild.LogSchemaChangerEventsFn
	sctestutils.WithBuilderDependenciesFromTestServer(tt, nodeID, func(dependencies scbuild.Dependencies) {
		stmt, err := parser.ParseOne(`DROP DATABASE defaultdb CASCADE`)
		require.NoError(t, err)
		incumbent, logSchemaChangesFn, err = scbuild.Build(ctx, dependencies, scpb.CurrentState{}, stmt.AST, mon.NewStandaloneUnlimitedAccount())
		require.NoError(t, err)
		require.NoError(t, logSchemaChangesFn(ctx))
	})

	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-sc-plan-mon"),
		Settings: tt.ClusterSettings(),
	})
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(5.243e+6 /* 5MiB */))
	memAcc := monitor.MakeBoundAccount()
	plan := sctestutils.MakePlan(t, incumbent, scop.EarliestPhase, &memAcc)

	_, err := plan.ExplainCompact()
	require.Regexp(t, `test-sc-plan-mon: memory budget exceeded: .*`, err.Error())
	memAcc.Clear(ctx)

	_, err = plan.ExplainVerbose()
	require.Regexp(t, `test-sc-plan-mon: memory budget exceeded: .*`, err.Error())
	memAcc.Clear(ctx)

	// SHAPE plans don't depend on the number of elements and won't
	// exceed the memory budget, so we don't test them for errors.
}
