// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSchemaChangerSideEffects executes DDL statements in the declarative
// schema changer injected with test dependencies and compares the accumulated
// side effects logs with expected results from the data-driven test file.
func TestSchemaChangerSideEffects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, filepath.Join("testdata"), func(t *testing.T, path string) {
		// Create a test cluster.
		// Its purpose is to seed the test dependencies with interesting states.
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
			execStmts := func() {
				for _, stmt := range stmts {
					tdb.Exec(t, stmt.SQL)
				}
				q := fmt.Sprintf(
					`SELECT count(*) FROM [SHOW JOBS] WHERE job_type IN ('%s', '%s', '%s') AND status <> 'succeeded'`,
					jobspb.TypeSchemaChange,
					jobspb.TypeTypeSchemaChange,
					jobspb.TypeNewSchemaChange,
				)
				tdb.CheckQueryResultsRetry(t, q, [][]string{{"0"}})
			}

			switch d.Cmd {
			case "setup":
				a := prettyNamespaceDump(t, tdb)
				execStmts()
				b := prettyNamespaceDump(t, tdb)
				return sctestutils.Diff(a, b, sctestutils.DiffArgs{CompactLevel: 1})

			case "test":
				require.Len(t, stmts, 1)
				stmt := stmts[0]
				// Keep test cluster in sync.
				defer execStmts()

				var deps *sctestdeps.TestState
				stageCounters := make(map[scop.Phase]int)
				testingKnobs := &scexec.NewSchemaChangerTestingKnobs{
					BeforeStage: func(ops scop.Ops, m scexec.TestingKnobMetadata) error {
						stage := stageCounters[m.Phase] + 1
						stageCounters[m.Phase] = stage
						deps.LogSideEffectf("## stage %d in %s: %d %s ops", stage, m.Phase, len(ops.Slice()), ops.Type())
						return nil
					},
				}
				// Create test dependencies and execute the schema changer.
				// The schema changer test dependencies do not hold any reference to the
				// test cluster, here the SQLRunner is only used to populate the mocked
				// catalog state.
				deps = sctestdeps.NewTestDependencies(ctx, t, tdb, testingKnobs, []string{stmt.SQL})
				execStatementWithTestDeps(ctx, t, deps, stmt)
				return deps.SideEffectLog()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// execStatementWithTestDeps executes the DDL statement using the declarative
// schema changer with testing dependencies injected.
func execStatementWithTestDeps(
	ctx context.Context, t *testing.T, deps *sctestdeps.TestState, stmt parser.Statement,
) {
	state, err := scbuild.Build(ctx, deps, scpb.State{} /* initial */, stmt.AST)
	require.NoError(t, err, "error in builder")

	var jobID jobspb.JobID
	deps.WithTxn(func(s *sctestdeps.TestState) {
		// Run statement phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		state, err = scrun.RunSchemaChangesInTxn(ctx, s, state)
		require.NoError(t, err, "error in %s", s.Phase())
		deps.LogSideEffectf("# end %s", deps.Phase())
		// Run pre-commit phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		state, err = scrun.RunSchemaChangesInTxn(ctx, s, state)
		require.NoError(t, err, "error in %s", s.Phase())
		jobID, err = scrun.CreateSchemaChangeJob(ctx, s, state)
		require.NoError(t, err, "error in mock schema change job creation")
		deps.LogSideEffectf("# end %s", deps.Phase())
	})

	if job := deps.JobRecord(jobID); job != nil {
		// Run post-commit phase in mock schema change job.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		details := job.Details.(jobspb.NewSchemaChangeDetails)
		progress := job.Progress.(jobspb.NewSchemaChangeProgress)
		err = scrun.RunSchemaChangesInJob(ctx, deps, jobID, job.DescriptorIDs, details, progress)
		require.NoError(t, err, "error in mock schema change job execution")
		deps.LogSideEffectf("# end %s", deps.Phase())
	}
}

// prettyNamespaceDump prints the state of the namespace table, minus
// descriptorless public schema and system database entries.
func prettyNamespaceDump(t *testing.T, tdb *sqlutils.SQLRunner) string {
	rows := tdb.QueryStr(t, fmt.Sprintf(`
		SELECT "parentID", "parentSchemaID", name, id
		FROM system.namespace
		WHERE id NOT IN (%d, %d) AND "parentID" <> %d
		ORDER BY id ASC`,
		keys.PublicSchemaID,
		keys.SystemDatabaseID,
		keys.SystemDatabaseID,
	))
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		parentID, parentSchemaID, name, id := row[0], row[1], row[2], row[3]
		nameType := "object"
		if parentSchemaID == "0" {
			if parentID == "0" {
				nameType = "database"
			} else {
				nameType = "schema"
			}
		}
		line := fmt.Sprintf("%s {%s %s %s} -> %s", nameType, parentID, parentSchemaID, name, id)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}
