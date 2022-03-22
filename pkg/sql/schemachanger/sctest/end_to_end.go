// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sctest contains tools to run end-to-end datadriven tests in both
// ccl and non-ccl settings.
package sctest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// NewClusterFunc provides functionality to construct a new cluster
// given testing knobs.
type NewClusterFunc func(
	t *testing.T, knobs *scrun.TestingKnobs,
) (_ *gosql.DB, cleanup func())

// SingleNodeCluster is a NewClusterFunc.
func SingleNodeCluster(t *testing.T, knobs *scrun.TestingKnobs) (*gosql.DB, func()) {
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLDeclarativeSchemaChanger: knobs,
			JobsTestingKnobs:            jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	return db, func() {
		s.Stopper().Stop(context.Background())
	}
}

// EndToEndSideEffects is a data-driven test runner that executes DDL statements in the
// declarative schema changer injected with test dependencies and compares the
// accumulated side effects logs with expected results from the data-driven
// test file.
//
// It shares a data-driven format with Rollback.
func EndToEndSideEffects(t *testing.T, dir string, newCluster NewClusterFunc) {
	ctx := context.Background()
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		// Create a test cluster.
		db, cleanup := newCluster(t, nil /* knobs */)
		tdb := sqlutils.MakeSQLRunner(db)
		defer cleanup()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)
			execStmts := func() {
				for _, stmt := range stmts {
					tdb.Exec(t, stmt.SQL)
				}
				waitForSchemaChangesToSucceed(t, tdb)
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

				// Wait for any jobs due to previous schema changes to finish.
				sctestdeps.WaitForNoRunningSchemaChanges(t, tdb)
				var deps *sctestdeps.TestState
				// Create test dependencies and execute the schema changer.
				// The schema changer test dependencies do not hold any reference to the
				// test cluster, here the SQLRunner is only used to populate the mocked
				// catalog state.
				deps = sctestdeps.NewTestDependencies(
					sctestdeps.WithDescriptors(sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb).Catalog),
					sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb).Catalog),
					sctestdeps.WithCurrentDatabase(sctestdeps.ReadCurrentDatabaseFromDB(t, tdb)),
					sctestdeps.WithSessionData(sctestdeps.ReadSessionDataFromDB(t, tdb, func(
						sd *sessiondata.SessionData,
					) {
						// For setting up a builder inside tests we will ensure that the new schema
						// changer will allow non-fully implemented operations.
						sd.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
						sd.ApplicationName = ""
					})),
					sctestdeps.WithTestingKnobs(&scrun.TestingKnobs{
						BeforeStage: func(p scplan.Plan, stageIdx int) error {
							deps.LogSideEffectf("## %s", p.Stages[stageIdx].String())
							return nil
						},
					}),
					sctestdeps.WithStatements(stmt.SQL))
				execStatementWithTestDeps(ctx, t, deps, stmt)
				return replaceNonDeterministicOutput(deps.SideEffectLog())

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// scheduleIDRegexp captures either `scheduleId: 384784` or `scheduleId: "374764"`.
var scheduleIDRegexp = regexp.MustCompile(`scheduleId: "?[0-9]+"?`)

// dropTimeRegexp captures either `dropTime: \"time\"`.
var dropTimeRegexp = regexp.MustCompile("dropTime: \"[0-9]+")

func replaceNonDeterministicOutput(text string) string {
	// scheduleIDs change based on execution time, so redact the output.
	nextString := scheduleIDRegexp.ReplaceAllString(text, "scheduleId: <redacted>")
	return dropTimeRegexp.ReplaceAllString(nextString, "dropTime: <redacted>")
}

// execStatementWithTestDeps executes the DDL statement using the declarative
// schema changer with testing dependencies injected.
func execStatementWithTestDeps(
	ctx context.Context, t *testing.T, deps *sctestdeps.TestState, stmt parser.Statement,
) {
	state, err := scbuild.Build(ctx, deps, scpb.CurrentState{}, stmt.AST)
	require.NoError(t, err, "error in builder")

	var jobID jobspb.JobID
	deps.WithTxn(func(s *sctestdeps.TestState) {
		// Run statement phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		state, _, err = scrun.RunStatementPhase(ctx, s.TestingKnobs(), s, state)
		require.NoError(t, err, "error in %s", s.Phase())
		deps.LogSideEffectf("# end %s", deps.Phase())
		// Run pre-commit phase.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		state, jobID, err = scrun.RunPreCommitPhase(ctx, s.TestingKnobs(), s, state)
		require.NoError(t, err, "error in %s", s.Phase())
		deps.LogSideEffectf("# end %s", deps.Phase())
	})

	if job := deps.JobRecord(jobID); job != nil {
		// Run post-commit phase in mock schema change job.
		deps.IncrementPhase()
		deps.LogSideEffectf("# begin %s", deps.Phase())
		const rollback = false
		err = scrun.RunSchemaChangesInJob(
			ctx, deps.TestingKnobs(), deps.ClusterSettings(), deps, jobID, job.DescriptorIDs, rollback,
		)
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

func waitForSchemaChangesToSucceed(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.CheckQueryResultsRetry(
		t, schemaChangeWaitQuery(`('succeeded')`), [][]string{},
	)
}

func waitForSchemaChangesToFinish(t *testing.T, tdb *sqlutils.SQLRunner) {
	tdb.CheckQueryResultsRetry(
		t, schemaChangeWaitQuery(`('succeeded', 'failed')`), [][]string{},
	)
}

func schemaChangeWaitQuery(statusInString string) string {
	q := fmt.Sprintf(
		`SELECT status, job_type, description FROM [SHOW JOBS] WHERE job_type IN ('%s', '%s', '%s') AND status NOT IN %s`,
		jobspb.TypeSchemaChange,
		jobspb.TypeTypeSchemaChange,
		jobspb.TypeNewSchemaChange,
		statusInString,
	)
	return q
}
