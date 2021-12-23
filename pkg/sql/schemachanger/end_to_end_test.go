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
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
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
				waitForSchemaChangesToComplete(t, tdb)
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
					sctestdeps.WithDescriptors(sctestdeps.ReadDescriptorsFromDB(ctx, t, tdb)),
					sctestdeps.WithNamespace(sctestdeps.ReadNamespaceFromDB(t, tdb)),
					sctestdeps.WithCurrentDatabase(sctestdeps.ReadCurrentDatabaseFromDB(t, tdb)),
					sctestdeps.WithSessionData(sctestdeps.ReadSessionDataFromDB(t, tdb, func(
						sd *sessiondata.SessionData,
					) {
						// For setting up a builder inside tests we will ensure that the new schema
						// changer will allow non-fully implemented operations.
						sd.NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
					})),
					sctestdeps.WithTestingKnobs(&scrun.TestingKnobs{
						BeforeStage: func(p scplan.Plan, stageIdx int) error {
							deps.LogSideEffectf("## %s", p.Stages[stageIdx].String())
							return nil
						},
					}),
					sctestdeps.WithStatements(stmt.SQL))
				execStatementWithTestDeps(ctx, t, deps, stmt)
				return deps.SideEffectLog()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func waitForSchemaChangesToComplete(t *testing.T, tdb *sqlutils.SQLRunner) {
	q := fmt.Sprintf(
		`SELECT count(*) FROM [SHOW JOBS] WHERE job_type IN ('%s', '%s', '%s') AND status <> 'succeeded'`,
		jobspb.TypeSchemaChange,
		jobspb.TypeTypeSchemaChange,
		jobspb.TypeNewSchemaChange,
	)
	tdb.CheckQueryResultsRetry(t, q, [][]string{{"0"}})
}

// execStatementWithTestDeps executes the DDL statement using the declarative
// schema changer with testing dependencies injected.
func execStatementWithTestDeps(
	ctx context.Context, t *testing.T, deps *sctestdeps.TestState, stmt parser.Statement,
) {
	state, err := scbuild.Build(ctx, deps, scpb.State{}, stmt.AST)
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
		details := job.Details.(jobspb.NewSchemaChangeDetails)
		progress := job.Progress.(jobspb.NewSchemaChangeProgress)
		const rollback = false
		err = scrun.RunSchemaChangesInJob(
			ctx, deps.TestingKnobs(), deps.ClusterSettings(), deps, jobID, job.DescriptorIDs, details, progress, rollback,
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

// TestRollback tests that the schema changer job rolls back properly.
// This data-driven test uses the same input as TestSchemaChangerSideEffects
// but ignores the expected output.
func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	datadriven.Walk(t, filepath.Join("testdata"), func(t *testing.T, path string) {
		var setup []parser.Statement

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			require.NotEmpty(t, stmts)

			switch d.Cmd {
			case "setup":
				// no-op
			case "test":
				var lines []string
				for _, stmt := range stmts {
					lines = append(lines, stmt.SQL)
				}
				t.Run(strings.Join(lines, "; "), func(t *testing.T) {
					numRevertibleStages := countRevertiblePostCommitStages(ctx, t, setup, stmts)
					if numRevertibleStages == 0 {
						t.Logf("test case has no revertible post-commit stages, skipping...")
						return
					}
					t.Logf("test case has %d revertible post-commit stages", numRevertibleStages)
					for i := 1; i <= numRevertibleStages; i++ {
						if !t.Run(fmt.Sprintf("rollback stage %d of %d", i, numRevertibleStages), func(t *testing.T) {
							testRollbackCase(ctx, t, setup, stmts, i)
						}) {
							return
						}
					}
				})
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			setup = append(setup, stmts...)
			return d.Expected
		})
	})
}

// testRollbackCase is a helper function for TestRollback.
func testRollbackCase(
	ctx context.Context,
	t *testing.T,
	setup []parser.Statement,
	stmts []parser.Statement,
	rollbackStageOrdinal int,
) {
	var numInjectedFailures uint32
	beforeStage := func(p scplan.Plan, stageIdx int) error {
		if atomic.LoadUint32(&numInjectedFailures) > 0 {
			return nil
		}
		s := p.Stages[stageIdx]
		if s.Phase == scop.PostCommitPhase && s.Ordinal == rollbackStageOrdinal {
			atomic.AddUint32(&numInjectedFailures, 1)
			return errors.Errorf("boom %d", rollbackStageOrdinal)
		}
		return nil
	}
	resetTxnState := func() {
		// Reset the counter in case the transaction is retried for whatever reason.
		atomic.StoreUint32(&numInjectedFailures, 0)
	}
	err := execRolledBackStatements(ctx, t, setup, stmts, beforeStage, resetTxnState)
	if atomic.LoadUint32(&numInjectedFailures) == 0 {
		require.NoError(t, err)
	} else {
		require.Regexp(t, fmt.Sprintf("boom %d", rollbackStageOrdinal), err)
	}
}

// countRevertiblePostCommitStages runs the test statements to count the number
// of stages in the post-commit phase which are revertible.
func countRevertiblePostCommitStages(
	ctx context.Context, t *testing.T, setup []parser.Statement, stmt []parser.Statement,
) (numRevertiblePostCommitStages int) {
	var nRevertible uint32
	beforeStage := func(p scplan.Plan, _ int) error {
		if p.Params.ExecutionPhase != scop.PostCommitPhase {
			return nil
		}
		n := uint32(0)
		for _, s := range p.Stages {
			if s.Phase == scop.PostCommitPhase {
				n++
			}
		}
		// Only store this value once.
		atomic.CompareAndSwapUint32(&nRevertible, 0, n)
		return nil
	}
	require.NoError(t, execRolledBackStatements(ctx, t, setup, stmt, beforeStage, func() {}))
	return int(atomic.LoadUint32(&nRevertible))
}

// execRolledBackStatements spins up a test cluster, executes the setup
// statements with the legacy schema changer, then executes the test statements
// with the declarative schema changer.
func execRolledBackStatements(
	ctx context.Context,
	t *testing.T,
	setup []parser.Statement,
	stmts []parser.Statement,
	beforeStage func(p scplan.Plan, stageIdx int) error,
	txnStartCallback func(),
) (err error) {
	const fetchDescriptorStateQuery = `
		SELECT
			create_statement
		FROM
			(
				SELECT descriptor_id, create_statement FROM crdb_internal.create_schema_statements
				UNION ALL SELECT descriptor_id, create_statement FROM crdb_internal.create_statements
				UNION ALL SELECT descriptor_id, create_statement FROM crdb_internal.create_type_statements
			)
		ORDER BY
			create_statement;`

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLDeclarativeSchemaChanger: &scrun.TestingKnobs{
					BeforeStage: beforeStage,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)

	// Execute the setup statements with the legacy schema changer so that the
	// declarative schema changer testing knobs don't get used.
	tdb.Exec(t, "SET experimental_use_new_schema_changer = 'off'")
	for _, stmt := range setup {
		tdb.Exec(t, stmt.SQL)
	}
	waitForSchemaChangesToComplete(t, tdb)
	before := tdb.QueryStr(t, fetchDescriptorStateQuery)

	// Execute the tested statements with the declarative schema changer and fail
	// the test if it all takes too long. This prevents the test suite from
	// hanging when a regression is introduced.
	tdb.Exec(t, "SET experimental_use_new_schema_changer = 'unsafe_always'")
	{
		c := make(chan error, 1)
		go func() {
			c <- crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
				txnStartCallback()
				for _, stmt := range stmts {
					if _, err := tx.Exec(stmt.SQL); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		testutils.SucceedsSoon(t, func() error {
			select {
			case e := <-c:
				err = e
				return nil
			default:
				return errors.New("waiting for statements to execute")
			}
		})
	}

	if err != nil {
		// If the statement execution failed, then we expect to end up in the same
		// state as when we started.
		require.Equal(t, before, tdb.QueryStr(t, fetchDescriptorStateQuery))
		return err
	}

	// Ensure we're really done here.
	waitForSchemaChangesToComplete(t, tdb)
	return nil
}
