// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/datadriven"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// cumulativeTest is a foundational helper for building tests over the
// datadriven format used by this package. This style of test will call
// the passed function for each test directive in the file. The setup
// statements passed to the function will be all statements from all
// previous test and setup blocks combined.
func cumulativeTest(t *testing.T, tf func(t *testing.T, setup, stmts []parser.Statement)) {
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
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
					tf(t, setup, stmts)
				})
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			setup = append(setup, stmts...)
			return d.Expected
		})
	})
}

// Rollback tests that the schema changer job rolls back properly.
// This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Rollback(t *testing.T, newCluster NewClusterFunc) {
	countRevertiblePostCommitStages := func(
		t *testing.T, setup, stmts []parser.Statement,
	) (n int) {
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase, func(
			p scplan.Plan,
		) {
			n = len(p.StagesForCurrentPhase())
		})
		return n
	}
	var testRollbackCase func(
		t *testing.T, setup, stmts []parser.Statement, ord int,
	)
	testFunc := func(t *testing.T, setup, stmts []parser.Statement) {
		n := countRevertiblePostCommitStages(t, setup, stmts)
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("rollback stage %d of %d", i, n),
				func(t *testing.T) { testRollbackCase(t, setup, stmts, i) },
			) {
				return
			}
		}
	}

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

	testRollbackCase = func(
		t *testing.T, setup, stmts []parser.Statement, ord int,
	) {
		var numInjectedFailures uint32
		beforeStage := func(p scplan.Plan, stageIdx int) error {
			if atomic.LoadUint32(&numInjectedFailures) > 0 {
				return nil
			}
			s := p.Stages[stageIdx]
			if s.Phase == scop.PostCommitPhase && s.Ordinal == ord {
				atomic.AddUint32(&numInjectedFailures, 1)
				return errors.Errorf("boom %d", ord)
			}
			return nil
		}

		db, cleanup := newCluster(t, &scrun.TestingKnobs{
			BeforeStage: beforeStage,
		})
		defer cleanup()

		tdb := sqlutils.MakeSQLRunner(db)
		var before [][]string
		beforeFunc := func() {
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		resetTxnState := func() {
			// Reset the counter in case the transaction is retried for whatever reason.
			atomic.StoreUint32(&numInjectedFailures, 0)
		}
		onError := func(err error) error {
			// If the statement execution failed, then we expect to end up in the same
			// state as when we started.
			require.Equal(t, before, tdb.QueryStr(t, fetchDescriptorStateQuery))
			return err
		}
		err := executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, beforeFunc, resetTxnState, onError,
		)
		if atomic.LoadUint32(&numInjectedFailures) == 0 {
			require.NoError(t, err)
		} else {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
		}
	}
	cumulativeTest(t, testFunc)
}

// Pause tests that the schema changer can handle being paused and resumed
// correctly. This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Pause(t *testing.T, newCluster NewClusterFunc) {
	var postCommit, nonRevertible int
	countStages := func(
		t *testing.T, setup, stmts []parser.Statement,
	) {
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase, func(
			p scplan.Plan,
		) {
			postCommit = len(p.StagesForCurrentPhase())
			nonRevertible = len(p.Stages) - postCommit
		})
	}
	var testPauseCase func(
		t *testing.T, setup, stmts []parser.Statement, ord int,
	)
	testFunc := func(t *testing.T, setup, stmts []parser.Statement) {
		countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("rollback stage %d of %d", i, n),
				func(t *testing.T) { testPauseCase(t, setup, stmts, i) },
			) {
				return
			}
		}
	}
	testPauseCase = func(t *testing.T, setup, stmts []parser.Statement, ord int) {
		var numInjectedFailures uint32
		resetTxnState := func() {
			// Reset the counter in case the transaction is retried for whatever reason.
			atomic.StoreUint32(&numInjectedFailures, 0)
		}
		db, cleanup := newCluster(t, &scrun.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if atomic.LoadUint32(&numInjectedFailures) > 0 {
					return nil
				}
				s := p.Stages[stageIdx]
				if s.Phase == scop.PostCommitPhase && s.Ordinal == ord ||
					s.Phase == scop.PostCommitNonRevertiblePhase && s.Ordinal+postCommit == ord {
					atomic.AddUint32(&numInjectedFailures, 1)
					return jobs.MarkPauseRequestError(errors.Errorf("boom %d", ord))
				}
				return nil
			},
		})
		defer cleanup()
		tdb := sqlutils.MakeSQLRunner(db)
		onError := func(err error) error {
			// Check that it's a pause error, with a job.
			// Resume the job and wait for the job.
			//require.NoError(t, err)
			re := regexp.MustCompile(`job (\d+) was paused before it completed with reason: boom (\d+)`)
			match := re.FindStringSubmatch(err.Error())
			require.NotNil(t, match)
			idx, err := strconv.Atoi(match[2])
			require.NoError(t, err)
			require.Equal(t, ord, idx)
			jobID, err := strconv.Atoi(match[1])
			require.NoError(t, err)
			t.Logf("found job %d", jobID)
			tdb.Exec(t, "RESUME JOB $1", jobID)
			tdb.CheckQueryResultsRetry(t, "SELECT status, error FROM [SHOW JOB "+match[1]+"]", [][]string{
				{"succeeded", ""},
			})
			return nil
		}
		require.NoError(t, executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, nil, resetTxnState, onError,
		))
	}
	cumulativeTest(t, testFunc)
}

// processPlanInPhase will call processFunc with the plan as of the first
// stage in the requested phase. The function will be called at most once.
func processPlanInPhase(
	t *testing.T,
	newCluster NewClusterFunc,
	setup, stmt []parser.Statement,
	phaseToProcess scop.Phase,
	processFunc func(p scplan.Plan),
) {
	var processOnce sync.Once
	db, cleanup := newCluster(t, &scrun.TestingKnobs{
		BeforeStage: func(p scplan.Plan, _ int) error {
			if p.Params.ExecutionPhase == phaseToProcess {
				processOnce.Do(func() { processFunc(p) })
			}
			return nil
		},
	})
	defer cleanup()
	require.NoError(t, executeSchemaChangeTxn(
		context.Background(), t, setup, stmt, db, nil, nil, nil,
	))
}

// executeSchemaChangeTxn spins up a test cluster, executes the setup
// statements with the legacy schema changer, then executes the test statements
// with the declarative schema changer.
func executeSchemaChangeTxn(
	ctx context.Context,
	t *testing.T,
	setup []parser.Statement,
	stmts []parser.Statement,
	db *gosql.DB,
	before func(),
	txnStartCallback func(),
	onError func(err error) error,
) (err error) {

	tdb := sqlutils.MakeSQLRunner(db)

	// Execute the setup statements with the legacy schema changer so that the
	// declarative schema changer testing knobs don't get used.
	tdb.Exec(t, "SET experimental_use_new_schema_changer = 'off'")
	for _, stmt := range setup {
		tdb.Exec(t, stmt.SQL)
	}
	waitForSchemaChangesToComplete(t, tdb)
	if before != nil {
		before()
	}

	// Execute the tested statements with the declarative schema changer and fail
	// the test if it all takes too long. This prevents the test suite from
	// hanging when a regression is introduced.
	tdb.Exec(t, "SET experimental_use_new_schema_changer = 'unsafe_always'")
	{
		c := make(chan error, 1)
		go func() {
			c <- crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
				if txnStartCallback != nil {
					txnStartCallback()
				}
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

	if err != nil && onError != nil {
		err = onError(err)
	}
	if err != nil {
		return err
	}

	// Ensure we're really done here.
	waitForSchemaChangesToComplete(t, tdb)
	return nil
}
