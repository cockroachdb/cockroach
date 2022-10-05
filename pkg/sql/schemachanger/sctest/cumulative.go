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
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// cumulativeTest is a foundational helper for building tests over the
// datadriven format used by this package. This style of test will call
// the passed function for each test directive in the file. The setup
// statements passed to the function will be all statements from all
// previous test and setup blocks combined.
func cumulativeTest(
	t *testing.T,
	relPath string,
	tf func(t *testing.T, path string, rewrite bool, setup, stmts []parser.Statement),
) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	path := testutils.RewritableDataPath(t, relPath)
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
				tf(t, path, d.Rewrite, setup, stmts)
			})
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
		setup = append(setup, stmts...)
		return d.Expected
	})
}

// TODO(ajwerner): For all the non-rollback variants, we'd really actually
// like them to run over each of the rollback stages too.

// Rollback tests that the schema changer job rolls back properly.
// This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Rollback(t *testing.T, relPath string, newCluster NewClusterFunc) {
	countRevertiblePostCommitStages := func(
		t *testing.T, setup, stmts []parser.Statement,
	) (n int) {
		processPlanInPhase(
			t, newCluster, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) { n = len(p.StagesForCurrentPhase()) },
			func(db *gosql.DB) {},
		)
		return n
	}
	var testRollbackCase func(
		t *testing.T, path string, rewrite bool, setup, stmts []parser.Statement, ord, n int,
	)
	testFunc := func(t *testing.T, path string, rewrite bool, setup, stmts []parser.Statement) {
		n := countRevertiblePostCommitStages(t, setup, stmts)
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("rollback stage %d of %d", i, n),
				func(t *testing.T) { testRollbackCase(t, path, rewrite, setup, stmts, i, n) },
			) {
				return
			}
		}
	}

	testRollbackCase = func(
		t *testing.T, path string, rewrite bool, setup, stmts []parser.Statement, ord, n int,
	) {
		var numInjectedFailures uint32
		var numCheckedExplainInRollback uint32
		beforeStage := func(p scplan.Plan, stageIdx int) error {
			s := p.Stages[stageIdx]
			if atomic.LoadUint32(&numInjectedFailures) > 0 {
				// At this point, if a failure has already been injected, any stage
				// should be non-revertible.
				require.Equal(t, scop.PostCommitNonRevertiblePhase, s.Phase)
				// EXPLAIN the rollback plan as early as possible.
				if atomic.LoadUint32(&numCheckedExplainInRollback) > 0 {
					return nil
				}
				atomic.AddUint32(&numCheckedExplainInRollback, 1)
				fileNameSuffix := fmt.Sprintf(".rollback_%d_of_%d", ord, n)
				explainedStmt := fmt.Sprintf("rollback at post-commit stage %d of %d", ord, n)
				const inRollback = true
				checkExplainDiagrams(t, path, setup, stmts, explainedStmt, fileNameSuffix, p.CurrentState, inRollback, rewrite)
				return nil
			}
			if s.Phase == scop.PostCommitPhase && s.Ordinal == ord {
				atomic.AddUint32(&numInjectedFailures, 1)
				return errors.Errorf("boom %d", ord)
			}
			return nil
		}

		db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: beforeStage,
			OnPostCommitPlanError: func(state *scpb.CurrentState, err error) error {
				panic(fmt.Sprintf("%+v", err))
			},
			OnPostCommitError: func(p scplan.Plan, stageIdx int, err error) error {
				if strings.Contains(err.Error(), "boom") {
					return err
				}
				panic(fmt.Sprintf("%+v", err))
			},
		})
		defer cleanup()

		tdb := sqlutils.MakeSQLRunner(db)
		var before [][]string
		beforeFunc := func() {
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		onError := func(err error) error {
			// If the statement execution failed, then we expect to end up in the same
			// state as when we started.
			require.Equal(t, before, tdb.QueryStr(t, fetchDescriptorStateQuery))
			return err
		}
		err := executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, beforeFunc, nil, onError,
		)
		if atomic.LoadUint32(&numInjectedFailures) == 0 {
			require.NoError(t, err)
		} else {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
			require.NotZero(t, atomic.LoadUint32(&numCheckedExplainInRollback))
		}
	}
	cumulativeTest(t, relPath, testFunc)
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
WHERE descriptor_id IN (SELECT id FROM system.namespace)
ORDER BY
	create_statement;`

// Pause tests that the schema changer can handle being paused and resumed
// correctly. This data-driven test uses the same input as EndToEndSideEffects
// but ignores the expected output.
func Pause(t *testing.T, relPath string, newCluster NewClusterFunc) {
	var postCommit, nonRevertible int
	countStages := func(
		t *testing.T, setup, stmts []parser.Statement,
	) {
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase, func(
			p scplan.Plan,
		) {
			postCommit = len(p.StagesForCurrentPhase())
			nonRevertible = len(p.Stages) - postCommit
		}, nil)
	}
	var testPauseCase func(
		t *testing.T, setup, stmts []parser.Statement, ord int,
	)
	testFunc := func(t *testing.T, _ string, _ bool, setup, stmts []parser.Statement) {
		countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		if n == 0 {
			t.Logf("test case has no revertible post-commit stages, skipping...")
			return
		}
		t.Logf("test case has %d revertible post-commit stages", n)
		for i := 1; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("pause stage %d of %d", i, n),
				func(t *testing.T) { testPauseCase(t, setup, stmts, i) },
			) {
				return
			}
		}

		// Need to reset "postCommit" and "nonRevertible" before testFunc being
		// called for next test. The reason is that if a test did not generate any
		// post commit phase, the "countStates()" function won't take any effect
		// since "processPlanInPhase()" only calls the input "processFunc" for the
		// specified phase. So that such test would inherit "postCommit" and
		// "nonRevertible" from a previous test which generates post commit phase
		// stages.
		postCommit = 0
		nonRevertible = 0
	}
	testPauseCase = func(t *testing.T, setup, stmts []parser.Statement, ord int) {
		var numInjectedFailures uint32
		// TODO(ajwerner): It'd be nice to assert something about the number of
		// remaining stages before the pause and then after. It's not totally
		// trivial, as we don't checkpoint during non-mutation stages, so we'd
		// need to look back and find the last mutation phase.
		db, cleanup := newCluster(t, &scexec.TestingKnobs{
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
			re := regexp.MustCompile(
				`job (\d+) was paused before it completed with reason: boom (\d+)`,
			)
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
			context.Background(), t, setup, stmts, db, nil, nil, onError,
		))
		require.Equal(t, uint32(1), atomic.LoadUint32(&numInjectedFailures))
	}
	cumulativeTest(t, relPath, testFunc)
}

// Used for saving corpus information in TestGenerateCorpus
var corpusPath string

func init() {
	flag.StringVar(&corpusPath, "declarative-corpus", "", "path to the corpus file")
}

// GenerateSchemaChangeCorpus executes each post commit stage of a given set of
// statements and writes them into a corpus file. This file can be later used to
// validate mixed version / forward compatibility.
func GenerateSchemaChangeCorpus(t *testing.T, path string, newCluster NewClusterFunc) {
	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires declarative-corpus path parameter")
	}
	cc, err := corpus.NewCorpusCollector(corpusPath)
	if err != nil {
		t.Fatalf("failed to create collection %v", err)
	}
	defer func() {
		err := cc.UpdateCorpus()
		if err != nil {
			panic(err)
		}
	}()
	var testCorpusCollect func(
		t *testing.T, setup, stmts []parser.Statement,
	)
	testFunc := func(t *testing.T, path string, rewrite bool, setup, stmts []parser.Statement) {
		if !t.Run("starting",
			func(t *testing.T) { testCorpusCollect(t, setup, stmts) },
		) {
			return
		}
	}
	testCorpusCollect = func(t *testing.T, setup, stmts []parser.Statement) {
		// If any of the statements are not supported, then skip over this
		// file for the corpus.
		for _, stmt := range stmts {
			if !scbuild.CheckIfSupported(stmt.AST) {
				return
			}
		}
		db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: cc.GetBeforeStage("EndToEndCorpus", t),
		})

		defer cleanup()
		require.NoError(t, executeSchemaChangeTxn(
			context.Background(), t, setup, stmts, db, nil, nil, nil,
		))
	}
	cumulativeTest(t, path, testFunc)
}

// runAllBackups runs all the backup tests, disabling the random skipping.
var runAllBackups = flag.Bool(
	"run-all-backups", false,
	"if true, run all backups instead of a random subset",
)

// Backup tests that the schema changer can handle being backed up and
// restored correctly. This data-driven test uses the same input as
// EndToEndSideEffects but ignores the expected output. Note that the
// cluster constructor needs to provide a cluster with CCL BACKUP/RESTORE
// functionality enabled.
func Backup(t *testing.T, path string, newCluster NewClusterFunc) {
	var after [][]string // CREATE_STATEMENT for all descriptors after finishing `stmts` in each test case.
	var dbName string
	r, _ := randutil.NewTestRand()
	const runRate = .5

	maybeRandomlySkip := func(t *testing.T) {
		if !*runAllBackups && r.Float64() >= runRate {
			skip.IgnoreLint(t, "skipping due to randomness")
		}
	}

	// A function that executes `setup` first and then count the number of
	// postCommit and postCommitNonRevertible stages for executing `stmts`.
	// It also initializes `after` and `dbName` here.
	countStages := func(
		t *testing.T, setup, stmts []parser.Statement,
	) (postCommit, nonRevertible int) {
		var pl scplan.Plan
		processPlanInPhase(t, newCluster, setup, stmts, scop.PostCommitPhase,
			func(p scplan.Plan) {
				pl = p
				postCommit = len(p.StagesForCurrentPhase())
				nonRevertible = len(p.Stages) - postCommit
			}, func(db *gosql.DB) {
				tdb := sqlutils.MakeSQLRunner(db)
				var ok bool
				dbName, ok = maybeGetDatabaseForIDs(t, tdb, screl.AllTargetDescIDs(pl.TargetState))
				if ok {
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
				}
				after = tdb.QueryStr(t, fetchDescriptorStateQuery)
			})
		return postCommit, nonRevertible
	}

	// A function that takes backup at `ord`-th stage while executing `stmts` after
	// finishing `setup`. It also takes `ord` backups at each of the preceding stage
	// if it's a revertible stage.
	// It then restores the backup(s) in various "flavors" (see
	// comment below for details) and expect the restore to finish the schema change job
	// as if the backup/restore had never happened.
	testBackupRestoreCase := func(
		t *testing.T, setup, stmts []parser.Statement, ord int,
	) {
		type stage struct {
			p        scplan.Plan
			stageIdx int
			resume   chan error
		}

		stageChan := make(chan stage)
		ctx, cancel := context.WithCancel(context.Background())
		db, cleanup := newCluster(t, &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if p.Stages[stageIdx].Phase < scop.PostCommitPhase {
					return nil
				}
				if stageChan != nil {
					s := stage{p: p, stageIdx: stageIdx, resume: make(chan error)}
					select {
					case stageChan <- s:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case err := <-s.resume:
						return err
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		})

		// Start with full database backup/restore.
		defer cleanup()
		defer cancel()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		tdb := sqlutils.MakeSQLRunner(conn)
		// TODO(postamar): remove this threshold bump
		//   This requires the test cases to be properly parallelized.
		tdb.SucceedsSoonDuration = testutils.RaceSucceedsSoonDuration
		tdb.Exec(t, "create database backups")
		var g errgroup.Group
		var before [][]string
		beforeFunc := func() {
			tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
		}
		g.Go(func() error {
			return executeSchemaChangeTxn(
				context.Background(), t, setup, stmts, db, beforeFunc, nil, nil,
			)
		})
		type backup struct {
			name       string
			isRollback bool
			url        string
			s          stage
		}
		var backups []backup
		var done bool
		var rollbackStage int
		type stageKey struct {
			stage    int
			rollback bool
		}
		completedStages := make(map[stageKey]struct{})
		for i := 0; !done; i++ {
			// We want to let the stages up to ord continue unscathed. Then, we'll
			// start taking backups at ord. If ord corresponds to a revertible
			// stage, we'll inject an error, forcing the schema change to revert.
			// At each subsequent stage, we also take a backup. At the very end,
			// we'll have one backup where things should succeed and N backups
			// where we're reverting. In each case, we want to have the end state
			// of the restored set of descriptors match what we have in the original
			// cluster.
			//
			// Lastly, we'll hit an ord corresponding to the first non-revertible
			// stage. At this point, we'll take a backup for each non-revertible
			// stage and confirm that restoring them and letting the jobs run
			// leaves the database in the right state.
			s := <-stageChan
			// Move the index backwards if we see the same stage repeat due to a txn
			// retry error for example.
			stage := stageKey{
				stage:    s.stageIdx,
				rollback: s.p.InRollback,
			}
			if _, ok := completedStages[stage]; ok {
				i--
				if stage.rollback {
					rollbackStage--
				}
			}
			completedStages[stage] = struct{}{}
			shouldFail := ord == i &&
				s.p.Stages[s.stageIdx].Phase != scop.PostCommitNonRevertiblePhase &&
				!s.p.InRollback
			done = len(s.p.Stages) == s.stageIdx+1 && !shouldFail
			t.Logf("stage %d/%d in %v (rollback=%v) %d %q %v",
				s.stageIdx+1, len(s.p.Stages), s.p.Stages[s.stageIdx].Phase, s.p.InRollback, ord, dbName, done)

			// If the database has been dropped, there is nothing for
			// us to do here.
			var exists bool
			tdb.QueryRow(t,
				`SELECT count(*) > 0 FROM system.namespace WHERE "parentID" = 0 AND name = $1`,
				dbName).Scan(&exists)
			if !exists || (i < ord && !done) {
				close(s.resume)
				continue
			}

			// This test assumes that all the descriptors being modified in the
			// transaction are in the same database.
			//
			// TODO(ajwerner): Deal with trying to restore just some of the tables.
			backupURL := fmt.Sprintf("userfile://backups.public.userfiles_$user/data%d", i)
			tdb.Exec(t, fmt.Sprintf(
				"BACKUP DATABASE %s INTO '%s'", dbName, backupURL))
			backups = append(backups, backup{
				name:       dbName,
				isRollback: rollbackStage > 0,
				url:        backupURL,
				s:          s,
			})

			if s.p.InRollback {
				rollbackStage++
			}
			if done {
				t.Logf("reached final stage, waiting for completion")
				stageChan = nil // allow the restored jobs to proceed
			}
			if shouldFail {
				s.resume <- errors.Newf("boom %d", i)
			} else {
				close(s.resume)
			}
		}
		if err := g.Wait(); rollbackStage > 0 {
			require.Regexp(t, fmt.Sprintf("boom %d", ord), err)
		} else {
			require.NoError(t, err)
		}

		t.Logf("finished")

		for i, b := range backups {
			// For each backup, we restore it in three flavors.
			// 1. RESTORE DATABASE
			// 2. RESTORE DATABASE WITH schema_only
			// 3. RESTORE TABLE tbl1, tbl2, ..., tblN
			// We then assert that the restored database should correctly finish
			// the ongoing schema change job when the backup was taken, and
			// reaches the expected state as if the back/restore had not happened at all.
			// Skip a backup randomly.
			type backupConsumptionFlavor struct {
				name         string
				restoreSetup []string
				restoreQuery string
			}
			flavors := []backupConsumptionFlavor{
				{
					name: "restore database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s'", dbName, b.url),
				},
				{
					name: "restore database with schema-only",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' with schema_only", dbName, b.url),
				},
			}

			// For the third flavor, we restore all tables in the backup.
			// Skip it if there is no tables.
			rows := tdb.QueryStr(t, `
			SELECT parent_schema_name, object_name
			FROM [SHOW BACKUP FROM LATEST IN $1]
			WHERE database_name = $2 AND object_type = 'table'`, b.url, dbName)
			var tablesToRestore []string
			for _, row := range rows {
				tablesToRestore = append(tablesToRestore, fmt.Sprintf("%s.%s.%s", dbName, row[0], row[1]))
			}

			if len(tablesToRestore) > 0 {
				flavors = append(flavors, backupConsumptionFlavor{
					name: "restore all tables in database",
					restoreSetup: []string{
						fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName),
						fmt.Sprintf("CREATE DATABASE %q", dbName),
						"SET use_declarative_schema_changer = 'off'",
					},
					restoreQuery: fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN '%s' WITH skip_missing_sequences",
						strings.Join(tablesToRestore, ","), b.url),
				})
			}

			// TODO (xiang): Add here the fourth flavor that restores
			// only a subset, maybe randomly chosen, of all tables with
			// `RESTORE TABLE`. Currently, it's blocked by issue #87518.
			// We will need to change what the expected output will be
			// in this case, since it will no longer be simply `before`
			// and `after`.

			for _, flavor := range flavors {
				t.Run(flavor.name, func(t *testing.T) {
					maybeRandomlySkip(t)
					t.Logf("testing backup %d (rollback=%v)", i, b.isRollback)
					tdb.ExecMultiple(t, flavor.restoreSetup...)
					tdb.Exec(t, flavor.restoreQuery)
					tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
					waitForSchemaChangesToFinish(t, tdb)
					afterRestore := tdb.QueryStr(t, fetchDescriptorStateQuery)
					if b.isRollback {
						require.Equal(t, before, afterRestore)
					} else {
						require.Equal(t, after, afterRestore)
					}
					// Hack to deal with corrupt userfiles tables due to #76764.
					const validateQuery = `
SELECT * FROM crdb_internal.invalid_objects WHERE database_name != 'backups'
`
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
					tdb.Exec(t, fmt.Sprintf("DROP DATABASE %q CASCADE", dbName))
					tdb.Exec(t, "USE backups")
					tdb.CheckQueryResults(t, validateQuery, [][]string{})
				})
			}
		}
	}

	testFunc := func(t *testing.T, _ string, _ bool, setup, stmts []parser.Statement) {
		postCommit, nonRevertible := countStages(t, setup, stmts)
		n := postCommit + nonRevertible
		t.Logf(
			"test case has %d revertible post-commit stages and %d non-revertible"+
				" post-commit stages", postCommit, nonRevertible,
		)
		for i := 0; i <= n; i++ {
			if !t.Run(
				fmt.Sprintf("backup/restore stage %d of %d", i, n),
				func(t *testing.T) {
					maybeRandomlySkip(t)
					testBackupRestoreCase(t, setup, stmts, i)
				},
			) {
				return
			}
		}
	}

	cumulativeTest(t, path, testFunc)
}

func maybeGetDatabaseForIDs(
	t *testing.T, tdb *sqlutils.SQLRunner, ids catalog.DescriptorIDSet,
) (dbName string, exists bool) {
	err := tdb.DB.QueryRowContext(context.Background(), `
SELECT name
  FROM system.namespace
 WHERE id
       IN (
            SELECT DISTINCT
                   COALESCE(
                    d->'database'->>'id',
                    d->'schema'->>'parentId',
                    d->'type'->>'parentId',
                    d->'table'->>'parentId'
                   )::INT8
              FROM (
                    SELECT crdb_internal.pb_to_json('desc', descriptor) AS d
                      FROM system.descriptor
                     WHERE id IN (SELECT * FROM ROWS FROM (unnest($1::INT8[])))
                   )
        )
`, pq.Array(ids.Ordered())).
		Scan(&dbName)
	if errors.Is(err, gosql.ErrNoRows) {
		return "", false
	}

	require.NoError(t, err)
	return dbName, true
}

// processPlanInPhase will call processFunc with the plan as of the first
// stage in the requested phase. The function will be called at most once.
func processPlanInPhase(
	t *testing.T,
	newCluster NewClusterFunc,
	setup, stmts []parser.Statement,
	phaseToProcess scop.Phase,
	processFunc func(p scplan.Plan),
	after func(db *gosql.DB),
) {
	var processOnce sync.Once
	db, cleanup := newCluster(t, &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, _ int) error {
			if p.Params.ExecutionPhase == phaseToProcess {
				processOnce.Do(func() { processFunc(p) })
			}
			return nil
		},
	})
	defer cleanup()
	require.NoError(t, executeSchemaChangeTxn(
		context.Background(), t, setup, stmts, db, nil, nil, nil,
	))
	if after != nil {
		after(db)
	}
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
	tdb.Exec(t, "SET use_declarative_schema_changer = 'off'")
	for _, stmt := range setup {
		tdb.Exec(t, stmt.SQL)
	}
	waitForSchemaChangesToSucceed(t, tdb)
	if before != nil {
		before()
	}

	// Execute the tested statements with the declarative schema changer and fail
	// the test if it all takes too long. This prevents the test suite from
	// hanging when a regression is introduced.
	{
		c := make(chan error, 1)
		go func() {
			conn, err := db.Conn(ctx)
			if err != nil {
				c <- err
				return
			}
			defer func() { _ = conn.Close() }()
			c <- crdb.Execute(func() (err error) {
				_, err = conn.ExecContext(
					ctx, "SET use_declarative_schema_changer = 'unsafe_always'",
				)
				if err != nil {
					return err
				}
				var tx *gosql.Tx
				tx, err = conn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer func() {
					if err != nil {
						err = errors.WithSecondaryError(err, tx.Rollback())
					} else {
						err = tx.Commit()
					}
				}()
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
	waitForSchemaChangesToSucceed(t, tdb)
	return nil
}
