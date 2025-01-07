// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/corpus"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Rollback tests that the schema changer job rolls back properly.
func Rollback(t *testing.T, relPath string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

	testRollbackCase := func(t *testing.T, cs CumulativeTestCaseSpec) {
		if cs.Phase != scop.PostCommitPhase {
			skip.IgnoreLint(t, "cannot roll back outside of post-commit phase")
			return
		}
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
				fileNameSuffix := fmt.Sprintf("__rollback_%d_of_%d", cs.StageOrdinal, cs.StagesCount)
				explainedStmt := fmt.Sprintf("rollback at post-commit stage %d of %d", cs.StageOrdinal, cs.StagesCount)
				const inRollback = true
				checkExplainDiagrams(t, cs.Path, cs.Setup, cs.Stmts, explainedStmt, fileNameSuffix, p.CurrentState, inRollback, cs.Rewrite)
				return nil
			}
			if s.Phase == scop.PostCommitPhase && s.Ordinal == cs.StageOrdinal {
				atomic.AddUint32(&numInjectedFailures, 1)
				return errors.Errorf("boom %d", cs.StageOrdinal)
			}
			return nil
		}
		knobs := &scexec.TestingKnobs{
			BeforeStage: beforeStage,
			OnPostCommitPlanError: func(err error) error {
				panic(fmt.Sprintf("%+v", err))
			},
			OnPostCommitError: func(p scplan.Plan, stageIdx int, err error) error {
				if strings.Contains(err.Error(), "boom") {
					return err
				}
				panic(fmt.Sprintf("%+v", err))
			},
		}
		ctx := context.Background()
		runfn := func(s serverutils.TestServerInterface, db *gosql.DB) {
			tdb := sqlutils.MakeSQLRunner(db)
			var before [][]string
			require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))
			before = tdb.QueryStr(t, fetchDescriptorStateQuery)
			err := executeSchemaChangeTxn(ctx, t, cs.CumulativeTestSpec, db)
			if err != nil {
				// If the statement execution failed, then we expect to end up in the same
				// state as when we started.
				require.Equal(t, before, tdb.QueryStr(t, fetchDescriptorStateQuery))
			} else {
				waitForSchemaChangesToFinish(t, tdb)
			}
			if atomic.LoadUint32(&numInjectedFailures) == 0 {
				require.NoError(t, err)
			} else {
				require.Regexp(t, fmt.Sprintf("boom %d", cs.StageOrdinal), err)
				require.NotZero(t, atomic.LoadUint32(&numCheckedExplainInRollback))
			}
		}
		factory.WithSchemaChangerKnobs(knobs).Run(ctx, t, runfn)
	}
	cumulativeTestForEachPostCommitStage(t, relPath, factory, testRollbackCase)
}

// ExecuteWithDMLInjection tests that the schema changer behaviour is sane
// once we start injecting DML statements into execution.
func ExecuteWithDMLInjection(t *testing.T, relPath string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

	jobErrorMutex := syncutil.Mutex{}
	var testDMLInjectionCase func(
		t *testing.T, ts CumulativeTestSpec, key stageKey, injectPreCommit bool,
	)
	testFunc := func(t *testing.T, ts CumulativeTestSpec) {
		// Count number of stages in PostCommit and PostCommitNonRevertible phase
		// for running `stmts` after properly running `setup`.
		var postCommit, nonRevertible int
		withPostCommitPlanAfterSchemaChange(t, ts, factory, func(_ *gosql.DB, p scplan.Plan) {
			for _, s := range p.Stages {
				switch s.Phase {
				case scop.PostCommitPhase:
					postCommit++
				case scop.PostCommitNonRevertiblePhase:
					nonRevertible++
				}
			}
		})
		injectionRanges := ts.stageExecMap.GetInjectionRuns(postCommit, nonRevertible)
		defer ts.stageExecMap.AssertMapIsUsed(t)
		injectPreCommits := []bool{false}
		if ts.stageExecMap.getExecStmts(makeStageKey(scop.PreCommitPhase, 1, false)) != nil {
			injectPreCommits = []bool{false, true}
		}
		// Test both happy and unhappy paths with pre-commit injection, this
		// maximizes our available coverage. When the pre-commit injects are
		// removed we still expect queries to behave correctly.
		for _, injectPreCommit := range injectPreCommits {
			for _, injection := range injectionRanges {
				if !t.Run(
					fmt.Sprintf("injection stage %+v", injection),
					func(t *testing.T) { testDMLInjectionCase(t, ts, injection, injectPreCommit) },
				) {
					return
				}
			}
		}
	}
	testDMLInjectionCase = func(t *testing.T, ts CumulativeTestSpec, injection stageKey,
		injectPreCommit bool) {
		// Create a new cluster with the `BeforeStage` knob properly set for the DML injection framework.
		var schemaChangeErrorRegex *regexp.Regexp
		var lastRollbackStageKey *stageKey
		usedStages := make(map[int]struct{})
		successfulStages := 0
		var clusterCreated atomic.Bool
		var tdb *sqlutils.SQLRunner
		ctx := context.Background()
		injectionFunc := ts.stageExecMap.GetInjectionCallback(t, ts.Rewrite)

		knobs := &scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if !clusterCreated.Load() {
					// Do nothing if cluster creation isn't finished. Certain schema
					// changes are run during cluster creation (e.g. `CREATE DATABASE
					// defaultdb`) and we don't want those to hijack this knob.
					return nil
				}

				// if t.Fail/Fatal has been called, try to end the job as quickly as
				// possible and avoid running any more test code.
				if t.Failed() && p.InRollback {
					t.Log("short-circuiting BeforeStage hook due to test failure")
					return nil
				}

				if t.Failed() {
					t.Log("forcing job failure from BeforeStage due to test failure")
					return jobs.MarkAsPermanentJobError(errors.New("t.Failed() is true"))
				}

				s := p.Stages[stageIdx]
				if (injection.phase == p.Stages[stageIdx].Phase &&
					p.Stages[stageIdx].Ordinal >= injection.minOrdinal &&
					p.Stages[stageIdx].Ordinal <= injection.maxOrdinal) ||
					(p.InRollback || p.CurrentState.InRollback) || /* Rollbacks are always injected */
					(p.Stages[stageIdx].Phase == scop.PreCommitPhase && injectPreCommit) {
					jobErrorMutex.Lock()
					defer jobErrorMutex.Unlock()
					key := makeStageKey(s.Phase, s.Ordinal, p.InRollback || p.CurrentState.InRollback)
					if _, ok := usedStages[key.AsInt()]; !ok {
						t.Logf("Injecting into stage: %s", &key)
						// Rollbacks don't count towards the successful count
						if !p.InRollback && !p.CurrentState.InRollback &&
							p.Stages[stageIdx].Phase != scop.PreCommitPhase {
							successfulStages++
						} else {
							lastRollbackStageKey = &key
						}
						injectStmts := injectionFunc(key, tdb, successfulStages)
						regexSetOnce := false
						for _, injectStmt := range injectStmts {
							if injectStmt != nil && injectStmt.HasAnySchemaChangeError() != nil {
								require.Falsef(t, regexSetOnce, "multiple statements are expecting errors in the same phase.")
								schemaChangeErrorRegex = injectStmt.HasAnySchemaChangeError()
								regexSetOnce = true
								t.Logf("Expecting schema change error: %v", schemaChangeErrorRegex)
							}
						}
						usedStages[key.AsInt()] = struct{}{}
						t.Logf("Completed stage: %s", &key)
					} else {
						t.Logf("Retrying stage: %s", &key)
					}
				}
				return nil
			},
		}

		runfn := func(s serverutils.TestServerInterface, db *gosql.DB) {
			clusterCreated.Store(true)
			tdb = sqlutils.MakeSQLRunner(db)

			// Now run the schema change and the `BeforeStage` knob will inject DMLs
			// as specified in `injection`.
			errorDetected := false
			require.NoError(t, setupSchemaChange(ctx, t, ts, db))
			defer waitForSchemaChangesToFinish(t, tdb)
			if err := executeSchemaChangeTxn(ctx, t, ts, db); err != nil {
				// Mute the error if it matches what the DML injection specifies.
				if schemaChangeErrorRegex != nil && schemaChangeErrorRegex.MatchString(err.Error()) {
					errorDetected = true
				} else {
					require.NoError(t, err)
				}
			} else {
				waitForSchemaChangesToFinish(t, tdb)
			}
			// Re-inject anything from the rollback once the job transaction
			// commits, this enforces any sanity checks one last time in
			// the final descriptor state.
			if lastRollbackStageKey != nil {
				t.Logf("Job transaction committed. Re-inject statements from rollback: %s", lastRollbackStageKey.String())
				injectionFunc(*lastRollbackStageKey, tdb, successfulStages)
			}
			require.Equal(t, errorDetected, schemaChangeErrorRegex != nil)
		}
		factory.WithSchemaChangerKnobs(knobs).Run(ctx, t, runfn)
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
func GenerateSchemaChangeCorpus(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

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
	testFunc := func(t *testing.T, ts CumulativeTestSpec) {
		// If any of the statements are not supported, then skip over this
		// file for the corpus.
		for _, stmt := range ts.Stmts {
			if !scbuild.IsFullySupportedWithFalsePositive(stmt.AST, clusterversion.TestingClusterVersion) {
				return
			}
		}
		ctx := context.Background()
		factory.WithSchemaChangerKnobs(&scexec.TestingKnobs{
			BeforeStage: cc.GetBeforeStage("EndToEndCorpus", t),
		}).Run(ctx, t, func(s serverutils.TestServerInterface, db *gosql.DB) {
			require.NoError(t, setupSchemaChange(ctx, t, ts, db))
			require.NoError(t, executeSchemaChangeTxn(ctx, t, ts, db))
			waitForSchemaChangesToFinish(t, sqlutils.MakeSQLRunner(db))
		})
	}
	cumulativeTest(t, path, testFunc)
}

// Pause tests that the schema changer can handle being paused and resumed
// correctly.
func Pause(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		pause(t, factory, cs)
	})
}

// PauseMixedVersion is like Pause but in a mixed-version cluster which gets
// upgraded while the job is paused.
func PauseMixedVersion(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)

	factory.WithMixedVersion()
	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		pause(t, factory, cs)
	})
}

func pause(t *testing.T, factory TestServerFactory, cs CumulativeTestCaseSpec) {
	re := regexp.MustCompile(`job (\d+) was paused before it completed with reason: boom (\d+)`)
	var numInjectedFailures uint32
	knobs := &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			if atomic.LoadUint32(&numInjectedFailures) > 0 {
				return nil
			}
			if s := p.Stages[stageIdx]; cs.Phase == s.Phase && cs.StageOrdinal == s.Ordinal {
				atomic.AddUint32(&numInjectedFailures, 1)
				return jobs.MarkPauseRequestError(errors.Errorf("boom %d", cs.StageOrdinal))
			}
			return nil
		},
	}
	ctx := context.Background()
	// TODO(ajwerner): It'd be nice to assert something about the number of
	// remaining stages before the pause and then after. It's not totally
	// trivial, as we don't checkpoint during non-mutation stages, so we'd
	// need to look back and find the last mutation phase.
	runfn := func(s serverutils.TestServerInterface, db *gosql.DB) {
		tdb := sqlutils.MakeSQLRunner(db)
		// Use shorter liveness heartbeat interval and longer liveness ttl to
		// avoid errors caused by refused connections.
		tdb.Exec(t, `SET CLUSTER SETTING server.sqlliveness.heartbeat = '1s'`)
		tdb.Exec(t, `SET CLUSTER SETTING server.sqlliveness.ttl = '120s'`)

		require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))
		err := executeSchemaChangeTxn(ctx, t, cs.CumulativeTestSpec, db)
		if err != nil {
			// Check that it's a pause error, with a job.
			match := re.FindStringSubmatch(err.Error())
			require.NotNil(t, match)
			idx, err := strconv.Atoi(match[2])
			require.NoError(t, err)
			require.Equal(t, cs.StageOrdinal, idx)
			jobID, err := strconv.Atoi(match[1])
			require.NoError(t, err)

			// Check that the job is paused.
			qStatus := fmt.Sprintf(`SELECT status FROM [SHOW JOB %d]`, jobID)
			tdb.CheckQueryResultsRetry(t, qStatus, [][]string{{"paused"}})
			t.Logf("job %d is paused", jobID)

			// Upgrade the cluster, if applicable.
			tdb.Exec(t, "SET CLUSTER SETTING VERSION=$1", clusterversion.Latest.String())

			// Resume the job and check that it succeeds.
			tdb.Exec(t, "RESUME JOB $1", jobID)
			t.Logf("job %d is resuming", jobID)
			qStatusWithError := fmt.Sprintf(`SELECT status, error FROM [SHOW JOB %d]`, jobID)
			tdb.CheckQueryResultsRetry(t, qStatusWithError, [][]string{{"succeeded", ""}})
		}
		waitForSchemaChangesToFinish(t, tdb)
		require.Equal(t, uint32(1), atomic.LoadUint32(&numInjectedFailures))
	}
	factory.WithSchemaChangerKnobs(knobs).Run(ctx, t, runfn)
}
