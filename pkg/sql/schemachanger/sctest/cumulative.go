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

// rollbackPrepData holds the server and shared scenario state created by
// rollbackPrepare for reuse by rollbackCase.
type rollbackPrepData struct {
	server   TestServer
	scenario *rollbackStageTracker
}

// rollbackStageTracker tracks the currently-executing rollback scenario. The
// BeforeStage knob reads this to decide when to inject a failure and when
// to check EXPLAIN diagrams during rollback. The pointer is shared between
// the knob closure (created in rollbackPrepare) and rollbackCase.
type rollbackStageTracker struct {
	syncutil.Mutex
	active      bool
	targetPhase scop.Phase
	// targetOrdinal is the 1-based ordinal of the post-commit stage where
	// BeforeStage should inject a failure. Set by rollbackCase before each
	// schema change execution.
	targetOrdinal int
	// injectedFailure is set when BeforeStage returns the injected "boom"
	// error. It guards against double-injection and signals rollbackCase
	// whether the failure was actually triggered so it can assert the
	// correct outcome.
	injectedFailure bool
	// checkedExplainInRollback is set when the EXPLAIN diagram check runs
	// during rollback. It ensures the expensive checkExplainDiagrams call
	// happens exactly once — on the first stage after failure injection —
	// and lets rollbackCase assert the check ran.
	checkedExplainInRollback bool
}

// stageDiscovery counts post-commit stages during the first schema change
// execution. It is populated from the BeforeStage knob under its mutex.
type stageDiscovery struct {
	syncutil.Mutex
	counted                      bool
	postCommitCount              int
	postCommitNonRevertibleCount int
}

// countStagesOnce records stage counts from p on the first call; subsequent
// calls are no-ops.
func (d *stageDiscovery) countStagesOnce(p scplan.Plan) {
	d.Lock()
	defer d.Unlock()
	if d.counted {
		return
	}
	d.counted = true
	for _, s := range p.Stages {
		switch s.Phase {
		case scop.PostCommitPhase:
			d.postCommitCount++
		case scop.PostCommitNonRevertiblePhase:
			d.postCommitNonRevertibleCount++
		}
	}
}

// counts returns the discovered stage counts.
func (d *stageDiscovery) counts() (postCommit, postCommitNonRevertible int) {
	d.Lock()
	defer d.Unlock()
	return d.postCommitCount, d.postCommitNonRevertibleCount
}

// postCommitStageCount returns the discovered post-commit stage count.
func (d *stageDiscovery) postCommitStageCount() int {
	d.Lock()
	defer d.Unlock()
	return d.postCommitCount
}

// Rollback tests that the schema changer job rolls back properly.
func Rollback(t *testing.T, relPath string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	var prepData rollbackPrepData
	t.Cleanup(func() {
		if prepData.server.Stopper != nil {
			prepData.server.Stopper(t)
		}
	})
	cumulativeTestForEachPostCommitStage(t, relPath, factory,
		func(t *testing.T, spec CumulativeTestSpec) PrepResult[rollbackPrepData] {
			result := rollbackPrepare(t, factory, spec)
			prepData = result.PrepData
			return result
		}, func(t *testing.T, cs CumulativeTestCaseSpec) {
			rollbackCase(t, prepData, cs)
		}, sampleAllPostCommitRevertible /* samplingFn */)
}

// rollbackPrepare creates a single cluster, runs a discovery pass to count
// stages, and returns the server for reuse by rollbackCase.
func rollbackPrepare(
	t *testing.T, factory TestServerFactory, spec CumulativeTestSpec,
) PrepResult[rollbackPrepData] {
	ctx := context.Background()

	var discovery stageDiscovery

	// The scenario state is shared between the BeforeStage knob and
	// rollbackCase. During discovery, the knob only counts stages; during
	// per-stage test runs, it injects failures.
	scenario := &rollbackStageTracker{}

	knobs := &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			discovery.countStagesOnce(p)

			// Read scenario state under the lock, then perform expensive
			// work (explain diagrams, assertions) outside it so we don't
			// block OnPostCommitError/OnPostCommitPlanError.
			type stageAction struct {
				active                   bool
				inject                   bool
				targetOrdinal            int
				injectedFailure          bool
				checkedExplainInRollback bool
			}
			action := func() stageAction {
				scenario.Lock()
				defer scenario.Unlock()
				a := stageAction{
					active:                   scenario.active,
					targetOrdinal:            scenario.targetOrdinal,
					injectedFailure:          scenario.injectedFailure,
					checkedExplainInRollback: scenario.checkedExplainInRollback,
				}
				if a.active && !a.injectedFailure {
					s := p.Stages[stageIdx]
					if s.Phase == scenario.targetPhase && s.Ordinal == a.targetOrdinal {
						scenario.injectedFailure = true
						a.inject = true
					}
				}
				if a.active && scenario.injectedFailure &&
					!a.checkedExplainInRollback {
					scenario.checkedExplainInRollback = true
				}
				return a
			}()

			if !action.active {
				return nil
			}

			s := p.Stages[stageIdx]
			if action.inject {
				return errors.Errorf("boom %d", action.targetOrdinal)
			}
			if action.injectedFailure {
				// After failure injection, any remaining stage should be
				// non-revertible (rollback).
				require.Equal(t, scop.PostCommitNonRevertiblePhase, s.Phase)
				// EXPLAIN the rollback plan as early as possible.
				if action.checkedExplainInRollback {
					return nil
				}
				postCommitCount := discovery.postCommitStageCount()
				fileNameSuffix := fmt.Sprintf(
					"__rollback_%d_of_%d",
					action.targetOrdinal, postCommitCount,
				)
				explainedStmt := fmt.Sprintf(
					"rollback at post-commit stage %d of %d",
					action.targetOrdinal, postCommitCount,
				)
				const inRollback = true
				checkExplainDiagrams(
					t, spec.Path, spec.Setup, spec.Stmts,
					explainedStmt, fileNameSuffix,
					p.CurrentState, inRollback, spec.Rewrite,
				)
				return nil
			}
			return nil
		},
		OnPostCommitPlanError: func(err error) error {
			scenario.Lock()
			active := scenario.active
			scenario.Unlock()
			if !active {
				return err
			}
			panic(fmt.Sprintf("%+v", err))
		},
		OnPostCommitError: func(
			p scplan.Plan, stageIdx int, err error,
		) error {
			scenario.Lock()
			active := scenario.active
			scenario.Unlock()
			if !active {
				return err
			}
			if strings.Contains(err.Error(), "boom") {
				return err
			}
			panic(fmt.Sprintf("%+v", err))
		},
	}

	args := rollbackPrepData{scenario: scenario}

	// Create one long-lived cluster.
	args.server = factory.WithSchemaChangerKnobs(knobs).Start(ctx, t)
	// Register cleanup immediately after server creation. This is necessary
	// because if the prepare function exits early (e.g. skip or require failure),
	// the caller's prepData closure variable is never hydrated, so the caller's
	// defer will not stop the server. Stopper.Stop is idempotent, so it is safe
	// for the caller's defer to also call it on the normal path.
	t.Cleanup(func() {
		if args.server.Stopper != nil {
			args.server.Stopper(t)
		}
	})
	db := args.server.DB
	db.SetMaxOpenConns(1)
	tdb := sqlutils.MakeSQLRunner(db)

	// Discovery pass: run the schema change to completion to discover stages.
	require.NoError(t, setupSchemaChange(ctx, t, spec, db))
	maybeSkipUnderSecondaryTenant(t, spec, args.server.Server)
	require.NoError(t, executeSchemaChangeTxn(ctx, t, spec, db))

	switchToSystemForDropDB(t, tdb, spec)
	waitForSchemaChangesToFinish(t, tdb)

	dbName, createDatabaseStmt := discoverDatabaseName(t, tdb)

	tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
	after := tdb.QueryStr(t, fetchDescriptorStateQuery)

	postCommitCount, postCommitNonRevertibleCount := discovery.counts()

	return PrepResult[rollbackPrepData]{
		PostCommitCount:              postCommitCount,
		PostCommitNonRevertibleCount: postCommitNonRevertibleCount,
		After:                        after,
		DatabaseName:                 dbName,
		CreateDatabaseStmt:           createDatabaseStmt,
		PrepData:                     args,
	}
}

// rollbackCase runs a single rollback scenario on the shared cluster,
// dropping and recreating the database to get a clean state.
func rollbackCase(t *testing.T, args rollbackPrepData, cs CumulativeTestCaseSpec) {
	if cs.Phase != scop.PostCommitPhase {
		skip.IgnoreLint(t, "cannot roll back outside of post-commit phase")
		return
	}

	ctx := context.Background()
	db := args.server.DB
	tdb := sqlutils.MakeSQLRunner(db)

	// Drop and recreate the database for a clean state.
	tdb.Exec(t, "USE system")
	tdb.Exec(t, "SET use_declarative_schema_changer = 'on'")
	tdb.Exec(t, fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", cs.DatabaseName))

	require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))
	tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))

	before := tdb.QueryStr(t, fetchDescriptorStateQuery)

	// Activate this rollback scenario. Deactivation is deferred so that the
	// scenario is marked inactive even if the test fails mid-execution,
	// preventing the OnPostCommitPlanError knob from panicking on stale state.
	func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		args.scenario.active = true
		args.scenario.targetPhase = cs.Phase
		args.scenario.targetOrdinal = cs.StageOrdinal
		args.scenario.injectedFailure = false
		args.scenario.checkedExplainInRollback = false
	}()
	defer func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		args.scenario.active = false
	}()

	err := executeSchemaChangeTxn(ctx, t, cs.CumulativeTestSpec, db)

	switchToSystemForDropDB(t, tdb, cs.CumulativeTestSpec)
	waitForSchemaChangesToFinish(t, tdb)

	// Capture post-rollback state.
	tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))
	afterRollback := tdb.QueryStr(t, fetchDescriptorStateQuery)

	// Collect results.
	var injectedFailure, checkedExplainInRollback bool
	func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		injectedFailure = args.scenario.injectedFailure
		checkedExplainInRollback = args.scenario.checkedExplainInRollback
	}()

	if !injectedFailure {
		require.NoError(t, err)
	} else {
		require.Regexp(t, fmt.Sprintf("boom %d", cs.StageOrdinal), err)
		require.True(t, checkedExplainInRollback)
		require.Equal(t, before, afterRollback)
	}
}

// switchToSystemForDropDB switches to the system database if the test
// statements include a DROP DATABASE, which would invalidate the current
// database context.
func switchToSystemForDropDB(t *testing.T, tdb *sqlutils.SQLRunner, spec CumulativeTestSpec) {
	for _, stmt := range spec.Stmts {
		if stmt.AST.StatementTag() == "DROP DATABASE" {
			tdb.Exec(t, "USE system")
			return
		}
	}
}

// discoverDatabaseName finds the test database name and its CREATE statement.
// It looks for a non-system database created by the test setup; if none is
// found it falls back to "defaultdb". Callers that need to skip when no
// test-specific database exists should check the returned name.
func discoverDatabaseName(
	t *testing.T, tdb *sqlutils.SQLRunner,
) (dbName string, createStmt string) {
	rows := tdb.QueryStr(
		t,
		"SELECT database_name FROM [SHOW DATABASES] WHERE "+
			"database_name NOT IN ('system', 'postgres', 'defaultdb')",
	)
	dbName = "defaultdb"
	if len(rows) > 0 {
		dbName = rows[0][0]
	}
	res := tdb.QueryStr(
		t,
		fmt.Sprintf(
			"SELECT create_statement FROM [SHOW CREATE DATABASE %q]",
			dbName,
		),
	)
	if len(res) > 0 {
		createStmt = res[0][0]
	}
	return dbName, createStmt
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
						injectStmts := injectionFunc(key, tdb, successfulStages, false /* isReinject */)
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
			maybeSkipUnderSecondaryTenant(t, ts, s)
			tdb = sqlutils.MakeSQLRunner(db)

			// Now run the schema change and the `BeforeStage` knob will inject DMLs
			// as specified in `injection`.
			errorDetected := false
			require.NoError(t, setupSchemaChange(ctx, t, ts, db))
			clusterCreated.Store(true)
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
				injectionFunc(*lastRollbackStageKey, tdb, successfulStages, true /* isReinject */)
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
	skip.UnderDeadlock(t)

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
		var knobEnabled atomic.Bool
		factory.WithSchemaChangerKnobs(&scexec.TestingKnobs{
			BeforeStage: func(p scplan.Plan, stageIdx int) error {
				if !knobEnabled.Load() {
					return nil
				}
				return cc.GetBeforeStage("EndToEndCorpus", t)(p, stageIdx)
			},
		}).Run(ctx, t, func(s serverutils.TestServerInterface, db *gosql.DB) {
			maybeSkipUnderSecondaryTenant(t, ts, s)
			require.NoError(t, setupSchemaChange(ctx, t, ts, db))
			knobEnabled.Swap(true)
			require.NoError(t, executeSchemaChangeTxn(ctx, t, ts, db))
			waitForSchemaChangesToFinish(t, sqlutils.MakeSQLRunner(db))
		})
	}
	cumulativeTest(t, path, testFunc)
}

// pausePrepData holds the server created by pausePrepare and a mutable
// scenario state used to coordinate the BeforeStage knob across
// pause/resume iterations. The scenario pointer is shared with the
// BeforeStage closure and must remain valid for the server's lifetime;
// pauseCase mutates the pointed-to struct between iterations rather than
// replacing the pointer.
type pausePrepData struct {
	server   TestServer
	scenario *pauseScenario
}

// pauseScenario tracks the currently-executing pause scenario. The
// BeforeStage knob reads this to decide when to inject a pause request.
type pauseScenario struct {
	syncutil.Mutex
	active      bool
	targetPhase scop.Phase
	// targetOrdinal is the 1-based ordinal of the post-commit stage where
	// BeforeStage should inject a pause request.
	targetOrdinal int
	// injectedFailure is set when BeforeStage injects the pause request.
	// It guards against double-injection and lets pauseCase assert that
	// the pause was triggered.
	injectedFailure bool
}

// Pause tests that the schema changer can handle being paused and resumed
// correctly.
func Pause(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	var prepData pausePrepData
	defer func() {
		if prepData.server.Stopper != nil {
			prepData.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory,
		func(t *testing.T, spec CumulativeTestSpec) PrepResult[pausePrepData] {
			result := pausePrepare(t, factory, spec)
			prepData = result.PrepData
			return result
		}, func(t *testing.T, cs CumulativeTestCaseSpec) {
			pauseCase(t, prepData, cs)
		},
		sampleAllPostCommitStages /* samplingFn */)
}

// PauseMixedVersion is like Pause but in a mixed-version cluster which gets
// upgraded while the job is paused.
func PauseMixedVersion(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	factory = factory.WithMixedVersion()
	var prepData pausePrepData
	defer func() {
		if prepData.server.Stopper != nil {
			prepData.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory,
		func(t *testing.T, spec CumulativeTestSpec) PrepResult[pausePrepData] {
			result := pausePrepare(t, factory, spec)
			prepData = result.PrepData
			return result
		}, func(t *testing.T, cs CumulativeTestCaseSpec) {
			pauseCase(t, prepData, cs)
		},
		sampleAllPostCommitStages /* samplingFn */)
}

// pausePrepare creates a single cluster, runs a discovery pass to count
// stages, and returns the server for reuse by pauseCase.
func pausePrepare(
	t *testing.T, factory TestServerFactory, spec CumulativeTestSpec,
) PrepResult[pausePrepData] {
	ctx := context.Background()

	var discovery stageDiscovery

	// The scenario state is shared between the BeforeStage knob and
	// pauseCase. During discovery, the knob only counts stages; during
	// per-stage test runs, it injects pause requests.
	scenario := &pauseScenario{}

	knobs := &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			discovery.countStagesOnce(p)

			// Check if a pause scenario is active.
			scenario.Lock()
			defer scenario.Unlock()
			if !scenario.active {
				return nil
			}
			if scenario.injectedFailure {
				return nil
			}
			if s := p.Stages[stageIdx]; scenario.targetPhase == s.Phase &&
				scenario.targetOrdinal == s.Ordinal {
				scenario.injectedFailure = true
				return jobs.MarkPauseRequestError(
					errors.Errorf("boom %d", scenario.targetOrdinal),
				)
			}
			return nil
		},
	}

	args := pausePrepData{scenario: scenario}
	args.server = factory.WithSchemaChangerKnobs(knobs).Start(ctx, t)
	// Register cleanup immediately after server creation. This is necessary
	// because if the prepare function exits early (e.g. skip or require failure),
	// the caller's prepData closure variable is never hydrated, so the caller's
	// defer will not stop the server. Stopper.Stop is idempotent, so it is safe
	// for the caller's defer to also call it on the normal path.
	t.Cleanup(func() {
		if args.server.Stopper != nil {
			args.server.Stopper(t)
		}
	})
	db := args.server.DB
	db.SetMaxOpenConns(1)
	tdb := sqlutils.MakeSQLRunner(db)

	// Use shorter liveness heartbeat interval and longer liveness ttl to
	// avoid errors caused by refused connections.
	tdb.Exec(t, `SET CLUSTER SETTING server.sqlliveness.heartbeat = '1s'`)
	tdb.Exec(t, `SET CLUSTER SETTING server.sqlliveness.ttl = '120s'`)

	// Discovery pass: run the schema change to completion to discover stages.
	require.NoError(t, setupSchemaChange(ctx, t, spec, db))
	maybeSkipUnderSecondaryTenant(t, spec, args.server.Server)
	require.NoError(t, executeSchemaChangeTxn(ctx, t, spec, db))

	switchToSystemForDropDB(t, tdb, spec)
	waitForSchemaChangesToFinish(t, tdb)

	dbName, createDatabaseStmt := discoverDatabaseName(t, tdb)

	tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
	after := tdb.QueryStr(t, fetchDescriptorStateQuery)

	postCommitCount, postCommitNonRevertibleCount := discovery.counts()

	return PrepResult[pausePrepData]{
		PostCommitCount:              postCommitCount,
		PostCommitNonRevertibleCount: postCommitNonRevertibleCount,
		After:                        after,
		DatabaseName:                 dbName,
		CreateDatabaseStmt:           createDatabaseStmt,
		PrepData:                     args,
	}
}

// pauseCase runs a single pause/resume scenario on an existing cluster,
// dropping and recreating the database to get a clean state.
func pauseCase(t *testing.T, args pausePrepData, cs CumulativeTestCaseSpec) {
	re := regexp.MustCompile(
		`job (\d+) was paused before it completed with reason: boom (\d+)`,
	)
	ctx := context.Background()
	db := args.server.DB
	tdb := sqlutils.MakeSQLRunner(db)

	// Drop and recreate the database for a clean state.
	tdb.Exec(t, "USE system")
	tdb.Exec(t, "SET use_declarative_schema_changer = 'on'")
	tdb.Exec(
		t,
		fmt.Sprintf(
			"DROP DATABASE IF EXISTS %q CASCADE", cs.DatabaseName,
		),
	)

	require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))

	// Activate this pause scenario. Deactivation is deferred so that the
	// scenario is marked inactive even if the test fails mid-execution.
	func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		args.scenario.active = true
		args.scenario.targetPhase = cs.Phase
		args.scenario.targetOrdinal = cs.StageOrdinal
		args.scenario.injectedFailure = false
	}()
	defer func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		args.scenario.active = false
	}()

	// TODO(ajwerner): It'd be nice to assert something about the number of
	// remaining stages before the pause and then after. It's not totally
	// trivial, as we don't checkpoint during non-mutation stages, so we'd
	// need to look back and find the last mutation phase.
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
		qStatus := fmt.Sprintf(
			`SELECT status FROM [SHOW JOB %d]`, jobID,
		)
		tdb.CheckQueryResultsRetry(t, qStatus, [][]string{{"paused"}})
		t.Logf("job %d is paused", jobID)

		// Upgrade the cluster, if applicable.
		tdb.Exec(
			t, "SET CLUSTER SETTING VERSION=$1",
			clusterversion.Latest.String(),
		)

		// Resume the job and check that it succeeds.
		tdb.Exec(t, "RESUME JOB $1", jobID)
		t.Logf("job %d is resuming", jobID)
		waitForSchemaChangesToFinish(t, tdb)
		qStatusWithError := fmt.Sprintf(
			`SELECT status, error FROM [SHOW JOB %d]`, jobID,
		)
		tdb.CheckQueryResults(
			t, qStatusWithError, [][]string{{"succeeded", ""}},
		)
	}
	waitForSchemaChangesToFinish(t, tdb)

	var injected bool
	func() {
		args.scenario.Lock()
		defer args.scenario.Unlock()
		injected = args.scenario.injectedFailure
	}()
	require.True(t, injected)
}
