// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// BackupSuccess tests that the schema changer can handle being backed up
// any time during a successful schema change.
func BackupSuccess(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	var backupArgs backupSuccessTestArgs
	defer func() {
		if backupArgs.server.Stopper != nil {
			backupArgs.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory,
		func(t *testing.T, spec CumulativeTestSpec) PrepResult[backupSuccessTestArgs] {
			result := backupSuccessPrepare(t, factory, spec)
			backupArgs = result.PrepData
			return result
		}, func(t *testing.T, cs CumulativeTestCaseSpec) {
			backupSuccess(t, backupArgs, cs)
		},
		nil /*samplingFn */)
}

// BackupRollbacks tests that the schema changer can handle being backed up
// any time during any rollback of a failed schema change.
func BackupRollbacks(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)
	// These tests are only marginally more useful than BackupSuccess
	// and at least as expensive to run.
	skip.UnderShort(t)
	factory = factory.WithSchemaLockDisabled()
	var rollbackArgs backupRollbacksTestArgs
	defer func() {
		if rollbackArgs.server.Stopper != nil {
			rollbackArgs.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory,
		func(t *testing.T, spec CumulativeTestSpec) PrepResult[backupRollbacksTestArgs] {
			result := backupRollbacksPrepare(t, factory, spec)
			rollbackArgs = result.PrepData
			return result
		}, func(t *testing.T, cs CumulativeTestCaseSpec) {
			backupRollbacks(t, rollbackArgs, cs, false /*isMixedVersion*/)
		},
		nil /* samplingFn */)
}

// BackupSuccessMixedVersion is like BackupSuccess but in a mixed-version
// cluster which is upgraded prior to restoring the backups.
func BackupSuccessMixedVersion(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)
	// These tests are only marginally more useful than BackupSuccess
	// and at least as expensive to run.
	skip.UnderShort(t)
	factory = factory.WithMixedVersion()
	var backupArgs backupSuccessTestArgs
	defer func() {
		if backupArgs.server.Stopper != nil {
			backupArgs.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, spec CumulativeTestSpec) PrepResult[backupSuccessTestArgs] {
		result := backupSuccessPrepare(t, factory, spec)
		backupArgs = result.PrepData
		if backupArgs.isMultiRegion {
			// Speed up the mixed version multiregion cases by excluding restores of individual
			// tables in the mixed version state.
			backupArgs.excludeAllTablesInDatabaseFlavor = true
		}
		return result
	}, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupSuccess(t, backupArgs, cs)
	},
		nil /* samplingFn */)
}

// BackupRollbacksMixedVersion is like BackupRollbacks but in a mixed-version
// cluster which is upgraded prior to restoring the backups.
func BackupRollbacksMixedVersion(t *testing.T, path string, factory TestServerFactory) {
	// These tests are expensive.
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)
	// These tests are only marginally more useful than BackupSuccess
	// and at least as expensive to run.
	skip.UnderShort(t)

	factory = factory.WithMixedVersion()
	// Disable schema_locked in mixed version tests, since we do not support
	// disabling schema_locked in a mixed version state yet.
	factory = factory.WithSchemaLockDisabled()
	var rollbackArgs backupRollbacksTestArgs
	defer func() {
		if rollbackArgs.server.Stopper != nil {
			rollbackArgs.server.Stopper(t)
		}
	}()
	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, spec CumulativeTestSpec) PrepResult[backupRollbacksTestArgs] {
		result := backupRollbacksPrepare(t, factory, spec)
		rollbackArgs = result.PrepData
		return result
	}, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupRollbacks(t, rollbackArgs, cs, true /*isMixedVersion*/)
	}, nil /* samplingFn */)
}

// runAllBackups runs all the backup tests, disabling the random skipping.
var runAllBackups = flag.Bool(
	"run-all-backups", false,
	"if true, run all backups instead of a random subset",
)

const skipRate = .6

// Stop once max stages has been hit.
const maxStagesToTest = 8
const maxStagesToTestMultiRegion = 4

// shouldSkipBackup samples about 60% of total stages or up to
// max stages.
func shouldSkipBackup(currentIdx int, totalBackups int, isMultiRegion bool) bool {
	// Sample at least one stage.
	sampleCount := max(int(math.Ceil(float64(totalBackups)*skipRate)), 1)
	// Clamp the maximum number of stages we can test.
	sampleCount = min(maxStagesToTest, sampleCount)
	if isMultiRegion {
		sampleCount = min(maxStagesToTestMultiRegion, sampleCount)
	}
	if !*runAllBackups &&
		(currentIdx >= sampleCount) {
		return true
	}
	return false
}

func maybeRandomlySkip(t *testing.T) {
	if !*runAllBackups && rand.Float64() < skipRate {
		skip.IgnoreLint(t, "skipping due to randomness")
	}
}

// backupKey is the key for the stage / phase of a schema changer job
// that has been backed up.
type backupKey struct {
	Phase   scop.Phase
	Ordinal int
}

// backupInfo information about individual backups.
type backupInfo struct {
	url            string
	isPostBackfill bool
}

// backupSuccessTestArgs are arguments generated by backupSuccessPrepare
type backupSuccessTestArgs struct {
	server                           TestServer
	backups                          map[backupKey]backupInfo
	before                           [][]string
	after                            [][]string
	pe                               PlanExplainer
	isMultiRegion                    bool
	excludeAllTablesInDatabaseFlavor bool
}

// rollbackScenario contains all data for testing one rollback scenario
type rollbackScenario struct {
	databaseName string     // database containing the schema change
	urls         []string   // Backup URLs taken during rollback
	expected     [][]string // Expected descriptor state after rollback
}

// backupRollbacksTestArgs are arguments generated by backupRollbacksPrepare
type backupRollbacksTestArgs struct {
	server    TestServer
	scenarios map[backupKey]rollbackScenario
}

// backupSuccessPrepare sets up a server, executes the schema change, and
// collects backups at each stage. It returns a PrepResult containing stage
// counts and test data, eliminating the need for a throwaway cluster to
// count stages before running tests.
func backupSuccessPrepare(
	t *testing.T, factory TestServerFactory, spec CumulativeTestSpec,
) PrepResult[backupSuccessTestArgs] {
	ctx := context.Background()

	backupArgs := backupSuccessTestArgs{
		backups: make(map[backupKey]backupInfo),
	}
	var mu struct {
		syncutil.Mutex
		postCommitCount              int
		postCommitNonRevertibleCount int
		stageCounted                 bool
		hasBackfill                  bool
		mutationAfterBackfill        bool
	}
	backupsToTake := make(map[backupKey]string)
	var dbForBackup atomic.Pointer[gosql.DB]
	var knobEnabled atomic.Bool
	var dbName string
	var createDatabaseStmt string
	backupArgs.pe = MakePlanExplainer()
	knobs := &scexec.TestingKnobs{
		// BeforeStage serves two purposes:
		// 1. Count stages during the first plan encounter to populate PrepResult.
		// 2. Collect backups at each stage during the main schema change execution.
		// The hook fires for both the original schema change and post-RESTORE
		// schema changes, so guards are necessary to avoid duplicate work.
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			// Only enabled after setup.
			if !knobEnabled.Load() {
				return nil
			}
			// Collect EXPLAIN (DDL) diagram for debug purposes.
			if err := backupArgs.pe.MaybeUpdateWithPlan(p); err != nil {
				return errors.Wrapf(err, "updating plan explainer (stage=%d, phase=%s)",
					stageIdx, p.Stages[stageIdx].Phase)
			}
			// Since this callback is also active during post-RESTORE
			// schema changes, we have to be careful to exit early in those cases.
			if dbForBackup.Load() == nil {
				return nil
			}

			// Count stages and collect backup info (all under single lock)
			mu.Lock()
			defer mu.Unlock()

			// Count stages on first plan encounter only.
			// The BeforeStage hook fires for both the original schema change and
			// post-RESTORE schema changes, so we guard against double-counting.
			if !mu.stageCounted {
				mu.stageCounted = true
				for _, s := range p.Stages {
					switch s.Phase {
					case scop.PostCommitPhase:
						mu.postCommitCount++
					case scop.PostCommitNonRevertiblePhase:
						mu.postCommitNonRevertibleCount++
					}
				}
			}

			key := backupKey{
				Phase:   p.Stages[stageIdx].Phase,
				Ordinal: p.Stages[stageIdx].Ordinal,
			}

			url := fmt.Sprintf("nodelocal://1/data_%s_%d",
				key.Phase, key.Ordinal)
			backupStmt := fmt.Sprintf("BACKUP DATABASE %s INTO '%s' AS OF SYSTEM TIME '%s'",
				dbName,
				url,
				backupArgs.server.Server.Clock().Now().AsOfSystemTime())

			// Before backing up, check whether the plan executed so far includes
			// backfill operations which are not going to be replayed post-RESTORE.
			// Backing up at this point may lead the post-RESTORE schema change to fail.
			if p.Stages[stageIdx].Type() == scop.BackfillType {
				mu.hasBackfill = true
			} else if p.Stages[stageIdx].Type() == scop.MutationType && mu.hasBackfill {
				mu.mutationAfterBackfill = true
			}
			backupArgs.backups[key] = backupInfo{
				url:            url,
				isPostBackfill: mu.mutationAfterBackfill,
			}
			backupsToTake[key] = backupStmt
			return nil
		},
	}
	// Setup a server for the backup test cases.
	backupArgs.server = factory.WithSchemaChangerKnobs(knobs).Start(ctx, t)
	// Register cleanup immediately after server creation. This is necessary
	// because if the prepare function exits early (e.g. skip or require failure),
	// the caller's backupArgs closure variable is never hydrated, so the caller's
	// defer will not stop the server. Stopper.Stop is idempotent, so it is safe
	// for the caller's defer to also call it on the normal path.
	t.Cleanup(func() {
		if backupArgs.server.Stopper != nil {
			backupArgs.server.Stopper(t)
		}
	})
	db := backupArgs.server.DB
	db.SetMaxOpenConns(1)
	dbForBackup.Store(db)
	tdb := sqlutils.MakeSQLRunner(db)
	require.NoError(t, setupSchemaChange(ctx, t, spec, db))

	// Skip test if it requires system tenant operations and we're running under a secondary tenant.
	maybeSkipUnderSecondaryTenant(t, spec, backupArgs.server.Server)

	// Determine which database contains the schema change before enabling knobs.
	// Some tests create a separate database (e.g. "db"), others use defaultdb.
	rows := tdb.QueryStr(t, "SELECT database_name FROM [SHOW DATABASES] WHERE database_name NOT IN ('system', 'postgres', 'defaultdb')")
	if len(rows) > 0 {
		dbName = rows[0][0]
	} else {
		dbName = "defaultdb"
	}
	res := tdb.QueryStr(t, fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE DATABASE %q]", dbName))
	if len(res) > 0 {
		createDatabaseStmt = res[0][0]
	}

	// Enable knobs - this will capture stage counts and plan when schema change runs.
	knobEnabled.Swap(true)

	// Fetch the state of the cluster before the schema change kicks off.
	tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
	backupArgs.before = parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

	// Kick off the schema change and perform the backup via the BeforeStage hook.
	// The knob will count stages and collect backup information.
	require.NoError(t, executeSchemaChangeTxn(ctx, t, spec, db))

	// Check if we dropped the database, and switch to system database if so.
	// For DROP DATABASE, the database no longer exists, so we can't fetch its state.
	droppedDatabase := false
	for _, stmt := range spec.Stmts {
		if stmt.AST.StatementTag() == "DROP DATABASE" {
			droppedDatabase = true
			tdb.Exec(t, "USE system")
			break
		}
	}

	require.True(t, hasLatestSchemaChangeSucceeded(t, tdb))

	// Fetch the state of the cluster after the schema change succeeds.
	if !droppedDatabase {
		tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
		backupArgs.after = parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))
	} else {
		// For DROP DATABASE, "after" state is empty since the database is gone.
		backupArgs.after = [][]string{}
	}

	knobEnabled.Store(false)
	dbForBackup.Store(nil)

	// Randomize the list of backups to take.
	backupList := make([]backupKey, 0, len(backupsToTake))
	for key := range backupsToTake {
		backupList = append(backupList, key)
	}
	rand.Shuffle(len(backupList), func(i, j int) {
		backupList[i], backupList[j] = backupList[j], backupList[i]
	})
	// Take all the required backups using the timestamps gathered.
	numStagesIncluded := 0
	tdb.QueryRow(t, "SELECT count(*) >1  FROM [SHOW REGIONS]").Scan(&backupArgs.isMultiRegion)
	for key := range backupsToTake {
		// Determine if we have too many backups to test. We will sample
		// at most 8 stages or 60% stages, whichever is smaller.
		if shouldSkipBackup(numStagesIncluded, len(backupsToTake), backupArgs.isMultiRegion) {
			backupArgs.backups[key] = backupInfo{}
			continue
		}
		// For DROP DATABASE, the database might not exist at this stage, so we need
		// to check and skip the backup if it fails.
		_, err := tdb.DB.ExecContext(ctx, backupsToTake[key])
		if err != nil && droppedDatabase {
			// Database doesn't exist, skip this backup.
			t.Logf("Skipping backup for dropped database (key=%v): %v", key, err)
			backupArgs.backups[key] = backupInfo{}
			continue
		}
		require.NoError(t, err, "failed to take backup: %s", backupsToTake[key])
		numStagesIncluded += 1
	}

	var postCommitCount, postCommitNonRevertibleCount int
	func() {
		mu.Lock()
		defer mu.Unlock()
		postCommitCount = mu.postCommitCount
		postCommitNonRevertibleCount = mu.postCommitNonRevertibleCount
	}()

	return PrepResult[backupSuccessTestArgs]{
		PostCommitCount:              postCommitCount,
		PostCommitNonRevertibleCount: postCommitNonRevertibleCount,
		After:                        backupArgs.after,
		DatabaseName:                 dbName,
		CreateDatabaseStmt:           createDatabaseStmt,
		PrepData:                     backupArgs,
	}
}

// backupRollbacksPrepare sets up a server and runs all rollback scenarios,
// collecting backups for each. It reuses a single cluster by dropping and
// recreating the test database for each rollback scenario.
func backupRollbacksPrepare(
	t *testing.T, factory TestServerFactory, spec CumulativeTestSpec,
) PrepResult[backupRollbacksTestArgs] {
	ctx := context.Background()
	args := backupRollbacksTestArgs{
		scenarios: make(map[backupKey]rollbackScenario),
	}

	// discovery is populated once during the discovery run (phase 1) and
	// read-only thereafter. It records stage counts and which stages to test.
	var discovery struct {
		syncutil.Mutex
		counted                      bool
		postCommitCount              int
		postCommitNonRevertibleCount int
		stagesToTest                 []backupKey
	}

	// scenario tracks the currently-executing rollback scenario (phase 2).
	// It is reset before each iteration and read by the BeforeStage hook.
	var scenario struct {
		syncutil.Mutex
		active      bool
		key         backupKey
		dbName      string
		urls        []string
		urlsSamples map[string]struct{}
	}

	// Create knobs that handle both discovery and rollback phases.
	knobs := &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			// Count stages on first plan encounter (discovery phase).
			func() {
				discovery.Lock()
				defer discovery.Unlock()
				if !discovery.counted {
					discovery.counted = true
					for _, s := range p.Stages {
						switch s.Phase {
						case scop.PostCommitPhase:
							discovery.postCommitCount++
							discovery.stagesToTest = append(discovery.stagesToTest, backupKey{
								Phase:   s.Phase,
								Ordinal: s.Ordinal,
							})
						case scop.PostCommitNonRevertiblePhase:
							discovery.postCommitNonRevertibleCount++
						}
					}
				}
			}()

			// Check if a rollback scenario is active and determine
			// what action to take.
			var shouldBackup bool
			var backupStmt string
			var shouldFail bool
			var failOrdinal int
			var activeKey backupKey

			func() {
				scenario.Lock()
				defer scenario.Unlock()

				if !scenario.active {
					return
				}
				activeKey = scenario.key

				// Inject failure at target stage.
				if s := p.Stages[stageIdx]; s.Phase == activeKey.Phase && s.Ordinal == activeKey.Ordinal {
					shouldFail = true
					failOrdinal = activeKey.Ordinal
					return
				}

				// Collect backups during rollback.
				// Skip stage 0 (first rollback stage) because the schema changer state
				// is identical to pre-rollback state due to how rollback initialization works.
				// Restoring this backup wouldn't test anything meaningful.
				if p.InRollback && stageIdx > 0 {
					url := fmt.Sprintf("nodelocal://1/data_%s_%d_%d",
						activeKey.Phase, activeKey.Ordinal, stageIdx)
					if _, ok := scenario.urlsSamples[url]; !ok {
						scenario.urlsSamples[url] = struct{}{}
						scenario.urls = append(scenario.urls, url)
						backupStmt = fmt.Sprintf("BACKUP DATABASE %s INTO '%s'", scenario.dbName, url)
						shouldBackup = true
					}
				}
			}()

			if shouldFail {
				return errors.Newf("boom %d", failOrdinal)
			}
			if shouldBackup {
				_, err := args.server.DB.ExecContext(ctx, backupStmt)
				if err != nil {
					return errors.Wrapf(err, "backup during rollback (phase=%s, ordinal=%d, stage=%d)",
						activeKey.Phase, activeKey.Ordinal, stageIdx)
				}
			}
			return nil
		},
	}

	// Create one long-lived cluster for discovery + all rollback scenarios.
	args.server = factory.WithSchemaChangerKnobs(knobs).Start(ctx, t)
	// Register cleanup immediately after server creation. This is necessary
	// because if the prepare function exits early (e.g. skip or require failure),
	// the caller's rollbackArgs closure variable is never hydrated, so the
	// caller's defer will not stop the server. Stopper.Stop is idempotent, so it
	// is safe for the caller's defer to also call it on the normal path.
	t.Cleanup(func() {
		if args.server.Stopper != nil {
			args.server.Stopper(t)
		}
	})
	db := args.server.DB
	db.SetMaxOpenConns(1)
	tdb := sqlutils.MakeSQLRunner(db)

	// Phase 1: Discovery — run the schema change to completion to discover stages.
	require.NoError(t, setupSchemaChange(ctx, t, spec, db))

	// Skip test if it requires system tenant operations and we're running under a secondary tenant.
	maybeSkipUnderSecondaryTenant(t, spec, args.server.Server)

	require.NoError(t, executeSchemaChangeTxn(ctx, t, spec, db))

	// Switch to system database before waiting for jobs if we might have dropped
	// the current database. This prevents "database does not exist" errors.
	for _, stmt := range spec.Stmts {
		if stmt.AST.StatementTag() == "DROP DATABASE" {
			tdb.Exec(t, "USE system")
			break
		}
	}

	waitForSchemaChangesToFinish(t, tdb)

	// Discover the database name and CREATE DATABASE statement.
	// Some tests create a separate database (e.g. "db"), others use defaultdb.
	rows := tdb.QueryStr(t, "SELECT database_name FROM [SHOW DATABASES] WHERE database_name NOT IN ('system', 'postgres', 'defaultdb')")
	dbName := "defaultdb"
	var createDatabaseStmt string
	if len(rows) > 0 {
		dbName = rows[0][0]
	}
	res := tdb.QueryStr(t, fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE DATABASE %q]", dbName))
	if len(res) > 0 {
		createDatabaseStmt = res[0][0]
	}

	// Phase 2: Run each rollback scenario by dropping and recreating the database.
	// Discovery is complete, so stagesToTest is read-only from here on.
	var stagesToTest []backupKey
	func() {
		discovery.Lock()
		defer discovery.Unlock()
		stagesToTest = append([]backupKey{}, discovery.stagesToTest...)
	}()

	for _, key := range stagesToTest {
		// Switch to system database before dropping the test database.
		tdb.Exec(t, "USE system")

		// Reset use_declarative_schema_changer for subsequent operations.
		// executeSchemaChangeTxn sets 'unsafe_always' which persists on the
		// connection and breaks setupSchemaChange's CREATE TABLE.
		tdb.Exec(t, "SET use_declarative_schema_changer = 'on'")

		// Drop the database from the previous iteration and let setupSchemaChange recreate it.
		tdb.Exec(t, fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", dbName))

		// Re-run setup from scratch.
		require.NoError(t, setupSchemaChange(ctx, t, spec, db))

		tdb.Exec(t, fmt.Sprintf("USE %q", dbName))

		// Capture expected state before schema change.
		expected := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

		// Store the highest job ID to track relevant jobs.
		var maxJobID int64
		tdb.QueryRow(t, "SELECT COALESCE(max(job_id), 0) FROM [SHOW JOBS]").Scan(&maxJobID)

		// Activate this scenario in the knobs.
		func() {
			scenario.Lock()
			defer scenario.Unlock()
			scenario.key = key
			scenario.dbName = dbName
			scenario.urls = nil
			scenario.urlsSamples = make(map[string]struct{})
			scenario.active = true
		}()

		// Execute schema change with failure injection.
		require.Regexp(t, fmt.Sprintf("boom %d", key.Ordinal),
			executeSchemaChangeTxn(ctx, t, spec, db))

		// Switch to system database before waiting for rollback if we might have
		// dropped the current database. This prevents "database does not exist" errors.
		for _, stmt := range spec.Stmts {
			if stmt.AST.StatementTag() == "DROP DATABASE" {
				tdb.Exec(t, "USE system")
				break
			}
		}

		// Wait for rollback to complete.
		waitForSchemaChangesToFinish(t, tdb)

		// Deactivate scenario and collect URLs.
		var urls []string
		func() {
			scenario.Lock()
			defer scenario.Unlock()
			scenario.active = false
			urls = append([]string{}, scenario.urls...)
		}()

		// Verify rollback was successful.
		succeeded, jobExists := hasLatestSchemaChangeSucceededWithMaxJobID(t, tdb, maxJobID)
		require.False(t, succeeded && jobExists)

		// Fetch state after rollback and verify it matches expected.
		tdb.Exec(t, fmt.Sprintf("USE %q", dbName))
		postRollback := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))
		require.Equal(t, expected, postRollback, "rolled back schema change should be no-op")

		// Store scenario for later test execution.
		args.scenarios[key] = rollbackScenario{
			databaseName: dbName,
			urls:         urls,
			expected:     postRollback,
		}
	}

	// Discovery is complete; extract final counts.
	var postCommitCount, postCommitNonRevertibleCount int
	func() {
		discovery.Lock()
		defer discovery.Unlock()
		postCommitCount = discovery.postCommitCount
		postCommitNonRevertibleCount = discovery.postCommitNonRevertibleCount
	}()

	return PrepResult[backupRollbacksTestArgs]{
		PostCommitCount:              postCommitCount,
		PostCommitNonRevertibleCount: postCommitNonRevertibleCount,
		DatabaseName:                 dbName,
		CreateDatabaseStmt:           createDatabaseStmt,
		PrepData:                     args,
	}
}

// backupSuccess executes cumulative tests against an existing server containing
// all required backups. backupSuccessPrepare must be invoked first to generate
// backups.
func backupSuccess(t *testing.T, args backupSuccessTestArgs, cs CumulativeTestCaseSpec) {
	// Skip comparing outputs, if there are any newly created objects, since
	// in transactional cases these may not exist within the image. We include
	// all relations and types since those are excluded in Adding state from
	// being restored. For functions this logic does not apply and they are always
	// backed up.
	skipOutputComparison := false
	skipTags := map[string]struct{}{
		"CREATE TABLE":    {},
		"CREATE SEQUENCE": {},
		"CREATE VIEW":     {},
		"CREATE TYPE":     {},
		"CREATE SCHEMA":   {},
		"CREATE DATABASE": {},
		"CREATE FUNCTION": {},
	}
	for _, stmt := range cs.Stmts {
		if _, found := skipTags[stmt.AST.StatementTag()]; found && len(cs.Stmts) > 1 {
			t.Logf("Skipping comparison of state after schema changes because new "+
				"objects are being created, which may not be persisted %s\n", stmt.AST.String())
			skipOutputComparison = true
		}
	}
	key := backupKey{
		Phase:   cs.Phase,
		Ordinal: cs.StageOrdinal,
	}
	// Determine whether the post-RESTORE schema change may perhaps
	// be rolled back.
	info := args.backups[key]
	if info.url == "" {
		skip.IgnoreLintf(t, "no backup for %v", key)
	}
	b := backupRestoreOutcome{
		url:                              info.url,
		maySucceed:                       true,
		expectedOnSuccess:                args.after,
		skipComparisonOnSuccess:          skipOutputComparison,
		mayRollback:                      false,
		expectedOnRollback:               args.before,
		excludeAllTablesInDatabaseFlavor: args.excludeAllTablesInDatabaseFlavor,
	}
	tdb := sqlutils.MakeSQLRunner(args.server.DB)
	if info.isPostBackfill {
		const countRowsQ = `
				SELECT coalesce(sum(rows), 0)
				FROM [SHOW BACKUP FROM LATEST IN $2]
				WHERE database_name = $1`
		rowCount := tdb.QueryStr(t, countRowsQ, cs.DatabaseName, info.url)
		if rowCount[0][0] != "0" {
			// The backed up database has tables with rows in them.
			// We expect the test cases to not populate the tables with rows
			// unless they're relevant to the tested schema change.
			// Considering that the schema change has had backfill operations
			// performed before the backup took place, we can infer that the
			// post-RESTORE schema change will fail.
			b.mayRollback = true
		}
	}

	// Upgrade the cluster if applicable.
	if args.server.Server.StartedDefaultTestTenant() {
		sysDB := args.server.Server.SystemLayer().SQLConn(t)
		_, err := sysDB.Exec("SET CLUSTER SETTING VERSION = $1", clusterversion.Latest.String())
		require.NoError(t, err)
	}
	tdb.Exec(t, "SET CLUSTER SETTING VERSION = $1", clusterversion.Latest.String())

	// Restore the backup of the database taken mid-successful-schema-change
	// in various ways, check that it ends up in the same state as present.
	exerciseBackupRestore(t, tdb, cs, b, args.pe)
}

func backupRollbacks(
	t *testing.T, args backupRollbacksTestArgs, cs CumulativeTestCaseSpec, isMixedVersion bool,
) {
	if cs.Phase != scop.PostCommitPhase {
		return
	}

	// TODO(msbutler): perhaps we can remove this, now that we're not creating a
	// cluster for every rollback.
	maybeRandomlySkip(t)

	key := backupKey{Phase: cs.Phase, Ordinal: cs.StageOrdinal}
	scenario, ok := args.scenarios[key]
	if !ok {
		skip.IgnoreLintf(t, "no rollback scenario for %v", key)
	}

	tdb := sqlutils.MakeSQLRunner(args.server.DB)

	// Upgrade cluster if mixed version.
	if isMixedVersion {
		if args.server.Server.StartedDefaultTestTenant() {
			sysDB := args.server.Server.SystemLayer().SQLConn(t)
			_, err := sysDB.Exec("SET CLUSTER SETTING VERSION = $1", clusterversion.Latest.String())
			require.NoError(t, err)
		}
		tdb.Exec(t, "SET CLUSTER SETTING VERSION = $1", clusterversion.Latest.String())
	}

	// Switch to system database which always exists (prior restore tests may
	// have dropped the test database).
	tdb.Exec(t, "USE system")

	// Check if multi-region for test optimization.
	isMultiRegion := false
	tdb.QueryRow(t, "SELECT count(*) >1 FROM [SHOW REGIONS]").Scan(&isMultiRegion)

	pe := MakePlanExplainer()

	// Test each backup from this rollback scenario.
	for i, url := range scenario.urls {
		b := backupRestoreOutcome{
			url:                url,
			mayRollback:        true,
			expectedOnRollback: scenario.expected,
			// Speed up multi-region or mixed version variants by excluding
			// individual table restores, due to the slower speed due to multiple nodes.
			excludeAllTablesInDatabaseFlavor: isMultiRegion || isMixedVersion,
		}
		name := fmt.Sprintf("post_%d_rollback_stages_exec", i+1)
		t.Run(name, func(t *testing.T) {
			exerciseBackupRestore(t, tdb, cs, b, pe)
		})
	}
}

type backupRestoreOutcome struct {
	url                                   string
	maySucceed, mayRollback               bool
	expectedOnSuccess, expectedOnRollback [][]string
	skipComparisonOnSuccess               bool
	excludeAllTablesInDatabaseFlavor      bool
}

func exerciseBackupRestore(
	t *testing.T,
	tdb *sqlutils.SQLRunner,
	cs CumulativeTestCaseSpec,
	b backupRestoreOutcome,
	pe PlanExplainer,
) {
	// Get backup contents.
	const showQ = `
			SELECT parent_schema_name, object_name, object_type 
			FROM [SHOW BACKUP FROM LATEST IN $2]
			WHERE database_name = $1`
	backupContents := tdb.QueryStr(t, showQ, cs.DatabaseName, b.url)

	// Backups can be exercised in various flavors:
	// 1. RESTORE DATABASE
	// 2. RESTORE DATABASE WITH schema_only
	// 3. RESTORE TABLE tbl1, tbl2, ..., tblN
	// We then assert that the restored database should correctly finish
	// the ongoing schema change job when the backup was taken, and
	// reaches the expected state as if the backup/restore had not happened
	// at all.
	type restoreFlavor string
	const (
		restoreDatabase               restoreFlavor = "restore_database"
		restoreDatabaseWithSchemaOnly restoreFlavor = "restore_database_with_schema_only"
		restoreAllTablesInDatabase    restoreFlavor = "restore_all_tables_in_database"
		// TODO (xiang): Add here the fourth flavor that restores
		// only a subset, maybe randomly chosen, of all tables with
		// `RESTORE TABLE`. Currently, it's blocked by issue #87518.
		// We will need to change what the expected output will be
		// in this case, since it will no longer be simply `before`
		// and `after`.
	)

	type restoreCase struct {
		restoreFlavor
		stmt string
	}

	var allRestoreCases = []restoreCase{
		{
			restoreFlavor: restoreDatabaseWithSchemaOnly,
			stmt: fmt.Sprintf("RESTORE DATABASE %q FROM LATEST IN '%s' WITH schema_only",
				cs.DatabaseName, b.url),
		},
		{
			restoreFlavor: restoreDatabase,
			stmt: fmt.Sprintf("RESTORE DATABASE %q FROM LATEST IN '%s'",
				cs.DatabaseName, b.url),
		},
	}

	// Determine which tables to restore when restoring tables only.
	if !b.excludeAllTablesInDatabaseFlavor {
		var tablesToRestore []string
	Loop:
		for _, row := range backupContents {
			switch row[2] {
			case "table":
				tn := tree.MakeTableNameWithSchema(
					tree.Name(cs.DatabaseName), tree.Name(row[0]), tree.Name(row[1]),
				)
				tablesToRestore = append(tablesToRestore, tn.FQString())
			case "schema":
				if row[1] != "public" {
					tablesToRestore = nil
					break Loop
				}
			}
		}
		if len(tablesToRestore) > 0 {
			allRestoreCases = append(allRestoreCases, restoreCase{
				restoreFlavor: restoreAllTablesInDatabase,
				stmt: fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN '%s' WITH "+
					"skip_missing_sequences, skip_missing_udfs", strings.Join(tablesToRestore, ","), b.url),
			})
		}
	}

	for _, rc := range allRestoreCases {
		t.Run(string(rc.restoreFlavor), func(t *testing.T) {
			if rc.restoreFlavor != restoreDatabaseWithSchemaOnly {
				// Testing any of the other flavors in addition to this one is only
				// of marginal increased usefulness.
				skip.UnderShort(t)
			}

			// Set up prior to RESTORE.
			tdb.Exec(t, "SET use_declarative_schema_changer = 'off'")
			tdb.Exec(t, fmt.Sprintf("DROP DATABASE IF EXISTS %q CASCADE", cs.DatabaseName))
			if rc.restoreFlavor == restoreAllTablesInDatabase {
				tdb.Exec(t, cs.CreateDatabaseStmt)
			}

			// Store the highest job ID, so we know which jobs are relevant.
			tdb.Exec(t, "USE system")
			jobID := tdb.QueryRow(t, "SELECT COALESCE(max(job_id), 0) FROM [SHOW JOBS]")
			var maxJobID int64
			jobID.Scan(&maxJobID)

			// RESTORE.
			pe.RequestPlan()
			tdb.Exec(t, rc.stmt)
			waitForSchemaChangesToFinish(t, tdb)

			// Collect the end state following the post-RESTORE schema change.
			tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))
			actual := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

			// Define expectations based on outcome.
			var expected [][]string
			skipComparison := false
			// If the only thing left is to clean up then this restore may have no
			// job so the startTime is important.
			if succeeded, jobExists := hasLatestSchemaChangeSucceededWithMaxJobID(t, tdb, maxJobID); (succeeded && jobExists) || (b.maySucceed && !jobExists) {
				expected = b.expectedOnSuccess
				skipComparison = b.skipComparisonOnSuccess
				if !b.maySucceed {
					t.Fatal("post-RESTORE schema change was expected to not succeed")
				}
			} else {
				expected = b.expectedOnRollback
				if !b.mayRollback {
					t.Fatal("post-RESTORE schema change was expected to succeed")
				}
			}

			// Handle special case of RESTORE TABLE.
			if rc.restoreFlavor == restoreAllTablesInDatabase {
				// Expected table schema state must be stripped of UDF references.
				expected = parserRoundTrip(t, expected) // deep copy
				tmpExpected := make([][]string, 0, len(expected))
				for i := range expected {
					stmt, err := parser.ParseOne(expected[i][0])
					require.NoError(t, err)
					if _, ok := stmt.AST.(*tree.CreateRoutine); ok {
						continue
					}
					if c, ok := stmt.AST.(*tree.CreateTable); ok {
						require.NoError(t, removeDefsDependOnUDFs(c))
					}
					tmpExpected = append(tmpExpected, []string{tree.AsString(stmt.AST)})
				}
				expected = tmpExpected
			}

			// Check expectations.
			if !skipComparison {
				require.Equalf(t, expected, actual,
					"backup contents:\nparent_schema_name,object_name,object_type\n%s\n%s",
					sqlutils.MatrixToStr(backupContents), pe.GetExplain())
			} else {
				t.Log("Skipping comparison of SQL afterwards as requested.")
			}

			// Clean up.
			tdb.Exec(t, fmt.Sprintf("DROP DATABASE %q CASCADE", cs.DatabaseName))
		})
	}
}

func parserRoundTrip(t *testing.T, in [][]string) (out [][]string) {
	out = make([][]string, len(in))
	for i, s := range in {
		require.Equal(t, 1, len(s))
		stmt, err := parser.ParseOne(s[0])
		require.NoError(t, err)
		out[i] = make([]string, len(s))
		out[i] = []string{tree.AsString(stmt.AST)}
	}
	return out
}

func removeDefsDependOnUDFs(n *tree.CreateTable) error {
	var newDefs tree.TableDefs
	for _, def := range n.Defs {
		switch d := def.(type) {
		case *tree.CheckConstraintTableDef:
			if usesUDF, err := containsUDF(d.Expr); err != nil {
				return err
			} else if usesUDF {
				continue
			}
		case *tree.ColumnTableDef:
			if d.DefaultExpr.Expr != nil {
				if usesUDF, err := containsUDF(d.DefaultExpr.Expr); err != nil {
					return err
				} else if usesUDF {
					d.DefaultExpr = struct {
						Expr           tree.Expr
						ConstraintName tree.Name
					}{}
				}
			}
		}
		newDefs = append(newDefs, def)
	}
	n.Defs = newDefs
	return nil
}

func containsUDF(expr tree.Expr) (bool, error) {
	var foundUDF bool
	_, err := tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, _ error) {
		if fe, ok := expr.(*tree.FuncExpr); ok {
			ref := fe.Func.FunctionReference.(*tree.UnresolvedName)
			fn, err := ref.ToRoutineName()
			if err != nil {
				return false, nil, err
			}
			fd := tree.GetBuiltinFuncDefinition(fn, &sessiondata.DefaultSearchPath)
			if fd == nil {
				foundUDF = true
			}
		}
		return true, expr, nil
	})
	if err != nil {
		return false, err
	}
	return foundUDF, nil
}
