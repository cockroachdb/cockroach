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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupSuccess(t, factory, cs)
	})
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

	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupRollbacks(t, factory, cs)
	})
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
	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupSuccess(t, factory, cs)
	})
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
	cumulativeTestForEachPostCommitStage(t, path, factory, func(t *testing.T, cs CumulativeTestCaseSpec) {
		backupRollbacks(t, factory, cs)
	})
}

// runAllBackups runs all the backup tests, disabling the random skipping.
var runAllBackups = flag.Bool(
	"run-all-backups", false,
	"if true, run all backups instead of a random subset",
)

const skipRate = .5

func maybeRandomlySkip(t *testing.T) {
	if !*runAllBackups && rand.Float64() < skipRate {
		skip.IgnoreLint(t, "skipping due to randomness")
	}
}

func backupSuccess(t *testing.T, factory TestServerFactory, cs CumulativeTestCaseSpec) {
	maybeRandomlySkip(t)
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
	ctx := context.Background()
	url := fmt.Sprintf("userfile://backups.public.userfiles_$user/data_%s_%d",
		cs.Phase, cs.StageOrdinal)
	var dbForBackup atomic.Pointer[gosql.DB]
	var isBackupPostBackfill atomic.Bool
	pe := MakePlanExplainer()
	knobs := &scexec.TestingKnobs{
		// Back up the database exactly once when reaching the stage prescribed
		// by the test case specification.
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			// Collect EXPLAIN (DDL) diagram for debug purposes.
			if err := pe.MaybeUpdateWithPlan(p); err != nil {
				return err
			}
			// Since this callback is also active during post-RESTORE
			// schema changes, we have to be careful to exit early in those cases.
			if dbForBackup.Load() == nil {
				return nil
			}
			if s := p.Stages[stageIdx]; s.Phase == cs.Phase && s.Ordinal == cs.StageOrdinal {
				// Before backing up, check whether the plan executed so far includes
				// backfill operations which are not going to be replayed post-RESTORE.
				// Backing up at this point may lead the post-RESTORE schema change to fail.
			OuterLoop:
				for i := 0; i < stageIdx; i++ {
					if p.Stages[i].Type() == scop.BackfillType {
						// We've performed backfill ops, but have we also performed
						// mutation ops afterwards? Only then will the completion of the
						// backfill have been persisted in the descriptors' declarative
						// schema changer state.
						for j := i + 1; j < stageIdx; j++ {
							if p.Stages[j].Type() == scop.MutationType {
								isBackupPostBackfill.Store(true)
								break OuterLoop
							}
						}
					}
				}
				// Perform the backup.
				defer dbForBackup.Store(nil)
				backupStmt := fmt.Sprintf("BACKUP DATABASE %s INTO '%s'", cs.DatabaseName, url)
				_, err := dbForBackup.Load().Exec(backupStmt)
				return err
			}
			return nil
		},
	}
	runfn := func(_ serverutils.TestServerInterface, db *gosql.DB) {
		dbForBackup.Store(db)
		tdb := sqlutils.MakeSQLRunner(db)

		// Setup the test cluster.
		tdb.Exec(t, "CREATE DATABASE backups")
		require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))

		// Fetch the state of the cluster before the schema change kicks off.
		tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))
		before := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

		// Kick off the schema change and perform the backup via the BeforeStage hook.
		require.NoError(t, executeSchemaChangeTxn(ctx, t, cs.CumulativeTestSpec, db))
		require.Nil(t, dbForBackup.Load())
		require.True(t, hasLatestSchemaChangeSucceeded(t, tdb))

		// Fetch the state of the cluster after the schema change succeeds.
		tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))
		after := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

		// Determine whether the post-RESTORE schema change may perhaps
		// be rolled back.
		b := backupRestoreOutcome{
			url:                     url,
			maySucceed:              true,
			expectedOnSuccess:       after,
			skipComparisonOnSuccess: skipOutputComparison,
			mayRollback:             false,
			expectedOnRollback:      before,
		}
		if isBackupPostBackfill.Load() {
			const countRowsQ = `
				SELECT coalesce(sum(rows), 0)
				FROM [SHOW BACKUP FROM LATEST IN $2]
				WHERE database_name = $1`
			rowCount := tdb.QueryStr(t, countRowsQ, cs.DatabaseName, url)
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
		tdb.Exec(t, "SET CLUSTER SETTING VERSION = $1", clusterversion.Latest.String())

		// Restore the backup of the database taken mid-successful-schema-change
		// in various ways, check that it ends up in the same state as present.
		exerciseBackupRestore(t, tdb, cs, b, pe)
	}
	factory.WithSchemaChangerKnobs(knobs).Run(ctx, t, runfn)
}

func backupRollbacks(t *testing.T, factory TestServerFactory, cs CumulativeTestCaseSpec) {
	if cs.Phase != scop.PostCommitPhase {
		return
	}
	maybeRandomlySkip(t)
	ctx := context.Background()
	var urls atomic.Value
	var dbForBackup atomic.Pointer[gosql.DB]
	pe := MakePlanExplainer()
	knobs := &scexec.TestingKnobs{
		// Inject an error when reaching the stage prescribed by the test case
		// specification. This will trigger a rollback.
		// Before each stage during the rollback, back up the database.
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			// Collect EXPLAIN (DDL) diagram for debug purposes.
			if err := pe.MaybeUpdateWithPlan(p); err != nil {
				return err
			}
			// Since this callback is also active during post-RESTORE
			// schema changes, we have to be careful to exit early in those cases.
			if dbForBackup.Load() == nil {
				return nil
			}
			if p.InRollback {
				url := fmt.Sprintf("userfile://backups.public.userfiles_$user/data_%s_%d_%d",
					cs.Phase, cs.StageOrdinal, stageIdx)
				if v := urls.Load(); v == nil {
					urls.Store([]string{url})
				} else {
					urls.Store(append(v.([]string), url))
				}
				backupStmt := fmt.Sprintf("BACKUP DATABASE %s INTO '%s'", cs.DatabaseName, url)
				_, err := dbForBackup.Load().Exec(backupStmt)
				return err
			}
			if s := p.Stages[stageIdx]; s.Phase == cs.Phase && s.Ordinal == cs.StageOrdinal {
				return errors.Newf("boom %d", cs.StageOrdinal)
			}
			return nil
		},
	}
	runfn := func(_ serverutils.TestServerInterface, db *gosql.DB) {
		dbForBackup.Store(db)
		tdb := sqlutils.MakeSQLRunner(db)

		// Setup the test cluster.
		tdb.Exec(t, "CREATE DATABASE backups")
		require.NoError(t, setupSchemaChange(ctx, t, cs.CumulativeTestSpec, db))

		// Fetch the state of the cluster before the schema change kicks off.
		tdb.Exec(t, fmt.Sprintf("USE %q", cs.DatabaseName))
		expected := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

		// Kick off the schema change, fail it at the prescribed stage,
		// and perform the backups during the rollback via the BeforeStage hook.
		require.Regexp(t, fmt.Sprintf("boom %d", cs.StageOrdinal),
			executeSchemaChangeTxn(ctx, t, cs.CumulativeTestSpec, db))
		waitForSchemaChangesToFinish(t, tdb)
		dbForBackup.Store(nil)

		// Fetch the state of the cluster after the schema change rolls back.
		require.False(t, hasLatestSchemaChangeSucceeded(t, tdb))
		postRollback := parserRoundTrip(t, tdb.QueryStr(t, fetchDescriptorStateQuery))

		// Check that it's the same as before the schema change was attempted.
		require.Equal(t, expected, postRollback, "rolled back schema change should be no-op")

		// Restore the backups of the database taken mid-rolled-back-schema-change
		// in various ways, check that they end up in the same state as present.
		for i, url := range urls.Load().([]string) {
			b := backupRestoreOutcome{
				url:                url,
				mayRollback:        true,
				expectedOnRollback: postRollback,
			}
			var name string
			switch i {
			case 0:
				// Discard the first backup, which is going to be the backup taken
				// right at the very beginning of the rollback. Due to various quirks,
				// the declarative schema changer state in that backup is going to be
				// exactly the same as the state pre-rollback.
				//
				// This doesn't matter in production, as whichever error triggered the
				// rollback in the first place will simply manifest itself again during
				// the post-RESTORE schema change and roll it back.
				continue
			case 1:
				name = "post_1_rollback_stage_exec"
			default:
				name = fmt.Sprintf("post_%d_rollback_stages_exec", i)
			}
			t.Run(name, func(t *testing.T) {
				exerciseBackupRestore(t, tdb, cs, b, pe)
			})
		}
	}
	factory.WithSchemaChangerKnobs(knobs).Run(ctx, t, runfn)
}

type backupRestoreOutcome struct {
	url                                   string
	maySucceed, mayRollback               bool
	expectedOnSuccess, expectedOnRollback [][]string
	skipComparisonOnSuccess               bool
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
	{
		var tablesToRestore []string
	Loop:
		for _, row := range backupContents {
			switch row[2] {
			case "table":
				fqn := fmt.Sprintf("%s.%s.%s", cs.DatabaseName, row[0], row[1])
				tablesToRestore = append(tablesToRestore, fqn)
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
				// Database must be created explicitly pre-RESTORE in this case.
				tdb.Exec(t, fmt.Sprintf("CREATE DATABASE %q", cs.DatabaseName))
			}

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
			if hasLatestSchemaChangeSucceeded(t, tdb) {
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
			fd, err := tree.GetBuiltinFuncDefinition(fn, &sessiondata.DefaultSearchPath)
			if err != nil {
				return false, nil, err
			}
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
