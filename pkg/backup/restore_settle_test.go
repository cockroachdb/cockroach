// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbackup"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestRestoreSettlesInFlightSchemaChange backs up a database while a
// declarative schema change is paused before a chosen stage, restores it, and
// checks the restored table. A restored job that depends on index data RESTORE
// does not bring back must be settled to the state at backup time; any other
// job must finish as it did on the source.
func TestRestoreSettlesInFlightSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type pausePoint struct {
		phase   scop.Phase
		ordinal int
	}
	type pauseRequest struct {
		at      pausePoint
		paused  chan struct{}
		release chan struct{}
	}
	var pending atomic.Pointer[pauseRequest]
	// stageFailures is the number of post-commit non-revertible stages that
	// fail with a retryable error; it is armed after the source schema change
	// finishes, so only the restored job's stages fail.
	var stageFailures atomic.Int32

	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.SQLDeclarativeSchemaChanger = &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			s := p.Stages[stageIdx]
			if n := stageFailures.Load(); n > 0 && s.Phase == scop.PostCommitNonRevertiblePhase &&
				stageFailures.CompareAndSwap(n, n-1) {
				return errors.New("injected stage failure")
			}
			req := pending.Load()
			if req == nil || s.Phase != req.at.phase || s.Ordinal != req.at.ordinal ||
				!pending.CompareAndSwap(req, nil) {
				return nil
			}
			close(req.paused)
			<-req.release
			return nil
		},
	}
	cluster, sqlDB, _, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()
	sqlDB.Exec(t, `SET use_declarative_schema_changer = 'unsafe_always'`)
	ctx := context.Background()

	showCreateTable := func(t *testing.T, db, table string) string {
		var name, stmt string
		sqlDB.QueryRow(t, fmt.Sprintf(`SHOW CREATE TABLE %s.%s`, db, table)).Scan(&name, &stmt)
		return stmt
	}
	countRows := func(t *testing.T, db, table string) int {
		var n int
		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s.%s`, db, table)).Scan(&n)
		return n
	}
	// normalizeSettled removes the differences a settled table is expected to
	// have from the backup-time table: schema_locked is restored, and recreated
	// secondary indexes whose source is kept, and indexes on a column not public
	// at backup time, which SHOW CREATE TABLE names with a placeholder, are
	// dropped.
	normalizeSettled := func(stmt string) string {
		stmt = strings.ReplaceAll(stmt, " WITH (schema_locked = true)", "")
		var lines []string
		for _, l := range strings.Split(stmt, "\n") {
			if strings.Contains(l, "_recreated") || strings.Contains(l, "_name_placeholder") {
				continue
			}
			// A NOT NULL being dropped shows as a NOT VALID check constraint.
			if strings.Contains(l, "_auto_not_null") && strings.Contains(l, "NOT VALID") {
				continue
			}
			lines = append(lines, strings.TrimSuffix(strings.TrimSpace(l), ","))
		}
		return strings.Join(lines, "\n")
	}
	// requireIndexesComplete checks that every forward, non-partial index of the
	// table holds expectedRows entries.
	requireIndexesComplete := func(t *testing.T, db, table string, expectedRows int) {
		rows := sqlDB.QueryStr(t, fmt.Sprintf(`
			SELECT index_name FROM %s.crdb_internal.table_indexes
			WHERE descriptor_name = $1 AND NOT is_inverted AND create_statement NOT LIKE '%% WHERE %%'`,
			db), table)
		require.NotEmpty(t, rows)
		for _, r := range rows {
			require.Equal(t, expectedRows, func() int {
				var n int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s.%s@{FORCE_INDEX=%s}`,
					db, table, tree.NameString(r[0]))).Scan(&n)
				return n
			}(), "index %s", r[0])
		}
	}
	// waitForRestoredSchemaChange waits for the schema change job created by a
	// restore that started at restoreStart to finish and returns its
	// description. A settled job runs as a rollback and ends as failed; any
	// other job must succeed. It reads crdb_internal.jobs because SHOW JOBS
	// truncates created to the second.
	waitForRestoredSchemaChange := func(t *testing.T, restoreStart time.Time) string {
		var description string
		testutils.SucceedsSoon(t, func() error {
			jobutils.NudgeAdoptionQueues(t, cluster)
			var status, jobErr string
			err := sqlDB.DB.QueryRowContext(ctx, `
				SELECT status, description, COALESCE(error, '') FROM crdb_internal.jobs
				WHERE job_type = 'NEW SCHEMA CHANGE' AND created > $1
				ORDER BY created DESC LIMIT 1`, restoreStart).Scan(&status, &description, &jobErr)
			if err != nil {
				return err
			}
			expected := "succeeded"
			if scbackup.IsSettledJobDescription(description) {
				expected = "failed"
			}
			switch status {
			case expected:
				return nil
			case "succeeded", "failed", "canceled":
				t.Fatalf("restored schema change ended %s: %s: %s", status, description, jobErr)
			}
			return errors.Newf("restored schema change is %s", status)
		})
		return description
	}
	// requireNoSchemaChangerState checks that the table no longer holds
	// declarative schema changer state. A settled job also ends as failed if its
	// remaining stages hit a permanent error, leaving its state behind.
	requireNoSchemaChangerState := func(t *testing.T, db, table string) {
		sqlDB.CheckQueryResults(t, fmt.Sprintf(`
			SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor)
				->'table'->'declarativeSchemaChangerState' IS NULL
			FROM system.descriptor WHERE id = '%s.%s'::REGCLASS::OID::INT8`, db, table),
			[][]string{{"true"}})
	}

	// requirePrimaryKeyNotNull checks that no key column of the table's primary
	// index is nullable.
	requirePrimaryKeyNotNull := func(t *testing.T, db, table string) {
		require.Empty(t, sqlDB.QueryStr(t, fmt.Sprintf(`
			SELECT i.column_name FROM [SHOW INDEXES FROM %[1]s.%[2]s] AS i
			JOIN [SHOW COLUMNS FROM %[1]s.%[2]s] AS c ON c.column_name = i.column_name
			WHERE i.index_name = (
				SELECT index_name FROM %[1]s.crdb_internal.table_indexes
				WHERE descriptor_name = '%[2]s' AND index_type = 'primary'
			) AND NOT i.storing AND c.is_nullable`, db, table)))
	}

	seedRows := `INSERT INTO {db}.t SELECT g, g FROM generate_series(-100, -1) AS g`
	tests := []struct {
		name         string
		table        string
		setup        []string // {db} is replaced with the source database name
		schemaChange string
		pauseBefore  pausePoint
		// restoredStageFailures is the number of the restored job's stages that
		// fail with a retryable error before it is allowed to proceed.
		restoredStageFailures int32
		expectSettled         bool
		// expectSettledToOriginal compares a settled table with the table before
		// the schema change instead of at backup time, for when SHOW CREATE TABLE
		// at backup time hides a NOT NULL that is being dropped but still
		// enforced.
		expectSettledToOriginal bool
	}{
		{
			name:  "empty final primary index would be promoted",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (a INT NOT NULL)`,
				`INSERT INTO {db}.t SELECT g FROM generate_series(-100, -1) AS g`,
			},
			schemaChange:  `ALTER TABLE {db}.t ALTER PRIMARY KEY USING COLUMNS (a)`,
			pauseBefore:   pausePoint{scop.PostCommitNonRevertiblePhase, 2},
			expectSettled: true,
		},
		{
			name:  "rollback would revive the unrestored old primary index",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (a INT NOT NULL)`,
				`INSERT INTO {db}.t SELECT g FROM generate_series(-100, -1) AS g`,
			},
			schemaChange:  `ALTER TABLE {db}.t ALTER PRIMARY KEY USING COLUMNS (a)`,
			pauseBefore:   pausePoint{scop.PostCommitPhase, 9},
			expectSettled: true,
		},
		{
			name:  "kept rowid primary index keeps the NOT NULL being dropped from rowid",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (a INT NOT NULL)`,
				`INSERT INTO {db}.t SELECT g FROM generate_series(-100, -1) AS g`,
			},
			schemaChange:            `ALTER TABLE {db}.t ALTER PRIMARY KEY USING COLUMNS (a)`,
			pauseBefore:             pausePoint{scop.PostCommitPhase, 5},
			expectSettled:           true,
			expectSettledToOriginal: true,
		},
		{
			name:  "public transient validation check of a type change dropped",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (a INT NOT NULL, j TEXT)`,
				`INSERT INTO {db}.t SELECT g, 'ab' FROM generate_series(-100, -1) AS g`,
			},
			schemaChange: `ALTER TABLE {db}.t ALTER COLUMN j SET DATA TYPE CHAR(5), ` +
				`ALTER PRIMARY KEY USING COLUMNS (a)`,
			pauseBefore:   pausePoint{scop.PostCommitNonRevertiblePhase, 2},
			expectSettled: true,
		},
		{
			name:  "rollback would revive an unrestored expression index",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (i INT PRIMARY KEY, j INT, INDEX ((j+1)))`,
				seedRows,
			},
			schemaChange:  `ALTER TABLE {db}.t DROP COLUMN j CASCADE`,
			pauseBefore:   pausePoint{scop.PostCommitPhase, 4},
			expectSettled: true,
		},
		{
			name:  "recreated secondary indexes dropped while their sources are kept",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (i INT PRIMARY KEY, j INT NOT NULL, k INT DEFAULT 54)`,
				`INSERT INTO {db}.t (i, j) SELECT g, g FROM generate_series(-100, -1) AS g`,
				`CREATE INDEX ON {db}.t (i) WHERE i <= 0`,
				`CREATE INDEX ON {db}.t (j) WHERE j <= 0`,
				`CREATE INDEX idx ON {db}.t (k)`,
			},
			schemaChange:  `ALTER TABLE {db}.t ALTER PRIMARY KEY USING COLUMNS (j)`,
			pauseBefore:   pausePoint{scop.PostCommitPhase, 10},
			expectSettled: true,
		},
		{
			name:  "settled job retries a failed stage",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (a INT NOT NULL)`,
				`INSERT INTO {db}.t SELECT g FROM generate_series(-100, -1) AS g`,
			},
			schemaChange:          `ALTER TABLE {db}.t ALTER PRIMARY KEY USING COLUMNS (a)`,
			pauseBefore:           pausePoint{scop.PostCommitNonRevertiblePhase, 2},
			restoredStageFailures: 3,
			expectSettled:         true,
		},
		{
			name:          "index build that has not backfilled yet resumes",
			table:         "t",
			setup:         []string{`CREATE TABLE {db}.t (i INT PRIMARY KEY, j INT)`, seedRows},
			schemaChange:  `CREATE INDEX t_j_idx ON {db}.t (j)`,
			pauseBefore:   pausePoint{scop.PostCommitPhase, 1},
			expectSettled: false,
		},
		{
			name:  "validation-only type change resumes",
			table: "t",
			setup: []string{
				`CREATE TABLE {db}.t (i INT PRIMARY KEY, j TEXT)`,
				`INSERT INTO {db}.t SELECT g, 'ab' FROM generate_series(-100, -1) AS g`,
			},
			schemaChange:  `ALTER TABLE {db}.t ALTER COLUMN j SET DATA TYPE CHAR(5)`,
			pauseBefore:   pausePoint{scop.PostCommitPhase, 1},
			expectSettled: false,
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src, dst := fmt.Sprintf("src%d", i), fmt.Sprintf("dst%d", i)
			sub := func(s string) string { return strings.ReplaceAll(s, "{db}", src) }
			sqlDB.Exec(t, `CREATE DATABASE `+src)
			for _, s := range tc.setup {
				sqlDB.Exec(t, sub(s))
			}
			original := showCreateTable(t, src, tc.table)

			req := &pauseRequest{
				at: tc.pauseBefore, paused: make(chan struct{}), release: make(chan struct{}),
			}
			// Release the paused schema change even if the test fails while it is
			// blocked, so the failure surfaces instead of hanging.
			release := sync.OnceFunc(func() { close(req.release) })
			defer release()
			pending.Store(req)
			done := make(chan error, 1)
			go func() {
				_, err := sqlDB.DB.ExecContext(ctx, sub(tc.schemaChange))
				done <- err
			}()
			select {
			case <-req.paused:
			case err := <-done:
				t.Fatalf("schema change finished without reaching %v: %v", tc.pauseBefore, err)
			}

			atBackup := showCreateTable(t, src, tc.table)
			rowsAtBackup := countRows(t, src, tc.table)
			uri := "nodelocal://1/" + src
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE %s INTO '%s'`, src, uri))
			release()
			require.NoError(t, <-done)
			final := showCreateTable(t, src, tc.table)

			stageFailures.Store(tc.restoredStageFailures)
			defer stageFailures.Store(0)
			var restoreStart time.Time
			sqlDB.QueryRow(t, `SELECT now()`).Scan(&restoreStart)
			sqlDB.Exec(t, fmt.Sprintf(
				`RESTORE DATABASE %s FROM LATEST IN '%s' WITH new_db_name = %s`, src, uri, dst))
			description := waitForRestoredSchemaChange(t, restoreStart)
			require.Zero(t, stageFailures.Load(), "injected stage failures not all hit")
			settled := scbackup.IsSettledJobDescription(description)

			restored := showCreateTable(t, dst, tc.table)
			if tc.expectSettled {
				require.True(t, settled, description)
				expected := atBackup
				if tc.expectSettledToOriginal {
					expected = original
				}
				require.Equal(t, normalizeSettled(expected), normalizeSettled(restored))
				// A placeholder name marks an element the schema change had not
				// finished, which a settled table must not keep.
				require.NotContains(t, restored, "_name_placeholder")
			} else {
				require.False(t, settled, description)
				require.Equal(t, final, restored)
			}
			requireNoSchemaChangerState(t, dst, tc.table)
			requirePrimaryKeyNotNull(t, dst, tc.table)
			require.Equal(t, rowsAtBackup, countRows(t, dst, tc.table))
			requireIndexesComplete(t, dst, tc.table, rowsAtBackup)
			sqlDB.CheckQueryResults(t,
				`SELECT count(*) FROM crdb_internal.invalid_objects`, [][]string{{"0"}})
			sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN zz INT`, dst, tc.table))
		})
	}
}
