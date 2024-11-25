// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestTruncateWithConcurrentMutations is a regression test to cover a
// situation where a table is truncated while concurrent schema changes are
// performed. Prior to the commit introducing this test, all concurrent
// mutations were made public and the corresponding mutation jobs would not be
// dealt with. This could lead to tables which cannot be changed by schema
// changes and have invalid secondary indexes. Instead we now allowlist specific
// interactions and reject the rest. This test exercises these scenarios.
func TestTruncateWithConcurrentMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	type blockType int
	const (
		blockAfterBackfill blockType = iota // default
		blockBeforeResume
	)
	type validateQuery struct {
		stmt            string
		optionalResults [][]string
	}
	type testCase struct {
		blockType    blockType
		setupStmts   []string
		name         string
		expErrRE     string
		truncateStmt string
		stmts        []string
		validations  []validateQuery
	}
	run := func(t *testing.T, testC testCase) {
		var (
			blocked = make(chan struct{})
			unblock = make(chan error)
			s       serverutils.TestServerInterface
			db      *gosql.DB
		)
		{
			settings := cluster.MakeTestingClusterSettings()
			stats.AutomaticStatisticsClusterMode.Override(ctx, &settings.SV, false)
			scKnobs := &sql.SchemaChangerTestingKnobs{}
			blockFunc := func(jobID jobspb.JobID) error {
				select {
				case blocked <- struct{}{}:
				case err := <-unblock:
					return err
				}
				return <-unblock
			}
			switch testC.blockType {
			case blockAfterBackfill:
				scKnobs.RunAfterBackfill = blockFunc
			case blockBeforeResume:
				scKnobs.RunBeforeResume = blockFunc
			}
			s, db, _ = serverutils.StartServer(t, base.TestServerArgs{
				Settings: settings,
				Knobs: base.TestingKnobs{
					SQLSchemaChanger: scKnobs,
				},
			})
			defer s.Stopper().Stop(ctx)
		}

		tdb := sqlutils.MakeSQLRunner(db)
		tdb.ExecMultiple(t, strings.Split(`
SET use_declarative_schema_changer = 'off';
SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off';
`, `;`)...)

		// Support setup schema changes.
		close(unblock)
		for _, stmt := range testC.setupStmts {
			tdb.Exec(t, stmt)
		}

		// Block the main schema changes.
		unblock = make(chan error)

		// Create an index concurrently and make sure it blocks.
		var g errgroup.Group
		g.Go(func() error {
			if len(testC.stmts) == 1 {
				_, err := db.Exec(testC.stmts[0])
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			for _, stmt := range testC.stmts {
				_, err = tx.Exec(stmt)
				if err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			return tx.Commit()
		})
		errCh := make(chan error)
		go func() { errCh <- g.Wait() }()

		select {
		case err := <-errCh:
			t.Fatal(err) // concurrent txn failed
		case <-blocked:
		}

		_, err := db.Exec(testC.truncateStmt)
		if testC.expErrRE == "" {
			assert.NoError(t, err)
		} else {
			assert.Regexp(t, testC.expErrRE, err)
		}

		close(unblock)
		require.NoError(t, <-errCh)

		for _, v := range testC.validations {
			if v.optionalResults != nil {
				tdb.CheckQueryResults(t, v.stmt, v.optionalResults)
			} else {
				tdb.Exec(t, v.stmt)
			}
		}
	}
	const (
		commonCreateTable  = `CREATE TABLE t (i INT PRIMARY KEY, j INT)`
		commonPopulateData = `INSERT INTO t SELECT i, i+1 as j from generate_series(1, 100, 1) as t(i)`
	)
	commonValidations := []validateQuery{
		{
			stmt: "ALTER TABLE t ADD COLUMN added_column INT",
		},
		{
			stmt:            "SELECT * FROM crdb_internal.invalid_objects",
			optionalResults: [][]string{},
		},
	}
	commonIdxValidations := append([]validateQuery{
		{
			stmt:            "SELECT count(*) FROM t",
			optionalResults: [][]string{{"0"}},
		},
		{
			stmt:            "SELECT count(*) FROM t@idx",
			optionalResults: [][]string{{"0"}},
		},
	}, commonValidations...)

	cases := []testCase{
		{
			name: "add index",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`CREATE INDEX idx ON t(j)`,
			},
			validations: commonIdxValidations,
		},
		{
			name: "add index with column",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ADD COLUMN k INT`,
				`CREATE INDEX idx ON t(j, k)`,
			},
			validations: commonIdxValidations,
		},
		{
			blockType: blockBeforeResume,
			name:      "drop index",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
				`ALTER TABLE t ADD COLUMN k INT`,
				`CREATE INDEX idx ON t(j, k)`,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`DROP INDEX t@idx`,
			},
			expErrRE: `unimplemented: cannot perform TRUNCATE on "t" which has indexes being dropped`,
		},
		{
			name: "drop column with user-defined type",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
				`CREATE TYPE typ AS ENUM ('a')`,
				`ALTER TABLE t ADD COLUMN k typ`,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t DROP COLUMN k`,
			},
			expErrRE: `pq: unimplemented: cannot perform TRUNCATE on "t" which has ` +
				`a column \("k"\) being dropped which depends on another object`,
		},
		{
			name: "drop column which uses sequence",
			setupStmts: []string{
				commonCreateTable,
				`SET serial_normalization='sql_sequence'`,
				`ALTER TABLE t ADD COLUMN k SERIAL`,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t DROP COLUMN k`,
			},
			validations: commonValidations,
		},
		{
			name: "drop column which owns sequence",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
				`CREATE SEQUENCE s OWNED BY t.j`,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t DROP COLUMN j`,
			},
			validations: commonValidations,
		},
		{
			name: "alter primary key",
			setupStmts: []string{
				`CREATE TABLE t (i INT PRIMARY KEY, j INT NOT NULL)`,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (j)`,
			},
			expErrRE: `pq: unimplemented: cannot perform TRUNCATE on "t" which has an ongoing primary key change`,
		},
		{
			name: "add column",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ADD COLUMN k INT`,
			},
			validations: commonValidations,
		},
		{
			name:      "add self fk",
			blockType: blockBeforeResume,
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (j) REFERENCES t(i)`,
				`INSERT INTO t VALUES (101, NULL)`,
			},
			expErrRE: `pq: unimplemented: cannot perform TRUNCATE on "t" which has an ` +
				`ongoing FOREIGN_KEY constraint change`,
		},
		{
			name: "add other fk",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
				`CREATE TABLE t2 (i INT PRIMARY KEY)`,
				`INSERT INTO t2 SELECT i+1 as i from generate_series(1, 100, 1) as t(i)`,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (j) REFERENCES t2(i)`,
			},
			expErrRE: `pq: unimplemented: cannot perform TRUNCATE on "t" which has an ` +
				`ongoing FOREIGN_KEY constraint change`,
		},
		{
			name: "add check constraint",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t ADD CONSTRAINT c CHECK (j > 1)`,
			},
			expErrRE: `pq: unimplemented: cannot perform TRUNCATE on "t" which has an ongoing CHECK constraint change`,
		},
		{
			name: "drop column",
			setupStmts: []string{
				commonCreateTable,
				commonPopulateData,
			},
			truncateStmt: "TRUNCATE TABLE t",
			stmts: []string{
				`ALTER TABLE t DROP COLUMN j`,
			},
			validations: commonValidations,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}

func TestTruncatePreservesSplitPoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	ctx := context.Background()

	testCases := []struct {
		nodes int
	}{
		{
			nodes: 1,
		},
		{
			nodes: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("nodes=%d", testCase.nodes), func(t *testing.T) {
			tc := testcluster.StartTestCluster(t, testCase.nodes, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableMergeQueue: true,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)
			s := tc.ApplicationLayer(0)
			tenantSettings := s.ClusterSettings()
			conn := s.SQLConn(t, serverutils.DBName("defaultdb"))

			{
				// This test asserts on KV-internal effects (i.e. range splits
				// and their boundaries) as a result of configs and manually
				// installed splits. To ensure it works with the span configs
				// infrastructure quickly enough, we set a low closed timestamp
				// target duration.
				sysDB := sqlutils.MakeSQLRunner(tc.SystemLayer(0).SQLConn(t))
				sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
			}

			var err error
			_, err = conn.ExecContext(ctx, `
CREATE TABLE a(a INT PRIMARY KEY, b INT, INDEX(b));
INSERT INTO a SELECT g,g FROM generate_series(1,10000) g(g);
ALTER TABLE a SPLIT AT VALUES(1000), (2000), (3000), (4000), (5000), (6000), (7000), (8000), (9000);
ALTER INDEX a_b_idx SPLIT AT VALUES(1000), (2000), (3000), (4000), (5000), (6000), (7000), (8000), (9000);
`)
			assert.NoError(t, err)

			const origNRanges = 19

			// Range split decisions happen asynchronously, hence the
			// succeeds-soon block here and below.
			testutils.SucceedsSoon(t, func() error {
				row := conn.QueryRowContext(ctx, `
SELECT count(*) FROM [SHOW RANGES FROM TABLE a]`)
				assert.NoError(t, row.Err())

				var nRanges int
				assert.NoError(t, row.Scan(&nRanges))
				if nRanges != origNRanges {
					return errors.Newf("expected %d ranges, found %d", origNRanges, nRanges)
				}
				return nil
			})

			_, err = conn.ExecContext(ctx, `TRUNCATE a`)
			assert.NoError(t, err)

			// We subtract 1 from the original n ranges because the first range
			// can't be migrated to the new keyspace, as its prefix doesn't
			// include an index ID.
			expRanges := origNRanges + testCase.nodes*int(sql.PreservedSplitCountMultiple.Get(
				&tenantSettings.SV))

			testutils.SucceedsSoon(t, func() error {
				row := conn.QueryRowContext(ctx, `
SELECT count(*) FROM [SHOW RANGES FROM TABLE a]`)
				assert.NoError(t, row.Err())

				var nRanges int
				assert.NoError(t, row.Scan(&nRanges))
				if nRanges != expRanges {
					return errors.Newf("expected %d ranges, found %d", expRanges, nRanges)
				}
				return nil
			})
		})
	}
}
