// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackfillSystemStatementsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_3_BackfillSystemStatementsTable)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         (clusterversion.V26_3_BackfillSystemStatementsTable - 1).Version(),
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	db := srv.InternalDB().(isql.DB)
	sqlDB := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	type testStmt struct {
		fingerprintID uint64
		query         string
		querySummary  string
		db            string
		implicitTxn   bool
	}

	stmts := []testStmt{
		{1, "SELECT * FROM t WHERE id = _", "SELECT FROM t", "defaultdb", true},
		{2, "INSERT INTO t VALUES (_)", "INSERT INTO t", "mydb", false},
		{3, "UPDATE t SET x = _ WHERE id = _", "UPDATE t", "defaultdb", true},
	}

	// Insert test data into statement_statistics using the internal
	// executor since root lacks INSERT privilege on this system table.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, s := range stmts {
			metadata := fmt.Sprintf(
				`{"query": %q, "querySummary": %q, "db": %q, "implicitTxn": %t, `+
					`"stmtType": "TypeDML", "distsql": false, "vec": true, "fullScan": false}`,
				s.query, s.querySummary, s.db, s.implicitTxn)
			if _, err := txn.Exec(ctx,
				"insert-test-stmt-stats", txn.KV(),
				`INSERT INTO system.statement_statistics
					(aggregated_ts, fingerprint_id, transaction_fingerprint_id,
					 plan_hash, app_name, node_id, agg_interval,
					 metadata, statistics, plan)
				 VALUES (now(), $1, $2, $3, $4, 1, '1h', $5, '{}', '{}')`,
				sqlstatsutil.EncodeUint64ToBytes(s.fingerprintID),
				sqlstatsutil.EncodeUint64ToBytes(0),
				sqlstatsutil.EncodeUint64ToBytes(0),
				"test",
				metadata,
			); err != nil {
				return err
			}
		}
		// Insert a duplicate fingerprint_id with a different node to
		// verify deduplication.
		if _, err := txn.Exec(ctx,
			"insert-test-stmt-stats-dup", txn.KV(),
			`INSERT INTO system.statement_statistics
				(aggregated_ts, fingerprint_id, transaction_fingerprint_id,
				 plan_hash, app_name, node_id, agg_interval,
				 metadata, statistics, plan)
			 VALUES (now(), $1, $2, $3, $4, 2, '1h', $5, '{}', '{}')`,
			sqlstatsutil.EncodeUint64ToBytes(1),
			sqlstatsutil.EncodeUint64ToBytes(0),
			sqlstatsutil.EncodeUint64ToBytes(0),
			"test",
			`{"query": "SELECT * FROM t WHERE id = _", "querySummary": "SELECT FROM t", `+
				`"db": "defaultdb", "implicitTxn": true, "stmtType": "TypeDML", `+
				`"distsql": false, "vec": true, "fullScan": false}`,
		); err != nil {
			return err
		}

		// Insert entries with empty query or query summary that should
		// be skipped by the backfill.
		skipped := []string{
			`{"query": "", "querySummary": "SELECT", "db": "defaultdb", "implicitTxn": true}`,
			`{"query": "SELECT 1", "querySummary": "", "db": "defaultdb", "implicitTxn": true}`,
			`{"querySummary": "SELECT", "db": "defaultdb", "implicitTxn": true}`,
			`{"query": "SELECT 1", "db": "defaultdb", "implicitTxn": true}`,
		}
		for i, md := range skipped {
			if _, err := txn.Exec(ctx,
				"insert-test-stmt-stats-skip", txn.KV(),
				`INSERT INTO system.statement_statistics
					(aggregated_ts, fingerprint_id, transaction_fingerprint_id,
					 plan_hash, app_name, node_id, agg_interval,
					 metadata, statistics, plan)
				 VALUES (now(), $1, $2, $3, $4, 1, '1h', $5, '{}', '{}')`,
				sqlstatsutil.EncodeUint64ToBytes(uint64(100+i)),
				sqlstatsutil.EncodeUint64ToBytes(0),
				sqlstatsutil.EncodeUint64ToBytes(0),
				"test",
				md,
			); err != nil {
				return err
			}
		}
		return nil
	}))

	// Verify system.statements is empty before the backfill.
	sqlDB.CheckQueryResults(t,
		"SELECT count(*) FROM system.statements", [][]string{{"0"}})

	// Run the backfill upgrade.
	upgrades.Upgrade(t, srv.SQLConn(t),
		clusterversion.V26_3_BackfillSystemStatementsTable, nil, false)

	// Verify all three distinct fingerprints were backfilled.
	sqlDB.CheckQueryResults(t,
		"SELECT count(*) FROM system.statements", [][]string{{"3"}})

	// Verify the data is correct for each fingerprint.
	for _, s := range stmts {
		var fingerprint, summary, dbName, metadata string
		sqlDB.QueryRow(t,
			`SELECT fingerprint, summary, db, metadata::STRING
			 FROM system.statements WHERE fingerprint_id = $1`,
			sqlstatsutil.EncodeUint64ToBytes(s.fingerprintID),
		).Scan(&fingerprint, &summary, &dbName, &metadata)

		require.Equal(t, s.query, fingerprint)
		require.Equal(t, s.querySummary, summary)
		require.Equal(t, s.db, dbName)
		if s.implicitTxn {
			require.Contains(t, metadata, `"implicit_txn": true`)
		} else {
			require.Contains(t, metadata, `"implicit_txn": false`)
		}
	}
}

func TestBackfillSystemStatementsTableIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_3_BackfillSystemStatementsTable)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         (clusterversion.V26_3_BackfillSystemStatementsTable - 1).Version(),
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	db := srv.InternalDB().(isql.DB)
	sqlDB := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	// Pre-populate system.statements with an entry.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx,
			"insert-pre-existing-stmt", txn.KV(),
			`INSERT INTO system.statements
			   (fingerprint_id, fingerprint, summary, db, metadata)
			 VALUES ($1, 'SELECT 1', 'SELECT', 'defaultdb', '{"implicit_txn": true}')`,
			sqlstatsutil.EncodeUint64ToBytes(42),
		)
		return err
	}))

	// Insert matching entry in statement_statistics.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.Exec(ctx,
			"insert-test-stmt-stats", txn.KV(),
			`INSERT INTO system.statement_statistics
				(aggregated_ts, fingerprint_id, transaction_fingerprint_id,
				 plan_hash, app_name, node_id, agg_interval,
				 metadata, statistics, plan)
			 VALUES (now(), $1, $2, $3, $4, 1, '1h',
				'{"query": "SELECT 1", "querySummary": "SELECT", "db": "defaultdb", "implicitTxn": true}',
				'{}', '{}')`,
			sqlstatsutil.EncodeUint64ToBytes(42),
			sqlstatsutil.EncodeUint64ToBytes(0),
			sqlstatsutil.EncodeUint64ToBytes(0),
			"test",
		)
		return err
	}))

	// Run the backfill; it should not fail on the pre-existing entry.
	upgrades.Upgrade(t, srv.SQLConn(t),
		clusterversion.V26_3_BackfillSystemStatementsTable, nil, false)

	// Verify the original entry was not overwritten (count is still 1).
	sqlDB.CheckQueryResults(t,
		"SELECT count(*) FROM system.statements", [][]string{{"1"}})
}
