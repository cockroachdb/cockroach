// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSqlActivityUpdateJob verifies that the
// job is created.
func TestSqlActivityUpdateJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "test is too slow to run under race")

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	// Disable the job since it is called manually from a new instance to avoid
	// any race conditions.
	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true,
		Knobs: base.TestingKnobs{UpgradeManager: &upgradebase.TestingKnobs{
			DontUseJobs:                       true,
			SkipUpdateSQLActivityJobBootstrap: true,
		}}})
	defer srv.Stopper().Stop(context.Background())
	defer db.Close()

	var count int
	row := db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_activity")
	err := row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "transaction_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "statement_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.jobs WHERE job_type = 'AUTO UPDATE SQL ACTIVITY' and id = 103 ")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "jobs: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_statistics")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "transaction_statistics: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_statistics")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "statement_statistics: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() FROM crdb_internal.transaction_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "crdb_internal.transaction_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() FROM crdb_internal.statement_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "crdb_internal.statement_activity: expect:0, actual:%d", count)

	execCfg := srv.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	updater := newSqlActivityUpdater(st, execCfg.InternalDB)

	// Transient failures from AOST queries: https://github.com/cockroachdb/cockroach/issues/97840
	testutils.SucceedsWithin(t, func() error {
		// Verify no error with empty stats
		return updater.TransferStatsToActivity(ctx)
	}, 30*time.Second)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "transaction_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "statement_activity: expect:0, actual:%d", count)

	appName := "TestSqlActivityUpdateJob"
	_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "SELECT 1;")
	require.NoError(t, err)
	srv.SQLServer().(*Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)
	srv.SQLServer().(*Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", "randomIgnore")
	require.NoError(t, err)

	// The check to calculate the rows uses the follower_read_timestamp which will
	// skip the upsert because it will see there are no rows.
	testutils.SucceedsWithin(t, func() error {
		var txnAggTs time.Time
		row = db.QueryRowContext(ctx, `SELECT count_rows(), aggregated_ts 
			FROM system.public.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp() 
			WHERE app_name = $1 
			GROUP BY aggregated_ts`, appName)
		err = row.Scan(&count, &txnAggTs)
		if err != nil {
			return err
		}
		if count <= 0 {
			return errors.New("Need to wait for row to populate with follower_read_timestamp.")
		}

		var stmtAggTs time.Time
		row = db.QueryRowContext(ctx, `SELECT count_rows(), aggregated_ts 
			FROM system.public.statement_statistics AS OF SYSTEM TIME follower_read_timestamp() 
			WHERE app_name = $1 
			GROUP BY aggregated_ts`, appName)
		err = row.Scan(&count, &stmtAggTs)
		if err != nil {
			return err
		}
		if count <= 0 {
			return errors.New("Need to wait for row to populate with follower_read_timestamp.")
		}
		require.Equal(t, stmtAggTs, txnAggTs)
		return nil
	}, 30*time.Second)

	// Run the updater to add rows to the activity tables
	// This will use the transfer all scenarios with there only
	// being a few rows
	err = updater.TransferStatsToActivity(ctx)
	require.NoError(t, err)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_activity WHERE app_name = $1", appName)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, count, 1, "transaction_activity after transfer: expect:1, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_activity WHERE app_name = $1", appName)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, count, 1, "statement_activity after transfer: expect:1, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM crdb_internal.transaction_activity WHERE app_name = $1", appName)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, count, 1, "transaction_activity after transfer: expect:1, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM crdb_internal.statement_activity WHERE app_name = $1", appName)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, count, 1, "statement_activity after transfer: expect:1, actual:%d", count)
}

// TestSqlActivityUpdateJob verifies that the
// job is created.
func TestSqlActivityUpdateTopLimitJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "test is too slow to run under race")

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true,
		Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}})
	defer srv.Stopper().Stop(context.Background())
	defer db.Close()

	// Verify all the tables are empty initially
	var count int
	row := db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_activity")
	err := row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "transaction_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_activity")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "statement_activity: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.transaction_statistics")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "transaction_statistics: expect:0, actual:%d", count)

	row = db.QueryRowContext(ctx, "SELECT count_rows() "+
		"FROM system.public.statement_statistics")
	err = row.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "statement_statistics: expect:0, actual:%d", count)

	execCfg := srv.ExecutorConfig().(ExecutorConfig)
	st := cluster.MakeTestingClusterSettings()
	su := st.MakeUpdater()
	topLimit := 5
	err = su.Set(ctx, "sql.stats.activity.top.max", settings.EncodedValue{
		Value: settings.EncodeInt(int64(topLimit)),
		Type:  "i",
	})
	require.NoError(t, err)

	updater := newSqlActivityUpdater(st, execCfg.InternalDB)

	appNamePrefix := "TestSqlActivityUpdateJobLoop"
	// Generate 100 unique rows for statistics tables
	for i := 0; i < 100; i++ {
		tempAppName := fmt.Sprintf("%s%d", appNamePrefix, i)
		_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", tempAppName)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, "SELECT 1;")
		require.NoError(t, err)
	}

	// Need to call it twice to actually cause a flush
	srv.SQLServer().(*Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)
	srv.SQLServer().(*Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)

	_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", "randomIgnore")
	require.NoError(t, err)

	// The check to calculate the rows uses the follower_read_timestamp which will
	// skip the upsert because it will see there are no rows.
	testutils.SucceedsWithin(t, func() error {
		var txnAggTs time.Time
		row = db.QueryRowContext(ctx, `SELECT count_rows(), aggregated_ts 
			FROM system.public.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp() 
			WHERE app_name LIKE 'TestSqlActivityUpdateJobLoop%' 
			GROUP BY aggregated_ts`)
		err = row.Scan(&count, &txnAggTs)
		if err != nil {
			return err
		}
		if count < 100 {
			return errors.New("Need to wait for row to populate with follower_read_timestamp.")
		}

		var stmtAggTs time.Time
		row = db.QueryRowContext(ctx, `SELECT count_rows(), aggregated_ts 
			 FROM system.public.statement_statistics AS OF SYSTEM TIME follower_read_timestamp() 
			 WHERE app_name LIKE 'TestSqlActivityUpdateJobLoop%' 
			 GROUP BY aggregated_ts`)
		err = row.Scan(&count, &stmtAggTs)
		if err != nil {
			return err
		}
		if count < 100 {
			return errors.New("Need to wait for row to populate with follower_read_timestamp.")
		}
		require.Equal(t, stmtAggTs, txnAggTs)
		return nil
	}, 30*time.Second)

	// Run the updater to add rows to the activity tables
	// This will use the transfer all scenarios with there only
	// being a few rows
	err = updater.TransferStatsToActivity(ctx)
	require.NoError(t, err)

	maxRows := topLimit * 6 // Number of top columns to select from
	row = db.QueryRowContext(ctx, `SELECT count_rows() 
		FROM system.public.transaction_activity 
		WHERE app_name LIKE 'TestSqlActivityUpdateJobLoop%'`)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.LessOrEqual(t, count, maxRows, "transaction_activity after transfer: actual:%d, max:%d", count, maxRows)

	row = db.QueryRowContext(ctx, `SELECT count_rows() 
		FROM system.public.statement_activity 
		WHERE app_name LIKE 'TestSqlActivityUpdateJobLoop%'`)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.LessOrEqual(t, count, maxRows, "statement_activity after transfer: actual:%d, max:%d", count, maxRows)
}

func TestScheduledSQLStatsCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStressRace(t, "test is too slow to run under race")

	// Start the cluster. (One node is sufficient; the outliers system is currently in-memory only.)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true,
		Settings: st,
		Knobs:    base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}})
	defer srv.Stopper().Stop(context.Background())
	defer db.Close()
	_, err := db.ExecContext(ctx, "SET CLUSTER SETTING sql.stats.flush.interval = '100ms'")
	require.NoError(t, err)
	appName := "TestScheduledSQLStatsCompaction"
	_, err = db.ExecContext(ctx, "SET SESSION application_name=$1", appName)
	require.NoError(t, err)

	testutils.SucceedsWithin(t, func() error {
		_, err = db.ExecContext(ctx, "SELECT 1;")
		require.NoError(t, err)

		row := db.QueryRowContext(ctx, "SELECT count_rows() "+
			"FROM system.public.transaction_activity WHERE app_name = $1", appName)
		var count int
		err = row.Scan(&count)
		if err != nil {
			return err
		}
		if count <= 0 {
			return fmt.Errorf("transaction_activity is empty: %d", count)
		}

		row = db.QueryRowContext(ctx, "SELECT count_rows() "+
			"FROM system.public.statement_activity WHERE app_name = $1", appName)
		err = row.Scan(&count)
		if err != nil {
			return err
		}
		if count <= 0 {
			return fmt.Errorf("statement_activity is empty: %d", count)
		}

		return nil
	}, 1*time.Minute)
}
