// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWaitForDelRangeInGCJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.V23_1WaitedForDelRangeInGCJob)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
	storage.MVCCRangeTombstonesEnabledInMixedClusters.Override(ctx, &settings.SV, false)
	testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer testServer.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = $1`,
		`gcjob.before_resume`)
	tdb.Exec(t, `SET CLUSTER SETTING jobs.registry.retry.max_delay = '10s'`)
	execStmts := func(t *testing.T, stmts ...string) {
		for _, stmt := range stmts {
			tdb.Exec(t, stmt)
		}
	}
	execStmts(t,
		"CREATE DATABASE db",
		"CREATE TABLE db.foo(i int primary key, j int)",
		"INSERT INTO db.foo(i) SELECT * FROM generate_series(1, 1000)",
		"CREATE TABLE foo (i int primary key, j int, index idx(j))",
		"INSERT INTO foo(i) SELECT * FROM generate_series(1, 1000)",
	)

	var beforeDrop string
	tdb.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&beforeDrop)

	// Grab the table ID for db.foo and the table id and index ID for foo.
	var dbFooID, fooID, idxID uint32
	tdb.QueryRow(t, `
SELECT 'db.foo'::REGCLASS::INT8,
       'foo'::REGCLASS::INT8,
       (
        SELECT index_id
          FROM crdb_internal.table_indexes
         WHERE descriptor_id = 'foo'::REGCLASS AND index_name = 'idx'
       );`).Scan(&dbFooID, &fooID, &idxID)
	execStmts(t,
		"DROP DATABASE db CASCADE",
		"DROP INDEX foo@idx",
	)

	// One for the index, one for the database.
	tdb.CheckQueryResultsRetry(t, `
SELECT count(*)
  FROM crdb_internal.jobs
 WHERE job_type = 'SCHEMA CHANGE GC'
   AND status = 'paused'`,
		[][]string{{"2"}})
	tdb.ExpectErr(t, `verifying precondition for version \d*22.2-\d+: `+
		`paused GC jobs prevent upgrading GC job behavior: \[\d+ \d+]`,
		"SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	tdb.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = DEFAULT`)

	// Now resume the jobs.
	tdb.Exec(t,
		"RESUME JOBS SELECT job_id FROM crdb_internal.jobs WHERE status = 'paused'")

	// Upgrade the version. Then ensure that the data has been
	// removed from the span but the jobs are still running.
	tdb.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

	codec := testServer.ExecutorConfig().(sql.ExecutorConfig).Codec
	testutils.SucceedsSoon(t,
		func() error {
			{
				dbFooStart := codec.TablePrefix(dbFooID)
				res, err := kvDB.Scan(ctx, dbFooStart, dbFooStart.PrefixEnd(), 1)
				if err != nil {
					return err
				}
				if len(res) != 0 {
					return errors.AssertionFailedf("unexpected number of table keys (got %d)", len(res))
				}
			}
			{
				idxStart := codec.IndexPrefix(fooID, idxID)
				res, err := kvDB.Scan(ctx, idxStart, idxStart.PrefixEnd(), 1)
				require.NoError(t, err)
				if err != nil {
					return err
				}
				if len(res) != 0 {
					return errors.AssertionFailedf("unexpected number of index keys (got %d)", len(res))
				}
			}
			return nil
		})

	// Make sure that there is still MVCC history.
	tdb.CheckQueryResults(t, "SELECT count(*) FROM foo@idx AS OF SYSTEM TIME "+beforeDrop,
		[][]string{{"1000"}})
}
