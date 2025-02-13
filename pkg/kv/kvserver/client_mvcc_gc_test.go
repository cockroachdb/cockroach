// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMVCCGCCorrectStats verifies that the mvcc gc queue corrects stats
// for a range that has bad ones that would unnecessarily trigger the mvcc
// gc queue.
func TestMVCCGCCorrectStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var args base.TestServerArgs
	args.Knobs.Store = &kvserver.StoreTestingKnobs{DisableCanAckBeforeApplication: true}
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(key))
	for i := 0; i < 10; i++ {
		if err := store.DB().Put(ctx, key, "foo"); err != nil {
			t.Fatal(err)
		}
		key = key.Next()
	}

	// Put some garbage in the stats, so it triggers the mvcc gc queue.
	ms := repl.GetMVCCStats()
	oldKeyBytes := ms.KeyBytes
	oldValBytes := ms.ValBytes
	ms.KeyBytes = 16 * (1 << 20) // 16mb
	ms.ValBytes = 32 * (1 << 20) // 16mb
	ms.GCBytesAge = 48 * (1 << 20) * 100 * int64(time.Hour.Seconds())

	repl.SetMVCCStatsForTesting(&ms)
	require.NoError(t, store.ManualMVCCGC(repl))

	// Verify that the mvcc gc queue restored the stats.
	newStats := repl.GetMVCCStats()
	require.Equal(t, oldKeyBytes, newStats.KeyBytes)
	require.Equal(t, oldValBytes, newStats.ValBytes)
}

// TestSystemSpanConfigProtectionPoliciesApplyAfterGC is a regression test for
// https://github.com/cockroachdb/cockroach/issues/113867. This test attempts
// to recreate the following observed timeline:
// - Drop table @ t=1
// - SCHEMA CHANGE GC Job writes range deletion @ t=1, waits for span to be empty
// - Write a cluster-wide PTS @ t=2
// - MVCC GC Runs up to t=2
// - Table span is now empty, SCHEMA CHANGE GC JOB sees span is empty, and
// deletes span configuration
// - Range with empty span configuration, falls back to the fallback span
// configuration and does not respect the cluster-wide protection thereby
// allowing its GC threshold past t2
// - Read @ t2 fails with BatchTimestampBeforeGCError
//
// The last two steps are where the bug lies as the range should have respected
// the cluster-wide protection and not allowed its GC threshold to be set past
// t2.
func TestSystemSpanConfigProtectionPoliciesApplyAfterGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer gcjob.SetSmallMaxGCIntervalForTest()()

	ctx := context.Background()
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	args.ServerArgs.Knobs.SpanConfig = &spanconfig.TestingKnobs{
		OverrideFallbackConf: func(config roachpb.SpanConfig) roachpb.SpanConfig {
			overrideCfg := config
			overrideCfg.GCPolicy.TTLSeconds = 1
			return overrideCfg
		},
	}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ApplicationLayer(0).SQLConn(t)
	st := tc.ApplicationLayer(0).ClusterSettings()
	gcjob.EmptySpanPollInterval.Override(ctx, &st.SV, 100*time.Millisecond)

	// Speed up propagation of span configs and protected timestamps.
	sysDB := sqlutils.MakeSQLRunner(tc.SystemLayer(0).SQLConn(t))
	sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	sysDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	sysDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '10ms'")

	// Infrastructure to create a protected timestamp record.
	mkRecordAndProtect := func(s serverutils.ApplicationLayerInterface, ts hlc.Timestamp, target *ptpb.Target) *ptpb.Record {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		ptp := execCfg.ProtectedTimestampProvider
		insqlDB := execCfg.InternalDB
		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(recordID, int64(1), ts, nil, /* deprecatedSpans */
			jobsprotectedts.Jobs, target)
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return ptp.WithTxn(txn).Protect(ctx, rec)
		}))
		return rec
	}

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY)`)

	var tableID int
	runner.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name = 'foo'").Scan(&tableID)
	require.NotEqual(t, 0, tableID)
	tablePrefix := tc.ApplicationLayer(0).Codec().TablePrefix(uint32(tableID))
	tc.SplitRangeOrFatal(t, tablePrefix)
	require.NoError(t, tc.WaitForSplitAndInitialization(tablePrefix))

	// Lower the GC TTL of table foo and allow the zone config to get reconciled.
	s := tc.Server(0)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(roachpb.RKey(tablePrefix))
	_, err = sqlDB.Exec(`ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1`)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		cfg, err := repl.LoadSpanConfig(ctx)
		require.NoError(t, err)
		if cfg.GCPolicy.TTLSeconds != 1 {
			return errors.New("waiting for span config to apply")
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		runner.Exec(t, `INSERT INTO foo VALUES ($1)`, i)
	}

	// Drop the table and wait for the MVCC GC schema change job to write its
	// range deletion tombstone and start polling to check if the target span
	// has been gc'ed.
	runner.Exec(t, `DROP TABLE foo CASCADE `)
	var jobID int64
	runner.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC'").Scan(&jobID)
	require.NotEqual(t, 0, jobID)
	runner.CheckQueryResultsRetry(t, `
SELECT count(*)
  FROM (
        SELECT job_id
          FROM [SHOW JOBS]
         WHERE job_type = 'SCHEMA CHANGE GC'
               AND status = 'running'
               AND running_status = 'waiting for MVCC GC'
       )`,
		[][]string{{"1"}})

	// Write a protected timestamp record targeting the entire cluster's keyspace.
	clusterTarget := ptpb.MakeClusterTarget()
	protectedTime := timeutil.Now().UnixNano()
	mkRecordAndProtect(tc.ApplicationLayer(0), hlc.Timestamp{WallTime: protectedTime}, clusterTarget)

	// Wait for the protection policy to apply to foo's range.
	repl = store.LookupReplica(roachpb.RKey(tablePrefix))
	testutils.SucceedsSoon(t, func() error {
		cfg, err := repl.LoadSpanConfig(ctx)
		require.NoError(t, err)
		if len(cfg.GCPolicy.ProtectionPolicies) == 0 {
			return errors.New("waiting for span config to apply")
		}
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
		return nil
	})

	runner.CheckQueryResultsRetry(t, `
SELECT count(*)
  FROM (
        SELECT job_id
          FROM [SHOW JOBS]
         WHERE job_type = 'SCHEMA CHANGE GC'
               AND status = 'running'
               AND running_status = 'waiting for MVCC GC'
       )`,
		[][]string{{"1"}})

	// Run table foo's range through the MVCC GC queue until the GCThreshold is
	// past the point keys and range deletion tombstone thereby classifying the
	// range as empty.
	db := tc.SystemLayer(0).DB()
	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, store.ManualMVCCGC(repl))
		var ba kv.Batch
		ba.Header.MaxSpanRequestKeys = 1
		ba.AddRawRequest(&kvpb.IsSpanEmptyRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: tablePrefix, EndKey: tablePrefix.PrefixEnd(),
			},
		})
		require.NoError(t, db.Run(ctx, &ba))
		if !ba.RawResponse().Responses[0].GetIsSpanEmpty().IsEmpty() {
			return errors.New("table span is not empty; re-run GC")
		}
		return nil
	})

	// Wait for the GC job to see that the span is empty and delete the zone
	// config for the dropped table.
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(jobID))

	// Verify that the span config for the table is gone.
	kvAccessor := s.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	kvSubscriber := s.SpanConfigKVSubscriber().(spanconfig.KVSubscriber)
	sp := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}
	testutils.SucceedsSoon(t, func() error {
		records, err := kvAccessor.GetSpanConfigRecords(ctx, spanconfig.Targets{
			spanconfig.MakeTargetFromSpan(sp),
		})
		require.NoError(t, err)
		if len(records) != 0 {
			return errors.New("expected no span config records for table foo")
		}
		return nil
	})

	// Refresh the KVSubscriber to ensure it is caught up to now. We expect the
	// cluster-wide protection to be combined with the fallback span config and
	// applied to foo's empty range.
	ptsReader := store.GetStoreConfig().ProtectedTimestampReader
	require.NoError(t,
		spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, s.Clock().Now()))
	ts, _, err := kvSubscriber.GetProtectionTimestamps(ctx, sp)
	require.NoError(t, err)
	require.Len(t, ts, 1)

	ptsTime := hlc.Timestamp{WallTime: protectedTime}
	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, store.ManualMVCCGC(repl))
		require.NoError(t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, s.Clock().Now()))
		threshold := repl.GetGCThreshold()
		if !threshold.Equal(ptsTime.Prev()) {
			require.False(t, ptsTime.Prev().Less(threshold), "range GC threshold should never pass protected timestamp")
			return errors.New("range GC threshold is not advanced far enough")
		}
		return nil
	})
	runner.Exec(t,
		fmt.Sprintf(`SELECT * FROM crdb_internal.scan(crdb_internal.table_span($1)) AS OF SYSTEM TIME '%d'`,
			protectedTime), tableID)
}
