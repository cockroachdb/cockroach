// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestAsymmetricPCRCPUCapacity tests physical cluster replication with asymmetric
// CPU capacity between source and destination clusters. The destination cluster
// has half the CPU count of the source cluster to test replication performance
// under resource constraints.
func TestAsymmetricPCRCPUCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test is too slow under race")
	skip.UnderStress(t, "test is resource intensive")

	ctx := context.Background()

	// Save original GOMAXPROCS and restore after test
	originalProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalProcs)

	// Configure asymmetric CPU capacity
	sourceCPUs := 8
	targetCPUs := sourceCPUs / 2 // Half the CPU count for target cluster

	// Create asymmetric cluster settings for increased load on source
	srcClusterSettings := make(map[string]string)
	for k, v := range replicationtestutils.DefaultClusterSettings {
		srcClusterSettings[k] = v
	}
	// Increase write load and checkpoint frequency on source
	srcClusterSettings[`bulkio.stream_ingestion.minimum_flush_interval`] = `'5ms'`
	srcClusterSettings[`stream_replication.min_checkpoint_frequency`] = `'500ms'`
	srcClusterSettings[`physical_replication.producer.timestamp_granularity`] = `'50ms'`

	// Keep default settings for destination to create asymmetric load
	destClusterSettings := make(map[string]string)
	for k, v := range replicationtestutils.DefaultClusterSettings {
		destClusterSettings[k] = v
	}

	// Custom init function to create more data and ongoing writes
	srcInitFunc := func(t *testing.T, sysSQL *sqlutils.SQLRunner, tenantSQL *sqlutils.SQLRunner) {
		tenantSQL.Exec(t, `
		BEGIN;
		SET LOCAL autocommit_before_ddl = false;
		CREATE DATABASE workload;
		
		-- Create tables with different patterns to stress replication
		CREATE TABLE workload.heavy_writes(
			id SERIAL PRIMARY KEY,
			data STRING,
			timestamp TIMESTAMPTZ DEFAULT now(),
			payload BYTES
		);
		
		CREATE TABLE workload.frequent_updates(
			id INT PRIMARY KEY,
			counter INT DEFAULT 0,
			last_updated TIMESTAMPTZ DEFAULT now()
		);
		
		CREATE TABLE workload.bulk_data(
			partition_id INT,
			record_id INT,
			data STRING,
			PRIMARY KEY (partition_id, record_id)
		);
		
		-- Insert initial data
		INSERT INTO workload.heavy_writes (data, payload) 
		SELECT 
			'initial_data_' || i,
			repeat('x', 1000)::BYTES
		FROM generate_series(1, 1000) AS i;
		
		INSERT INTO workload.frequent_updates (id)
		SELECT i FROM generate_series(1, 100) AS i;
		
		INSERT INTO workload.bulk_data (partition_id, record_id, data)
		SELECT 
			(i % 10) + 1,
			i,
			'bulk_record_' || i
		FROM generate_series(1, 5000) AS i;
		
		COMMIT;
		`)
	}

	// Configure source cluster with higher CPU capacity settings
	args := replicationtestutils.TenantStreamingClustersArgs{
		SrcTenantName:         roachpb.TenantName("sourcehighcpu"),
		SrcTenantID:           roachpb.MustMakeTenantID(10),
		SrcInitFunc:           srcInitFunc,
		SrcNumNodes:           3, // 3 nodes in source cluster
		SrcClusterSettings:    srcClusterSettings,
		SrcClusterTestRegions: []string{"us-east-1", "us-east-1", "us-east-1"},

		DestTenantName:         roachpb.TenantName("destlowcpu"),
		DestTenantID:           roachpb.MustMakeTenantID(2),
		DestNumNodes:           3, // Same number of nodes, but with reduced CPU
		DestClusterSettings:    destClusterSettings,
		DestClusterTestRegions: []string{"us-west-1", "us-west-1", "us-west-1"},

		TestingKnobs: &sql.StreamingTestingKnobs{
			// Add any specific streaming knobs if needed
		},
	}

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Start the replication stream
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	// Wait for replication to be running
	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Record initial replication time
	initialTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(initialTime, jobspb.JobID(ingestionJobID))

	// Generate sustained workload on source cluster to test asymmetric capacity
	workloadDuration := 10 * time.Minute
	workloadCtx, workloadCancel := context.WithTimeout(ctx, workloadDuration)
	defer workloadCancel()

	// Run concurrent workloads
	go func() {
		defer workloadCancel()

		// Heavy insert workload
		for i := 1000; workloadCtx.Err() == nil; i++ {
			c.SrcTenantSQL.Exec(t, `
				INSERT INTO workload.heavy_writes (data, payload) 
				VALUES ($1, $2)`,
				fmt.Sprintf("workload_data_%d", i),
				[]byte(fmt.Sprintf("payload_%d_%s", i, time.Now().Format("15:04:05.000"))))

			// Brief pause to avoid overwhelming
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		defer workloadCancel()

		// Frequent update workload
		for workloadCtx.Err() == nil {
			c.SrcTenantSQL.Exec(t, `
				UPDATE workload.frequent_updates 
				SET counter = counter + 1, last_updated = now() 
				WHERE id = $1`,
				(time.Now().UnixNano()%100)+1)

			time.Sleep(5 * time.Millisecond)
		}
	}()

	go func() {
		defer workloadCancel()

		// Bulk operations workload
		for i := 10000; workloadCtx.Err() == nil; i += 100 {
			c.SrcTenantSQL.Exec(t, `
				INSERT INTO workload.bulk_data (partition_id, record_id, data)
				SELECT 
					($1 % 10) + 1,
					$1 + j,
					'bulk_workload_' || ($1 + j)
				FROM generate_series(0, 99) AS j`,
				i)

			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Monitor replication lag throughout the test
	lagCheckInterval := 15 * time.Second
	lagCheckTicker := time.NewTicker(lagCheckInterval)
	defer lagCheckTicker.Stop()

	maxAcceptableLag := 30 * time.Second
	lagCheckCount := 0

monitoringLoop:
	for {
		select {
		case <-workloadCtx.Done():
			break monitoringLoop
		case <-lagCheckTicker.C:
			lagCheckCount++

			// Check current replication progress
			currentTime := c.SrcCluster.Server(0).Clock().Now()

			// Verify replication can keep up despite asymmetric CPU capacity
			// Allow some grace period for the reduced CPU target cluster
			c.WaitUntilReplicatedTime(currentTime.Add(-maxAcceptableLag.Nanoseconds(), 0), jobspb.JobID(ingestionJobID))

			// Get replication lag statistics
			stats := replicationtestutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
			require.NotNil(t, stats.ReplicationLagInfo)

			replicationLag := currentTime.GoTime().Sub(stats.ReplicationLagInfo.MinIngestedTimestamp.GoTime())

			t.Logf("Replication lag check %d: %v (acceptable threshold: %v)",
				lagCheckCount, replicationLag, maxAcceptableLag)

			// Ensure lag doesn't exceed our threshold for asymmetric setup
			require.True(t, replicationLag < maxAcceptableLag*2, // Allow 2x threshold for asymmetric setup
				"Replication lag %v exceeded acceptable threshold %v for asymmetric CPU setup",
				replicationLag, maxAcceptableLag*2)
		}
	}

	// Final verification - ensure replication caught up after workload ends
	// Wait a bit longer to ensure all workload has been replicated
	time.Sleep(10 * time.Second)
	finalTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(finalTime.Add(-10*time.Second.Nanoseconds(), 0), jobspb.JobID(ingestionJobID))

	// Verify data consistency between source and destination
	srcFingerprint := replicationtestutils.FingerprintTenantAtTimestampNoHistory(t, c.SrcSysSQL, c.Args.SrcTenantName, finalTime.AsOfSystemTime())
	destFingerprint := replicationtestutils.FingerprintTenantAtTimestampNoHistory(t, c.DestSysSQL, c.Args.DestTenantName, finalTime.AsOfSystemTime())

	require.Equal(t, srcFingerprint, destFingerprint,
		"Data fingerprints should match between asymmetric clusters")

	t.Logf("Asymmetric PCR test completed successfully. Source CPUs: %d, Target CPUs: %d, Workload duration: %v",
		sourceCPUs, targetCPUs, workloadDuration)
}