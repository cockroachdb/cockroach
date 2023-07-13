// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestAlterTenantCompleteToTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

func TestAlterTenantCompleteToLatest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	targetReplicatedTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(targetReplicatedTime, jobspb.JobID(ingestionJobID))

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`,
		args.DestTenantName).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.GreaterOrEqual(t, cutoverOutput.GoTime(), targetReplicatedTime.GoTime())
	require.LessOrEqual(t, cutoverOutput.GoTime(), c.SrcCluster.Server(0).Clock().Now().GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

func TestAlterTenantPauseResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	// Pause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Unpause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	cleanupTenant := c.StartDestTenant(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	t.Run("pause-nonexistant-tenant", func(t *testing.T) {
		c.DestSysSQL.ExpectErr(t, "tenant \"nonexistent\" does not exist", `ALTER TENANT $1 PAUSE REPLICATION`, "nonexistent")
	})

	t.Run("pause-resume-tenant-with-no-replication", func(t *testing.T) {
		c.DestSysSQL.Exec(t, `CREATE TENANT noreplication`)
		c.DestSysSQL.ExpectErr(t, `tenant "noreplication" \(3\) does not have an active replication job`,
			`ALTER TENANT $1 PAUSE REPLICATION`, "noreplication")
		c.DestSysSQL.ExpectErr(t, `tenant "noreplication" \(3\) does not have an active replication job`,
			`ALTER TENANT $1 RESUME REPLICATION`, "noreplication")
	})

	t.Run("pause-resume-in-readonly-txn", func(t *testing.T) {
		c.DestSysSQL.Exec(t, `set default_transaction_read_only = on;`)
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER VIRTUAL CLUSTER REPLICATION in a read-only transaction", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER VIRTUAL CLUSTER REPLICATION in a read-only transaction", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
		c.DestSysSQL.Exec(t, `set default_transaction_read_only = off;`)
	})

	t.Run("pause-resume-as-non-system-tenant", func(t *testing.T) {
		c.DestTenantSQL.Exec(t, `SET CLUSTER SETTING cross_cluster_replication.enabled = true`)
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
	})
}

// TestAlterTenantUpdateExistingCutoverTime verifies we can set a new cutover
// time if the cutover process did not start yet.
func TestAlterTenantUpdateExistingCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT data_state FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}
	getCutoverTime := func() hlc.Timestamp {
		var cutoverStr string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT cutover_time FROM [SHOW TENANT %s WITH REPLICATION STATUS]",
			c.Args.DestTenantName)).Scan(&cutoverStr)
		cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
		return cutoverOutput
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	replicatedTimeTarget := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(replicatedTimeTarget, jobspb.JobID(ingestionJobID))

	// First cutover to a future time.
	var cutoverStr string
	cutoverTime := replicatedTimeTarget.Add(time.Hour.Nanoseconds(), 0)
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime.AsOfSystemTime()).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput)
	require.Equal(c.T, "replication pending cutover", getTenantStatus())
	require.Equal(t, cutoverOutput, getCutoverTime())

	// And cutover to an even further time.
	cutoverTime = replicatedTimeTarget.Add((time.Hour * 2).Nanoseconds(), 0)
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime.AsOfSystemTime()).Scan(&cutoverStr)
	cutoverOutput = replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput)
	require.Equal(c.T, "replication pending cutover", getTenantStatus())
	require.Equal(t, cutoverOutput, getCutoverTime())
}

// TestAlterTenantFailUpdatingCutoverTime verifies that once a cutover has
// started the cutover time cannot be updated.
func TestAlterTenantFailUpdatingCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	cutoverCh := make(chan struct{})
	cutoverStartedCh := make(chan struct{})
	// Set a knob to hang after we transition to the cutover state, and before the
	// job finishes.
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		AfterCutoverStarted: func() {
			close(cutoverStartedCh)
			select {
			case <-cutoverCh:
			case <-ctx.Done():
			}
		},
	}
	unblockJob := func() {
		select {
		case cutoverCh <- struct{}{}:
		case <-ctx.Done():
		}
	}

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	defer ctxCancel()

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT data_state FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "replicating", getTenantStatus())

	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`, args.DestTenantName)

	// Wait for cutover to start.
	<-cutoverStartedCh

	// Another cutover should fail, verify the error.
	c.DestSysSQL.ExpectErr(t, "already started cutting over to timestamp",
		fmt.Sprintf("ALTER TENANT %s COMPLETE REPLICATION TO LATEST", args.DestTenantName))

	// Done, the tenant is cutting over, unblock the job.
	unblockJob()
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
}

// blockingResumer hangs until signaled, before and after running the real
// resumer.
type blockingResumer struct {
	orig       jobs.Resumer
	waitBefore chan struct{}
	waitAfter  chan struct{}
	ctx        context.Context
}

var _ jobs.Resumer = (*blockingResumer)(nil)

func (br *blockingResumer) Resume(ctx context.Context, execCtx interface{}) error {
	select {
	case <-br.ctx.Done():
	case <-br.waitBefore:
	}
	r := br.orig.Resume(ctx, execCtx)
	select {
	case <-br.ctx.Done():
	case <-br.waitAfter:
	}
	return r
}

func (br *blockingResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	panic("unimplemented")
}

// TestTenantStatusWithFutureCutoverTime verifies we go through the tenants
// states, including the state that the tenant is waiting for a future cutover.
func TestTenantStatusWithFutureCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	waitBeforeCh := make(chan struct{})
	waitAfterCh := make(chan struct{})
	registry := c.DestSysServer.JobRegistry().(*jobs.Registry)
	registry.TestingWrapResumerConstructor(jobspb.TypeStreamIngestion,
		func(raw jobs.Resumer) jobs.Resumer {
			r := blockingResumer{
				orig:       raw,
				waitBefore: waitBeforeCh,
				waitAfter:  waitAfterCh,
				ctx:        ctx,
			}
			return &r
		})
	defer ctxCancel()
	unblockResumerStart := func() {
		waitBeforeCh <- struct{}{}
	}
	unblockResumerExit := func() {
		waitAfterCh <- struct{}{}
	}

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT data_state FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	// The resumer cannot start at this point, therefore the tenant will stay in
	// the init state.
	require.Equal(c.T, "initializing replication", getTenantStatus())
	unblockResumerStart()

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "replicating", getTenantStatus())

	c.DestSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "replication paused", getTenantStatus())

	// On pause the resumer exits, we should unblock it.
	unblockResumerExit()
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.DestTenantName)
	unblockResumerStart()
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "replicating", getTenantStatus())

	// Cutover to a time far in the future, to make sure we see the pending-cutover state.
	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
	cutoverTime = cutoverTime.Add(time.Hour * 24)
	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime)

	require.Equal(c.T, "replication pending cutover", getTenantStatus())
	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`, args.DestTenantName)
	unblockResumerExit()
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	require.Equal(c.T, "ready", getTenantStatus())
}

// TestTenantStatusWithLatestCutoverTime verifies we go through the actual
// cutting-over state, which was not verified in the test above because we
// cannot (currently) alter the cutover time.
func TestTenantStatusWithLatestCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, ctxCancel := context.WithCancel(context.Background())
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	cutoverCh := make(chan struct{})
	// Set a knob to hang after we transition to the cutover state, and before the
	// job finishes.
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		AfterCutoverStarted: func() { <-cutoverCh },
	}

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	waitBeforeCh := make(chan struct{})
	waitAfterCh := make(chan struct{})
	registry := c.DestSysServer.JobRegistry().(*jobs.Registry)
	registry.TestingWrapResumerConstructor(jobspb.TypeStreamIngestion,
		func(raw jobs.Resumer) jobs.Resumer {
			r := blockingResumer{
				orig:       raw,
				waitBefore: waitBeforeCh,
				waitAfter:  waitAfterCh,
				ctx:        ctx,
			}
			return &r
		})
	defer ctxCancel()
	unblockResumerStart := func() {
		waitBeforeCh <- struct{}{}
	}
	unblockResumerExit := func() {
		waitAfterCh <- struct{}{}
	}

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT data_state FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	require.Equal(c.T, "initializing replication", getTenantStatus())
	unblockResumerStart()

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "replicating", getTenantStatus())

	c.DestSysSQL.Exec(c.T, fmt.Sprintf("ALTER TENANT %s COMPLETE REPLICATION TO LATEST", args.DestTenantName))

	testutils.SucceedsSoon(t, func() error {
		s := getTenantStatus()
		if s == "replication pending cutover" {
			return errors.Errorf("tenant status is still 'replication pending cutover', waiting")
		}
		require.Equal(c.T, "replication cutting over", s)
		return nil
	})

	// Done, the tenant is cutting over, unblock the job.
	cutoverCh <- struct{}{}
	unblockResumerExit()
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	require.Equal(c.T, "ready", getTenantStatus())
}

// TestTenantReplicationStatus verifies replication status errors.
func TestTenantReplicationStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	registry := c.DestSysServer.JobRegistry().(*jobs.Registry)

	// Pass a nonexistent job id.
	_, status, err := getReplicationStatsAndStatus(ctx, registry, nil, jobspb.JobID(-1))
	require.ErrorContains(t, err, "job with ID -1 does not exist")
	require.Equal(t, "replication error", status)

	// The producer job in the source cluster should not have a replication status.
	registry = c.SrcSysServer.JobRegistry().(*jobs.Registry)
	_, status, err = getReplicationStatsAndStatus(ctx, registry, nil, jobspb.JobID(producerJobID))
	require.ErrorContains(t, err, "is not a stream ingestion job")
	require.Equal(t, "replication error", status)
}

// TestAlterTenantHandleFutureProtectedTimestamp verifies that cutting over "TO
// LATEST" doesn't fail if the destination cluster clock is ahead of the source
// cluster clock. See issue #96477.
func TestAlterTenantHandleFutureProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	// Push the clock on the destination cluster forward.
	destNow := c.DestCluster.Server(0).Clock().NowAsClockTimestamp()
	destNow.WallTime += (200 * time.Millisecond).Nanoseconds()
	c.DestCluster.Server(0).Clock().Update(destNow)

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilReplicatedTime(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO LATEST`, args.DestTenantName)
}
