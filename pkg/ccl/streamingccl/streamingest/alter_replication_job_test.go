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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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

	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	// Pause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Unpause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	cleanupTenant := c.CreateDestTenantSQL(ctx)
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
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
		c.DestSysSQL.Exec(t, `set default_transaction_read_only = off;`)
	})

	t.Run("pause-resume-as-non-system-tenant", func(t *testing.T) {
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
	})
}

// blockingResumer hangs until signaled, before and after running the real
// resumer.
type blockingResumer struct {
	orig       jobs.Resumer
	waitBefore chan struct{}
	waitAfter  chan struct{}
}

var _ jobs.Resumer = (*blockingResumer)(nil)

func (fr *blockingResumer) Resume(ctx context.Context, execCtx interface{}) error {
	<-fr.waitBefore
	r := fr.orig.Resume(ctx, execCtx)
	<-fr.waitAfter
	return r
}

func (fr *blockingResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	panic("unimplemented")
}

// TestTenantStatusWithFutureCutoverTime verifies we go through the tenants
// states, including the state that the tenant is waiting for a future cutover.
func TestTenantStatusWithFutureCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	waitBeforeCh := make(chan struct{})
	waitAfterCh := make(chan struct{})
	registry := c.DestSysServer.JobRegistry().(*jobs.Registry)
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeStreamIngestion: func(raw jobs.Resumer) jobs.Resumer {
			r := blockingResumer{
				orig:       raw,
				waitBefore: waitBeforeCh,
				waitAfter:  waitAfterCh,
			}
			return &r
		},
	}

	unblockResumerStart := func() {
		waitBeforeCh <- struct{}{}
	}
	unblockResumerExit := func() {
		waitAfterCh <- struct{}{}
	}

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT status FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	// The resumer cannot start at this point, therefore the tenant will stay in
	// the init state.
	require.Equal(c.T, "INITIALIZING REPLICATION", getTenantStatus())
	unblockResumerStart()

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "REPLICATING", getTenantStatus())

	c.DestSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "REPLICATION PAUSED", getTenantStatus())

	// On pause the resumer exits, we should unblock it.
	unblockResumerExit()
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.DestTenantName)
	unblockResumerStart()
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "REPLICATING", getTenantStatus())

	// Cutover to a time far in the future, to make sure we see the pending-cutover state.
	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)
	cutoverTime.Add(time.Hour * 24)
	c.DestSysSQL.Exec(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime)

	require.Equal(c.T, "REPLICATION PENDING CUTOVER", getTenantStatus())
	unblockResumerExit()
}

// TestTenantStatusWithLatestCutoverTime verifies we go through the actual
// cutting-over state, which was not verified in the test above because we
// cannot (currently) alter the cutover time.
func TestTenantStatusWithLatestCutoverTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
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
	registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		jobspb.TypeStreamIngestion: func(raw jobs.Resumer) jobs.Resumer {
			r := blockingResumer{
				orig:       raw,
				waitBefore: waitBeforeCh,
				waitAfter:  waitAfterCh,
			}
			return &r
		},
	}

	unblockResumerStart := func() {
		waitBeforeCh <- struct{}{}
	}
	unblockResumerExit := func() {
		waitAfterCh <- struct{}{}
	}

	getTenantStatus := func() string {
		var status string
		c.DestSysSQL.QueryRow(c.T, fmt.Sprintf("SELECT status FROM [SHOW TENANT %s]",
			c.Args.DestTenantName)).Scan(&status)
		return status
	}

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	require.Equal(c.T, "INITIALIZING REPLICATION", getTenantStatus())
	unblockResumerStart()

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	require.Equal(c.T, "REPLICATING", getTenantStatus())

	c.DestSysSQL.Exec(c.T, fmt.Sprintf("ALTER TENANT %s COMPLETE REPLICATION TO LATEST", args.DestTenantName))

	testutils.SucceedsSoon(t, func() error {
		s := getTenantStatus()
		if s == "REPLICATION PENDING CUTOVER" {
			return errors.Errorf("tenant status is still REPLICATION PENDING CUTOVER, waiting")
		}
		require.Equal(c.T, "REPLICATION CUTTING OVER", s)
		return nil
	})

	// Done, the tenant is cutting over, unblock the job.
	cutoverCh <- struct{}{}
	unblockResumerExit()
	require.Equal(c.T, "ACTIVE", getTenantStatus())
}
