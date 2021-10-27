// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestStreamReplicationProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	source := tc.Server(0)
	sql := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	registry := source.JobRegistry().(*jobs.Registry)

	registerConstructor := func(initialTime time.Time) *timeutil.ManualTime {
		mt := timeutil.NewManualTime(initialTime)
		jobs.RegisterConstructor(jobspb.TypeStreamReplication, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &producerJobResumer{
				job:        job,
				timeSource: mt,
			}
		})
		return mt
	}

	startJob := func(jobID jobspb.JobID, jr jobs.Record) error {
		var sj *jobs.StartableJob
		if err := source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return registry.CreateStartableJobWithTxn(ctx, &sj, jobID, txn, jr)
		}); err != nil {
			return err
		}
		return sj.Start(ctx)
	}

	timeout, username := 1*time.Second, security.MakeSQLUsernameFromPreNormalizedString("user")
	t.Run("inactive-job-fails-at-beginning", func(t *testing.T) {
		jobID, jr := producerJob(registry, 10, timeout, username)
		expiration := jr.Progress.(jobspb.StreamReplicationProgress).Expiration

		// Set the initial time to be after the timeout
		_ = registerConstructor(expiration.Add(1 * time.Millisecond))
		if err := startJob(jobID, jr); err != nil {
			t.Fatal(err)
		}

		jobsQuery := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
		sql.SucceedsSoonDuration = 1 * time.Second
		sql.CheckQueryResultsRetry(t, jobsQuery, [][]string{{"failed"}})
	})

	t.Run("inactive-job-eventually-fails", func(t *testing.T) {
		reset := streamingccl.TestingSetDefaultJobLivenessTrackingFrequency(100 * time.Millisecond)
		defer reset()

		jobID, jr := producerJob(registry, 20, timeout, username)
		jobsQuery := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
		expiration := jr.Progress.(jobspb.StreamReplicationProgress).Expiration

		// Set the initial time to be after the timeout
		mt := registerConstructor(expiration.Add(-1 * time.Millisecond))
		if err := startJob(jobID, jr); err != nil {
			t.Fatal(err)
		}
		sql.CheckQueryResults(t, jobsQuery, [][]string{{"running"}})

		// Reset the time to be after the timeout
		mt.AdvanceTo(expiration.Add(1 * time.Millisecond))
		sql.SucceedsSoonDuration = time.Second
		sql.CheckQueryResultsRetry(t, jobsQuery, [][]string{{"failed"}})
	})

	t.Run("active-job-continuously-running", func(t *testing.T) {
		reset := streamingccl.TestingSetDefaultJobLivenessTrackingFrequency(1 * time.Millisecond)
		defer reset()

		jobID, jr := producerJob(registry, 30, timeout, username)
		jobsQuery := fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
		expiration := jr.Progress.(jobspb.StreamReplicationProgress).Expiration

		// Set the initial time to be after the timeout
		mt := registerConstructor(expiration.Add(-10 * time.Millisecond))
		if err := startJob(jobID, jr); err != nil {
			t.Fatal(err)
		}
		sql.CheckQueryResults(t, jobsQuery, [][]string{{"running"}})

		// Reset the time to be still before the timeout
		mt.AdvanceTo(expiration.Add(-5 * time.Millisecond))

		// Wait for the resumer to be invoked
		time.Sleep(200 * time.Millisecond)
		sql.CheckQueryResults(t, jobsQuery, [][]string{{"running"}})
	})
}
