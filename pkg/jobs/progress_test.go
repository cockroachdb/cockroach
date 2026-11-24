// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChunkProgressLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer TestingSetProgressThresholds()()

	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Set up a stub job.
	defaultRecord := Record{
		Details:  jobspb.BackupDetails{},
		Progress: jobspb.BackupProgress{},
		Username: username.TestUserName(),
	}
	jobID := jobspb.JobID(1)
	_, err := s.JobRegistry().(*Registry).CreateJobWithTxn(ctx, defaultRecord, jobID, nil /* txn */)
	require.NoError(t, err)

	loggerGroup := ctxgroup.WithContext(ctx)

	requestFinishedCh := make(chan struct{}, 100)
	loggerGroup.GoCtx(func(ctx context.Context) error {
		return NewChunkProgressLoggerForJob(
			jobID, s.InternalDB().(isql.DB), 100 /* expectedChunks */, 0 /* startFraction */, 1.0, /* targetFraction */
		).Loop(ctx, requestFinishedCh)
	})

	db := sqlutils.MakeSQLRunner(s.SQLConn(t))
	validateFrac := func(expected float64) {
		testutils.SucceedsSoon(t, func() error {
			var frac float64
			db.QueryRow(t, fmt.Sprintf(
				"SELECT fraction_completed FROM [SHOW JOBS] WHERE job_id = %d",
				jobID,
			)).Scan(&frac)
			if frac != expected {
				return fmt.Errorf("fraction not yet caught up")
			}
			return nil
		})
	}

	// Do some progress updates.
	for range 10 {
		requestFinishedCh <- struct{}{}
	}
	validateFrac(0.1)
	for range 50 {
		requestFinishedCh <- struct{}{}
	}
	validateFrac(0.6)

	// Reset to a new logger, mimicing a job pause then resume.
	close(requestFinishedCh) // Closing this chanel will cancel the previous loop.
	requestFinishedCh2 := make(chan struct{}, 100)
	loggerGroup.GoCtx(func(ctx context.Context) error {
		return NewChunkProgressLoggerForJob(
			jobID, s.InternalDB().(isql.DB), 40 /* expectedChunks */, 0.6 /* startFraction */, 1.0, /* targetFraction */
		).Loop(ctx, requestFinishedCh2)
	})

	// Verify that we pick up where we left off.
	for range 39 {
		requestFinishedCh2 <- struct{}{}
	}
	validateFrac(0.99)
	requestFinishedCh2 <- struct{}{}
	validateFrac(1)

	// Wait for loggers to finish so that the TestingSetProgressThresholds defer doesn't race with
	// their reads of progressTimeThreshold and progressFractionThreshold.
	close(requestFinishedCh2)
	require.NoError(t, loggerGroup.Wait())
}

func TestChunkProgressLoggerLimitsFloatingPointError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer TestingSetProgressThresholds()()

	rangeCount := 1240725

	var lastReported float64
	l := NewChunkProgressLogger(func(_ context.Context, pct float64) error {
		require.Less(t, pct, float64(1.01))
		lastReported = pct
		return nil
	}, rangeCount, 0, 1.0)
	for i := 0; i < rangeCount; i++ {
		require.NoError(t, l.chunkFinished(ctx), "failed at update %d", i)
	}
	require.Greater(t, lastReported, float64(0.99))
}

func TestChunkProgressLoggerTargetFraction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer TestingSetProgressThresholds()()

	const targetFraction = 0.05
	chunkCount := 100

	var lastReported float64
	l := NewChunkProgressLogger(func(_ context.Context, pct float64) error {
		require.LessOrEqual(t, pct, targetFraction+0.001)
		lastReported = pct
		return nil
	}, chunkCount, 0, targetFraction)
	for i := 0; i < chunkCount; i++ {
		require.NoError(t, l.chunkFinished(ctx), "failed at update %d", i)
	}
	require.InDelta(t, targetFraction, lastReported, 0.001)
}
