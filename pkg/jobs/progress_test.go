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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	idb := s.InternalDB().(isql.DB)
	r := s.JobRegistry().(*Registry)

	// Set up a stub job.
	defaultRecord := Record{
		Details:  jobspb.BackupDetails{},
		Progress: jobspb.BackupProgress{},
		Username: username.TestUserName(),
	}
	jobID := jobspb.JobID(1)
	_, err := r.CreateJobWithTxn(ctx, defaultRecord, jobID, nil /* txn */)
	require.NoError(t, err)

	logger := NewChunkProgressLoggerForJob(
		jobID, idb, 100 /* expectedChunks */, 0, /* startFraction */
	)

	requestFinishedCh := make(chan struct{}, 100)
	progLoop := func(ctx context.Context) error {
		return logger.Loop(ctx, requestFinishedCh)
	}
	ping := func(ctx context.Context) error {
		validateFrac := func(expected float64) {
			testutils.SucceedsSoon(t, func() error {
				return idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					d, err := txn.QueryRow(ctx, "read-fraction-completed", txn.KV(), fmt.Sprintf(
						"SELECT fraction_completed FROM [SHOW JOBS] WHERE job_id = %d",
						jobID,
					))
					if err != nil {
						return err
					}
					val, ok := d[0].(*tree.DFloat)
					if !ok {
						return fmt.Errorf("not a float")
					}
					frac := float64(*val)
					if frac != expected {
						return fmt.Errorf("fraction not yet caught up")
					}
					return nil
				})
			})
		}

		for range 10 {
			requestFinishedCh <- struct{}{}
		}
		validateFrac(0.1)

		for range 50 {
			requestFinishedCh <- struct{}{}
		}
		validateFrac(0.6)

		for range 39 {
			requestFinishedCh <- struct{}{}
		}
		validateFrac(0.99)

		requestFinishedCh <- struct{}{}
		validateFrac(1)

		close(requestFinishedCh)
		return nil
	}

	require.NoError(t, ctxgroup.GoAndWait(ctx, ping, progLoop))
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
	}, rangeCount, 0)
	for i := 0; i < rangeCount; i++ {
		require.NoError(t, l.chunkFinished(ctx), "failed at update %d", i)
	}
	require.Greater(t, lastReported, float64(0.99))
}
