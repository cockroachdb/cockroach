// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingutils

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCutoverBuiltin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db := sqlDB.DB

	startTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	streamIngestJobRecord := jobs.Record{
		Description: "test stream ingestion",
		Username:    security.RootUserName(),
		Details: jobspb.StreamIngestionDetails{
			StreamAddress: "some://address/here",
			Span:          roachpb.Span{Key: keys.LocalMax, EndKey: keys.LocalMax.Next()},
			StartTime:     startTimestamp,
		},
		Progress: jobspb.StreamIngestionProgress{},
	}
	job, err := jobs.TestingCreateAndStartJob(ctx, registry, tc.Server(0).DB(), streamIngestJobRecord)
	require.NoError(t, err)

	// Check that sentinel is not set.
	progress := job.Progress()
	sp, ok := progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	require.True(t, sp.StreamIngest.CutoverTime.IsEmpty())

	cutoverTime := timeutil.Now()
	var jobID int
	err = db.QueryRowContext(
		ctx,
		`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
		job.ID(), cutoverTime).Scan(&jobID)
	require.NoError(t, err)
	require.Equal(t, job.ID(), jobspb.JobID(jobID))

	// Check that sentinel is set on the job progress.
	sj, err := registry.LoadJob(ctx, job.ID())
	require.NoError(t, err)
	progress = sj.Progress()
	sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest)
	require.True(t, ok)
	// The builtin only offers microsecond precision and so we must account for
	// that when comparing against our chosen time.
	cutoverTime = cutoverTime.Round(time.Microsecond)
	require.Equal(t, hlc.Timestamp{WallTime: cutoverTime.UnixNano()}, sp.StreamIngest.CutoverTime)
}
