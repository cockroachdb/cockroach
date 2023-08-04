// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

// TestUpdaterUpdatesJobInfo tests that all the exported methods of Updater that
// touch payload and progress correctly update the system.jobs, system.job_info,
// and in-memory job object.
func TestUpdaterUpdatesJobInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Avoiding jobs to be adopted.
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)
	ief := s.InternalDB().(isql.DB)

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(record jobs.Record) *jobs.Job {
		job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		return job
	}

	defaultRecord := jobs.Record{
		// Job does not accept an empty Details field, so arbitrarily provide
		// ImportDetails.
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.TestUserName(),
	}

	verifyPayloadAndProgress := func(t *testing.T, createdJob *jobs.Job, txn isql.Txn, expectedPayload jobspb.Payload,
		expectedProgress jobspb.Progress) {
		infoStorage := createdJob.InfoStorage(txn)

		payload, exists, err := infoStorage.GetLegacyPayload(ctx)
		require.NoError(t, err)
		require.True(t, exists)
		data, err := protoutil.Marshal(&expectedPayload)
		if err != nil {
			panic(err)
		}
		require.Equal(t, data, payload)

		progress, exists, err := infoStorage.GetLegacyProgress(ctx)
		require.NoError(t, err)
		require.True(t, exists)
		data, err = protoutil.Marshal(&expectedProgress)
		if err != nil {
			panic(err)
		}
		require.Equal(t, data, progress)
	}

	runTests := func(t *testing.T, createdJob *jobs.Job) {
		t.Run("verify against system.jobs", func(t *testing.T) {
			require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				countSystemJobs := `SELECT count(*)  FROM system.jobs`
				row, err := txn.QueryRowEx(ctx, "verify-job-query", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, countSystemJobs)
				if err != nil {
					return err
				}
				jobsCount := tree.MustBeDInt(row[0])

				countSystemJobInfo := `SELECT count(*)  FROM system.job_info;`
				row, err = txn.QueryRowEx(ctx, "verify-job-query", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, countSystemJobInfo)
				if err != nil {
					return err
				}
				jobInfoCount := tree.MustBeDInt(row[0])
				require.Equal(t, jobsCount*2, jobInfoCount)

				// Ensure no progress and payload is written to system.jobs.
				nullPayloadAndProgress := `SELECT count(*) FROM system.jobs WHERE progress IS NOT NULL OR payload IS NOT NULL;`
				row, err = txn.QueryRowEx(ctx, "verify-job-query", txn.KV(),
					sessiondata.NodeUserSessionDataOverride, nullPayloadAndProgress)
				if err != nil {
					return err
				}
				nullProgressAndPayload := tree.MustBeDInt(row[0])
				require.Equal(t, 0, int(nullProgressAndPayload))
				return nil
			}))
		})

		t.Run("verify against in-memory job", func(t *testing.T) {
			require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				verifyPayloadAndProgress(t, createdJob, txn, createdJob.Payload(), createdJob.Progress())
				return nil
			}))
		})
	}

	j := createJob(defaultRecord)
	t.Run("SetDetails", func(t *testing.T) {
		newDetails := jobspb.ImportDetails{URIs: []string{"new"}}
		require.NoError(t, j.NoTxn().SetDetails(ctx, newDetails))
		runTests(t, j)
	})

	t.Run("SetProgress", func(t *testing.T) {
		newProgress := jobspb.ImportProgress{WriteProgress: []float32{1.0}}
		require.NoError(t, j.NoTxn().SetProgress(ctx, newProgress))
		runTests(t, j)
	})

	t.Run("Update", func(t *testing.T) {
		require.NoError(t, j.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Payload.StartedMicros = timeutil.Now().UnixNano()
			md.Progress.TraceID = tracingpb.TraceID(123)
			ju.UpdateProgress(md.Progress)
			ju.UpdatePayload(md.Payload)
			return nil
		}))
		runTests(t, j)
	})

	t.Run("FractionProgressed", func(t *testing.T) {
		require.NoError(t, j.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.9)))
		runTests(t, j)
	})
}

// TestUpdateDoesNotReadPayloadFromJobsTable is a temporary test that ensures we
// can update a jobs payload and progress even after nulling out their
// corresponding values in system.jobs i.e. only relying on system.job_info.
// In a follow-up when we stop writing a job's payload and progress to the
// system.jobs table we won't require this test as there will be no option but
// to read from system.job_info.
func TestUpdateDoesNotReadPayloadFromJobsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Avoiding jobs to be adopted.
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)
	ief := s.InternalDB().(isql.DB)
	nullOutPayloadAndProgressInSystemJobs := func() {
		require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := ief.Executor().ExecEx(ctx, "null-payload-progress", txn.KV(),
				sessiondata.RootUserSessionDataOverride, `SET sql_safe_updates = false`)
			if err != nil {
				return err
			}
			_, err = ief.Executor().ExecEx(ctx, "null-payload-progress", txn.KV(),
				sessiondata.RootUserSessionDataOverride, `UPDATE system.jobs SET payload = 'nope'::BYTES, progress = NULL`)
			return err
		}))
	}

	registry := s.JobRegistry().(*jobs.Registry)

	createJob := func(record jobs.Record) *jobs.Job {
		job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
		require.NoError(t, err)
		return job
	}

	defaultRecord := jobs.Record{
		// Job does not accept an empty Details field, so arbitrarily provide
		// ImportDetails.
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.TestUserName(),
	}

	j := createJob(defaultRecord)
	nullOutPayloadAndProgressInSystemJobs()
	newDetails := jobspb.ImportDetails{URIs: []string{"new"}}
	require.NoError(t, j.NoTxn().SetDetails(ctx, newDetails))
	nullOutPayloadAndProgressInSystemJobs()

	newProgress := jobspb.ImportProgress{WriteProgress: []float32{1.0}}
	require.NoError(t, j.NoTxn().SetProgress(ctx, newProgress))
	nullOutPayloadAndProgressInSystemJobs()

	require.NoError(t, j.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		md.Payload.StartedMicros = timeutil.Now().UnixNano()
		md.Progress.TraceID = tracingpb.TraceID(123)
		ju.UpdateProgress(md.Progress)
		ju.UpdatePayload(md.Payload)
		return nil
	}))
	nullOutPayloadAndProgressInSystemJobs()
	require.NoError(t, j.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(0.9)))
}
