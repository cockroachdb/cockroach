// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// execCtxResumer is a test-only Resumer that forwards the exec context
// to its callback, allowing tests to exercise the job's database
// accessors.
type execCtxResumer struct {
	onResume func(ctx context.Context, execCtx interface{}) error
}

func (r execCtxResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return r.onResume(ctx, execCtx)
}
func (r execCtxResumer) OnFailOrCancel(context.Context, interface{}, error) error {
	return nil
}
func (r execCtxResumer) CollectProfile(context.Context, interface{}) error {
	return nil
}

// TestJobKVWorkloadIDPropagation verifies that KV requests made
// through ExecCfg().DB within a job carry the job's ID in the
// BatchRequest.Header.WorkloadID field. The workload ID is injected
// into the context by the jobs registry.
func TestJobKVWorkloadIDPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	queryDone := make(chan struct{})
	resumeDone := make(chan struct{})

	// expectedJobID is set after the job is created but before the
	// resumer runs. The request filter reads it atomically.
	var expectedJobID atomic.Uint64
	var sawJobWorkloadID atomic.Bool

	defer jobs.ResetConstructors()()
	cleanup := jobs.TestingRegisterConstructor(
		jobspb.TypeImport,
		func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return execCtxResumer{
				onResume: func(ctx context.Context, execCtx interface{}) error {
					jec := execCtx.(sql.JobExecContext)
					// Run a transactional KV read through ExecCfg().DB.
					// The context carries the workload ID injected by
					// the registry, so the BatchRequest should carry
					// WorkloadID in its header.
					if err := jec.ExecCfg().DB.Txn(ctx, func(
						ctx context.Context, txn *kv.Txn,
					) error {
						_, err := txn.Get(ctx, "job-kv-test-key")
						return err
					}); err != nil {
						return err
					}
					close(queryDone)
					<-resumeDone
					return nil
				},
			}
		},
		jobs.UsesTenantCostControl,
	)
	defer cleanup()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			KeyVisualizer:    &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(
					_ context.Context, ba *kvpb.BatchRequest,
				) *kvpb.Error {
					if wid := expectedJobID.Load(); wid != 0 &&
						ba.Header.WorkloadID == wid {
						sawJobWorkloadID.Store(true)
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	// Unblock the resumer before the stopper stops, so the job
	// goroutine can exit cleanly (even if an assertion fails).
	defer close(resumeDone)

	r := srv.JobRegistry().(*jobs.Registry)
	idb := srv.InternalDB().(isql.DB)
	jobID := r.MakeJobID()
	expectedJobID.Store(uint64(jobID))

	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := r.CreateJobWithTxn(ctx, jobs.Record{
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
			Username: username.TestUserName(),
		}, jobID, txn)
		return err
	}))

	// Wait for the resumer to finish its KV read.
	<-queryDone

	require.True(t, sawJobWorkloadID.Load(),
		"expected BatchRequest.Header.WorkloadID to contain the job ID %d", jobID)
}

// TestJobWorkloadIDContextPropagation verifies that the context
// passed to Resume carries the workload ID set by the registry,
// and that it is not present outside job execution.
func TestJobWorkloadIDContextPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Verify that the background context has no workload info.
	wid, wtype := kv.WorkloadInfoFromContext(ctx)
	require.Equal(t, uint64(0), wid)
	require.Equal(t, workloadid.WorkloadTypeUnknown, wtype)

	resumeDone := make(chan struct{})
	var observedWID atomic.Uint64
	var observedType atomic.Uint32

	defer jobs.ResetConstructors()()
	cleanup := jobs.TestingRegisterConstructor(
		jobspb.TypeImport,
		func(_ *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return jobstest.FakeResumer{
				OnResume: func(ctx context.Context) error {
					id, wt := kv.WorkloadInfoFromContext(ctx)
					observedWID.Store(id)
					observedType.Store(uint32(wt))
					close(resumeDone)
					return nil
				},
			}
		},
		jobs.UsesTenantCostControl,
	)
	defer cleanup()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			KeyVisualizer:    &keyvisualizer.TestingKnobs{SkipJobBootstrap: true},
			SpanConfig: &spanconfig.TestingKnobs{
				ManagerDisableJobCreation: true,
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	r := srv.JobRegistry().(*jobs.Registry)
	idb := srv.InternalDB().(isql.DB)
	jobID := r.MakeJobID()

	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := r.CreateJobWithTxn(ctx, jobs.Record{
			Details:  jobspb.ImportDetails{},
			Progress: jobspb.ImportProgress{},
			Username: username.TestUserName(),
		}, jobID, txn)
		return err
	}))

	<-resumeDone

	require.Equal(t, uint64(jobID), observedWID.Load(),
		"expected Resume context to carry workload ID matching job ID %d", jobID)
	require.Equal(t, uint32(workloadid.WorkloadTypeJob), observedType.Load(),
		"expected Resume context to carry WorkloadTypeJob")
}
