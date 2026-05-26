// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register ExternalStorage providers.
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// fakeExecResumer calls optional callbacks during the job lifecycle.
type fakeExecResumer struct {
	OnResume     func(context.Context) error
	FailOrCancel func(context.Context) error
}

func (d fakeExecResumer) ForceRealSpan() bool {
	return true
}

func (d fakeExecResumer) DumpTraceAfterRun() bool {
	return true
}

var _ jobs.Resumer = fakeExecResumer{}
var _ jobs.TraceableJob = fakeExecResumer{}

func (d fakeExecResumer) Resume(ctx context.Context, execCtx interface{}) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d fakeExecResumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

func (d fakeExecResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// TestJobExecutionDetailsRouting tests that the request job execution details
// endpoint redirects the request to the current coordinator node of the job.
func TestJobExecutionDetailsRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	hasStartedCh := make(chan struct{})
	defer close(hasStartedCh)
	canContinueCh := make(chan struct{})
	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(j *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeExecResumer{
			OnResume: func(ctx context.Context) error {
				hasStartedCh <- struct{}{}
				<-canContinueCh
				return nil
			},
		}
	}, jobs.UsesTenantCostControl)()
	defer jobs.ResetConstructors()()

	dialedNodeID := roachpb.NodeID(-1)
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &TestingKnobs{
					DialNodeCallback: func(ctx context.Context, nodeID roachpb.NodeID) error {
						// We only care about the first call to dialNode.
						if dialedNodeID == -1 {
							dialedNodeID = nodeID
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(1)
	sqlDB := s.SQLConn(t, serverutils.DBName("n1"))
	defer sqlDB.Close()

	_, err := sqlDB.Exec(`CREATE TABLE defaultdb.t (id INT)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO defaultdb.t SELECT generate_series(1, 100)`)
	require.NoError(t, err)
	var importJobID int
	err = sqlDB.QueryRow(`IMPORT INTO defaultdb.t CSV DATA ('nodelocal://1/foo') WITH DETACHED`).Scan(&importJobID)
	require.NoError(t, err)
	<-hasStartedCh

	// Get the job's current coordinator ID.
	var claimInstanceID int
	err = sqlDB.QueryRow(`SELECT claim_instance_id FROM system.jobs WHERE id = $1`, importJobID).Scan(&claimInstanceID)
	require.NoError(t, err)

	nonCoordinatorIDIdx := 0
	if claimInstanceID == 1 {
		// We want to pick the non-coordinator node to send our request to. Idx is
		// zero-indexed, so if the coordinator is node 1, we want to pick node 2.
		nonCoordinatorIDIdx = 1
	}
	nonCoordServer := tc.Server(nonCoordinatorIDIdx)
	var resp serverpb.RequestJobProfilerExecutionDetailsResponse
	path := fmt.Sprintf("/_status/request_job_profiler_execution_details/%d", importJobID)
	err = serverutils.GetJSONProto(nonCoordServer, path, &resp)
	require.NoError(t, err)
	require.Equal(t, serverpb.RequestJobProfilerExecutionDetailsResponse{}, resp)
	require.Equal(t, claimInstanceID, int(dialedNodeID))
	close(canContinueCh)
}
