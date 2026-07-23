// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlinit

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// CreateRowLevelTTLScheduledJob creates a new row-level TTL schedule.
func CreateRowLevelTTLScheduledJob(
	ctx context.Context,
	knobs *jobs.TestingKnobs,
	s jobs.ScheduledJobStorage,
	owner username.SQLUsername,
	tblDesc catalog.TableDescriptor,
	clusterID uuid.UUID,
	version clusterversion.ClusterVersion,
) (*jobs.ScheduledJob, error) {
	if !tblDesc.HasRowLevelTTL() {
		return nil, errors.AssertionFailedf("CreateRowLevelTTLScheduledJob called with no .RowLevelTTL: %#v", tblDesc)
	}

	telemetry.Inc(sqltelemetry.RowLevelTTLCreated)
	env := jobs.JobSchedulerEnv(knobs)
	j, err := newRowLevelTTLScheduledJob(env, owner, tblDesc, clusterID, version)
	if err != nil {
		return nil, err
	}
	if err := s.Create(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

// newRowLevelTTLScheduledJob returns a *jobs.ScheduledJob for row level TTL
// for a given table. newRowLevelTTLScheduledJob assumes that
// tblDesc.RowLevelTTL is not nil.
func newRowLevelTTLScheduledJob(
	env scheduledjobs.JobSchedulerEnv,
	owner username.SQLUsername,
	tblDesc catalog.TableDescriptor,
	clusterID uuid.UUID,
	clusterVersion clusterversion.ClusterVersion,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(ttlbase.BuildScheduleLabel(tblDesc))
	sj.SetOwner(owner)
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		Wait: jobspb.ScheduleDetails_SKIP,
		// If a job fails, try again at the allocated cron time.
		OnError:                jobspb.ScheduleDetails_RETRY_SCHED,
		ClusterID:              clusterID,
		CreationClusterVersion: clusterVersion,
	})

	if err := sj.SetScheduleAndNextRun(tblDesc.GetRowLevelTTL().DeletionCronOrDefault()); err != nil {
		return nil, err
	}
	args := &catpb.ScheduledRowLevelTTLArgs{
		TableID: tblDesc.GetID(),
	}
	anyArgs, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledRowLevelTTLExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: anyArgs},
	)
	return sj, nil
}
