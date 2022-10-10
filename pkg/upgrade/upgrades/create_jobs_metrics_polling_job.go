// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func createJobsMetricsPollingJob(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		exists, err := jobs.JobExists(ctx, jobspb.InvalidJobID, d.InternalExecutor, txn, false, func(payload *jobspb.Payload) bool {
			return payload.Type() == jobspb.TypePollJobsStats
		})
		if err != nil {
			return err
		}

		if exists {
			return nil
		}

		jr := jobs.Record{
			JobID:         jobs.JobMetricsPollingJobID,
			Description:   jobspb.TypePollJobsStats.String(),
			Details:       jobspb.PollJobsStatsDetails{},
			Progress:      jobspb.PollJobsStatsProgress{},
			CreatedBy:     &jobs.CreatedByInfo{Name: username.RootUser, ID: username.RootUserID},
			Username:      username.RootUserName(),
			NonCancelable: true,
		}
		if _, err := d.JobRegistry.CreateAdoptableJobWithTxn(ctx, jr, jobs.JobMetricsPollingJobID, txn); err != nil {
			return err
		}

		return nil
	})
}
