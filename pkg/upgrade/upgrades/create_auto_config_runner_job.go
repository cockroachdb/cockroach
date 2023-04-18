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
	_ "github.com/cockroachdb/cockroach/pkg/jobs/metricspoller" // Ensure job implementation is linked.
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func createAutoConfigRunnerJob(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.TestingKnobs != nil && d.TestingKnobs.SkipAutoConfigRunnerJobBootstrap {
		return nil
	}

	if err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		row, err := d.DB.Executor().QueryRowEx(
			ctx,
			"check for existing auto config runner job",
			nil,
			sessiondata.RootUserSessionDataOverride,
			"SELECT 1 FROM system.jobs WHERE id = $1",
			jobs.AutoConfigRunnerJobID,
		)
		if err != nil {
			return err
		}

		if row != nil {
			// Job already exists. Nothing to do.
			return nil
		}

		jr := jobs.Record{
			JobID:         jobs.AutoConfigRunnerJobID,
			Description:   "applies automatic configuration",
			Details:       jobspb.AutoConfigRunnerDetails{},
			Progress:      jobspb.AutoConfigRunnerProgress{},
			CreatedBy:     &jobs.CreatedByInfo{Name: username.RootUser, ID: int64(username.RootUserID)},
			Username:      username.RootUserName(),
			NonCancelable: true,
		}
		// Use CreateJob instead of CreateAdoptableJob to ensure this node
		// has a claim on the job and can start it immediately below.
		_, err = d.JobRegistry.CreateJobWithTxn(ctx, jr, jobs.AutoConfigRunnerJobID, txn)
		return err
	}); err != nil {
		return err
	}

	// Start the job immediately. This speeds up the application
	// of initial configuration tasks.
	d.JobRegistry.NotifyToResume(ctx, jobs.AutoConfigRunnerJobID)
	return nil
}
