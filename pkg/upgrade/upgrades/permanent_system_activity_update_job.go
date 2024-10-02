// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// createActivityUpdateJobMigration creates the job to update the
// system.statement_activity and system.transaction_activity tables.
func createActivityUpdateJobMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	if d.TestingKnobs != nil && d.TestingKnobs.SkipUpdateSQLActivityJobBootstrap {
		return nil
	}

	record := jobs.Record{
		JobID:         jobs.SqlActivityUpdaterJobID,
		Description:   "sql activity job",
		Username:      username.NodeUserName(),
		Details:       jobspb.AutoUpdateSQLActivityDetails{},
		Progress:      jobspb.AutoUpdateSQLActivityProgress{},
		NonCancelable: true, // The job can't be canceled, but it can be paused.
	}

	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, record, txn)
	})
}
