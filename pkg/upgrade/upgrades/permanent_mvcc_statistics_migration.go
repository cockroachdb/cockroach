// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	// Import for the side effect of registering the MVCC statistics update job.
	_ "github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func createMVCCStatisticsJob(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.TestingKnobs != nil && d.TestingKnobs.SkipMVCCStatisticsJobBootstrap {
		return nil
	}

	record := jobs.Record{
		JobID:         jobs.MVCCStatisticsJobID,
		Description:   "mvcc statistics update job",
		Username:      username.NodeUserName(),
		Details:       jobspb.MVCCStatisticsJobDetails{},
		Progress:      jobspb.MVCCStatisticsJobProgress{},
		NonCancelable: true, // The job can't be canceled, but it can be paused.
	}
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, record, txn)
	})
}
