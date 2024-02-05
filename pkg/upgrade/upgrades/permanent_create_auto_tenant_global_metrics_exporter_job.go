// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func createAutoTenantGlobalMetricsExporterJobMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.TestingKnobs != nil && d.TestingKnobs.SkipAutoTenantGlobalMetricsExporterJobBootstrap {
		return nil
	}
	record := jobs.Record{
		JobID:         jobs.AutoTenantGlobalMetricsExporterJobID,
		Description:   jobspb.TypeAutoTenantGlobalMetricsExporter.String(),
		Username:      username.NodeUserName(),
		CreatedBy:     &jobs.CreatedByInfo{Name: username.NodeUser, ID: username.NodeUserID},
		Details:       jobspb.AutoTenantGlobalMetricsExporterDetails{},
		Progress:      jobspb.AutoTenantGlobalMetricsExporterProgress{},
		NonCancelable: true, // The job can't be canceled, but it can be paused.
	}
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, record, txn)
	})
}
