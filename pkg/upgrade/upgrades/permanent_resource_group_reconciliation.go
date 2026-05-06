// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	// Import for the side effect of registering the resumer.
	_ "github.com/cockroachdb/cockroach/pkg/resourcegroup/rgreconciler"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// resourceGroupReconciliationJobMigration creates the singleton resource
// group reconciliation job for this tenant at the static job ID
// jobs.ResourceGroupReconciliationJobID. CreateIfNotExistAdoptableJobWithTxn
// makes it safe to call from both bootstrapCluster (new tenants) and the
// versioned upgrade (existing tenants).
func resourceGroupReconciliationJobMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	record := jobs.Record{
		JobID:         jobs.ResourceGroupReconciliationJobID,
		Description:   "resource group reconciliation",
		Username:      username.NodeUserName(),
		Details:       jobspb.ResourceGroupReconciliationDetails{},
		Progress:      jobspb.ResourceGroupReconciliationProgress{},
		NonCancelable: true,
	}
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, record, txn)
	})
}
