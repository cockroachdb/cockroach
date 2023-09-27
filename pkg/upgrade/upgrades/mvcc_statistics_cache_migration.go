package upgrades

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	// Import for the side effect of registering the MVCC statistics update job.
	_ "github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func createMVCCStatisticsCacheTableAndJobMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	// Create the table.
	err := createSystemTable(
		ctx,
		d.DB.KV(),
		d.Settings,
		keys.SystemSQLCodec,
		systemschema.SystemMVCCStatisticsCacheTable,
	)
	if err != nil {
		return err
	}

	// Bake the job.
	if d.TestingKnobs != nil && d.TestingKnobs.SkipMVCCStatisticsCacheJobBootstrap {
		return nil
	}

	record := jobs.Record{
		JobID:         jobs.MVCCStatisticsJobID,
		Description:   "mvcc statistics cache update job",
		Username:      username.NodeUserName(),
		Details:       jobspb.MVCCStatisticsJobDetails{},
		Progress:      jobspb.MVCCStatisticsJobProgress{},
		NonCancelable: true, // The job can't be canceled, but it can be paused.
	}
	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return d.JobRegistry.CreateIfNotExistAdoptableJobWithTxn(ctx, record, txn)
	})
}
