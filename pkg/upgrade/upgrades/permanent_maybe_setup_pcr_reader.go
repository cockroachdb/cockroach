// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func maybeSetupPCRStandbyReader(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.TenantInfoAccessor == nil {
		return nil
	}
	id, ts, err := d.TenantInfoAccessor.ReadFromTenantInfo(ctx)
	if err != nil {
		return err
	}
	if !id.IsSet() {
		return nil
	}
	if ts.IsEmpty() {
		log.Warning(ctx, "replicated timestamp is zero")
	} else {
		log.Infof(ctx, "setting up read-only catalog as of %s reading from tenant %s", ts, id)
		if err := replication.SetupOrAdvanceStandbyReaderCatalog(ctx, id, ts, d.DB, d.Settings); err != nil {
			return err
		}
	}

	if err := createStandbyReadTSPollerJob(ctx, d); err != nil {
		return err
	}
	return nil
}

func createStandbyReadTSPollerJob(ctx context.Context, d upgrade.TenantDeps) error {
	record := jobs.Record{
		JobID:         d.JobRegistry.MakeJobID(),
		Description:   "standby read-only timestamp poller job",
		Username:      d.SessionData.User(),
		Details:       jobspb.StandbyReadTSPollerDetails{},
		Progress:      jobspb.StandbyReadTSPollerProgress{},
		NonCancelable: true,
	}

	return d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if _, err := d.JobRegistry.CreateJobWithTxn(ctx, record, record.JobID, txn); err != nil {
			return err
		}
		return nil
	})
}
