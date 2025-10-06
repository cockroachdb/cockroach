// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// gcTenant drops the data of tenant that has an expired deadline and updates
// the job details to mark the work it did. The job progress is updated in
// place, but needs to be persisted to the job.
func gcTenant(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tenID uint64,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if log.V(2) {
		log.Infof(ctx, "GC is being considered for tenant: %d", tenID)
	}

	if progress.Tenant.Status == jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR {
		return errors.AssertionFailedf(
			"tenant ID %d is expired and should not be in state %+v",
			tenID, jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR,
		)
	}

	var info *mtinfopb.TenantInfo
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		info, err = sql.GetTenantRecordByID(ctx, txn /* txn */, roachpb.MustMakeTenantID(tenID), execCfg.Settings)
		return err
	}); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			// The tenant row is deleted only after its data is cleared so there is
			// nothing to do in this case but mark the job as done.
			if progress.Tenant.Status != jobspb.SchemaChangeGCProgress_CLEARED {
				// This will happen if the job deletes the tenant row and fails to update
				// its progress. In this case there's nothing to do but update the job
				// progress.
				log.Errorf(ctx, "tenant ID %d not found while attempting to GC", tenID)
				progress.Tenant.Status = jobspb.SchemaChangeGCProgress_CLEARED
			}
			return nil
		}
		return errors.Wrapf(err, "fetching tenant %d", tenID)
	}

	// This case should never happen.
	if progress.Tenant.Status == jobspb.SchemaChangeGCProgress_CLEARED {
		return errors.AssertionFailedf("GC state for tenant is DELETED yet the tenant row still exists: %+v", *info)
	}

	if err := sql.GCTenantSync(ctx, execCfg, info); err != nil {
		return errors.Wrapf(err, "gc tenant %d", info.ID)
	}

	progress.Tenant.Status = jobspb.SchemaChangeGCProgress_CLEARED
	return nil
}
