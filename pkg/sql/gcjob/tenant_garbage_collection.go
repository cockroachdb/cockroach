// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

	if progress.Tenant.Status == jobspb.SchemaChangeGCProgress_WAITING_FOR_GC {
		return errors.AssertionFailedf(
			"Tenant id %d is expired and should not be in state %+v",
			tenID, jobspb.SchemaChangeGCProgress_WAITING_FOR_GC,
		)
	}

	info, err := sql.GetTenantRecord(ctx, execCfg, nil /* txn */, tenID)
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			// The tenant row is deleted only after its data is cleared so there is
			// nothing to do in this case but mark the job as done.
			if progress.Tenant.Status != jobspb.SchemaChangeGCProgress_DELETED {
				// This will happen if the job deletes the tenant row and fails to update
				// its progress. In this case there's nothing to do but update the job
				// progress.
				log.Errorf(ctx, "tenant id %d not found while attempting to GC", tenID)
				progress.Tenant.Status = jobspb.SchemaChangeGCProgress_DELETED
			}
			return nil
		}
		return errors.Wrapf(err, "fetching tenant %d", info.ID)
	}

	// This case should never happen.
	if progress.Tenant.Status == jobspb.SchemaChangeGCProgress_DELETED {
		return errors.AssertionFailedf("GC state for tenant %+v is DELETED yet the tenant row still exists", info)
	}

	if info.State != descpb.TenantInfo_DROP {
		return errors.AssertionFailedf("tenant %+v is not in state DROP", info)
	}

	// First, delete all the tenant data.
	if err := clearTenantData(ctx, execCfg.DB, info); err != nil {
		return errors.Wrapf(err, "clearing data for tenant %d", info.ID)
	}

	// Finished deleting all the table data, now delete the tenant row.
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return errors.Wrapf(
			sql.DeleteTenantRecord(ctx, execCfg, txn, info), "deleting tenant %d", info.ID,
		)
	}); err != nil {
		return err
	}

	progress.Tenant.Status = jobspb.SchemaChangeGCProgress_DELETED
	return nil
}

// clearTenantData deletes all of the data in the specified table.
func clearTenantData(ctx context.Context, db *kv.DB, tenant *descpb.TenantInfo) error {
	log.Infof(ctx, "clearing data for tenant %d", tenant.ID)

	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenant.ID))
	prefixEnd := prefix.PrefixEnd()

	log.VEventf(ctx, 2, "ClearRange %s - %s", prefix, prefixEnd)
	// ClearRange cannot be run in a transaction, so create a non-transactional
	// batch to send the request.
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: prefix, EndKey: prefixEnd},
	})
	return db.Run(ctx, b)
}
