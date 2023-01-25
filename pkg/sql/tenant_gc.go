// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GCTenantSync clears the tenant's data and removes its record.
// This is the function triggered by the RESTORE and GC jobs.
//
// The caller is responsible for checking that the user is authorized
// to take this action.
func GCTenantSync(ctx context.Context, execCfg *ExecutorConfig, info *mtinfopb.TenantInfo) error {
	const op = "gc"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, op); err != nil {
		return err
	}

	if err := clearTenant(ctx, execCfg, info); err != nil {
		return errors.Wrap(err, "clear tenant")
	}

	err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if num, err := txn.ExecEx(
			ctx, "delete-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenants WHERE id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d", info.ID)
		} else if num != 1 {
			// It's possible for us to be GCing a tenant record that hasn't
			// been fully written yet, e.g. during RESTORE.
			log.Warningf(ctx, "tenant GC: no record to delete for %d", info.ID)
		}

		if _, err := txn.ExecEx(
			ctx, "delete-tenant-usage", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenant_usage WHERE tenant_id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d usage", info.ID)
		}

		if _, err := txn.ExecEx(
			ctx, "delete-tenant-settings", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenant_settings WHERE tenant_id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d settings", info.ID)
		}

		// Clear out all span config records left over by the tenant.
		tenID := roachpb.MustMakeTenantID(info.ID)
		tenantPrefix := keys.MakeTenantPrefix(tenID)
		tenantSpan := roachpb.Span{
			Key:    tenantPrefix,
			EndKey: tenantPrefix.PrefixEnd(),
		}

		systemTarget, err := spanconfig.MakeTenantKeyspaceTarget(tenID, tenID)
		if err != nil {
			return err
		}
		scKVAccessor := execCfg.SpanConfigKVAccessor.WithTxn(ctx, txn.KV())
		records, err := scKVAccessor.GetSpanConfigRecords(
			ctx, []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(tenantSpan),
				spanconfig.MakeTargetFromSystemTarget(systemTarget),
			},
		)
		if err != nil {
			return err
		}

		toDelete := make([]spanconfig.Target, len(records))
		for i, record := range records {
			toDelete[i] = record.GetTarget()
		}
		return scKVAccessor.UpdateSpanConfigRecords(
			ctx, toDelete, nil, hlc.MinTimestamp, hlc.MaxTimestamp,
		)
	})
	return errors.Wrapf(err, "deleting tenant %d record", info.ID)
}

// clearTenant deletes the tenant's data.
func clearTenant(ctx context.Context, execCfg *ExecutorConfig, info *mtinfopb.TenantInfo) error {
	// Confirm tenant is ready to be cleared.
	if info.DataState != mtinfopb.DataStateDrop {
		return errors.Errorf("tenant %d is not in state DROP", info.ID)
	}

	log.Infof(ctx, "clearing data for tenant %d", info.ID)

	prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(info.ID))
	prefixEnd := prefix.PrefixEnd()

	log.VEventf(ctx, 2, "ClearRange %s - %s", prefix, prefixEnd)
	// ClearRange cannot be run in a transaction, so create a non-transactional
	// batch to send the request.
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: prefix, EndKey: prefixEnd},
	})

	return errors.Wrapf(execCfg.DB.Run(ctx, b), "clearing tenant %d data", info.ID)
}

// GCTenant implements the tree.TenantOperator interface.
// This is the function used by the crdb_internal.gc_tenant built-in function.
// It garbage-collects a tenant already in the DROP state.
//
// TODO(jeffswenson): Delete crdb_internal.gc_tenant after the DestroyTenant
// changes are deployed to all Cockroach Cloud serverless hosts.
func (p *planner) GCTenant(ctx context.Context, tenID uint64) error {
	if !p.extendedEvalCtx.TxnIsSingleStmt {
		return errors.Errorf("gc_tenant cannot be used inside a multi-statement transaction")
	}
	if err := p.RequireAdminRole(ctx, "gc tenant"); err != nil {
		return err
	}
	info, err := GetTenantRecordByID(ctx, p.InternalSQLTxn(), roachpb.MustMakeTenantID(tenID), p.ExecCfg().Settings)
	if err != nil {
		return errors.Wrapf(err, "retrieving tenant %d", tenID)
	}

	// Confirm tenant is ready to be cleared.
	if info.DataState != mtinfopb.DataStateDrop {
		return errors.Errorf("tenant %d is not in data state DROP", info.ID)
	}

	_, err = createGCTenantJob(
		ctx, p.ExecCfg().JobRegistry, p.InternalSQLTxn(), p.User(), tenID, false, /* synchronous */
	)
	return err
}
