// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func seedTenantSpanConfigsMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	if !d.Codec.ForSystemTenant() {
		return nil
	}

	return d.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const getTenantIDsQuery = `SELECT id from system.tenants`
		it, err := d.InternalExecutor.QueryIteratorEx(ctx, "get-tenant-ids", txn,
			sessiondata.NodeUserSessionDataOverride, getTenantIDsQuery,
		)
		if err != nil {
			return errors.Wrap(err, "unable to fetch existing tenant IDs")
		}

		var tenantIDs []roachpb.TenantID
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			tenantID := roachpb.MakeTenantID(uint64(tree.MustBeDInt(row[0])))
			tenantIDs = append(tenantIDs, tenantID)
		}
		if err != nil {
			return err
		}

		scKVAccessor := d.SpanConfig.KVAccessor.WithTxn(ctx, txn)
		for _, tenantID := range tenantIDs {
			// Install a single key span config at the start of tenant's
			// keyspace; elsewhere this ensures that we split on the tenant
			// boundary. Look towards CreateTenantRecord for more details.
			tenantSpanConfig := d.SpanConfig.Default
			tenantPrefix := keys.MakeTenantPrefix(tenantID)
			tenantTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
				Key:    tenantPrefix,
				EndKey: tenantPrefix.PrefixEnd(),
			})
			tenantSeedSpan := roachpb.Span{
				Key:    tenantPrefix,
				EndKey: tenantPrefix.Next(),
			}
			toUpsert := []spanconfig.Record{
				{
					Target: spanconfig.MakeTargetFromSpan(tenantSeedSpan),
					Config: tenantSpanConfig,
				},
			}
			scRecords, err := scKVAccessor.GetSpanConfigRecords(ctx, []spanconfig.Target{tenantTarget})
			if err != nil {
				return err
			}
			if len(scRecords) != 0 {
				// This tenant already has span config records. It was either
				// already migrated (migrations need to be idempotent) or it was
				// created after PreSeedTenantSpanConfigs was activated. There's
				// nothing left to do here.
				continue
			}
			if err := scKVAccessor.UpdateSpanConfigRecords(
				ctx, nil /* toDelete */, toUpsert,
			); err != nil {
				return errors.Wrapf(err, "failed to seed span config for tenant %d", tenantID)
			}
		}

		return nil
	})
}
