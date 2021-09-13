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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/errors"
)

func tenantUsageTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	// Only create the table on the system tenant.
	if !d.Codec.ForSystemTenant() {
		return nil
	}
	return startupmigrations.CreateSystemTable(
		ctx, d.DB, d.Codec, d.Settings, systemschema.TenantUsageTable,
	)
}

func setTTLForTenantUsageTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	// The table is only relevant on the system tenant.
	if !d.Codec.ForSystemTenant() {
		return nil
	}
	stmt := fmt.Sprintf(
		"ALTER TABLE system.tenant_usage CONFIGURE ZONE USING gc.ttlseconds = %d",
		int(systemschema.TenantUsageTableTTL.Seconds()),
	)
	_, err := d.InternalExecutor.Exec(ctx, "set-tenant-usage-ttl", nil /* txn */, stmt)
	return errors.Wrapf(err, "failed to set TTL on tenant_usage")
}
