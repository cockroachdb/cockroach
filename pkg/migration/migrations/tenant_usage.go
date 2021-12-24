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
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
)

// tenantUsageSingleConsumptionColumn modifies the system.tenant_usage table to
// use a single column for consumption (which encodes a protobuf).
//
// This migration does not preserve any existing consumption data - it is simply
// reset to 0.
func tenantUsageSingleConsumptionColumn(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	// This table is only used on the system tenant.
	if !d.Codec.ForSystemTenant() {
		return nil
	}
	op := operation{
		name:       "tenant-usage-consumption-proto-column",
		schemaList: []string{"total_consumption"},
		query: `ALTER TABLE system.tenant_usage
		  DROP COLUMN total_ru_usage,
		  DROP COLUMN total_read_requests,
		  DROP COLUMN total_read_bytes,
		  DROP COLUMN total_write_requests,
		  DROP COLUMN total_write_bytes,
		  DROP COLUMN total_sql_pod_cpu_seconds,
		  ADD COLUMN total_consumption BYTES FAMILY "primary"`,
		// TODO(radu): ideally we would be also checking whether the dropped columns
		// exist.
		schemaExistsFn: hasColumn,
	}

	return migrateTable(ctx, cs, d, op, keys.TenantUsageTableID, systemschema.TenantUsageTable)
}
