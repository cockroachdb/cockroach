// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const tenantMetadataQuery = `
SELECT
  tenants.id,
  tenants.active,
  tenants.info,
  tenant_usage.ru_burst_limit,
  tenant_usage.ru_refill_rate,
  tenant_usage.ru_current,
  tenant_usage.total_ru_usage,
  tenant_usage.total_read_requests,
  tenant_usage.total_read_bytes,
  tenant_usage.total_write_requests,
  tenant_usage.total_write_bytes,
  tenant_usage.total_sql_pod_cpu_seconds
FROM
  system.tenants
  LEFT JOIN system.tenant_usage ON
	  tenants.id = tenant_usage.tenant_id AND tenant_usage.instance_id = 0`

func tenantMetadataFromRow(row tree.Datums) (descpb.TenantInfoWithUsage, error) {
	id := uint64(tree.MustBeDInt(row[0]))
	res := descpb.TenantInfoWithUsage{
		TenantInfo: descpb.TenantInfo{
			ID: id,
		},
	}
	infoBytes := []byte(tree.MustBeDBytes(row[2]))
	if err := protoutil.Unmarshal(infoBytes, &res.TenantInfo); err != nil {
		return descpb.TenantInfoWithUsage{}, err
	}
	// If this tenant had no reported consumption and its token bucket was not
	// configured, the tenant_usage values are all NULL. Otherwise none of them
	// are NULL.
	//
	// It should be sufficient to check any one value, but we check all of them
	// just to be defensive (in case the table contains invalid data).
	for _, d := range row[3:] {
		if d == tree.DNull {
			return res, nil
		}
	}
	res.Usage = &descpb.TenantInfoWithUsage_Usage{
		RUBurstLimit: float64(tree.MustBeDFloat(row[3])),
		RURefillRate: float64(tree.MustBeDFloat(row[4])),
		RUCurrent:    float64(tree.MustBeDFloat(row[5])),
		Consumption: roachpb.TenantConsumption{
			RU:                float64(tree.MustBeDFloat(row[6])),
			ReadRequests:      uint64(tree.MustBeDInt(row[7])),
			ReadBytes:         uint64(tree.MustBeDInt(row[8])),
			WriteRequests:     uint64(tree.MustBeDInt(row[9])),
			WriteBytes:        uint64(tree.MustBeDInt(row[10])),
			SQLPodsCPUSeconds: float64(tree.MustBeDFloat(row[11])),
		},
	}
	return res, nil
}

func retrieveSingleTenantMetadata(
	ctx context.Context, ie *sql.InternalExecutor, txn *kv.Txn, tenantID roachpb.TenantID,
) (descpb.TenantInfoWithUsage, error) {
	row, err := ie.QueryRow(
		ctx, "backup-lookup-tenant", txn,
		tenantMetadataQuery+` WHERE id = $1`, tenantID.ToUint64(),
	)
	if err != nil {
		return descpb.TenantInfoWithUsage{}, err
	}
	if row == nil {
		return descpb.TenantInfoWithUsage{}, errors.Errorf("tenant %s does not exist", tenantID)
	}
	if !tree.MustBeDBool(row[1]) {
		return descpb.TenantInfoWithUsage{}, errors.Errorf("tenant %s is not active", tenantID)
	}

	return tenantMetadataFromRow(row)
}

func retrieveAllTenantsMetadata(
	ctx context.Context, ie *sql.InternalExecutor, txn *kv.Txn,
) ([]descpb.TenantInfoWithUsage, error) {
	rows, err := ie.QueryBuffered(
		ctx, "backup-lookup-tenants", txn,
		// XXX Should we add a `WHERE active`? We require the tenant to be active
		// when it is specified..
		tenantMetadataQuery,
	)
	if err != nil {
		return nil, err
	}
	res := make([]descpb.TenantInfoWithUsage, len(rows))
	for i := range rows {
		res[i], err = tenantMetadataFromRow(rows[i])
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
