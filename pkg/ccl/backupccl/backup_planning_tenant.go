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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const tenantMetadataQuery = `
SELECT
  tenants.id,                        /* 0 */
  tenants.active,                    /* 1 */
  tenants.info,                      /* 2 */
  tenant_usage.ru_burst_limit,       /* 3 */
  tenant_usage.ru_refill_rate,       /* 4 */
  tenant_usage.ru_current,           /* 5 */
  tenant_usage.total_consumption     /* 6 */
FROM
  system.tenants
  LEFT JOIN system.tenant_usage ON
	  tenants.id = tenant_usage.tenant_id AND tenant_usage.instance_id = 0
`

func tenantMetadataFromRow(row tree.Datums) (descpb.TenantInfoWithUsage, error) {
	if len(row) != 7 {
		return descpb.TenantInfoWithUsage{}, errors.AssertionFailedf(
			"unexpected row size %d from tenant metadata query", len(row),
		)
	}

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
	// configured, the tenant_usage values are all NULL.
	//
	// It should be sufficient to check any one value, but we check all of them
	// just to be defensive (in case the table contains invalid data).
	for _, d := range row[3:5] {
		if d == tree.DNull {
			return res, nil
		}
	}
	res.Usage = &descpb.TenantInfoWithUsage_Usage{
		RUBurstLimit: float64(tree.MustBeDFloat(row[3])),
		RURefillRate: float64(tree.MustBeDFloat(row[4])),
		RUCurrent:    float64(tree.MustBeDFloat(row[5])),
	}
	if row[6] != tree.DNull {
		consumptionBytes := []byte(tree.MustBeDBytes(row[6]))
		if err := protoutil.Unmarshal(consumptionBytes, &res.Usage.Consumption); err != nil {
			return descpb.TenantInfoWithUsage{}, err
		}
	}
	return res, nil
}

func retrieveSingleTenantMetadata(
	ctx context.Context, txn isql.Txn, tenantID roachpb.TenantID,
) (descpb.TenantInfoWithUsage, error) {
	row, err := txn.QueryRow(
		ctx, "backupccl.retrieveSingleTenantMetadata", txn.KV(),
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
	ctx context.Context, txn isql.Txn,
) ([]descpb.TenantInfoWithUsage, error) {
	rows, err := txn.QueryBuffered(
		ctx, "backupccl.retrieveAllTenantsMetadata", txn.KV(),
		// TODO(?): Should we add a `WHERE active`? We require the tenant to be active
		// when it is specified.
		// See: https://github.com/cockroachdb/cockroach/issues/89997
		tenantMetadataQuery+` WHERE id != $1`,
		roachpb.SystemTenantID.ToUint64(),
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
