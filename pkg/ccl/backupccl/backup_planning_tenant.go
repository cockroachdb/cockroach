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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func tenantMetadataQuery(ctx context.Context, settings *cluster.Settings) string {
	q := `
SELECT
  tenants.id,                        /* 0 */
  tenants.info,                      /* 1 */
  tenants.name,                      /* 2 */
  tenants.data_state,                /* 3 */
  tenants.service_mode,              /* 4 */
  tenant_usage.ru_burst_limit,       /* 5 */
  tenant_usage.ru_refill_rate,       /* 6 */
  tenant_usage.ru_current,           /* 7 */
  tenant_usage.total_consumption     /* 8 */
FROM
  system.tenants
  LEFT JOIN system.tenant_usage ON
	  tenants.id = tenant_usage.tenant_id AND tenant_usage.instance_id = 0
`
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		q = `
SELECT
  tenants.id,                        /* 0 */
  tenants.info,                      /* 1 */
  NULL,                              /* 2 */
  NULL,                              /* 3 */
  NULL,                              /* 4 */
  tenant_usage.ru_burst_limit,       /* 5 */
  tenant_usage.ru_refill_rate,       /* 6 */
  tenant_usage.ru_current,           /* 7 */
  tenant_usage.total_consumption     /* 8 */
FROM
  system.tenants
  LEFT JOIN system.tenant_usage ON
	  tenants.id = tenant_usage.tenant_id AND tenant_usage.instance_id = 0`
	}
	return q
}

func tenantMetadataFromRow(row tree.Datums) (mtinfopb.TenantInfoWithUsage, error) {
	if len(row) != 9 {
		return mtinfopb.TenantInfoWithUsage{}, errors.AssertionFailedf(
			"unexpected row size %d from tenant metadata query", len(row),
		)
	}

	id := uint64(tree.MustBeDInt(row[0]))
	res := mtinfopb.TenantInfoWithUsage{
		ProtoInfo: mtinfopb.ProtoInfo{
			// for compatibility
			DeprecatedID: id,
		},
		SQLInfo: mtinfopb.SQLInfo{
			ID: id,
		},
	}
	infoBytes := []byte(tree.MustBeDBytes(row[1]))
	if err := protoutil.Unmarshal(infoBytes, &res.ProtoInfo); err != nil {
		return mtinfopb.TenantInfoWithUsage{}, err
	}
	if row[2] != tree.DNull {
		res.Name = roachpb.TenantName(tree.MustBeDString(row[2]))
	}
	if row[3] != tree.DNull {
		res.DataState = mtinfopb.TenantDataState(tree.MustBeDInt(row[3]))
	} else {
		// Pre-v23.1 info struct.
		switch res.ProtoInfo.DeprecatedDataState {
		case mtinfopb.ProtoInfo_READY:
			res.DataState = mtinfopb.DataStateReady
		case mtinfopb.ProtoInfo_ADD:
			res.DataState = mtinfopb.DataStateAdd
		case mtinfopb.ProtoInfo_DROP:
			res.DataState = mtinfopb.DataStateDrop
		default:
			return res, errors.AssertionFailedf("unhandled: %d", res.ProtoInfo.DeprecatedDataState)
		}
	}
	res.ServiceMode = mtinfopb.ServiceModeNone
	if row[4] != tree.DNull {
		res.ServiceMode = mtinfopb.TenantServiceMode(tree.MustBeDInt(row[4]))
	} else if res.DataState == mtinfopb.DataStateReady {
		// Records created for CC Serverless pre-v23.1.
		res.ServiceMode = mtinfopb.ServiceModeExternal
	}
	// If this tenant had no reported consumption and its token bucket was not
	// configured, the tenant_usage values are all NULL.
	//
	// It should be sufficient to check any one value, but we check all of them
	// just to be defensive (in case the table contains invalid data).
	for _, d := range row[5:] {
		if d == tree.DNull {
			return res, nil
		}
	}
	res.Usage = &mtinfopb.UsageInfo{
		RUBurstLimit: float64(tree.MustBeDFloat(row[5])),
		RURefillRate: float64(tree.MustBeDFloat(row[6])),
		RUCurrent:    float64(tree.MustBeDFloat(row[7])),
	}
	if row[8] != tree.DNull {
		consumptionBytes := []byte(tree.MustBeDBytes(row[8]))
		if err := protoutil.Unmarshal(consumptionBytes, &res.Usage.Consumption); err != nil {
			return mtinfopb.TenantInfoWithUsage{}, err
		}
	}
	return res, nil
}

func retrieveSingleTenantMetadata(
	ctx context.Context, txn isql.Txn, tenantID roachpb.TenantID, settings *cluster.Settings,
) (mtinfopb.TenantInfoWithUsage, error) {
	row, err := txn.QueryRow(
		ctx, "backupccl.retrieveSingleTenantMetadata", txn.KV(),
		tenantMetadataQuery(ctx, settings)+` WHERE id = $1`, tenantID.ToUint64(),
	)
	if err != nil {
		return mtinfopb.TenantInfoWithUsage{}, err
	}
	if row == nil {
		return mtinfopb.TenantInfoWithUsage{}, errors.Errorf("tenant %s does not exist", tenantID)
	}
	info, err := tenantMetadataFromRow(row)
	if err != nil {
		return mtinfopb.TenantInfoWithUsage{}, err
	}
	if info.DataState != mtinfopb.DataStateReady {
		return mtinfopb.TenantInfoWithUsage{}, errors.Errorf("tenant %s is not active", tenantID)
	}
	return info, nil
}

func retrieveAllTenantsMetadata(
	ctx context.Context, txn isql.Txn, settings *cluster.Settings,
) ([]mtinfopb.TenantInfoWithUsage, error) {
	rows, err := txn.QueryBuffered(
		ctx, "backupccl.retrieveAllTenantsMetadata", txn.KV(),
		// TODO(?): Should we add a `WHERE active`? We require the tenant to be active
		// when it is specified.
		// See: https://github.com/cockroachdb/cockroach/issues/89997
		tenantMetadataQuery(ctx, settings)+` WHERE id != $1`,
		roachpb.SystemTenantID.ToUint64(),
	)
	if err != nil {
		return nil, err
	}
	res := make([]mtinfopb.TenantInfoWithUsage, len(rows))
	for i := range rows {
		res[i], err = tenantMetadataFromRow(rows[i])
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
