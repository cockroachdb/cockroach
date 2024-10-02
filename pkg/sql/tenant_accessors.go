// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfo"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

// rejectIfCantCoordinateMultiTenancy returns an error if the current tenant is
// disallowed from coordinating tenant management operations on behalf of a
// multi-tenant cluster. Only the system tenant has permissions to do so.
func rejectIfCantCoordinateMultiTenancy(
	codec keys.SQLCodec, op string, st *cluster.Settings,
) error {
	var err error
	// NOTE: even if we got this wrong, the rest of the function would fail for
	// a non-system tenant because they would be missing a system.tenants table.
	if !codec.ForSystemTenant() {
		err = pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can %s other tenants", op)
	}
	err = maybeAddSystemInterfaceHint(err, "manage tenants", codec, st)
	return err
}

// rejectIfSystemTenant returns an error if the provided tenant ID is the system
// tenant's ID.
func rejectIfSystemTenant(tenID uint64, op string) error {
	if roachpb.IsSystemTenantID(tenID) {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"cannot %s tenant \"%d\", ID assigned to system tenant", op, tenID)
	}
	return nil
}

// GetAllNonDropTenantIDs returns all tenants in the system table, excluding
// those in the DROP state.
func GetAllNonDropTenantIDs(
	ctx context.Context, txn isql.Txn, settings *cluster.Settings,
) ([]roachpb.TenantID, error) {
	q := `SELECT id FROM system.tenants WHERE data_state != $1 ORDER BY id`
	var arg interface{} = mtinfopb.DataStateDrop
	rows, err := txn.QueryBufferedEx(ctx, "get-tenant-ids", txn.KV(), sessiondata.NodeUserSessionDataOverride, q, arg)
	if err != nil {
		return nil, err
	}

	tenants := make([]roachpb.TenantID, 0, len(rows))
	for _, tenant := range rows {
		iTenantId := uint64(tree.MustBeDInt(tenant[0]))
		tenantId, err := roachpb.MakeTenantID(iTenantId)
		if err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(
				err, "stored tenant ID %d does not convert to TenantID", iTenantId)
		}
		tenants = append(tenants, tenantId)
	}

	return tenants, nil
}

// GetTenantRecordByName retrieves a tenant with the provided name from
// system.tenants.
func GetTenantRecordByName(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, tenantName roachpb.TenantName,
) (*mtinfopb.TenantInfo, error) {
	row, err := txn.QueryRowEx(
		ctx, "get-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT id, info, name, data_state, service_mode FROM system.tenants WHERE name = $1`, tenantName,
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant %q does not exist", tenantName)
	}
	_, info, err := mtinfo.GetTenantInfoFromSQLRow(row)
	return info, err
}

// GetTenantRecordByID retrieves a tenant in system.tenants.
func GetTenantRecordByID(
	ctx context.Context, txn isql.Txn, tenID roachpb.TenantID, settings *cluster.Settings,
) (*mtinfopb.TenantInfo, error) {
	q := `SELECT id, info, name, data_state, service_mode FROM system.tenants WHERE id = $1`
	row, err := txn.QueryRowEx(
		ctx, "get-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		q, tenID.ToUint64(),
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenID.ToUint64())
	}

	_, info, err := mtinfo.GetTenantInfoFromSQLRow(row)
	return info, err
}

// LookupTenantID implements the tree.TenantOperator interface.
func (p *planner) LookupTenantID(
	ctx context.Context, tenantName roachpb.TenantName,
) (tid roachpb.TenantID, err error) {
	const op = "get-tenant-info"
	if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
		return tid, err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op, p.execCfg.Settings); err != nil {
		return tid, err
	}

	rec, err := GetTenantRecordByName(ctx, p.execCfg.Settings, p.InternalSQLTxn(), tenantName)
	if err != nil {
		return tid, err
	}
	return roachpb.MustMakeTenantID(rec.ID), nil
}

// GetExtendedTenantInfo hydrates a TenantInfoWithUsage with the
// additional data beyond the TenantInfo record.
func GetExtendedTenantInfo(
	ctx context.Context, txn isql.Txn, info *mtinfopb.TenantInfo,
) (*mtinfopb.TenantInfoWithUsage, error) {
	res := &mtinfopb.TenantInfoWithUsage{
		ProtoInfo: info.ProtoInfo,
		SQLInfo:   info.SQLInfo,
	}
	rows, err := txn.QueryBufferedEx(ctx, "get-tenant-setting-overrides", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT name, value, value_type, reason FROM system.tenant_settings WHERE tenant_id = $1`,
		info.ID,
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		override := &mtinfopb.SettingOverride{
			Name:      string(tree.MustBeDString(row[0])),
			Value:     string(tree.MustBeDString(row[1])),
			ValueType: string(tree.MustBeDString(row[2])),
		}
		if row[3] != tree.DNull {
			s := string(tree.MustBeDString(row[3]))
			override.Reason = &s
		}
		res.SettingOverrides = append(res.SettingOverrides, override)
	}

	row, err := txn.QueryRowEx(ctx, "get-tenant-usage-config", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT ru_current, ru_burst_limit, ru_refill_rate
       FROM system.tenant_usage
      WHERE tenant_id = $1 AND instance_id = 0`,
		info.ID,
	)
	if err != nil {
		return nil, err
	}
	if row != nil {
		res.Usage = &mtinfopb.UsageInfo{
			RUCurrent:    float64(tree.MustBeDFloat(row[0])),
			RUBurstLimit: float64(tree.MustBeDFloat(row[1])),
			RURefillRate: float64(tree.MustBeDFloat(row[2])),
		}
	}

	return res, nil
}
