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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// rejectIfCantCoordinateMultiTenancy returns an error if the current tenant is
// disallowed from coordinating tenant management operations on behalf of a
// multi-tenant cluster. Only the system tenant has permissions to do so.
func rejectIfCantCoordinateMultiTenancy(codec keys.SQLCodec, op string) error {
	// NOTE: even if we got this wrong, the rest of the function would fail for
	// a non-system tenant because they would be missing a system.tenants table.
	if !codec.ForSystemTenant() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can %s other tenants", op)
	}
	return nil
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
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		q = `SELECT id FROM system.tenants WHERE active = $1 ORDER BY id`
		arg = true
	}
	rows, err := txn.QueryBuffered(ctx, "get-tenant-ids", txn.KV(), q, arg)
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
) (*mtinfopb.ExtendedTenantInfo, error) {
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		return nil, errors.Newf("tenant names not supported until upgrade to %s or higher is completed",
			clusterversion.V23_1TenantNamesStateAndServiceMode.String())
	}
	row, err := txn.QueryRowEx(
		ctx, "get-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT id, info, name, data_state, service_mode
FROM system.tenants WHERE name = $1`, tenantName,
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant %q does not exist", tenantName)
	}
	return getTenantInfoFromRow(row)
}

func getTenantInfoFromRow(row tree.Datums) (*mtinfopb.ExtendedTenantInfo, error) {
	info := &mtinfopb.ExtendedTenantInfo{}
	info.ID = uint64(tree.MustBeDInt(row[0]))

	// For the benefit of pre-23.1 BACKUP/RESTORE.
	info.DeprecatedID = info.ID

	infoBytes := []byte(tree.MustBeDBytes(row[1]))
	if err := protoutil.Unmarshal(infoBytes, &info.TenantInfo); err != nil {
		return nil, err
	}

	// Load the name if defined.
	if row[2] != tree.DNull {
		info.Name = roachpb.TenantName(tree.MustBeDString(row[2]))
	}

	// Load the data state column if defined.
	if row[3] != tree.DNull {
		info.DataState = mtinfopb.TenantDataState(tree.MustBeDInt(row[3]))
	} else {
		// Pre-v23.1 info struct.
		switch info.TenantInfo.DeprecatedDataState {
		case mtinfopb.TenantInfo_READY:
			info.DataState = mtinfopb.DataStateReady
		case mtinfopb.TenantInfo_ADD:
			info.DataState = mtinfopb.DataStateAdd
		case mtinfopb.TenantInfo_DROP:
			info.DataState = mtinfopb.DataStateDrop
			return nil, errors.AssertionFailedf("unhandled: %d", info.TenantInfo.DeprecatedDataState)
		}
	}

	// Load the service mode if defined.
	info.ServiceMode = mtinfopb.ServiceModeNone
	if row[4] != tree.DNull {
		info.ServiceMode = mtinfopb.TenantServiceMode(tree.MustBeDInt(row[4]))
	} else if info.DataState == mtinfopb.DataStateReady {
		// Records created for CC Serverless pre-v23.1.
		info.ServiceMode = mtinfopb.ServiceModeExternal
	}
	return info, nil
}

// GetTenantRecordByID retrieves a tenant in system.tenants.
func GetTenantRecordByID(
	ctx context.Context, txn isql.Txn, tenID roachpb.TenantID, settings *cluster.Settings,
) (*mtinfopb.ExtendedTenantInfo, error) {
	q := `SELECT id, info, name, data_state, service_mode
FROM system.tenants WHERE id = $1`
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		q = `SELECT id, info, NULL, NULL, NULL
FROM system.tenants WHERE id = $1`
	}
	row, err := txn.QueryRowEx(
		ctx, "get-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		q, tenID.ToUint64(),
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenID.ToUint64())
	}

	return getTenantInfoFromRow(row)
}

// LookupTenantID implements the tree.TenantOperator interface.
func (p *planner) LookupTenantID(
	ctx context.Context, tenantName roachpb.TenantName,
) (tid roachpb.TenantID, err error) {
	const op = "get-tenant-info"
	if err := p.RequireAdminRole(ctx, op); err != nil {
		return tid, err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return tid, err
	}

	rec, err := GetTenantRecordByName(ctx, p.execCfg.Settings, p.InternalSQLTxn(), tenantName)
	if err != nil {
		return tid, err
	}
	return roachpb.MustMakeTenantID(rec.ID), nil
}
