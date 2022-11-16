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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// UpdateTenantRecord updates a tenant in system.tenants.
//
// Caller is expected to check the user's permission.
func UpdateTenantRecord(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, info *descpb.TenantInfo,
) error {
	if err := validateTenantInfo(info); err != nil {
		return err
	}

	tenID := info.ID
	active := info.State == descpb.TenantInfo_ACTIVE
	infoBytes, err := protoutil.Marshal(info)
	if err != nil {
		return err
	}

	if num, err := txn.ExecEx(
		ctx, "activate-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.tenants SET active = $2, info = $3 WHERE id = $1`,
		tenID, active, infoBytes,
	); err != nil {
		return errors.Wrap(err, "activating tenant")
	} else if num != 1 {
		logcrash.ReportOrPanic(ctx, &settings.SV, "unexpected number of rows affected: %d", num)
	}
	return nil
}

func validateTenantInfo(info *descpb.TenantInfo) error {
	if info.TenantReplicationJobID != 0 && info.State == descpb.TenantInfo_ACTIVE {
		return errors.Newf("tenant in state %v with replication job ID %d", info.State, info.TenantReplicationJobID)
	}
	if info.DroppedName != "" && info.State != descpb.TenantInfo_DROP {
		return errors.Newf("tenant in state %v with dropped name %q", info.State, info.DroppedName)
	}
	return nil
}

// TestingUpdateTenantRecord is a public wrapper around updateTenantRecord
// intended for testing purposes.
func TestingUpdateTenantRecord(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, info *descpb.TenantInfo,
) error {
	return UpdateTenantRecord(ctx, settings, txn, info)
}

// UpdateTenantResourceLimits implements the tree.TenantOperator interface.
func (p *planner) UpdateTenantResourceLimits(
	ctx context.Context,
	tenantID uint64,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	const op = "update-resource-limits"
	if err := p.RequireAdminRole(ctx, "update tenant resource limits"); err != nil {
		return err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, op); err != nil {
		return err
	}

	return p.ExecCfg().TenantUsageServer.ReconfigureTokenBucket(
		ctx, p.InternalSQLTxn(), roachpb.MustMakeTenantID(tenantID), availableRU, refillRate,
		maxBurstRU, asOf, asOfConsumedRequestUnits,
	)
}

// ActivateTenant marks a tenant active.
//
// The caller is responsible for checking that the user is authorized
// to take this action.
func ActivateTenant(
	ctx context.Context, settings *cluster.Settings, codec keys.SQLCodec, txn isql.Txn, tenID uint64,
) error {
	const op = "activate"
	if err := rejectIfCantCoordinateMultiTenancy(codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	// Retrieve the tenant's info.
	info, err := GetTenantRecordByID(ctx, txn, roachpb.MustMakeTenantID(tenID))
	if err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	// Mark the tenant as active.
	info.State = descpb.TenantInfo_ACTIVE
	if err := UpdateTenantRecord(ctx, settings, txn, info); err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	return nil
}

func (p *planner) renameTenant(
	ctx context.Context, tenantID uint64, tenantName roachpb.TenantName,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("ALTER TENANT RENAME TO")
	}

	if err := p.RequireAdminRole(ctx, "rename tenant"); err != nil {
		return err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, "rename tenant"); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, "rename"); err != nil {
		return err
	}

	if tenantName != "" {
		if err := tenantName.IsValid(); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.Syntax)
		}

		if !p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
			return pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
	}

	if num, err := p.InternalSQLTxn().ExecEx(
		ctx, "rename-tenant", p.txn, sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.public.tenants
SET info =
crdb_internal.json_to_pb('cockroach.sql.sqlbase.TenantInfo',
  crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) ||
  json_build_object('name', $2))
WHERE id = $1`, tenantID, tenantName); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "name %q is already taken", tenantName)
		}
		return errors.Wrap(err, "renaming tenant")
	} else if num != 1 {
		return pgerror.Newf(pgcode.UndefinedObject, "tenant %d not found", tenantID)
	}

	return nil
}
