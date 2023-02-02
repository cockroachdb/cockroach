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
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// UpdateTenantRecord updates a tenant in system.tenants.
//
// Caller is expected to check the user's permission.
func UpdateTenantRecord(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, info *mtinfopb.TenantInfo,
) error {
	if err := validateTenantInfo(ctx, settings, info); err != nil {
		return err
	}

	// Populate the deprecated DataState field for compatibility
	// with pre-v23.1 servers.
	switch info.DataState {
	case mtinfopb.DataStateReady:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_READY
	case mtinfopb.DataStateAdd:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_ADD
	case mtinfopb.DataStateDrop:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_DROP
	default:
		return errors.AssertionFailedf("unhandled: %d", info.DataState)
	}
	// For the benefit of pre-v23.1 servers.
	info.DeprecatedID = info.ID

	infoBytes, err := protoutil.Marshal(&info.ProtoInfo)
	if err != nil {
		return err
	}
	// active is a deprecated column preserved for compatibiliy
	// with pre-v23.1.
	active := info.DataState == mtinfopb.DataStateReady
	var name tree.Datum
	if info.Name != "" {
		name = tree.NewDString(string(info.Name))
	} else {
		name = tree.DNull
	}

	query := `UPDATE system.tenants
SET active = $2, info = $3, name = $4, data_state = $5, service_mode = $6
WHERE id = $1`
	args := []interface{}{info.ID, active, infoBytes, name, info.DataState, info.ServiceMode}
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		// Ensure the update can succeed if the upgrade is not finalized yet.
		query = `UPDATE system.tenants SET active = $2, info = $3 WHERE id = $1`
		args = args[:3]
	}

	if num, err := txn.ExecEx(
		ctx, "update-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		query, args...,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "name %q is already taken", info.Name)
		}
		return err
	} else if num != 1 {
		logcrash.ReportOrPanic(ctx, &settings.SV, "unexpected number of rows affected: %d", num)
	}
	return nil
}

func validateTenantInfo(
	ctx context.Context, settings *cluster.Settings, info *mtinfopb.TenantInfo,
) error {
	if info.TenantReplicationJobID != 0 && info.DataState == mtinfopb.DataStateReady {
		return errors.Newf("tenant in data state %v with replication job ID %d", info.DataState, info.TenantReplicationJobID)
	}
	if info.DroppedName != "" && info.DataState != mtinfopb.DataStateDrop {
		return errors.Newf("tenant in data state %v with dropped name %q", info.DataState, info.DroppedName)
	}

	if settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		// We can only check the service mode after upgrading to a version
		// that supports the service mode column.
		if info.ServiceMode != mtinfopb.ServiceModeNone && info.DataState != mtinfopb.DataStateReady {
			return errors.Newf("cannot use tenant service mode %v with data state %v",
				info.ServiceMode, info.DataState)
		}
	}

	return nil
}

// TestingUpdateTenantRecord is a public wrapper around updateTenantRecord
// intended for testing purposes.
func TestingUpdateTenantRecord(
	ctx context.Context, settings *cluster.Settings, txn isql.Txn, info *mtinfopb.TenantInfo,
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
	ctx context.Context,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	txn isql.Txn,
	tenID uint64,
	serviceMode mtinfopb.TenantServiceMode,
) error {
	const op = "activate"
	if err := rejectIfCantCoordinateMultiTenancy(codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	// Retrieve the tenant's info.
	info, err := GetTenantRecordByID(ctx, txn, roachpb.MustMakeTenantID(tenID), settings)
	if err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	// Mark the tenant as active.
	info.DataState = mtinfopb.DataStateReady
	info.ServiceMode = serviceMode
	if err := UpdateTenantRecord(ctx, settings, txn, info); err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	return nil
}

func (p *planner) setTenantService(
	ctx context.Context, info *mtinfopb.TenantInfo, newMode mtinfopb.TenantServiceMode,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("ALTER TENANT SERVICE")
	}

	if err := p.RequireAdminRole(ctx, "set tenant service"); err != nil {
		return err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, "set tenant service"); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, "set tenant service"); err != nil {
		return err
	}

	if newMode == info.ServiceMode {
		// No-op. Do nothing.
		return nil
	}

	if newMode != mtinfopb.ServiceModeNone && info.ServiceMode != mtinfopb.ServiceModeNone {
		return errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot change service mode %v to %v directly",
			info.ServiceMode, newMode),
			"Use ALTER TENANT STOP SERVICE first.")
	}

	info.ServiceMode = newMode
	return UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), info)
}

func (p *planner) renameTenant(
	ctx context.Context, info *mtinfopb.TenantInfo, newName roachpb.TenantName,
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
	if err := rejectIfSystemTenant(info.ID, "rename"); err != nil {
		return err
	}

	if newName != "" {
		if err := newName.IsValid(); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.Syntax)
		}

		if !p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
			return pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
	}

	if info.ServiceMode != mtinfopb.ServiceModeNone {
		return errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot rename tenant in service mode %v", info.ServiceMode),
			"Use ALTER TENANT STOP SERVICE before renaming a tenant.")
	}

	info.Name = newName
	return errors.Wrap(
		UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), info),
		"renaming tenant")
}
