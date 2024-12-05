// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	if info.PhysicalReplicationConsumerJobID != 0 && info.DataState == mtinfopb.DataStateReady {
		return errors.Newf("tenant in data state %v with replication job ID %d", info.DataState, info.PhysicalReplicationConsumerJobID)
	}
	if info.DroppedName != "" && info.DataState != mtinfopb.DataStateDrop {
		return errors.Newf("tenant in data state %v with dropped name %q", info.DataState, info.DroppedName)
	}

	if info.ServiceMode != mtinfopb.ServiceModeNone && info.DataState != mtinfopb.DataStateReady {
		return errors.Newf("cannot use tenant service mode %v with data state %v",
			info.ServiceMode, info.DataState)
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
	availableTokens float64,
	refillRate float64,
	maxBurstTokens float64,
) error {
	const op = "update-resource-limits"
	if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER); err != nil {
		return err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op, p.execCfg.Settings); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, op); err != nil {
		return err
	}

	return p.ExecCfg().TenantUsageServer.ReconfigureTokenBucket(
		ctx, p.InternalSQLTxn(), roachpb.MustMakeTenantID(tenantID), availableTokens, refillRate,
		maxBurstTokens,
	)
}

// ActivateRestoredTenant marks a restored tenant active.
//
// The caller is responsible for checking that the user is authorized
// to take this action.
func ActivateRestoredTenant(
	ctx context.Context,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	txn isql.Txn,
	tenID uint64,
	serviceMode mtinfopb.TenantServiceMode,
) error {
	const op = "activate"
	if err := rejectIfCantCoordinateMultiTenancy(codec, op, settings); err != nil {
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
	info.PreviousSourceTenant.CutoverAsOf = txn.KV().DB().Clock().Now()
	if err := UpdateTenantRecord(ctx, settings, txn, info); err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	return nil
}

func (p *planner) setTenantService(
	ctx context.Context, info *mtinfopb.TenantInfo, targetMode mtinfopb.TenantServiceMode,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("ALTER VIRTUAL CLUSTER SERVICE")
	}

	if err := CanManageTenant(ctx, p); err != nil {
		return err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, "set tenant service", p.ExecCfg().Settings); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, "set tenant service"); err != nil {
		return err
	}

	if !p.extendedEvalCtx.TxnIsSingleStmt {
		return errors.Errorf("ALTER VIRTUAL CLUSTER SERVICE cannot be used inside a multi-statement transaction")
	}

	lastMode := info.ServiceMode
	for {
		mode, err := stepTenantServiceState(ctx,
			p.ExecCfg().InternalDB,
			p.execCfg.SQLStatusServer,
			p.ExecCfg().Settings, info, targetMode)
		if err != nil {
			return err
		}
		if targetMode == mode {
			return nil
		}
		if mode == lastMode {
			return errors.AssertionFailedf("service mode did not advance from %q", lastMode)
		}
	}
}

func stepTenantServiceState(
	ctx context.Context,
	db *InternalDB,
	statusSrv serverpb.SQLStatusServer,
	settings *cluster.Settings,
	inInfo *mtinfopb.TenantInfo,
	targetMode mtinfopb.TenantServiceMode,
) (mtinfopb.TenantServiceMode, error) {
	updateTenantMode := func(txn isql.Txn, info *mtinfopb.TenantInfo, mode mtinfopb.TenantServiceMode) error {
		// Zero the LastRevertTenantTimestamp since
		// starting the service invalidates any
		// previous revert.
		if mode == mtinfopb.ServiceModeShared || mode == mtinfopb.ServiceModeExternal {
			info.LastRevertTenantTimestamp = hlc.Timestamp{}
		}
		log.Infof(ctx, "transitioning tenant %d from %s to %s", info.ID, info.ServiceMode, mode)
		info.ServiceMode = mode
		return UpdateTenantRecord(ctx, settings, txn, info)
	}

	// In 24.1 we support the following state transitions
	//
	//      +-> External -+
	// None-|             |-> Stopping
	//   ^  +->  Shared  -+      |
	//   |                       |
	//   +-----------------------+
	//
	// NB: We could eliminate some of the error cases here. For
	// instance we could support External -> Shared now but still
	// return an error in that case.
	nextMode := func(currentMode mtinfopb.TenantServiceMode, targetMode mtinfopb.TenantServiceMode) (mtinfopb.TenantServiceMode, error) {
		switch currentMode {
		case mtinfopb.ServiceModeNone:
			switch targetMode {
			case mtinfopb.ServiceModeShared, mtinfopb.ServiceModeExternal:
				return targetMode, nil
			default:
				return 0, errors.AssertionFailedf("unsupported service mode transition from %s to %s", currentMode, targetMode)
			}
		case mtinfopb.ServiceModeShared, mtinfopb.ServiceModeExternal:
			switch targetMode {
			case mtinfopb.ServiceModeNone:
				return mtinfopb.ServiceModeStopping, nil
			case mtinfopb.ServiceModeExternal, mtinfopb.ServiceModeShared:
				return 0, errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"cannot change service mode %v to %v directly", currentMode, targetMode),
					"Use ALTER VIRTUAL CLUSTER STOP SERVICE first.")
			default:
				return 0, errors.AssertionFailedf("unsupported service mode transition from %s to %s", currentMode, targetMode)
			}
		case mtinfopb.ServiceModeStopping:
			switch targetMode {
			case mtinfopb.ServiceModeNone:
				return targetMode, nil
			case mtinfopb.ServiceModeShared, mtinfopb.ServiceModeExternal:
				return 0, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"service currently stopping, cannot start service until stop has completed")
			default:
				return 0, errors.AssertionFailedf("unsupported service mode transition from %s to %s", currentMode, targetMode)
			}
		default:
			return 0, errors.AssertionFailedf("unknown service mode %q", currentMode)
		}

	}

	var mode mtinfopb.TenantServiceMode
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tid, err := roachpb.MakeTenantID(inInfo.ID)
		if err != nil {
			return err
		}

		info, err := GetTenantRecordByID(ctx, txn, tid, settings)
		if err != nil {
			return err
		}

		if targetMode == info.ServiceMode {
			// No-op. Do nothing.
			mode = targetMode
			return nil
		}

		mode, err = nextMode(info.ServiceMode, targetMode)
		if err != nil {
			return err
		}
		return updateTenantMode(txn, info, mode)
	}); err != nil {
		return mode, err
	}

	if mode == mtinfopb.ServiceModeStopping {
		// The timeout here is to protect against a scenario
		// in which another client also stops the service,
		// observes the stopped state first, and then restarts
		// the service while we are still waiting for all
		// nodes to observe the stopped state.
		//
		// We could avoid this by treating these transitions
		// more like schema changes.
		log.Infof(ctx, "waiting for all nodes to stop service for tenant %d", inInfo.ID)
		if err := timeutil.RunWithTimeout(ctx, "wait-for-tenant-stop", 10*time.Minute, func(ctx context.Context) error {
			retryOpts := retry.Options{MaxBackoff: 10 * time.Second}
			for re := retry.StartWithCtx(ctx, retryOpts); re.Next(); {
				resp, err := statusSrv.TenantServiceStatus(ctx, &serverpb.TenantServiceStatusRequest{TenantID: inInfo.ID})
				if err != nil {
					return errors.Wrap(err, "failed waiting for tenant service status")
				}
				if len(resp.ErrorsByNodeID) > 0 {
					for _, errStr := range resp.ErrorsByNodeID {
						return errors.Newf("failed to poll service status on all nodes (first error): %s", errStr)
					}
				}
				allStopped := true
				for n, info := range resp.StatusByNodeID {
					stoppedOrStopping := info.ServiceMode == mtinfopb.ServiceModeStopping || info.ServiceMode == mtinfopb.ServiceModeNone
					if !stoppedOrStopping {
						log.Infof(ctx, "tenant %d is still running on node %s", info.ID, n)
						allStopped = false
					}
				}
				if allStopped {
					break
				}
			}
			return nil
		}); err != nil {
			return mode, err
		}
	}

	return mode, nil
}

func (p *planner) renameTenant(
	ctx context.Context, info *mtinfopb.TenantInfo, newName roachpb.TenantName,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("ALTER VIRTUAL CLUSTER RENAME TO")
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, "rename tenant", p.ExecCfg().Settings); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, "rename"); err != nil {
		return err
	}
	if err := CanManageTenant(ctx, p); err != nil {
		return err
	}

	if newName != "" {
		if err := newName.IsValid(); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
	}

	if info.ServiceMode != mtinfopb.ServiceModeNone {
		// No name changes while there is a service mode.
		//
		// If this is ever allowed, the logic to update the tenant name in
		// logging output for tenant servers must be updated as well.
		return errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot rename tenant in service mode %v", info.ServiceMode),
			"Use ALTER VIRTUAL CLUSTER STOP SERVICE before renaming a tenant.")
	}

	info.Name = newName
	return errors.Wrap(
		UpdateTenantRecord(ctx, p.ExecCfg().Settings, p.InternalSQLTxn(), info),
		"renaming tenant")
}
