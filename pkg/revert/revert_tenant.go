// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// RevertTenantToTimestamp reverts a tenant to the passed timestamp.
func RevertTenantToTimestamp(
	ctx context.Context,
	evalCtx *eval.Context,
	tenantName roachpb.TenantName,
	revertTo hlc.Timestamp,
	sessionID clusterunique.ID,
) (err error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	defer func() {
		if err == nil {
			telemetry.Count("virtual_cluster.data_reset")
		} else {
			telemetry.Count("virtual_cluster.data_reset_failed")
		}
	}()
	// These vars are set in Txn below. This transaction checks
	// the service state of the tenant record, moves the tenant's
	// data state to ADD, and installs a PTS for the revert
	// timestamp.
	//
	// NB: We do this using a different txn since we want to be
	// able to commit the state change during the
	// non-transactional RevertSpans below.
	var (
		originalDataState mtinfopb.TenantDataState
		tenantID          roachpb.TenantID
		ptsCleanup        func()
	)
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantRecord, err := sql.GetTenantRecordByName(ctx, execCfg.Settings, txn, tenantName)
		if err != nil {
			return err
		}
		tenantID, err = roachpb.MakeTenantID(tenantRecord.ID)
		if err != nil {
			return err
		}

		if tenantID.Equal(roachpb.SystemTenantID) {
			return errors.New("cannot revert the system tenant")
		}

		if tenantRecord.ServiceMode != mtinfopb.ServiceModeNone {
			return errors.Newf("cannot revert tenant %q (%d) in service mode %s; service mode must be %s",
				tenantRecord.Name,
				tenantRecord.ID,
				tenantRecord.ServiceMode,
				mtinfopb.ServiceModeNone,
			)
		}

		originalDataState = tenantRecord.DataState

		ptsCleanup, err = protectTenantSpanWithSession(ctx, txn, execCfg, tenantID, sessionID, revertTo)
		if err != nil {
			return errors.Wrap(err, "protecting revert timestamp")
		}

		// Set the data state to Add during the destructive operation.
		tenantRecord.LastRevertTenantTimestamp = revertTo
		tenantRecord.DataState = mtinfopb.DataStateAdd
		return sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenantRecord)
	}); err != nil {
		return err
	}
	defer ptsCleanup()

	spanToRevert := keys.MakeTenantSpan(tenantID)
	if err := RevertSpansFanout(ctx, execCfg.DB, evalCtx.JobExecContext.(sql.JobExecContext),
		[]roachpb.Span{spanToRevert},
		revertTo,
		false, /* ignoreGCThreshold */
		RevertDefaultBatchSize,
		nil /* onCompletedCallback */); err != nil {
		return err
	}

	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantRecord, err := sql.GetTenantRecordByName(ctx, execCfg.Settings, txn, tenantName)
		if err != nil {
			return err
		}
		tenantRecord.DataState = originalDataState
		return sql.UpdateTenantRecord(ctx, execCfg.Settings, txn, tenantRecord)
	})
}

func protectTenantSpanWithSession(
	ctx context.Context,
	txn isql.Txn,
	execCfg *sql.ExecutorConfig,
	tenantID roachpb.TenantID,
	sessionID clusterunique.ID,
	timestamp hlc.Timestamp,
) (func(), error) {
	ptsRecordID := uuid.MakeV4()
	ptsRecord := sessionprotectedts.MakeRecord(
		ptsRecordID,
		[]byte(sessionID.String()),
		timestamp,
		ptpb.MakeTenantsTarget([]roachpb.TenantID{tenantID}),
	)
	log.Infof(ctx, "protecting tenant %s as of timestamp: %v", tenantID, timestamp)
	pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
	if err := pts.Protect(ctx, ptsRecord); err != nil {
		return nil, err
	}
	releasePTS := func() {
		if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			return pts.Release(ctx, ptsRecordID)
		}); err != nil {
			log.Warningf(ctx, "failed to release protected timestamp %s: %v", ptsRecordID, err)
		}
	}
	return releasePTS, nil
}
