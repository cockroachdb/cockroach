// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type tenantStatus string

const (
	initReplication   tenantStatus = "INITIALIZING REPLICATION"
	replicating       tenantStatus = "REPLICATING"
	replicationPaused tenantStatus = "REPLICATION PAUSED"
	cuttingOver       tenantStatus = "REPLICATION CUTTING OVER"
	// Users should not see this status normally.
	replicationUnknownFormat tenantStatus = "REPLICATION UNKNOWN (%s)"
)

type showTenantNode struct {
	name               tree.TypedExpr
	tenantInfo         *descpb.TenantInfo
	tenantStatus       tenantStatus
	withReplication    bool
	replicationInfo    *streampb.StreamIngestionStats
	protectedTimestamp hlc.Timestamp
	columns            colinfo.ResultColumns
	done               bool
}

func (p *planner) ShowTenant(ctx context.Context, n *tree.ShowTenant) (planNode, error) {
	if err := p.RequireAdminRole(ctx, "show tenant"); err != nil {
		return nil, err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "show"); err != nil {
		return nil, err
	}

	var dummyHelper tree.IndexedVarHelper
	strName := paramparse.UnresolvedNameToStrVal(n.Name)
	typedName, err := p.analyzeExpr(
		ctx, strName, nil, dummyHelper, types.String,
		true, "SHOW TENANT ... WITH REPLICATION STATUS")
	if err != nil {
		return nil, err
	}

	node := &showTenantNode{
		name:            typedName,
		withReplication: n.WithReplication,
	}
	if n.WithReplication {
		node.columns = colinfo.TenantColumnsWithReplication
	} else {
		node.columns = colinfo.TenantColumns
	}

	return node, nil
}

func (n *showTenantNode) getTenantName(params runParams) (roachpb.TenantName, error) {
	dName, err := eval.Expr(params.ctx, params.p.EvalContext(), n.name)
	if err != nil {
		return "", err
	}
	name, ok := dName.(*tree.DString)
	if !ok || name == nil {
		return "", errors.Newf("expected a string, got %T", dName)
	}
	return roachpb.TenantName(*name), nil
}

func (n *showTenantNode) initReplicationStats(params runParams, job *jobs.Job) error {
	mgr, err := params.p.EvalContext().StreamManagerFactory.GetStreamIngestManager(params.ctx)
	if err != nil {
		return err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}
	stats, err := mgr.GetStreamIngestionStats(params.ctx, details, job.Progress())
	if err != nil {
		// An error means we don't have stats but we can still present some info,
		// therefore we don't fail here.
		// TODO(lidor): we need a better signal from GetStreamIngestionStats(), instead of
		// ignoring all errors.
		log.Infof(params.ctx, "stream ingestion stats unavailable for tenant %q and job %d",
			details.DestinationTenantName, job.ID())
	} else {
		n.replicationInfo = stats
		if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
			// We don't have the protected timestamp record but we still want to show
			// the info we do have about tenant replication status, logging an error
			// and continuing.
			log.Warningf(params.ctx, "protected timestamp unavailable for tenant %q and job %d",
				details.DestinationTenantName, job.ID())
		} else {
			ptp := params.p.execCfg.ProtectedTimestampProvider
			record, err := ptp.GetRecord(params.ctx, params.p.Txn(), *stats.IngestionDetails.ProtectedTimestampRecordID)
			if err != nil {
				return err
			}
			n.protectedTimestamp = record.Timestamp
		}
	}
	return nil
}

func getTenantStatus(
	jobStatus jobs.Status, replicationInfo *streampb.StreamIngestionStats,
) tenantStatus {
	switch jobStatus {
	case jobs.StatusPending, jobs.StatusRunning, jobs.StatusPauseRequested:
		if replicationInfo == nil || replicationInfo.ReplicationLagInfo == nil {
			// Still no lag info which means we never recorded progress, and
			// replication did not complete the initial scan yet.
			return initReplication
		} else {
			progress := replicationInfo.IngestionProgress
			if progress != nil && !progress.CutoverTime.IsEmpty() {
				return cuttingOver
			} else {
				return replicating
			}
		}
	case jobs.StatusPaused:
		return replicationPaused
	default:
		return tenantStatus(fmt.Sprintf(string(replicationUnknownFormat), jobStatus))
	}
}

func (n *showTenantNode) startExec(params runParams) error {
	tenantName, err := n.getTenantName(params)
	if err != nil {
		return err
	}
	tenantRecord, err := GetTenantRecordByName(params.ctx, params.p.execCfg, params.p.Txn(), tenantName)
	if err != nil {
		return err
	}
	n.tenantInfo = tenantRecord
	jobId := n.tenantInfo.TenantReplicationJobID
	if jobId == 0 {
		// No replication job, this is a non-replicating tenant.
		if n.withReplication {
			return errors.Newf("tenant %q does not have an active replication job", tenantName)
		}
		n.tenantStatus = tenantStatus(n.tenantInfo.State.String())
		return nil
	}

	switch n.tenantInfo.State {
	case descpb.TenantInfo_ADD:
		// There is a replication job, we need to get the job info and the
		// replication stats in order to generate the exact tenant status.
		registry := params.p.execCfg.JobRegistry
		job, err := registry.LoadJobWithTxn(params.ctx, jobId, params.p.Txn())
		if err != nil {
			return err
		}
		if err := n.initReplicationStats(params, job); err != nil {
			return err
		}

		n.tenantStatus = getTenantStatus(job.Status(), n.replicationInfo)
	case descpb.TenantInfo_ACTIVE, descpb.TenantInfo_DROP:
		n.tenantStatus = tenantStatus(n.tenantInfo.State.String())
	default:
		return errors.Newf("unknown tenant status %s", n.tenantInfo.State)
	}

	return nil
}

func (n *showTenantNode) Next(_ runParams) (bool, error) {
	if n.done {
		return false, nil
	}
	n.done = true
	return true, nil
}

func (n *showTenantNode) Values() tree.Datums {
	tenantId := tree.NewDInt(tree.DInt(n.tenantInfo.ID))
	tenantName := tree.NewDString(string(n.tenantInfo.Name))
	tenantStatus := tree.NewDString(string(n.tenantStatus))
	if !n.withReplication {
		// This is a simple 'SHOW TENANT name'.
		return tree.Datums{
			tenantId,
			tenantName,
			tenantStatus,
		}
	}

	// This is a 'SHOW TENANT name WITH REPLICATION STATUS' command.
	sourceTenantName := tree.DNull
	sourceClusterUri := tree.DNull
	replicationJobId := tree.NewDInt(tree.DInt(n.tenantInfo.TenantReplicationJobID))
	replicatedTimestamp := tree.DNull
	retainedTimestamp := tree.DNull
	cutoverTimestamp := tree.DNull

	if n.replicationInfo != nil {
		sourceTenantName = tree.NewDString(string(n.replicationInfo.IngestionDetails.SourceTenantName))
		sourceClusterUri = tree.NewDString(n.replicationInfo.IngestionDetails.StreamAddress)
		if n.replicationInfo.ReplicationLagInfo != nil {
			minIngested := n.replicationInfo.ReplicationLagInfo.MinIngestedTimestamp
			// The latest fully replicated time. Truncating to the nearest microsecond
			// because if we don't, then MakeDTimestamp rounds to the nearest
			// microsecond. In that case a user may want to cutover to a rounded-up
			// time, which is a time that we may never replicate to. Instead, we show
			// a time that we know we replicated to.
			replicatedTimestamp, _ = tree.MakeDTimestampTZ(minIngested.GoTime().Truncate(time.Microsecond), time.Nanosecond)
		}
		// The protected timestamp on the destination cluster. Same as with the
		// replicatedTimestamp, we want to show a retained time that is within the
		// window (retained to replicated) and not below it. We take a timestamp
		// that is greater than the protected timestamp by a microsecond or less
		// (it's not exactly ceil but close enough).
		retainedCeil := n.protectedTimestamp.GoTime().Truncate(time.Microsecond).Add(time.Microsecond)
		retainedTimestamp, _ = tree.MakeDTimestampTZ(retainedCeil, time.Nanosecond)
		progress := n.replicationInfo.IngestionProgress
		if progress != nil && !progress.CutoverTime.IsEmpty() {
			cutoverTimestamp = eval.TimestampToDecimalDatum(progress.CutoverTime)
		}
	}

	return tree.Datums{
		tenantId,
		tenantName,
		tenantStatus,
		sourceTenantName,
		sourceClusterUri,
		replicationJobId,
		replicatedTimestamp,
		retainedTimestamp,
		cutoverTimestamp,
	}
}

func (n *showTenantNode) Close(_ context.Context) {}
