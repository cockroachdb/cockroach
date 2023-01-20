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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type tenantStatus string

const (
	initReplication   tenantStatus = "initializing replication"
	replicating       tenantStatus = "replicating"
	replicationPaused tenantStatus = "replication paused"
	cuttingOver       tenantStatus = "replication cutting over"
	// Users should not see this status normally.
	replicationUnknownFormat tenantStatus = "replication unknown (%s)"
)

type tenantValues struct {
	tenantInfo         *descpb.ExtendedTenantInfo
	tenantStatus       tenantStatus
	replicationInfo    *streampb.StreamIngestionStats
	protectedTimestamp hlc.Timestamp
}

type showTenantNode struct {
	tenantSpec      tenantSpec
	withReplication bool
	columns         colinfo.ResultColumns
	row             int
	tenantIds       []roachpb.TenantID
	values          *tenantValues
}

// ShowTenant constructs a showTenantNode.
func (p *planner) ShowTenant(ctx context.Context, n *tree.ShowTenant) (planNode, error) {
	if err := p.RequireAdminRole(ctx, "show tenant"); err != nil {
		return nil, err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "show"); err != nil {
		return nil, err
	}

	tspec, err := p.planTenantSpec(ctx, n.TenantSpec, "SHOW TENANT")
	if err != nil {
		return nil, err
	}

	node := &showTenantNode{
		tenantSpec:      tspec,
		withReplication: n.WithReplication,
	}

	node.columns = colinfo.TenantColumns
	if n.WithReplication {
		node.columns = append(node.columns, colinfo.TenantColumnsWithReplication...)
	}

	return node, nil
}

func (n *showTenantNode) startExec(params runParams) error {
	if _, ok := n.tenantSpec.(tenantSpecAll); ok {
		ids, err := GetAllNonDropTenantIDs(params.ctx, params.p.InternalSQLTxn())
		if err != nil {
			return err
		}
		n.tenantIds = ids
	} else {
		tenantRecord, err := n.tenantSpec.getTenantInfo(params.ctx, params.p)
		if err != nil {
			return err
		}
		n.tenantIds = []roachpb.TenantID{roachpb.MustMakeTenantID(tenantRecord.ID)}
	}
	return nil
}

func getReplicationStats(
	params runParams, job *jobs.Job,
) (*streampb.StreamIngestionStats, *hlc.Timestamp, error) {
	mgr, err := params.p.EvalContext().StreamManagerFactory.GetStreamIngestManager(params.ctx)
	if err != nil {
		return nil, nil, err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return nil, nil, errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}
	stats, err := mgr.GetStreamIngestionStats(params.ctx, details, job.Progress())
	var protectedTimestamp hlc.Timestamp
	if err != nil {
		// An error means we don't have stats but we can still present some info,
		// therefore we don't fail here.
		// TODO(lidor): we need a better signal from GetStreamIngestionStats(), instead of
		// ignoring all errors.
		log.Infof(params.ctx, "stream ingestion stats unavailable for tenant %q and job %d",
			details.DestinationTenantName, job.ID())
	} else {
		if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
			// We don't have the protected timestamp record, but we still want to show
			// the info we do have about tenant replication status, logging an error
			// and continuing.
			log.Warningf(params.ctx, "protected timestamp unavailable for tenant %q and job %d",
				details.DestinationTenantName, job.ID())
		} else {
			ptp := params.p.execCfg.ProtectedTimestampProvider.WithTxn(params.p.InternalSQLTxn())
			record, err := ptp.GetRecord(params.ctx, *stats.IngestionDetails.ProtectedTimestampRecordID)
			if err != nil {
				// Protected timestamp might not be set yet, no need to fail.
				log.Warningf(params.ctx, "protected timestamp unavailable for tenant %q and job %d: %v",
					details.DestinationTenantName, job.ID(), err)
				return stats, nil, nil
			}
			protectedTimestamp = record.Timestamp
		}
	}
	return stats, &protectedTimestamp, nil
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

func (n *showTenantNode) getTenantValues(
	params runParams, tenantInfo *descpb.ExtendedTenantInfo,
) (*tenantValues, error) {
	var values tenantValues
	values.tenantInfo = tenantInfo
	jobId := values.tenantInfo.TenantReplicationJobID
	if jobId == 0 {
		// No replication job, this is a non-replicating tenant.
		if n.withReplication {
			return nil, errors.Newf("tenant %q does not have an active replication job", tenantInfo.Name)
		}
		dataState := strings.ToLower(values.tenantInfo.DataState.String())
		values.tenantStatus = tenantStatus(dataState)
		return &values, nil
	}

	switch values.tenantInfo.DataState {
	case descpb.DataStateAdd:
		// There is a replication job, we need to get the job info and the
		// replication stats in order to generate the exact tenant status.
		registry := params.p.execCfg.JobRegistry
		job, err := registry.LoadJobWithTxn(params.ctx, jobId, params.p.InternalSQLTxn())
		if err != nil {
			log.Errorf(params.ctx, "cannot load job info for replicated tenant %q and job %d: %v",
				tenantInfo.Name, jobId, err)
			values.tenantStatus = tenantStatus(fmt.Sprintf(string(replicationUnknownFormat), err))
			return &values, nil
		}
		stats, protectedTimestamp, err := getReplicationStats(params, job)
		if err != nil {
			log.Errorf(params.ctx, "cannot load replication stats for replicated tenant %q and job %d: %v",
				tenantInfo.Name, jobId, err)
			values.tenantStatus = tenantStatus(fmt.Sprintf(string(replicationUnknownFormat), err))
			return &values, nil
		}
		values.replicationInfo = stats
		if protectedTimestamp != nil {
			values.protectedTimestamp = *protectedTimestamp
		}

		values.tenantStatus = getTenantStatus(job.Status(), values.replicationInfo)
	case descpb.DataStateReady, descpb.DataStateDrop:
		dataState := values.tenantInfo.DataState.String()
		values.tenantStatus = tenantStatus(dataState)
	default:
		return nil, errors.Newf("tenant %q state is unknown: %s", tenantInfo.Name, values.tenantInfo.DataState)
	}
	return &values, nil
}

func (n *showTenantNode) Next(params runParams) (bool, error) {
	if n.row >= len(n.tenantIds) {
		return false, nil
	}

	tenantInfo, err := GetTenantRecordByID(params.ctx, params.p.InternalSQLTxn(), n.tenantIds[n.row])
	if err != nil {
		return false, err
	}

	values, err := n.getTenantValues(params, tenantInfo)
	if err != nil {
		return false, err
	}
	n.values = values
	n.row++
	return true, nil
}

func (n *showTenantNode) Values() tree.Datums {
	v := n.values
	tenantInfo := v.tenantInfo
	result := tree.Datums{
		tree.NewDInt(tree.DInt(tenantInfo.ID)),
		tree.NewDString(string(tenantInfo.Name)),
		tree.NewDString(string(v.tenantStatus)),
		tree.NewDString(strings.ToLower(tenantInfo.ServiceMode.String())),
	}

	if n.withReplication {
		// This is a 'SHOW TENANT name WITH REPLICATION STATUS' command.
		sourceTenantName := tree.DNull
		sourceClusterUri := tree.DNull
		replicationJobId := tree.NewDInt(tree.DInt(tenantInfo.TenantReplicationJobID))
		replicatedTimestamp := tree.DNull
		retainedTimestamp := tree.DNull
		cutoverTimestamp := tree.DNull

		replicationInfo := v.replicationInfo
		if replicationInfo != nil {
			sourceTenantName = tree.NewDString(string(replicationInfo.IngestionDetails.SourceTenantName))
			sourceClusterUri = tree.NewDString(replicationInfo.IngestionDetails.StreamAddress)
			if replicationInfo.ReplicationLagInfo != nil {
				minIngested := replicationInfo.ReplicationLagInfo.MinIngestedTimestamp
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
			retainedCeil := v.protectedTimestamp.GoTime().Truncate(time.Microsecond).Add(time.Microsecond)
			retainedTimestamp, _ = tree.MakeDTimestampTZ(retainedCeil, time.Nanosecond)
			progress := replicationInfo.IngestionProgress
			if progress != nil && !progress.CutoverTime.IsEmpty() {
				cutoverTimestamp = eval.TimestampToDecimalDatum(progress.CutoverTime)
			}
		}

		result = append(result,
			sourceTenantName,
			sourceClusterUri,
			replicationJobId,
			replicatedTimestamp,
			retainedTimestamp,
			cutoverTimestamp,
		)
	}

	return result
}

func (n *showTenantNode) Close(_ context.Context) {}
