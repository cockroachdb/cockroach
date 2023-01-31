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
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type tenantValues struct {
	tenantInfo         *mtinfopb.TenantInfo
	dataState          string
	replicationInfo    *streampb.StreamIngestionStats
	protectedTimestamp hlc.Timestamp
	capabilities       []showTenantNodeCapability
}

type showTenantNodeCapability struct {
	name  string
	value string
}

type showTenantNode struct {
	tenantSpec       tenantSpec
	withReplication  bool
	withCapabilities bool
	columns          colinfo.ResultColumns
	tenantIDIndex    int
	tenantIds        []roachpb.TenantID
	initTenantValues bool
	values           *tenantValues
	capabilityIndex  int
	capability       showTenantNodeCapability
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
		tenantSpec:       tspec,
		withReplication:  n.WithReplication,
		withCapabilities: n.WithCapabilities,
		initTenantValues: true,
	}

	node.columns = colinfo.TenantColumns
	if n.WithReplication {
		node.columns = append(node.columns, colinfo.TenantColumnsWithReplication...)
	}
	if n.WithCapabilities {
		node.columns = append(node.columns, colinfo.TenantColumnsWithCapabilities...)
	}

	return node, nil
}

func (n *showTenantNode) startExec(params runParams) error {
	if _, ok := n.tenantSpec.(tenantSpecAll); ok {
		ids, err := GetAllNonDropTenantIDs(params.ctx, params.p.InternalSQLTxn(), params.p.ExecCfg().Settings)
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

func (n *showTenantNode) getTenantValues(
	params runParams, tenantInfo *mtinfopb.TenantInfo,
) (*tenantValues, error) {
	// Common fields.
	var values tenantValues
	values.tenantInfo = tenantInfo

	// Add capabilities if requested.
	if n.withCapabilities {
		capabilities := tenantInfo.Capabilities
		values.capabilities = []showTenantNodeCapability{
			{
				name:  canAdminSplitCapabilityName,
				value: strconv.FormatBool(capabilities.CanAdminSplit),
			},
			{
				name: canAdminUnsplitCapabilityName,
				// TODO(sql-sessions): handle this capability.
				value: strconv.FormatBool(false),
			},
		}
	}

	// Tenant status + replication status fields.
	jobId := tenantInfo.TenantReplicationJobID
	if jobId == 0 {
		// No replication job, this is a non-replicating tenant.
		if n.withReplication {
			return nil, errors.Newf("tenant %q does not have an active replication job", tenantInfo.Name)
		}
		values.dataState = values.tenantInfo.DataState.String()
	} else {
		switch values.tenantInfo.DataState {
		case mtinfopb.DataStateAdd:
			mgr, err := params.p.EvalContext().StreamManagerFactory.GetStreamIngestManager(params.ctx)
			if err != nil {
				return nil, err
			}
			stats, status, err := mgr.GetReplicationStatsAndStatus(params.ctx, jobId)
			values.dataState = status
			if err != nil {
				log.Warningf(params.ctx, "replication stats unavailable for tenant %q and job %d: %v",
					tenantInfo.Name, jobId, err)
			} else if n.withReplication {
				values.replicationInfo = stats

				if stats != nil && stats.IngestionDetails != nil && stats.IngestionDetails.ProtectedTimestampRecordID != nil {
					ptp := params.p.execCfg.ProtectedTimestampProvider.WithTxn(params.p.InternalSQLTxn())
					record, err := ptp.GetRecord(params.ctx, *stats.IngestionDetails.ProtectedTimestampRecordID)
					if err != nil {
						// Protected timestamp might not be set yet, no need to fail.
						log.Warningf(params.ctx, "protected timestamp unavailable for tenant %q and job %d: %v",
							tenantInfo.Name, jobId, err)
					}
					values.protectedTimestamp = record.Timestamp
				}
			}
		case mtinfopb.DataStateReady, mtinfopb.DataStateDrop:
			values.dataState = values.tenantInfo.DataState.String()
		default:
			return nil, errors.Newf("tenant %q state is unknown: %s", tenantInfo.Name, values.tenantInfo.DataState)
		}
	}

	return &values, nil
}

func (n *showTenantNode) Next(params runParams) (bool, error) {
	if n.tenantIDIndex >= len(n.tenantIds) {
		return false, nil
	}

	if n.initTenantValues {
		tenantInfo, err := GetTenantRecordByID(params.ctx, params.p.InternalSQLTxn(), n.tenantIds[n.tenantIDIndex], params.p.ExecCfg().Settings)
		if err != nil {
			return false, err
		}
		values, err := n.getTenantValues(params, tenantInfo)
		if err != nil {
			return false, err
		}
		n.values = values
		n.initTenantValues = false
	}

	if n.withCapabilities {
		capabilities := n.values.capabilities
		n.capability = capabilities[n.capabilityIndex]
		if n.capabilityIndex == len(capabilities)-1 {
			n.capabilityIndex = 0
			n.tenantIDIndex++
			n.initTenantValues = true
		} else {
			n.capabilityIndex++
		}
	} else {
		n.tenantIDIndex++
		n.initTenantValues = true
	}

	return true, nil
}

func (n *showTenantNode) Values() tree.Datums {
	v := n.values
	tenantInfo := v.tenantInfo
	result := tree.Datums{
		tree.NewDInt(tree.DInt(tenantInfo.ID)),
		tree.NewDString(string(tenantInfo.Name)),
		tree.NewDString(v.dataState),
		tree.NewDString(tenantInfo.ServiceMode.String()),
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

	if n.withCapabilities {
		capability := n.capability
		result = append(result,
			tree.NewDString(capability.name),
			tree.NewDString(capability.value),
		)
	}

	return result
}

func (n *showTenantNode) Close(_ context.Context) {}
