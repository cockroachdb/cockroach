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
	"time"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type showTenantNode struct {
	name               tree.Expr
	info               *descpb.TenantInfo
	withReplication    bool
	replicationInfo    *streampb.StreamIngestionStats
	protectedTimestamp hlc.Timestamp
	columns            colinfo.ResultColumns
	done               bool
}

func (p *planner) ShowTenant(_ context.Context, n *tree.ShowTenant) (planNode, error) {
	node := &showTenantNode{
		name:            n.Name,
		withReplication: n.WithReplication,
	}
	if n.WithReplication {
		node.columns = colinfo.TenantColumnsWithReplication
	} else {
		node.columns = colinfo.TenantColumns
	}
	return node, nil
}

func (n *showTenantNode) startExec(params runParams) error {
	if err := params.p.RequireAdminRole(params.ctx, "show tenant"); err != nil {
		return err
	}

	if err := rejectIfCantCoordinateMultiTenancy(params.p.execCfg.Codec, "show"); err != nil {
		return err
	}

	info, err := GetTenantRecordByName(params.ctx, params.p.execCfg, params.p.Txn(), roachpb.TenantName(n.name.String()))
	if err != nil {
		return err
	}
	n.info = info
	if n.withReplication {
		if info.TenantReplicationJobID == 0 {
			return errors.Newf("tenant %q does not have an active replication job", n.name)
		}
		mgr, err := params.p.EvalContext().StreamManagerFactory.GetStreamIngestManager(params.ctx)
		if err != nil {
			return err
		}
		stats, err := mgr.GetStreamIngestionStats(params.ctx, info.TenantReplicationJobID)
		// An error means we don't have stats but we can still present some info,
		// therefore we don't fail here.
		// TODO(lidor): we need a better signal from GetStreamIngestionStats(), instead of
		// ignoring all errors.
		if err == nil {
			n.replicationInfo = stats
			if stats.IngestionDetails.ProtectedTimestampRecordID == nil {
				// We don't have the protected timestamp record but we still want to show
				// the info we do have about tenant replication status, logging an error
				// and continuing.
				log.Warningf(params.ctx, "protected timestamp unavailable for tenant %q and job %d",
					n.name, info.TenantReplicationJobID)
			} else {
				ptp := params.p.execCfg.ProtectedTimestampProvider
				record, err := ptp.GetRecord(params.ctx, params.p.Txn(), *stats.IngestionDetails.ProtectedTimestampRecordID)
				if err != nil {
					return err
				}
				n.protectedTimestamp = record.Timestamp
			}
		}
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
	tenantId := tree.NewDInt(tree.DInt(n.info.ID))
	tenantName := tree.NewDString(string(n.info.Name))
	tenantStatus := tree.NewDString(n.info.State.String())
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
	replicationJobId := tree.NewDInt(tree.DInt(n.info.TenantReplicationJobID))
	replicatedTimestamp := tree.DNull
	retainedTimestamp := tree.DNull

	// New replication clusters don't have replicationInfo initially.
	if n.replicationInfo != nil {
		sourceTenantName = tree.NewDString(string(n.replicationInfo.IngestionDetails.SourceTenantName))
		sourceClusterUri = tree.NewDString(n.replicationInfo.IngestionDetails.StreamAddress)
		if n.replicationInfo.ReplicationLagInfo != nil {
			minIngested := n.replicationInfo.ReplicationLagInfo.MinIngestedTimestamp.WallTime
			// The latest fully replicated time.
			replicatedTimestamp, _ = tree.MakeDTimestamp(timeutil.Unix(0, minIngested), time.Nanosecond)
		}
		// The protected timestamp on the destination cluster.
		retainedTimestamp, _ = tree.MakeDTimestamp(timeutil.Unix(0, n.protectedTimestamp.WallTime), time.Nanosecond)
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
	}
}

func (n *showTenantNode) Close(_ context.Context) {}
