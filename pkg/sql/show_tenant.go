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
	name            roachpb.TenantName
	info            *descpb.TenantInfo
	withReplication bool
	replicationInfo *streampb.StreamIngestionStats
	pts             hlc.Timestamp
	columns         colinfo.ResultColumns
	done            bool
}

func (p *planner) ShowTenant(_ context.Context, n *tree.ShowTenant) (planNode, error) {
	node := &showTenantNode{
		name:            roachpb.TenantName(n.Name),
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
	info, err := params.p.GetTenantInfo(params.ctx, n.name)
	if err != nil {
		return err
	}
	n.info = info
	if n.withReplication {
		if info.TenantReplicationJobID == 0 {
			return errors.Newf("tenant %s does not have replication info", n.name)
		}
		repInfo, err := params.p.GetTenantReplicationInfo(params.ctx, info.TenantReplicationJobID)
		if err != nil {
			return err
		}
		n.replicationInfo = repInfo
		if repInfo.IngestionDetails.ProtectedTimestampRecordID == nil {
			// We don't have the PTS record but we still want to show the info we do
			// have about tenant replication status, logging an error and continuing.
			log.Errorf(params.ctx, "protected timestamp unavailable")
		} else {
			ptp := params.p.execCfg.ProtectedTimestampProvider
			record, err := ptp.GetRecord(params.ctx, params.p.Txn(), *repInfo.IngestionDetails.ProtectedTimestampRecordID)
			if err != nil {
				return err
			}
			n.pts = record.Timestamp
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
	tenantId := tree.DInt(n.info.ID)
	tenantName := string(n.info.Name)
	tenantStatus := n.info.State.String()
	if n.withReplication {
		sourceTenantName := string(n.replicationInfo.IngestionDetails.SourceTenantName)
		sourceClusterUri := n.replicationInfo.IngestionDetails.StreamAddress
		replicationJobId := tree.DInt(n.info.TenantReplicationJobID)
		minIngested := n.replicationInfo.ReplicationLagInfo.MinIngestedTimestamp.WallTime
		latestFullyReplicatedTimestamp, _ := tree.MakeDTimestamp(timeutil.Unix(0, minIngested), time.Nanosecond)
		protectedTimestamp, _ := tree.MakeDTimestamp(timeutil.Unix(0, n.pts.WallTime), time.Nanosecond)
		return tree.Datums{
			tree.NewDInt(tenantId),
			tree.NewDString(tenantName),
			tree.NewDString(tenantStatus),
			tree.NewDString(sourceTenantName),
			tree.NewDString(sourceClusterUri),
			tree.NewDInt(replicationJobId),
			latestFullyReplicatedTimestamp,
			protectedTimestamp,
		}
	}
	return tree.Datums{
		tree.NewDInt(tenantId),
		tree.NewDString(tenantName),
		tree.NewDString(tenantStatus),
	}
}
func (n *showTenantNode) Close(_ context.Context) {}
