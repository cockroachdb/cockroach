// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/errors"
)

const (
	debugBase         = "debug"
	eventsName        = debugBase + "/events"
	livenessName      = debugBase + "/liveness"
	nodesPrefix       = debugBase + "/nodes"
	rangelogName      = debugBase + "/rangelog"
	reportsPrefix     = debugBase + "/reports"
	schemaPrefix      = debugBase + "/schema"
	settingsName      = debugBase + "/settings"
	problemRangesName = reportsPrefix + "/problemranges"
)

// makeClusterWideZipRequests defines the zipRequests that are to be
// performed just once for the entire cluster.
func makeClusterWideZipRequests(
	admin serverpb.AdminClient, status serverpb.StatusClient,
) []zipRequest {
	return []zipRequest{
		// NB: we intentionally omit liveness since it's already pulled manually (we
		// act on the output to special case decommissioned nodes).
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Events(ctx, &serverpb.EventsRequest{})
			},
			pathName: eventsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.RangeLog(ctx, &serverpb.RangeLogRequest{})
			},
			pathName: rangelogName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Settings(ctx, &serverpb.SettingsRequest{})
			},
			pathName: settingsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
			},
			pathName: problemRangesName,
		},
	}
}

// Tables containing cluster-wide info that are collected using SQL
// into a debug zip.
var debugZipTablesPerCluster = []string{
	"crdb_internal.cluster_contention_events",
	"crdb_internal.cluster_distsql_flows",
	"crdb_internal.cluster_database_privileges",
	"crdb_internal.cluster_queries",
	"crdb_internal.cluster_sessions",
	"crdb_internal.cluster_settings",
	"crdb_internal.cluster_transactions",

	"crdb_internal.default_privileges",

	"crdb_internal.jobs",
	"system.jobs",       // get the raw, restorable jobs records too.
	"system.descriptor", // descriptors also contain job-like mutation state.
	"system.namespace",
	"system.scheduled_jobs",

	// The synthetic SQL CREATE statements for all tables.
	// Note the "". to collect across all databases.
	`"".crdb_internal.create_statements`,
	// Ditto, for CREATE TYPE.
	`"".crdb_internal.create_type_statements`,

	"crdb_internal.kv_node_liveness",
	"crdb_internal.kv_node_status",
	"crdb_internal.kv_store_status",

	"crdb_internal.regions",
	"crdb_internal.schema_changes",
	"crdb_internal.partitions",
	"crdb_internal.zones",
	"crdb_internal.invalid_objects",
	"crdb_internal.index_usage_statistics",
}

// collectClusterData runs the data collection that only needs to
// occur once for the entire cluster.
func (zc *debugZipContext) collectClusterData(
	ctx context.Context, firstNodeDetails *serverpb.DetailsResponse,
) (nodeList []statuspb.NodeStatus, livenessByNodeID nodeLivenesses, err error) {
	clusterWideZipRequests := makeClusterWideZipRequests(zc.admin, zc.status)

	for _, r := range clusterWideZipRequests {
		if err := zc.runZipRequest(ctx, zc.clusterPrinter, r); err != nil {
			return nil, nil, err
		}
	}

	for _, table := range debugZipTablesPerCluster {
		query := fmt.Sprintf(`SELECT * FROM %s`, table)
		if override, ok := customQuery[table]; ok {
			query = override
		}
		if err := zc.dumpTableDataForZip(zc.clusterPrinter, zc.firstNodeSQLConn, debugBase, table, query); err != nil {
			return nil, nil, errors.Wrapf(err, "fetching %s", table)
		}
	}

	{
		var nodes *serverpb.NodesResponse
		s := zc.clusterPrinter.start("requesting nodes")
		err := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			nodes, err = zc.status.Nodes(ctx, &serverpb.NodesRequest{})
			return err
		})
		if cErr := zc.z.createJSONOrError(s, debugBase+"/nodes.json", nodes, err); cErr != nil {
			return nil, nil, cErr
		}

		// In case nodes came up back empty (the Nodes() RPC failed), we
		// still want to inspect the per-node endpoints on the head
		// node. As per the above, we were able to connect at least to
		// that.
		nodeList = []statuspb.NodeStatus{{Desc: roachpb.NodeDescriptor{
			NodeID:     firstNodeDetails.NodeID,
			Address:    firstNodeDetails.Address,
			SQLAddress: firstNodeDetails.SQLAddress,
		}}}
		if nodes != nil {
			// If the nodes were found, use that instead.
			nodeList = nodes.Nodes
		}

		// We'll want livenesses to decide whether a node is decommissioned.
		var lresponse *serverpb.LivenessResponse
		s = zc.clusterPrinter.start("requesting liveness")
		err = zc.runZipFn(ctx, s, func(ctx context.Context) error {
			lresponse, err = zc.admin.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		})
		if cErr := zc.z.createJSONOrError(s, livenessName+".json", nodes, err); cErr != nil {
			return nil, nil, cErr
		}
		livenessByNodeID = map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
		if lresponse != nil {
			livenessByNodeID = lresponse.Statuses
		}
	}

	return nodeList, livenessByNodeID, nil
}
