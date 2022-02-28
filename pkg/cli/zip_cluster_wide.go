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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	tenantRangesName  = debugBase + "/tenant_ranges"
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
	"system.settings", // get the raw settings to determine what's explicitly set.
	"system.replication_stats",
	"system.replication_critical_localities",
	"system.replication_constraint_stats",

	// The synthetic SQL CREATE statements for all tables.
	// Note the "". to collect across all databases.
	`"".crdb_internal.create_schema_statements`,
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
	"crdb_internal.table_indexes",
	"crdb_internal.transaction_contention_events",
}

// nodesInfo holds node details pulled from a SQL or storage node.
// SQL only servers will only return nodesListResponse for all SQL nodes.
// Storage servers will return both nodesListResponse and nodesStatusResponse
// for all storage nodes.
type nodesInfo struct {
	nodesStatusResponse *serverpb.NodesResponse
	nodesListResponse   *serverpb.NodesListResponse
}

// collectClusterData runs the data collection that only needs to
// occur once for the entire cluster.
func (zc *debugZipContext) collectClusterData(
	ctx context.Context, firstNodeDetails *serverpb.DetailsResponse,
) (ni nodesInfo, livenessByNodeID nodeLivenesses, err error) {
	clusterWideZipRequests := makeClusterWideZipRequests(zc.admin, zc.status)

	for _, r := range clusterWideZipRequests {
		if err := zc.runZipRequest(ctx, zc.clusterPrinter, r); err != nil {
			return nodesInfo{}, nil, err
		}
	}

	for _, table := range debugZipTablesPerCluster {
		query := fmt.Sprintf(`SELECT * FROM %s`, table)
		if override, ok := customQuery[table]; ok {
			query = override
		}
		if err := zc.dumpTableDataForZip(zc.clusterPrinter, zc.firstNodeSQLConn, debugBase, table, query); err != nil {
			return nodesInfo{}, nil, errors.Wrapf(err, "fetching %s", table)
		}
	}

	{
		s := zc.clusterPrinter.start("requesting nodes")
		err := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			ni, err = zc.nodesInfo(ctx)
			return err
		})
		if ni.nodesStatusResponse != nil {
			if cErr := zc.z.createJSONOrError(s, debugBase+"/nodes.json", ni.nodesStatusResponse, err); cErr != nil {
				return nodesInfo{}, nil, cErr
			}
		} else {
			if cErr := zc.z.createJSONOrError(s, debugBase+"/nodes.json", ni.nodesListResponse, err); cErr != nil {
				return nodesInfo{}, nil, cErr
			}
		}

		if ni.nodesListResponse == nil {
			// In case nodes came up back empty (the Nodes()/NodesList() RPC failed), we
			// still want to inspect the per-node endpoints on the head
			// node. As per the above, we were able to connect at least to
			// that.
			ni.nodesListResponse = &serverpb.NodesListResponse{
				Nodes: []serverpb.NodeDetails{{
					NodeID:     int32(firstNodeDetails.NodeID),
					Address:    firstNodeDetails.Address,
					SQLAddress: firstNodeDetails.SQLAddress,
				}},
			}
		}

		// We'll want livenesses to decide whether a node is decommissioned.
		var lresponse *serverpb.LivenessResponse
		s = zc.clusterPrinter.start("requesting liveness")
		err = zc.runZipFn(ctx, s, func(ctx context.Context) error {
			lresponse, err = zc.admin.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		})
		if cErr := zc.z.createJSONOrError(s, livenessName+".json", nodes, err); cErr != nil {
			return nodesInfo{}, nil, cErr
		}
		livenessByNodeID = map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
		if lresponse != nil {
			livenessByNodeID = lresponse.Statuses
		}
	}

	{
		var tenantRanges *serverpb.TenantRangesResponse
		s := zc.clusterPrinter.start("requesting tenant ranges")
		if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			var err error
			tenantRanges, err = zc.status.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
			return err
		}); requestErr != nil {
			if err := zc.z.createError(s, tenantRangesName, requestErr); err != nil {
				return nodesInfo{}, nil, errors.Wrap(err, "fetching tenant ranges")
			}
		} else {
			s.done()
			rangesFound := 0
			for locality, rangeList := range tenantRanges.RangesByLocality {
				rangesFound += len(rangeList.Ranges)
				sort.Slice(rangeList.Ranges, func(i, j int) bool {
					return rangeList.Ranges[i].RangeID > rangeList.Ranges[j].RangeID
				})
				sLocality := zc.clusterPrinter.start("writing tenant ranges for locality: %s", locality)
				prefix := fmt.Sprintf("%s/%s", tenantRangesName, locality)
				for _, r := range rangeList.Ranges {
					sRange := zc.clusterPrinter.start("writing tenant range %d", r.RangeID)
					name := fmt.Sprintf("%s/%d", prefix, r.RangeID)
					if err := zc.z.createJSON(sRange, name+".json", r); err != nil {
						return nodesInfo{}, nil, errors.Wrapf(err, "writing tenant range %d for locality %s", r.RangeID, locality)
					}
				}
				sLocality.done()
			}
			zc.clusterPrinter.info("%d tenant ranges found", rangesFound)
		}
	}

	return ni, livenessByNodeID, nil
}

// nodesInfo constructs debug data for all nodes for the debug zip output.
// For SQL only servers, only the NodesListResponse is populated.
// For regular storage servers, the more detailed NodesResponse is
// returned along with the nodesListResponse.
func (zc *debugZipContext) nodesInfo(ctx context.Context) (ni nodesInfo, _ error) {
	nodesResponse, err := zc.status.Nodes(ctx, &serverpb.NodesRequest{})
	nodesList := &serverpb.NodesListResponse{}
	if code := status.Code(errors.Cause(err)); code == codes.Unimplemented {
		// Likely a SQL only server; try the NodesList endpoint.
		nodesList, err = zc.status.NodesList(ctx, &serverpb.NodesListRequest{})
	}
	if err != nil {
		return nodesInfo{}, err
	}
	if nodesResponse != nil {
		// Build a nodesListResponse from the nodes data. nodesListResponse is needed
		// further downstream to perform other debug zip related functionality such as
		// collecting per node debug data. This will only be executed for storage
		// servers.
		for _, node := range nodesResponse.Nodes {
			nodeDetails := serverpb.NodeDetails{
				NodeID:     int32(node.Desc.NodeID),
				Address:    node.Desc.Address,
				SQLAddress: node.Desc.SQLAddress,
			}
			nodesList.Nodes = append(nodesList.Nodes, nodeDetails)
		}
	}
	ni.nodesListResponse = nodesList
	ni.nodesStatusResponse = nodesResponse

	return ni, nil
}
