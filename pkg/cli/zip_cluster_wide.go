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
	eventsName        = "/events"
	livenessName      = "/liveness"
	nodesPrefix       = "/nodes"
	rangelogName      = "/rangelog"
	reportsPrefix     = "/reports"
	schemaPrefix      = "/schema"
	settingsName      = "/settings"
	problemRangesName = reportsPrefix + "/problemranges"
	tenantRangesName  = "/tenant_ranges"
)

// makeClusterWideZipRequests defines the zipRequests that are to be
// performed just once for the entire cluster.
func makeClusterWideZipRequests(
	admin serverpb.AdminClient, status serverpb.StatusClient, prefix string,
) []zipRequest {
	return []zipRequest{
		// NB: we intentionally omit liveness since it's already pulled manually (we
		// act on the output to special case decommissioned nodes).
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Events(ctx, &serverpb.EventsRequest{})
			},
			pathName: prefix + eventsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.RangeLog(ctx, &serverpb.RangeLogRequest{})
			},
			pathName: prefix + rangelogName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Settings(ctx, &serverpb.SettingsRequest{})
			},
			pathName: prefix + settingsName,
		},
		{
			fn: func(ctx context.Context) (interface{}, error) {
				return status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
			},
			pathName: prefix + problemRangesName,
		},
	}
}

// collectClusterData runs the data collection that only needs to
// occur once for the entire cluster.
func (zc *debugZipContext) collectClusterData(
	ctx context.Context,
) (nodesList *serverpb.NodesListResponse, livenessByNodeID nodeLivenesses, err error) {
	clusterWideZipRequests := makeClusterWideZipRequests(zc.admin, zc.status, zc.prefix)

	for _, r := range clusterWideZipRequests {
		if err := zc.runZipRequest(ctx, zc.clusterPrinter, r); err != nil {
			return &serverpb.NodesListResponse{}, nil, err
		}
	}

	queryAndDumpTables := func(reg DebugZipTableRegistry) error {
		for _, table := range reg.GetTables() {
			query, err := reg.QueryForTable(table, zipCtx.redact)
			if err != nil {
				return err
			}
			if err := zc.dumpTableDataForZip(zc.clusterPrinter, zc.firstNodeSQLConn, zc.prefix, table, query); err != nil {
				return errors.Wrapf(err, "fetching %s", table)
			}
		}
		return nil
	}
	if err := queryAndDumpTables(zipInternalTablesPerCluster); err != nil {
		return &serverpb.NodesListResponse{}, nil, err
	}
	if err := queryAndDumpTables(zipSystemTables); err != nil {
		return &serverpb.NodesListResponse{}, nil, err
	}

	{
		s := zc.clusterPrinter.start("requesting nodes")
		var nodesStatus *serverpb.NodesResponse
		err := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			nodesList, err = zc.status.NodesList(ctx, &serverpb.NodesListRequest{})
			nodesStatus, err = zc.status.Nodes(ctx, &serverpb.NodesRequest{})
			return err
		})

		if code := status.Code(errors.Cause(err)); code == codes.Unimplemented {
			// running on non system tenant, use data from NodesList()
			if cErr := zc.z.createJSONOrError(s, debugBase+"/nodes.json", nodesList, err); cErr != nil {
				return &serverpb.NodesListResponse{}, nil, cErr
			}
		} else {
			if cErr := zc.z.createJSONOrError(s, debugBase+"/nodes.json", nodesStatus, err); cErr != nil {
				return &serverpb.NodesListResponse{}, nil, cErr
			}
		}

		if nodesList == nil {
			// In case the NodesList() RPC failed), we still want to inspect the
			// per-node endpoints on the head node.
			s = zc.clusterPrinter.start("retrieving the node status")
			firstNodeDetails, err := zc.status.Details(ctx, &serverpb.DetailsRequest{NodeId: "local"})
			if err != nil {
				return &serverpb.NodesListResponse{}, nil, err
			}
			s.done()
			nodesList = &serverpb.NodesListResponse{
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
		if cErr := zc.z.createJSONOrError(s, zc.prefix+livenessName+".json", nodes, err); cErr != nil {
			return &serverpb.NodesListResponse{}, nil, cErr
		}
		livenessByNodeID = map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
		if lresponse != nil {
			livenessByNodeID = lresponse.Statuses
		}
	}

	if zipCtx.includeRangeInfo {
		var tenantRanges *serverpb.TenantRangesResponse
		s := zc.clusterPrinter.start("requesting tenant ranges")
		if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			var err error
			tenantRanges, err = zc.status.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
			return err
		}); requestErr != nil {
			if err := zc.z.createError(s, zc.prefix+tenantRangesName, requestErr); err != nil {
				return &serverpb.NodesListResponse{}, nil, errors.Wrap(err, "fetching tenant ranges")
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
				prefix := fmt.Sprintf("%s/%s/%s", zc.prefix, tenantRangesName, locality)
				for _, r := range rangeList.Ranges {
					sRange := zc.clusterPrinter.start("writing tenant range %d", r.RangeID)
					name := fmt.Sprintf("%s/%d", prefix, r.RangeID)
					if err := zc.z.createJSON(sRange, name+".json", r); err != nil {
						return &serverpb.NodesListResponse{}, nil, errors.Wrapf(err, "writing tenant range %d for locality %s", r.RangeID, locality)
					}
				}
				sLocality.done()
			}
			zc.clusterPrinter.info("%d tenant ranges found", rangesFound)
		}
	}

	return nodesList, livenessByNodeID, nil
}
