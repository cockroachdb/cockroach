// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	debugBase         = "debug"
	eventsName        = "/events"
	nodesPrefix       = "/nodes"
	rangelogName      = "/rangelog"
	reportsPrefix     = "/reports"
	schemaPrefix      = "/schema"
	settingsName      = "/settings"
	problemRangesName = reportsPrefix + "/problemranges"
	tenantRangesName  = "/tenant_ranges"
	nodesFile         = "nodes.json"
	livenessFile      = "liveness.json"
	eventsFile        = "events.json"
	rangeLogFile      = "rangelog.json"
	settingsFile      = "settings.json"
	problemRangesFile = "problemranges.json"
)

// makeClusterWideZipRequests defines the zipRequests that are to be
// performed just once for the entire cluster.
func makeClusterWideZipRequests(
	zr *zipReporter, admin serverpb.AdminClient, status serverpb.StatusClient, prefix string,
) []zipRequest {
	var zipRequests []zipRequest

	if zipCtx.files.shouldIncludeFile(eventsFile) {
		zipRequests = append(zipRequests, zipRequest{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Events(ctx, &serverpb.EventsRequest{})
			},
			pathName: prefix + eventsName,
		})
	} else {
		zr.info("skipping %s due to file filters", eventsFile)
	}

	if zipCtx.files.shouldIncludeFile(rangeLogFile) {
		zipRequests = append(zipRequests, zipRequest{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.RangeLog(ctx, &serverpb.RangeLogRequest{})
			},
			pathName: prefix + rangelogName,
		})
	} else {
		zr.info("skipping %s due to file filters", rangeLogFile)
	}

	if zipCtx.files.shouldIncludeFile(settingsFile) {
		zipRequests = append(zipRequests, zipRequest{
			fn: func(ctx context.Context) (interface{}, error) {
				return admin.Settings(ctx, &serverpb.SettingsRequest{})
			},
			pathName: prefix + settingsName,
		})
	} else {
		zr.info("skipping %s due to file filters", settingsFile)
	}

	if zipCtx.includeRangeInfo {
		if zipCtx.files.shouldIncludeFile(problemRangesFile) {
			zipRequests = append(zipRequests, zipRequest{
				fn: func(ctx context.Context) (interface{}, error) {
					return status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
				},
				pathName: prefix + problemRangesName,
			})
		} else {
			zr.info("skipping %s due to file filters", problemRangesFile)
		}

	}
	return zipRequests
}

// collectClusterData runs the data collection that only needs to
// occur once for the entire cluster.
func (zc *debugZipContext) collectClusterData(
	ctx context.Context,
) (
	nodesList *serverpb.NodesListResponse,
	nodesListRedacted *serverpb.NodesListResponse,
	livenessByNodeID nodeLivenesses,
	err error,
) {
	clusterWideZipRequests := makeClusterWideZipRequests(zc.clusterPrinter, zc.admin, zc.status, zc.prefix)

	for _, r := range clusterWideZipRequests {
		if err := zc.runZipRequest(ctx, zc.clusterPrinter, r); err != nil {
			return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
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
		return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
	}
	if err := queryAndDumpTables(zipSystemTables); err != nil {
		return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
	}

	{
		s := zc.clusterPrinter.start("requesting nodes")

		var nodesStatus *serverpb.NodesResponse
		err = zc.runZipFn(ctx, s, func(ctx context.Context) error {
			nodesList, err = zc.status.NodesList(ctx, &serverpb.NodesListRequest{})
			if err != nil {
				return err
			}
			nodesStatus, err = zc.status.Nodes(ctx, &serverpb.NodesRequest{Redact: zipCtx.redact})
			if err != nil {
				return err
			}
			nodesListRedacted, err = zc.status.NodesList(ctx, &serverpb.NodesListRequest{Redact: zipCtx.redact})
			return err
		})

		if zipCtx.files.shouldIncludeFile(nodesFile) {
			if code := status.Code(errors.Cause(err)); code == codes.Unimplemented {
				// running on non system tenant, use data from redacted NodesList()
				if cErr := zc.z.createJSONOrError(s, debugBase+"/"+nodesFile, nodesListRedacted, err); cErr != nil {
					return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, cErr
				}
			} else {
				if cErr := zc.z.createJSONOrError(s, debugBase+"/"+nodesFile, nodesStatus, err); cErr != nil {
					return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, cErr
				}
			}
		} else {
			s.info("skipping %s due to file filters", nodesFile)
		}

		if nodesList == nil {
			// In case the NodesList() RPC failed), we still want to inspect the
			// per-node endpoints on the head node.
			s = zc.clusterPrinter.start("retrieving the node status")
			firstNodeDetails, err := zc.status.Details(ctx, &serverpb.DetailsRequest{NodeId: "local", Redact: zipCtx.redact})
			if err != nil {
				return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
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
		if zipCtx.files.shouldIncludeFile(livenessFile) {
			if cErr := zc.z.createJSONOrError(s, zc.prefix+"/"+livenessFile, nodes, err); cErr != nil {
				return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, cErr
			}
		} else {
			s.info("skipping %s due to file filters", livenessFile)
		}

		livenessByNodeID = map[roachpb.NodeID]livenesspb.NodeLivenessStatus{}
		if lresponse != nil {
			livenessByNodeID = lresponse.Statuses
		}
	}

	err = zc.getTenantRange(ctx)
	if err != nil {
		return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
	}

	err = zc.getJobTraces(ctx)
	if err != nil {
		return &serverpb.NodesListResponse{}, &serverpb.NodesListResponse{}, nil, err
	}

	return nodesList, nodesListRedacted, livenessByNodeID, nil
}

func (zc *debugZipContext) getTenantRange(ctx context.Context) error {
	if zipCtx.includeRangeInfo {
		var tenantRanges *serverpb.TenantRangesResponse
		s := zc.clusterPrinter.start("requesting tenant ranges")
		if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			var err error
			tenantRanges, err = zc.status.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
			return err
		}); requestErr != nil {
			if err := zc.z.createError(s, zc.prefix+tenantRangesName, requestErr); err != nil {
				return s.fail(err)
			}
		} else {
			s.done()
			rangesFound := 0
			for locality, rangeList := range tenantRanges.RangesByLocality {
				rangesFound += len(rangeList.Ranges)
				sort.Slice(rangeList.Ranges, func(i, j int) bool {
					return rangeList.Ranges[i].RangeID > rangeList.Ranges[j].RangeID
				})
				sLocality := zc.clusterPrinter.start(redact.Sprintf("writing tenant ranges for locality: %s", locality))
				name := fmt.Sprintf("%s/%s/%s", zc.prefix, tenantRangesName, locality)
				if !zipCtx.files.shouldIncludeFile(locality + ".json") {
					s.info("skipping tenant ranges for locality %s due to file filters", locality)
					continue
				}
				s := zc.clusterPrinter.start(redact.Sprintf("writing tenant ranges for locality %s", locality))
				if err := zc.z.createJSON(s, name+".json", rangeList.Ranges); err != nil {
					return s.fail(err)
				}
				sLocality.done()
			}
			zc.clusterPrinter.info("%d tenant ranges found", rangesFound)
		}
	}
	return nil
}

func (zc *debugZipContext) getJobTraces(ctx context.Context) error {
	if zipCtx.includeRunningJobTraces {
		s := zc.clusterPrinter.start("collecting the inflight traces for jobs")
		if requestErr := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			return zc.dumpTraceableJobTraces(ctx)
		}); requestErr != nil {
			if err := zc.z.createError(s, zc.prefix+"/jobs", requestErr); err != nil {
				return s.fail(err)
			}
		} else {
			s.done()
		}
	}
	return nil
}
