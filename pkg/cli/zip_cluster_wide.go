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
	"bytes"
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

	"crdb_internal.jobs",
	"system.jobs",       // get the raw, restorable jobs records too.
	"system.descriptor", // descriptors also contain job-like mutation state.
	"system.namespace",
	"system.scheduled_jobs",

	"crdb_internal.kv_node_liveness",
	"crdb_internal.kv_node_status",
	"crdb_internal.kv_store_status",

	"crdb_internal.schema_changes",
	"crdb_internal.partitions",
	"crdb_internal.zones",
	"crdb_internal.invalid_objects",
}

// collectClusterData runs the data collection that only needs to
// occur once for the entire cluster.
// Also see collectSchemaData below.
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

// collectSchemaData collects the SQL logical schema once, for the entire cluster
// using the first node. This runs at the end, after all the per-node queries have
// been completed, because it has a higher likelihood to fail.
func (zc *debugZipContext) collectSchemaData(ctx context.Context) error {
	// Run the debug doctor code over the schema.
	{
		var doctorData bytes.Buffer
		s := zc.clusterPrinter.start("doctor examining cluster...")
		descs, ns, jobs, doctorErr := fromCluster(zc.firstNodeSQLConn, zc.timeout)
		if doctorErr == nil {
			doctorErr = runDoctor("examine", descs, ns, jobs, &doctorData)
		}
		if err := zc.z.createRawOrError(s, reportsPrefix+"/doctor.txt", doctorData.Bytes(), doctorErr); err != nil {
			return err
		}
	}

	// Collect the SQL schema.
	{
		var databases *serverpb.DatabasesResponse
		s := zc.clusterPrinter.start("requesting list of SQL databases")
		if err := zc.runZipFn(ctx, s, func(ctx context.Context) error {
			var err error
			databases, err = zc.admin.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}); err != nil {
			if err := zc.z.createError(s, schemaPrefix, err); err != nil {
				return err
			}
		} else {
			s.done()
			zc.clusterPrinter.info("%d databases found", len(databases.Databases))
			var dbEscaper fileNameEscaper
			for _, dbName := range databases.Databases {
				dbPrinter := zc.clusterPrinter.withPrefix("database: %s", dbName)

				prefix := schemaPrefix + "/" + dbEscaper.escape(dbName)
				var database *serverpb.DatabaseDetailsResponse
				s := dbPrinter.start("requesting database details")
				requestErr := zc.runZipFn(ctx, s,
					func(ctx context.Context) error {
						var err error
						database, err = zc.admin.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{Database: dbName})
						return err
					})
				if err := zc.z.createJSONOrError(s, prefix+"@details.json", database, requestErr); err != nil {
					return err
				}
				if requestErr != nil {
					continue
				}

				dbPrinter.info("%d tables found", len(database.TableNames))
				var tbEscaper fileNameEscaper
				for _, tableName := range database.TableNames {
					tbPrinter := dbPrinter.withPrefix("table: %s", tableName)
					name := prefix + "/" + tbEscaper.escape(tableName)
					var table *serverpb.TableDetailsResponse
					s := tbPrinter.start("requesting table details")
					requestErr := zc.runZipFn(ctx, s,
						func(ctx context.Context) error {
							var err error
							table, err = zc.admin.TableDetails(ctx, &serverpb.TableDetailsRequest{Database: dbName, Table: tableName})
							return err
						})
					if err := zc.z.createJSONOrError(s, name+".json", table, requestErr); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
