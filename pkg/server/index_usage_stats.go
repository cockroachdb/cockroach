// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"time"
)

// IndexUsageStatistics is the GRPC handler for serving index usage statistics.
// If the NodeID in the request payload is left empty, the handler will issue
// a cluster-wide RPC fanout to aggregate all index usage statistics from all
// the nodes. If the NodeID is specified, then the handler will handle the
// request either locally (if the NodeID matches the current node's NodeID) or
// forward it to the correct node.
func (s *statusServer) IndexUsageStatistics(
	ctx context.Context, req *serverpb.IndexUsageStatisticsRequest,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	localReq := &serverpb.IndexUsageStatisticsRequest{
		NodeID: "local",
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			statsReader := s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics()
			return indexUsageStatsLocal(statsReader)
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}

		// We issue a localReq instead of the incoming req to other nodes. This is
		// to instruct other nodes to only return us their node-local stats and
		// do not further propagates the RPC call.
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	fetchIndexUsageStats := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.IndexUsageStatistics(ctx, localReq)
	}

	resp := &serverpb.IndexUsageStatisticsResponse{}
	aggFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		stats := nodeResp.(*serverpb.IndexUsageStatisticsResponse)
		resp.Statistics = append(resp.Statistics, stats.Statistics...)
	}

	var combinedError error
	errFn := func(_ roachpb.NodeID, nodeFnError error) {
		combinedError = errors.CombineErrors(combinedError, nodeFnError)
	}

	// It's unfortunate that we cannot use paginatedIterateNodes here because we
	// need to aggregate all stats before returning. Returning a partial result
	// yields an incorrect result.
	if err := s.iterateNodes(ctx,
		fmt.Sprintf("requesting index usage stats for node %s", req.NodeID),
		dialFn, fetchIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	// Append last reset time.
	resp.LastReset = s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().GetLastReset()

	return resp, nil
}

func indexUsageStatsLocal(
	idxUsageStats *idxusage.LocalIndexUsageStats,
) (*serverpb.IndexUsageStatisticsResponse, error) {
	resp := &serverpb.IndexUsageStatisticsResponse{}
	if err := idxUsageStats.ForEach(idxusage.IteratorOptions{}, func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error {
		resp.Statistics = append(resp.Statistics, roachpb.CollectedIndexUsageStatistics{Key: *key,
			Stats: *value,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	// Append last reset time.
	resp.LastReset = idxUsageStats.GetLastReset()
	return resp, nil
}

func (s *statusServer) TableIndexStats(
	ctx context.Context, req *serverpb.TableIndexStatsRequest,
) (*serverpb.TableIndexStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}
	return getIndexUsageStatsExternal(ctx, req, s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics(),
		s.sqlServer.internalExecutor)
}

func getTableIDFromDatabaseAndTableName(
	ctx context.Context,
	database string,
	table string,
	ie *sql.InternalExecutor,
)(int, error) {
	// Fully qualified table name is either database.table or database.schema.table
	fqtName, err := getFullyQualifiedTableName(database, table)
	names := strings.Split(fqtName, ".")

	q := makeSQLQuery()
	q.Append(`SELECT table_id `)
	q.Append(`FROM crdb_internal.tables `)
	q.Append(`WHERE database_name = $ `, names[0])

	if len(names) == 2 {
		q.Append(`AND name = $`, names[1])
	} else if len(names) == 3 {
		q.Append(`AND schema_name = $ AND name = $`, names[1], names[2])
	} else {
		return 0, errors.Newf("expected array length 2 or 3, received %d", len(names))
	}
	if len(q.Errors()) > 0 {
		return 0, combineAllErrors(q.Errors())
	}

	it, err := ie.QueryIteratorEx(ctx, "get-table-id", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
			Database: database,
		}, q.String(), q.QueryArguments()...)

	if err != nil {
		return 0, err
	}

	_, err = it.Next(ctx)
	if err != nil {
		return 0, err
	}
	row := it.Cur()
	table_id := int(tree.MustBeDInt(row[0]))
	return table_id, nil
}

func getIndexUsageStatsExternal(
	ctx context.Context,
	req *serverpb.TableIndexStatsRequest,
	idxUsageStatsProvider *idxusage.LocalIndexUsageStats,
	ie *sql.InternalExecutor,
) (*serverpb.TableIndexStatsResponse, error) {

	table_id, err := getTableIDFromDatabaseAndTableName(ctx, req.Database, req.Table, ie)

	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		`SELECT
       ti.index_name,
       ti.index_type,
			 total_reads,
			 last_read
       FROM crdb_internal.index_usage_statistics AS us
			 JOIN crdb_internal.table_indexes ti
			 ON us.index_id = ti.index_id
			 AND us.table_id = ti.descriptor_id
       WHERE ti.descriptor_id = %d`,
		   table_id,
		)

	const expectedNumDatums = 4

	it, err := ie.QueryIteratorEx(ctx, "index-usage-stats", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
			Database: req.Database,
		}, query)

	if err != nil {
		return nil, err
	}

	var idxUsageStats []roachpb.CollectedIndexUsageStatistics
	var ok bool

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()

	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums, row.Len())
		}

		index_name := tree.MustBeDString(row[0])
		index_type := tree.MustBeDString(row[1])
		total_reads := uint64(tree.MustBeDInt(row[2]))
		last_read := time.Time{}
		if (row[3] != tree.DNull) {
			last_read = tree.MustBeDTimestampTZ(row[3]).Time
		}

		if err != nil {
			return nil, err
		}

		idxStatsRow := roachpb.CollectedIndexUsageStatistics{
			Stats: roachpb.IndexUsageStatistics{
				TotalReadCount: total_reads,
				LastRead: last_read,
			},
			IndexName: string(index_name),
			IndexType: string(index_type),
		}

		idxUsageStats = append(idxUsageStats, idxStatsRow)
	}

	resp := &serverpb.TableIndexStatsResponse{
		Statistics: idxUsageStats,
		LastReset: idxUsageStatsProvider.GetLastReset(),
	}

	return resp, nil
}

func (s *statusServer) ResetIndexUsageStats(
	ctx context.Context, req *serverpb.ResetIndexUsageStatsRequest,
) (*serverpb.ResetIndexUsageStatsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	localReq := &serverpb.ResetIndexUsageStatsRequest{
		NodeID: "local",
	}
	resp := &serverpb.ResetIndexUsageStatsResponse{}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().Reset()
			return resp, nil
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}

		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}

	resetIndexUsageStats := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		statusClient := client.(serverpb.StatusClient)
		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	aggFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		// Nothing to do here.
	}

	var combinedError error
	errFn := func(_ roachpb.NodeID, nodeFnError error) {
		combinedError = errors.CombineErrors(combinedError, nodeFnError)
	}

	if err := s.iterateNodes(ctx,
		fmt.Sprintf("Resetting index usage stats for node %s", req.NodeID),
		dialFn, resetIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	return resp, nil
}
