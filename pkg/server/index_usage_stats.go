// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
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

	fetchIndexUsageStats := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
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
	if err := iterateNodes(ctx,
		s.serverIterator, s.stopper,
		"requesting index usage stats",
		noTimeout,
		s.dialNode,
		fetchIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	// Append last reset time.
	resp.LastReset = s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().GetClusterLastReset()

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

// ResetIndexUsageStats is the gRPC handler for resetting index usage stats.
// This endpoint resets index usage statistics for all tables.
func (s *statusServer) ResetIndexUsageStats(
	ctx context.Context, req *serverpb.ResetIndexUsageStatsRequest,
) (*serverpb.ResetIndexUsageStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireRepairClusterPermission(ctx); err != nil {
		return nil, err
	}

	var clusterResetStartTime time.Time
	// If the reset time is empty in the request, set the reset time to the
	// current time. Otherwise, use the reset time in the request. This
	// conditional allows us to propagate a single reset time value to all nodes.
	// The propagated reset time represents the time at which the reset was
	// requested.
	if req.ClusterResetStartTime.IsZero() {
		clusterResetStartTime = timeutil.Now()
	} else {
		clusterResetStartTime = req.ClusterResetStartTime
	}

	localReq := &serverpb.ResetIndexUsageStatsRequest{
		NodeID:                "local",
		ClusterResetStartTime: clusterResetStartTime,
	}
	resp := &serverpb.ResetIndexUsageStatsResponse{}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if local {
			s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics().Reset(clusterResetStartTime)
			return resp, nil
		}

		statusClient, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}

		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	resetIndexUsageStats := func(ctx context.Context, statusClient serverpb.StatusClient, _ roachpb.NodeID) (interface{}, error) {
		return statusClient.ResetIndexUsageStats(ctx, localReq)
	}

	aggFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		// Nothing to do here.
	}

	var combinedError error
	errFn := func(_ roachpb.NodeID, nodeFnError error) {
		combinedError = errors.CombineErrors(combinedError, nodeFnError)
	}

	if err := iterateNodes(ctx,
		s.serverIterator, s.stopper,
		"Resetting index usage stats",
		noTimeout,
		s.dialNode,
		resetIndexUsageStats, aggFn, errFn); err != nil {
		return nil, err
	}

	return resp, nil
}

// TableIndexStats is the gRPC handler for retrieving index usage statistics
// by table. This function reads index usage statistics directly from the
// database and is meant for external usage.
func (s *statusServer) TableIndexStats(
	ctx context.Context, req *serverpb.TableIndexStatsRequest,
) (*serverpb.TableIndexStatsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}
	return getTableIndexUsageStats(ctx, req, s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics(),
		s.sqlServer.internalExecutor, s.st, s.sqlServer.execCfg)
}

// getTableIndexUsageStats is a helper function that reads the indexes
// and their usage stats for a given database and table. This is meant
// for external usages e.g. front-end.
func getTableIndexUsageStats(
	ctx context.Context,
	req *serverpb.TableIndexStatsRequest,
	idxUsageStatsProvider *idxusage.LocalIndexUsageStats,
	ie isql.Executor,
	st *cluster.Settings,
	execConfig *sql.ExecutorConfig,
) (*serverpb.TableIndexStatsResponse, error) {
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, err
	}

	tableID, dbID, err := getIDFromDatabaseAndTableName(ctx, req.Database, req.Table, ie, userName)

	if err != nil {
		return nil, err
	}

	q := safesql.NewQuery()
	// TODO(#72930): Implement virtual indexes on index_usages_statistics and table_indexes
	q.Append(`
		SELECT
			ti.index_id,
			ti.index_name,
			ti.index_type,
			total_reads,
			last_read,
			indexdef,
			ti.created_at,
			ti.is_unique
		FROM crdb_internal.index_usage_statistics AS us
  	JOIN crdb_internal.table_indexes AS ti ON us.index_id = ti.index_id 
		AND us.table_id = ti.descriptor_id
  	JOIN pg_catalog.pg_index AS pgidx ON indrelid = us.table_id
  	JOIN pg_catalog.pg_indexes AS pgidxs ON pgidxs.crdb_oid = indexrelid
		AND indexname = ti.index_name
 		WHERE ti.descriptor_id = $::REGCLASS`,
		tableID,
	)
	it, err := ie.QueryIteratorEx(ctx, "index-usage-stats", nil,
		sessiondata.InternalExecutorOverride{
			User:     userName,
			Database: req.Database,
		}, q.String(), q.QueryArguments()...)

	if err != nil {
		return nil, err
	}

	var idxUsageStats []*serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics
	var idxRecommendations []*serverpb.IndexRecommendation
	var ok bool

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()

	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()

		indexID := tree.MustBeDInt(row[0])
		indexName := tree.MustBeDString(row[1])
		indexType := tree.MustBeDString(row[2])
		totalReads := uint64(tree.MustBeDInt(row[3]))
		lastRead := time.Time{}
		if row[4] != tree.DNull {
			lastRead = tree.MustBeDTimestampTZ(row[4]).Time
		}
		createStmt := tree.MustBeDString(row[5])
		var createdAt *time.Time
		if row[6] != tree.DNull {
			ts := tree.MustBeDTimestamp(row[6])
			createdAt = &ts.Time
		}
		isUnique := tree.MustBeDBool(row[7])

		if err != nil {
			return nil, err
		}

		idxStatsRow := &serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics{
			Statistics: &roachpb.CollectedIndexUsageStatistics{
				Key: roachpb.IndexUsageKey{
					TableID: roachpb.TableID(tableID),
					IndexID: roachpb.IndexID(indexID),
				},
				Stats: roachpb.IndexUsageStatistics{
					TotalReadCount: totalReads,
					LastRead:       lastRead,
				},
			},
			IndexName:       string(indexName),
			IndexType:       string(indexType),
			CreateStatement: string(createStmt),
			CreatedAt:       createdAt,
		}

		statsRow := idxusage.IndexStatsRow{
			TableID:          idxStatsRow.Statistics.Key.TableID,
			IndexID:          idxStatsRow.Statistics.Key.IndexID,
			CreatedAt:        idxStatsRow.CreatedAt,
			LastRead:         idxStatsRow.Statistics.Stats.LastRead,
			IndexType:        idxStatsRow.IndexType,
			IsUnique:         bool(isUnique),
			UnusedIndexKnobs: execConfig.UnusedIndexRecommendationsKnobs,
		}
		recommendations := statsRow.GetRecommendationsFromIndexStats(req.Database, st)
		idxRecommendations = append(idxRecommendations, recommendations...)
		idxUsageStats = append(idxUsageStats, idxStatsRow)
	}

	lastReset := idxUsageStatsProvider.GetClusterLastReset()

	resp := &serverpb.TableIndexStatsResponse{
		Statistics:           idxUsageStats,
		LastReset:            &lastReset,
		IndexRecommendations: idxRecommendations,
		DatabaseID:           int32(dbID),
	}

	return resp, nil
}

// getIDFromDatabaseAndTableName is a helper function that retrieves
// the tableID given the database and table name. The tablename must be of
// the form schema.table if a schema exists.
func getIDFromDatabaseAndTableName(
	ctx context.Context,
	database string,
	table string,
	ie isql.Executor,
	userName username.SQLUsername,
) (tableID, databaseID int, err error) {
	// Fully qualified table name is either database.table or database.schema.table
	fqtName, err := getFullyQualifiedTableName(database, table)
	if err != nil {
		return 0, 0, err
	}

	row, err := ie.QueryRowEx(
		ctx, "get-table-id", nil,
		sessiondata.InternalExecutorOverride{User: userName, Database: database},
		"SELECT $1::regclass::oid, crdb_internal.get_database_id($2)", table, database,
	)
	if err != nil {
		return 0, 0, err
	}
	if row == nil {
		return 0, 0, errors.Newf("expected to find table ID for table %s, but found nothing", fqtName)
	}
	tableID = int(tree.MustBeDOid(row[0]).Oid)
	databaseID = int(tree.MustBeDInt(row[1]))
	return tableID, databaseID, nil
}

func getDatabaseIndexRecommendations(
	ctx context.Context,
	dbName string,
	ie isql.Executor,
	st *cluster.Settings,
	knobs *idxusage.UnusedIndexRecommendationTestingKnobs,
) ([]*serverpb.IndexRecommendation, error) {

	// Omit fetching index recommendations for the 'system' database.
	if dbName == catconstants.SystemDatabaseName {
		return []*serverpb.IndexRecommendation{}, nil
	}

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return []*serverpb.IndexRecommendation{}, err
	}

	escDBName := tree.NameString(dbName)
	query := fmt.Sprintf(`
		SELECT
			ti.descriptor_id as table_id,
			ti.index_id,
			ti.index_type,
			last_read,
			ti.created_at,
			ti.is_unique
		FROM %[1]s.crdb_internal.index_usage_statistics AS us
		 JOIN %[1]s.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id AND index_type = 'secondary')
		 JOIN %[1]s.crdb_internal.tables AS t ON (ti.descriptor_id = t.table_id AND t.database_name != 'system');`, escDBName)

	it, err := ie.QueryIteratorEx(ctx, "db-index-recommendations", nil,
		sessiondata.InternalExecutorOverride{
			User:     userName,
			Database: dbName,
		}, query)

	if err != nil {
		return []*serverpb.IndexRecommendation{}, err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { err = errors.CombineErrors(err, it.Close()) }()

	var ok bool
	var idxRecommendations []*serverpb.IndexRecommendation

	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()

		tableID := tree.MustBeDInt(row[0])
		indexID := tree.MustBeDInt(row[1])
		indexType := tree.MustBeDString(row[2])
		lastRead := time.Time{}
		if row[3] != tree.DNull {
			lastRead = tree.MustBeDTimestampTZ(row[3]).Time
		}
		var createdAt *time.Time
		if row[4] != tree.DNull {
			ts := tree.MustBeDTimestamp(row[4])
			createdAt = &ts.Time
		}
		isUnique := tree.MustBeDBool(row[5])

		if err != nil {
			return []*serverpb.IndexRecommendation{}, err
		}

		statsRow := idxusage.IndexStatsRow{
			TableID:          roachpb.TableID(tableID),
			IndexID:          roachpb.IndexID(indexID),
			CreatedAt:        createdAt,
			LastRead:         lastRead,
			IndexType:        string(indexType),
			IsUnique:         bool(isUnique),
			UnusedIndexKnobs: knobs,
		}
		recommendations := statsRow.GetRecommendationsFromIndexStats(dbName, st)
		idxRecommendations = append(idxRecommendations, recommendations...)
	}
	return idxRecommendations, nil
}
