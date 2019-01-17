// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	// "google.golang.org/grpc/codes"
	// "google.golang.org/grpc/status"
)

func buildNumericStat(mean, stddev tree.Datum) roachpb.NumericStat {
	var sd float64
	if stddev != tree.DNull {
		sd = float64(*stddev.(*tree.DFloat))
	}
	return roachpb.NumericStat{
		Mean:         float64(*mean.(*tree.DFloat)),
		SquaredDiffs: sd * sd,
	}
}

func (s *statusServer) Statements(
	ctx context.Context, req *serverpb.StatementsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.StatementsResponse{
		Statements: []serverpb.StatementsResponse_CollectedStatementStatistics{},
		LastReset:  timeutil.Now(),
	}

	statsQuery := `
SELECT
    statement_key, node_id, application_name,
    distributed, optimized, has_error,
    sum(automatic_retry_count)::INT AS retries,
    max(automatic_retry_count)::INT AS max_retries,
    count(id)::INT AS first_attempt_count,
    avg(rows_affected)::FLOAT AS rows_affected_mean,
    stddev(rows_affected)::FLOAT AS rows_affected_stddev,
    avg(parse_lat::FLOAT)::FLOAT AS parse_lat_mean,
    stddev(parse_lat::FLOAT)::FLOAT AS parse_lat_stddev,
    avg(plan_lat::FLOAT)::FLOAT AS plan_lat_mean,
    stddev(plan_lat::FLOAT)::FLOAT AS plan_lat_stddev,
    avg(run_lat::FLOAT)::FLOAT AS run_lat_mean,
    stddev(run_lat::FLOAT)::FLOAT AS run_lat_stddev,
    avg(
        service_lat::FLOAT - parse_lat::FLOAT - plan_lat::FLOAT - run_lat::FLOAT
    )::FLOAT AS overhead_lat_mean,
    stddev(
        service_lat::FLOAT - parse_lat::FLOAT - plan_lat::FLOAT - run_lat::FLOAT
    )::FLOAT AS overhead_lat_stddev,
    avg(service_lat::FLOAT)::FLOAT AS service_lat,
    stddev(service_lat::FLOAT)::FLOAT AS service_lat_stddev
FROM (
    SELECT *, (CASE error WHEN '' THEN false ELSE true END) AS has_error
    FROM system.statement_executions
)
GROUP BY statement_key, node_id, application_name, distributed, optimized, has_error
ORDER BY statement_key, node_id, application_name, distributed, optimized, has_error;
`
	rows, _ /* columns */, err := s.admin.server.internalExecutor.Query(
		ctx,
		"query-statement-execution-log",
		nil, /* txn */
		statsQuery,
	)
	if err != nil {
		log.Warningf(ctx, "Unable to query execution log: %v", err)
		return nil, err
	}

	for _, row := range rows {
		nodeID, ok := row[1].(*tree.DInt)
		if !ok {
			continue
		}

		firstAttempts := int64(*row[8].(*tree.DInt))
		retries := int64(*row[6].(*tree.DInt))

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: roachpb.StatementStatisticsKey{
					Query:   string(*row[0].(*tree.DString)),
					App:     string(*row[2].(*tree.DString)),
					DistSQL: bool(*row[3].(*tree.DBool)),
					Failed:  bool(*row[5].(*tree.DBool)),
					Opt:     bool(*row[4].(*tree.DBool)),
				},
				NodeID: roachpb.NodeID(*nodeID),
			},
			Stats: roachpb.StatementStatistics{
				Count:             firstAttempts + retries,
				FirstAttemptCount: firstAttempts,
				MaxRetries:        int64(*row[7].(*tree.DInt)),
				NumRows:           buildNumericStat(row[9], row[10]),
				ParseLat:          buildNumericStat(row[11], row[12]),
				PlanLat:           buildNumericStat(row[13], row[14]),
				RunLat:            buildNumericStat(row[15], row[16]),
				OverheadLat:       buildNumericStat(row[17], row[18]),
				ServiceLat:        buildNumericStat(row[19], row[20]),
			},
		}

		response.Statements = append(response.Statements, stmt)
	}

	/*
		localReq := &serverpb.StatementsRequest{
			NodeID: "local",
		}

		if len(req.NodeID) > 0 {
			requestedNodeID, local, err := s.parseNodeID(req.NodeID)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, err.Error())
			}
			if local {
				return s.StatementsLocal(ctx)
			}
			status, err := s.dialNode(ctx, requestedNodeID)
			if err != nil {
				return nil, err
			}
			return status.Statements(ctx, localReq)
		}

		dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
			client, err := s.dialNode(ctx, nodeID)
			return client, err
		}
		nodeStatement := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
			status := client.(serverpb.StatusClient)
			return status.Statements(ctx, localReq)
		}

		if err := s.iterateNodes(ctx, fmt.Sprintf("statement statistics for node %s", req.NodeID),
			dialFn,
			nodeStatement,
			func(nodeID roachpb.NodeID, resp interface{}) {
				statementsResp := resp.(*serverpb.StatementsResponse)
				response.Statements = append(response.Statements, statementsResp.Statements...)
				if response.LastReset.After(statementsResp.LastReset) {
					response.LastReset = statementsResp.LastReset
				}
			},
			func(nodeID roachpb.NodeID, err error) {
				// TODO(couchand): do something here...
			},
		); err != nil {
			return nil, err
		}
	*/

	return response, nil
}

func (s *statusServer) StatementsLocal(ctx context.Context) (*serverpb.StatementsResponse, error) {
	stmtStats := s.admin.server.pgServer.SQLServer.GetStmtExecutionsAggregated()
	lastReset := s.admin.server.pgServer.SQLServer.GetStmtExecutionsLastReset()

	resp := &serverpb.StatementsResponse{
		Statements: make([]serverpb.StatementsResponse_CollectedStatementStatistics, len(stmtStats)),
		LastReset:  lastReset,
	}

	for i, stmt := range stmtStats {
		resp.Statements[i] = serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: stmt.Key,
				NodeID:  s.gossip.NodeID.Get(),
			},
			Stats: stmt.Stats,
		}
	}

	return resp, nil
}
