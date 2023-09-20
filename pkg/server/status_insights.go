// Copyright 2023 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// StatementExecutionInsights requests statement insights that satisfy specified
// parameters in request payload if any provided.
func (s *statusServer) StatementExecutionInsights(
	ctx context.Context, req *serverpb.StatementExecutionInsightsRequest,
) (*serverpb.StatementExecutionInsightsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}
	// Validate that either both start and end time are defined or not.
	if (req.StartTime != nil && req.EndTime == nil) || (req.EndTime != nil && req.StartTime == nil) {
		return nil, errors.New("required that both StartTime and EndTime to be set")
	}

	resp := &serverpb.StatementExecutionInsightsResponse{
		StatementInsights: make([]*serverpb.StatementExecutionInsightsResponse_StatementInsight, 0),
	}

	// Get contention events and group them per transaction and statement IDs
	contentionEventsQuery := `
	SELECT
		collection_ts,
		blocking_txn_id,
		blocking_txn_fingerprint_id,
		waiting_txn_id,
		waiting_txn_fingerprint_id,
		contention_duration,
		contending_key,
		contending_pretty_key,
		waiting_stmt_id,
		waiting_stmt_fingerprint_id,
		database_name,
		schema_name,
		table_name,
		index_name
	FROM crdb_internal.transaction_contention_events`
	iterContEvents, err := s.internalExecutor.QueryIteratorEx(
		ctx,
		"select-contention-events",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		contentionEventsQuery,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = iterContEvents.Close()
	}()
	stmtIDToContentionMap := make(map[clusterunique.ID][]*insights.ContentionEvent)
	txnIDToContentionMap := make(map[uuid.UUID][]*insights.ContentionEvent)
	for ok, innerErr := iterContEvents.Next(ctx); ok; ok, err = iterContEvents.Next(ctx) {
		if innerErr != nil {
			return nil, innerErr
		}
		row := iterContEvents.Cur()
		if row == nil {
			return nil, errors.New("unexpected null row on crdb_internal.transaction_contention_events")
		}
		event, err := decodeInMemoryContentionEventRow(row)
		if err != nil {
			return nil, err
		}
		stmtIDToContentionMap[*event.WaitingStmtID] = append(stmtIDToContentionMap[*event.WaitingStmtID], event)
		txnIDToContentionMap[*event.WaitingTxnID] = append(txnIDToContentionMap[*event.WaitingTxnID], event)
	}

	// Request in-memory statement insights in case persisted data is incomplete.
	inMemoryStmtInsightsQuery := strings.Builder{}
	inMemoryStmtInsightsQuery.WriteString(`
	SELECT
		session_id,
		txn_id,
		txn_fingerprint_id,
		stmt_id,
		stmt_fingerprint_id,
		problem,
		causes,
		query,
		status,
		start_time,
		end_time,
		full_scan,
		user_name,
		app_name,
		database_name,
		plan_gist,
		rows_read,
		rows_written,
		priority,
		retries,
		last_retry_reason,
		exec_node_ids,
		contention,
		index_recommendations,
		implicit_txn,
		cpu_sql_nanos,
  		error_code,
		latency_in_seconds
	FROM crdb_internal.cluster_execution_insights
	WHERE true
	`)
	var args []interface{}
	if req.StatementID != nil {
		args = append(args, req.StatementID.String())
		inMemoryStmtInsightsQuery.WriteString(fmt.Sprintf("AND stmt_id = $%d", len(args)))
	}
	if req.StmtFingerprintID != appstatspb.StmtFingerprintID(0) {
		args = append(args, req.StmtFingerprintID.Encode())
		inMemoryStmtInsightsQuery.WriteString(fmt.Sprintf("AND stmt_fingerprint_id = $%d", len(args)))
	}
	if req.StartTime != nil {
		args = append(args, req.StartTime)
		inMemoryStmtInsightsQuery.WriteString(fmt.Sprintf("AND start_time = $%d", len(args)))
	}
	if req.EndTime != nil {
		args = append(args, req.EndTime)
		inMemoryStmtInsightsQuery.WriteString(fmt.Sprintf("AND end_time = $%d", len(args)))
	}
	iter, err := s.internalExecutor.QueryIteratorEx(
		ctx,
		"select-stmt-exec-insights",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		inMemoryStmtInsightsQuery.String(),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = iter.Close()
	}()
	for ok, innerErr1 := iter.Next(ctx); ok; ok, err = iter.Next(ctx) {
		if innerErr1 != nil {
			return nil, innerErr1
		}
		row := iter.Cur()
		if row == nil {
			return nil, errors.New("unexpected null row on crdb_internal.cluster_execution_insights")
		}
		stmtInsight, err := decodeInMemoryStmtInsightRow(row)
		if err != nil {
			return nil, err
		}
		if events, ok := stmtIDToContentionMap[stmtInsight.Statement.ID]; ok {
			stmtInsight.Statement.ContentionEvents = events
		}
		resp.StatementInsights = append(resp.StatementInsights, stmtInsight)
	}
	return resp, nil
}

func decodeInMemoryStmtInsightRow(datum tree.Datums) (*serverpb.StatementExecutionInsightsResponse_StatementInsight, error) {
	sessionID, err := clusterunique.IDFromString(string(tree.MustBeDString(datum[0]))) // session_id
	if err != nil {
		return nil, err
	}
	txnID := tree.MustBeDUuid(datum[1]).UUID                                                                                          // txn_id
	txnFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.TransactionFingerprintID]([]byte(tree.MustBeDBytes(datum[2]))) // txn_fingerprint_id
	if err != nil {
		return nil, err
	}
	stmtID, err := clusterunique.IDFromString(string(tree.MustBeDString(datum[3]))) // stmt_id
	if err != nil {
		return nil, err
	}
	stmtFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.StmtFingerprintID]([]byte(tree.MustBeDBytes(datum[4]))) // stmt_fingerprint_id
	if err != nil {
		return nil, err
	}
	problem := insights.Problem(insights.Problem_value[string(tree.MustBeDString(datum[5]))]) // problem
	dCauses := tree.MustBeDArray(datum[6]).Array                                              // causes
	causes := make([]insights.Cause, dCauses.Len())
	for i, d := range dCauses {
		causes[i] = insights.Cause(insights.Cause_value[string(tree.MustBeDString(d))])
	}
	stmtQuery := string(tree.MustBeDString(datum[7]))                                                              // query
	stmtStatus := insights.Statement_Status(insights.Statement_Status_value[string(tree.MustBeDString(datum[8]))]) // status
	startTime := tree.MustBeDTimestamp(datum[9]).Time                                                              // start_time
	endTime := tree.MustBeDTimestamp(datum[10]).Time                                                               // end_time
	fullScan := bool(tree.MustBeDBool(datum[11]))                                                                  // full_scan
	userName := string(tree.MustBeDString(datum[12]))                                                              // user_name
	appName := string(tree.MustBeDString(datum[13]))                                                               // app_name
	dbName := string(tree.MustBeDString(datum[14]))                                                                // database_name
	planGist := string(tree.MustBeDString(datum[15]))                                                              // plan_gist
	rowsRead := int64(tree.MustBeDInt(datum[16]))                                                                  // rows_read
	rowsWritten := int64(tree.MustBeDInt(datum[17]))                                                               // rows_written
	priority := string(tree.MustBeDString(datum[18]))                                                              // priority
	retries := int64(tree.MustBeDInt(datum[19]))                                                                   // retries
	retryReason, _ := tree.AsDString(datum[20])                                                                    // last_retry_reason
	execNodes := tree.MustBeDArray(datum[21])                                                                      // exec_node_ids
	nodes := make([]int64, execNodes.Len())
	for i, n := range execNodes.Array {
		nodes[i] = int64(tree.MustBeDInt(n))
	}
	var contentionTime time.Duration
	if c, ok := tree.AsDInterval(datum[22]); ok { // contention
		if durationInSeconds, ok := c.AsInt64(); ok {
			contentionTime = time.Duration(durationInSeconds) * time.Second
		}
	}
	dIndexRecommendations := tree.MustBeDArray(datum[23]) // index_recommendations
	indexRecommendations := make([]string, dIndexRecommendations.Len())
	for i, d := range dIndexRecommendations.Array {
		indexRecommendations[i] = string(tree.MustBeDString(d))
	}
	implicitTxn := bool(tree.MustBeDBool(datum[24])) // implicit_txn
	cpuUsageNanos, _ := tree.AsDInt(datum[25])       // cpu_sql_nanos
	errorCode, _ := tree.AsDString(datum[26])        // error_code
	serviceLatSeconds, _ := tree.AsDFloat(datum[27]) // service_lat_seconds

	statement := &insights.Statement{
		ID:                   stmtID,
		FingerprintID:        *stmtFingerprintID,
		LatencyInSeconds:     float64(*serviceLatSeconds),
		Query:                stmtQuery,
		Status:               stmtStatus,
		StartTime:            startTime,
		EndTime:              endTime,
		FullScan:             fullScan,
		Database:             dbName,
		PlanGist:             planGist,
		RowsRead:             rowsRead,
		RowsWritten:          rowsWritten,
		Retries:              retries,
		AutoRetryReason:      string(retryReason),
		Nodes:                nodes,
		Contention:           &contentionTime,
		IndexRecommendations: indexRecommendations,
		Problem:              problem,
		Causes:               causes,
		CPUSQLNanos:          int64(cpuUsageNanos),
		ErrorCode:            string(errorCode),
	}
	return &serverpb.StatementExecutionInsightsResponse_StatementInsight{
		SessionID:        sessionID,
		TransactionID:    txnID,
		TxnFingerprintID: *txnFingerprintID,
		Statement:        statement,
		UserPriority:     priority,
		User:             userName,
		ApplicationName:  appName,
		ImplicitTxn:      implicitTxn,
	}, nil
}

func decodeInMemoryContentionEventRow(datum tree.Datums) (*insights.ContentionEvent, error) {
	collectionTs := tree.MustBeDTimestampTZ(datum[0]).Time                                                                                    // collection_ts
	blockingTxnID := tree.MustBeDUuid(datum[1]).UUID                                                                                          // blocking_txn_id
	blockingTxnFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.TransactionFingerprintID]([]byte(tree.MustBeDBytes(datum[2]))) // blocking_txn_fingerprint_id
	if err != nil {
		return nil, err
	}
	waitingTxnID := tree.MustBeDUuid(datum[3]).UUID                                                                                          // waiting_txn_id
	waitingTxnFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.TransactionFingerprintID]([]byte(tree.MustBeDBytes(datum[4]))) // waiting_txn_fingerprint_id
	if err != nil {
		return nil, err
	}
	var contentionTime time.Duration
	if c, ok := tree.MustBeDInterval(datum[5]).AsInt64(); ok { // contention_duration
		contentionTime = time.Duration(c) * time.Second
	}
	contendingKey := []byte(tree.MustBeDBytes(datum[6]))        // contending_key
	contendingPrettyKey := string(tree.MustBeDString(datum[7])) // contending_pretty_key

	waitingStmtID, err := clusterunique.IDFromString(string(tree.MustBeDString(datum[8]))) // waiting_stmt_id
	if err != nil {
		return nil, err
	}
	waitingStmtFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.StmtFingerprintID]([]byte(tree.MustBeDBytes(datum[9]))) // waiting_stmt_fingerprint_id
	if err != nil {
		return nil, err
	}
	databaseName := string(tree.MustBeDString(datum[10])) // database_name
	schemaName := string(tree.MustBeDString(datum[11]))   // schema_name
	tableName := string(tree.MustBeDString(datum[12]))    // table_name
	indexName := string(tree.MustBeDString(datum[13]))    // index_name

	return &insights.ContentionEvent{
		Key:                      contendingKey,
		PrettyKey:                contendingPrettyKey,
		BlockingTxnID:            blockingTxnID,
		BlockingTxnFingerprintID: *blockingTxnFingerprintID,
		Duration:                 &contentionTime,
		WaitingTxnID:             &waitingTxnID,
		WaitingTxnFingerprintID:  *waitingTxnFingerprintID,
		CollectionTs:             &collectionTs,
		WaitingStmtFingerprintID: *waitingStmtFingerprintID,
		WaitingStmtID:            &waitingStmtID,
		DatabaseName:             databaseName,
		SchemaName:               schemaName,
		IndexName:                indexName,
		TableName:                tableName,
	}, nil
}
