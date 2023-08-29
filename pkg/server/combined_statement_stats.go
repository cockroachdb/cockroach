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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func getTimeFromSeconds(seconds int64) *time.Time {
	if seconds != 0 {
		t := timeutil.Unix(seconds, 0)
		return &t
	}
	return nil
}

func closeIterator(it isql.Rows, err error) error {
	if it != nil {
		closeErr := it.Close()
		if closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}
	return err
}

func (s *statusServer) CombinedStatementStats(
	ctx context.Context, req *serverpb.CombinedStatementsStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	return getCombinedStatementStats(
		ctx,
		req,
		s.sqlServer.pgServer.SQLServer.GetSQLStatsProvider(),
		s.internalExecutor,
		s.st,
		s.sqlServer.execCfg.SQLStatsTestingKnobs)
}

func getCombinedStatementStats(
	ctx context.Context,
	req *serverpb.CombinedStatementsStatsRequest,
	statsProvider sqlstats.Provider,
	ie *sql.InternalExecutor,
	settings *cluster.Settings,
	testingKnobs *sqlstats.TestingKnobs,
) (*serverpb.StatementsResponse, error) {
	var err error
	showInternal := SQLStatsShowInternal.Get(&settings.SV)
	whereClause, orderAndLimit, args := getCombinedStatementsQueryClausesAndArgs(
		req, testingKnobs, showInternal, settings)

	// Used for mixed cluster version, where we need to use the persisted view with _v22_2.
	tableSuffix := ""
	if !settings.Version.IsActive(ctx, clusterversion.V23_1AddSQLStatsComputedIndexes) {
		tableSuffix = "_v22_2"
	}
	// Check if the activity tables contains all the data required for the selected period from the request.
	activityHasAllData := false
	reqStartTime := getTimeFromSeconds(req.Start)
	if settings.Version.IsActive(ctx, clusterversion.V23_1AddSystemActivityTables) {
		sort := serverpb.StatsSortOptions_SERVICE_LAT
		if req.FetchMode != nil {
			sort = req.FetchMode.Sort
		}
		activityHasAllData, err = activityTablesHaveFullData(
			ctx,
			ie,
			settings,
			testingKnobs,
			reqStartTime,
			req.Limit,
			sort,
		)

		if err != nil {
			log.Errorf(ctx, "Error on activityTablesHaveFullData: %s", err)
		}
	}

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics

	if req.FetchMode == nil || req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		transactions, err = collectCombinedTransactions(
			ctx,
			ie,
			whereClause,
			args,
			orderAndLimit,
			testingKnobs,
			activityHasAllData,
			tableSuffix)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	if req.FetchMode != nil && req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		// If we're fetching for txns, the client still expects statement stats for
		// stmts in the txns response.
		statements, err = collectStmtsForTxns(
			ctx,
			ie,
			req,
			transactions,
			testingKnobs,
			activityHasAllData,
			tableSuffix)
	} else {
		statements, err = collectCombinedStatements(
			ctx,
			ie,
			whereClause,
			args,
			orderAndLimit,
			testingKnobs,
			activityHasAllData,
			tableSuffix)
	}

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	stmtsRunTime, txnsRunTime, oldestDate, stmtSourceTable, txnSourceTable, err := getSourceStatsInfo(
		ctx,
		req,
		ie,
		testingKnobs,
		activityHasAllData,
		tableSuffix,
		showInternal)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	response := &serverpb.StatementsResponse{
		Statements:                 statements,
		Transactions:               transactions,
		LastReset:                  statsProvider.GetLastReset(),
		InternalAppNamePrefix:      catconstants.InternalAppNamePrefix,
		StmtsTotalRuntimeSecs:      stmtsRunTime,
		TxnsTotalRuntimeSecs:       txnsRunTime,
		OldestAggregatedTsReturned: oldestDate,
		StmtsSourceTable:           stmtSourceTable,
		TxnsSourceTable:            txnSourceTable,
	}

	return response, nil
}

func activityTablesHaveFullData(
	ctx context.Context,
	ie *sql.InternalExecutor,
	settings *cluster.Settings,
	testingKnobs *sqlstats.TestingKnobs,
	reqStartTime *time.Time,
	limit int64,
	order serverpb.StatsSortOptions,
) (result bool, err error) {

	if !StatsActivityUIEnabled.Get(&settings.SV) {
		return false, nil
	}

	if (limit > 0 && !isLimitOnActivityTable(limit)) || !isSortOptionOnActivityTable(order) {
		return false, nil
	}

	if reqStartTime == nil {
		return false, nil
	}

	// Used to verify the table contained data.
	zeroDate := time.Time{}

	const queryWithPlaceholders = `
SELECT 
    COALESCE(min(aggregated_ts), '%s')
FROM crdb_internal.statement_activity 
%s
`

	// Format string "2006-01-02 15:04:05.00" is a golang-specific string
	it, err := ie.QueryIteratorEx(
		ctx,
		"console-combined-stmts-activity-min-ts",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(queryWithPlaceholders, zeroDate.Format("2006-01-02 15:04:05.00"), testingKnobs.GetAOSTClause()))

	if err != nil {
		return false, err
	}
	ok, err := it.Next(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.New("expected one row but got none on activityTablesHaveFullData")
	}

	var row tree.Datums
	if row = it.Cur(); row == nil {
		return false, errors.New("unexpected null row on activityTablesHaveFullData")
	}

	defer func() {
		err = closeIterator(it, err)
	}()

	minAggregatedTs := tree.MustBeDTimestampTZ(row[0]).Time

	hasData := !minAggregatedTs.IsZero() && (reqStartTime.After(minAggregatedTs) || reqStartTime.Equal(minAggregatedTs))
	return hasData, nil
}

// getSourceStatsInfo returns information about the stats returned:
// - the total runtime (in seconds) on the selected period for
// statement and transactions
// - the oldest aggregated_ts we have data for on the
// selected period
// - which table the data was retrieve from
func getSourceStatsInfo(
	ctx context.Context,
	req *serverpb.CombinedStatementsStatsRequest,
	ie *sql.InternalExecutor,
	testingKnobs *sqlstats.TestingKnobs,
	activityTableHasAllData bool,
	tableSuffix string,
	showInternal bool,
) (
	stmtsRuntime float32,
	txnsRuntime float32,
	oldestDate *time.Time,
	stmtSourceTable string,
	txnSourceTable string,
	err error,
) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())
	var args []interface{}
	startTime := getTimeFromSeconds(req.Start)
	endTime := getTimeFromSeconds(req.End)

	buffer.WriteString(" WHERE true")

	if startTime != nil {
		args = append(args, *startTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}

	if endTime != nil {
		args = append(args, *endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	whereClause := buffer.String()

	if !showInternal {
		// Filter out internal statements by app name.
		buffer.WriteString(fmt.Sprintf(
			" AND app_name NOT LIKE '%s%%' AND app_name NOT LIKE '%s%%'",
			catconstants.InternalAppNamePrefix,
			catconstants.DelegatedAppNamePrefix))
	}
	whereClauseOldestDate := buffer.String()

	getRuntime := func(table string) (float32, error) {
		var queryToGetClusterTotalRunTime string
		if activityTableHasAllData {
			queryToGetClusterTotalRunTime = fmt.Sprintf(`
SELECT COALESCE(
         execution_total_cluster_seconds,
       0)
FROM %s %s LIMIT 1`, table, whereClause)
		} else {
			queryToGetClusterTotalRunTime = fmt.Sprintf(`
SELECT COALESCE(
          sum(
             (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
             (statistics -> 'statistics' ->> 'cnt')::FLOAT
          ),
       0)
FROM %s %s`, table, whereClause)
		}

		it, err := ie.QueryIteratorEx(
			ctx,
			fmt.Sprintf(`console-combined-stmts-%s-total-runtime`, table),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			queryToGetClusterTotalRunTime, args...)

		if err != nil {
			return 0, err
		}
		ok, err := it.Next(ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, errors.New("expected one row but got none on getSourceStatsInfo.getRuntime")
		}

		var row tree.Datums
		if row = it.Cur(); row == nil {
			return 0, errors.New("unexpected null row on getSourceStatsInfo.getRuntime")
		}

		defer func() {
			err = closeIterator(it, err)
		}()

		return float32(tree.MustBeDFloat(row[0])), nil
	}

	getOldestDate := func(table string) (*time.Time, error) {
		it, err := ie.QueryIteratorEx(
			ctx,
			fmt.Sprintf(`console-combined-stmts-%s-oldest_date`, table),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`
SELECT min(aggregated_ts)
FROM %s %s`, table, whereClauseOldestDate), args...)

		if err != nil {
			return nil, err
		}
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("expected one row but got none on getSourceStatsInfo.getOldestDate")
		}

		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, nil
		}
		defer func() {
			err = closeIterator(it, err)
		}()

		if row[0] == tree.DNull {
			return nil, nil
		}
		oldestTs := tree.MustBeDTimestampTZ(row[0]).Time
		return &oldestTs, nil
	}

	// We return statement info for both req modes (statements only and transactions only),
	// since statements are also returned for transactions only mode.
	stmtsRuntime = 0
	if activityTableHasAllData {
		stmtSourceTable = "crdb_internal.statement_activity"
		stmtsRuntime, err = getRuntime(stmtSourceTable)
		if err != nil {
			return 0, 0, nil, stmtSourceTable, "", err
		}
		oldestDate, err = getOldestDate(stmtSourceTable)
		if err != nil {
			return stmtsRuntime, 0, nil, stmtSourceTable, "", err
		}
	}
	// If there are no results from the activity table, retrieve the data from the persisted table.
	if stmtsRuntime == 0 {
		stmtSourceTable = "crdb_internal.statement_statistics_persisted" + tableSuffix
		stmtsRuntime, err = getRuntime(stmtSourceTable)
		if err != nil {
			return 0, 0, nil, stmtSourceTable, "", err
		}
		oldestDate, err = getOldestDate(stmtSourceTable)
		if err != nil {
			return stmtsRuntime, 0, nil, stmtSourceTable, "", err
		}
	}
	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if stmtsRuntime == 0 {
		stmtSourceTable = "crdb_internal.statement_statistics"
		stmtsRuntime, err = getRuntime(stmtSourceTable)
		if err != nil {
			return 0, 0, nil, stmtSourceTable, "", err
		}
		oldestDate, err = getOldestDate(stmtSourceTable)
		if err != nil {
			return stmtsRuntime, 0, nil, stmtSourceTable, "", err
		}
	}

	txnsRuntime = 0
	if req.FetchMode == nil || req.FetchMode.StatsType != serverpb.CombinedStatementsStatsRequest_StmtStatsOnly {
		if activityTableHasAllData {
			txnSourceTable = "crdb_internal.transaction_activity"
			txnsRuntime, err = getRuntime(txnSourceTable)
			if err != nil {
				return 0, 0, nil, stmtSourceTable, txnSourceTable, err
			}
		}
		// If there are no results from the activity table, retrieve the data from the persisted table.
		if txnsRuntime == 0 {
			txnSourceTable = "crdb_internal.transaction_statistics_persisted" + tableSuffix
			txnsRuntime, err = getRuntime(txnSourceTable)
			if err != nil {
				return 0, 0, nil, stmtSourceTable, txnSourceTable, err
			}
		}
		// If there are no results from the persisted table, retrieve the data from the combined view
		// with data in-memory.
		if txnsRuntime == 0 {
			txnSourceTable = "crdb_internal.transaction_statistics"
			txnsRuntime, err = getRuntime(txnSourceTable)
			if err != nil {
				return 0, 0, nil, stmtSourceTable, txnSourceTable, err
			}
		}
	}

	return stmtsRuntime, txnsRuntime, oldestDate, stmtSourceTable, txnSourceTable, err
}

// Return true is the limit request is within the limit
// on the Activity tables (500)
func isLimitOnActivityTable(limit int64) bool {
	return limit <= 500
}

func isSortOptionOnActivityTable(sort serverpb.StatsSortOptions) bool {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT,
		serverpb.StatsSortOptions_CPU_TIME,
		serverpb.StatsSortOptions_EXECUTION_COUNT,
		serverpb.StatsSortOptions_P99_STMTS_ONLY,
		serverpb.StatsSortOptions_CONTENTION_TIME,
		serverpb.StatsSortOptions_PCT_RUNTIME:
		return true
	}
	return false
}

const (
	sortSvcLatDesc         = `(statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT DESC`
	sortCPUTimeDesc        = `(statistics -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::FLOAT DESC`
	sortExecCountDesc      = `(statistics -> 'statistics' ->> 'cnt')::INT DESC`
	sortContentionTimeDesc = `(statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::FLOAT DESC`
	sortPCTRuntimeDesc     = `((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
                         (statistics -> 'statistics' ->> 'cnt')::FLOAT) DESC`
	sortLatencyInfoP50Desc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'p50')::FLOAT DESC`
	sortLatencyInfoP90Desc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'p90')::FLOAT DESC`
	sortLatencyInfoP99Desc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'p99')::FLOAT DESC`
	sortLatencyInfoMinDesc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'min')::FLOAT DESC`
	sortLatencyInfoMaxDesc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'max')::FLOAT DESC`
	sortRowsProcessedDesc  = `((statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::FLOAT + 
												 (statistics -> 'statistics' -> 'rowsWritten' ->> 'mean')::FLOAT) DESC`
	sortMaxMemoryDesc = `(statistics -> 'execution_statistics' -> 'maxMemUsage' ->> 'mean')::FLOAT DESC`
	sortNetworkDesc   = `(statistics -> 'execution_statistics' -> 'networkBytes' ->> 'mean')::FLOAT DESC`
	sortRetriesDesc   = `(statistics -> 'statistics' ->> 'maxRetries')::INT DESC`
	sortLastExecDesc  = `(statistics -> 'statistics' ->> 'lastExecAt') DESC`
)

func getStmtColumnFromSortOption(sort serverpb.StatsSortOptions) string {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT:
		return sortSvcLatDesc
	case serverpb.StatsSortOptions_CPU_TIME:
		return sortCPUTimeDesc
	case serverpb.StatsSortOptions_EXECUTION_COUNT:
		return sortExecCountDesc
	case serverpb.StatsSortOptions_P99_STMTS_ONLY:
		return sortLatencyInfoP99Desc
	case serverpb.StatsSortOptions_CONTENTION_TIME:
		return sortContentionTimeDesc
	case serverpb.StatsSortOptions_LATENCY_INFO_P50:
		return sortLatencyInfoP50Desc
	case serverpb.StatsSortOptions_LATENCY_INFO_P90:
		return sortLatencyInfoP90Desc
	case serverpb.StatsSortOptions_LATENCY_INFO_MIN:
		return sortLatencyInfoMinDesc
	case serverpb.StatsSortOptions_LATENCY_INFO_MAX:
		return sortLatencyInfoMaxDesc
	case serverpb.StatsSortOptions_ROWS_PROCESSED:
		return sortRowsProcessedDesc
	case serverpb.StatsSortOptions_MAX_MEMORY:
		return sortMaxMemoryDesc
	case serverpb.StatsSortOptions_NETWORK:
		return sortNetworkDesc
	case serverpb.StatsSortOptions_RETRIES:
		return sortRetriesDesc
	case serverpb.StatsSortOptions_LAST_EXEC:
		return sortLastExecDesc
	default:
		return sortSvcLatDesc
	}
}

func getTxnColumnFromSortOption(sort serverpb.StatsSortOptions) string {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT:
		return sortSvcLatDesc
	case serverpb.StatsSortOptions_CPU_TIME:
		return sortCPUTimeDesc
	case serverpb.StatsSortOptions_EXECUTION_COUNT:
		return sortExecCountDesc
	case serverpb.StatsSortOptions_CONTENTION_TIME:
		return sortContentionTimeDesc
	case serverpb.StatsSortOptions_PCT_RUNTIME:
		return sortPCTRuntimeDesc
	default:
		return sortSvcLatDesc
	}
}

// buildWhereClauseForStmtsByTxn builds the where clause to get the statement
// stats based on a list of transactions. The list of transactions provided must
// contain no duplicate transaction fingerprint ids.
func buildWhereClauseForStmtsByTxn(
	req *serverpb.CombinedStatementsStatsRequest,
	transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
	testingKnobs *sqlstats.TestingKnobs,
) (whereClause string, args []interface{}) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())

	buffer.WriteString(" WHERE true")

	// Add start and end filters from request.
	startTime := getTimeFromSeconds(req.Start)
	endTime := getTimeFromSeconds(req.End)
	if startTime != nil {
		args = append(args, *startTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}

	if endTime != nil {
		args = append(args, *endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	txnFingerprints := make([]string, 0, len(transactions))
	for i := range transactions {
		fingerprint := uint64(transactions[i].StatsData.TransactionFingerprintID)
		txnFingerprints = append(txnFingerprints, fmt.Sprintf("\\x%016x", fingerprint))
	}

	args = append(args, txnFingerprints)
	buffer.WriteString(fmt.Sprintf(" AND transaction_fingerprint_id = any $%d", len(args)))

	return buffer.String(), args
}

// getCombinedStatementsQueryClausesAndArgs returns:
// - where clause (filtering by name and aggregates_ts when defined)
// - order and limit clause
// - args that will replace the clauses above
// The whereClause will be in the format `WHERE A = $1 AND B = $2` and
// args will return the list of arguments in order that will replace the actual values.
func getCombinedStatementsQueryClausesAndArgs(
	req *serverpb.CombinedStatementsStatsRequest,
	testingKnobs *sqlstats.TestingKnobs,
	showInternal bool,
	settings *cluster.Settings,
) (whereClause string, orderAndLimitClause string, args []interface{}) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())

	if showInternal {
		buffer.WriteString(" WHERE true")
	} else {
		// Filter out internal statements by app name.
		buffer.WriteString(fmt.Sprintf(
			" WHERE app_name NOT LIKE '%s%%' AND app_name NOT LIKE '%s%%'",
			catconstants.InternalAppNamePrefix,
			catconstants.DelegatedAppNamePrefix))
	}

	// Add start and end filters from request.
	startTime := getTimeFromSeconds(req.Start)
	endTime := getTimeFromSeconds(req.End)
	if startTime != nil {
		buffer.WriteString(" AND aggregated_ts >= $1")
		args = append(args, *startTime)
	}

	if endTime != nil {
		args = append(args, *endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	// Add LIMIT from request.
	limit := req.Limit
	if limit == 0 {
		limit = SQLStatsResponseMax.Get(&settings.SV)
	}
	args = append(args, limit)

	// Determine sort column.
	var col string
	if req.FetchMode == nil {
		col = "fingerprint_id"
	} else if req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_StmtStatsOnly {
		col = getStmtColumnFromSortOption(req.FetchMode.Sort)
	} else if req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		col = getTxnColumnFromSortOption(req.FetchMode.Sort)
	}

	orderAndLimitClause = fmt.Sprintf(` ORDER BY %s LIMIT $%d`, col, len(args))

	return buffer.String(), orderAndLimitClause, args
}

func collectCombinedStatements(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	orderAndLimit string,
	testingKnobs *sqlstats.TestingKnobs,
	activityTableHasAllData bool,
	tableSuffix string,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {
	aostClause := testingKnobs.GetAOSTClause()
	const expectedNumDatums = 11
	const queryFormat = `
SELECT 
    fingerprint_id,
    txn_fingerprints,
    app_name,
    aggregated_ts,
    COALESCE(CAST(metadata -> 'distSQLCount' AS INT), 0)  AS distSQLCount,
    COALESCE(CAST(metadata -> 'fullScanCount' AS INT), 0) AS fullScanCount,
    COALESCE(CAST(metadata -> 'failedCount' AS INT), 0)   AS failedCount,
    metadata ->> 'query'                                  AS query,
    metadata ->> 'querySummary'                           AS querySummary,
    (SELECT string_agg(elem::text, ',') 
    FROM json_array_elements_text(metadata->'db') AS elem) AS databases,
    statistics
FROM (SELECT fingerprint_id,
             array_agg(distinct transaction_fingerprint_id)             AS txn_fingerprints,
             app_name,
             max(aggregated_ts)                                         AS aggregated_ts,
             crdb_internal.merge_stats_metadata(array_agg(metadata))    AS metadata,
             crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
      FROM %s %s
      GROUP BY
          fingerprint_id,
          app_name) %s
%s`

	var it isql.Rows
	var err error
	defer func() {
		err = closeIterator(it, err)
	}()

	if activityTableHasAllData {
		it, err = getIterator(
			ctx,
			ie,
			// The statement activity table has aggregated metadata.
			`
SELECT 
    fingerprint_id,
    txn_fingerprints,
    app_name,
    aggregated_ts,
    COALESCE(CAST(metadata -> 'distSQLCount' AS INT), 0)  AS distSQLCount,
    COALESCE(CAST(metadata -> 'fullScanCount' AS INT), 0) AS fullScanCount,
    COALESCE(CAST(metadata -> 'failedCount' AS INT), 0)   AS failedCount,
    metadata ->> 'query'                                  AS query,
    metadata ->> 'querySummary'                           AS querySummary,
    (SELECT string_agg(elem::text, ',') 
    FROM json_array_elements_text(metadata->'db') AS elem) AS databases,
    statistics
FROM (SELECT fingerprint_id,
             array_agg(distinct transaction_fingerprint_id)                    AS txn_fingerprints,
             app_name,
             max(aggregated_ts)                                                AS aggregated_ts,
             crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata)) AS metadata,
             crdb_internal.merge_statement_stats(array_agg(statistics))        AS statistics
      FROM %s %s
      GROUP BY
          fingerprint_id,
          app_name) %s
%s`,
			"crdb_internal.statement_activity",
			"combined-stmts-activity-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the activity table, retrieve the data from the persisted table.
	if it == nil || !it.HasResults() {
		if it != nil {
			err = closeIterator(it, err)
		}
		it, err = getIterator(
			ctx,
			ie,
			queryFormat,
			"crdb_internal.statement_statistics_persisted"+tableSuffix,
			"combined-stmts-persisted-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		it, err = getIterator(
			ctx,
			ie,
			queryFormat,
			"crdb_internal.statement_statistics",
			"combined-stmts-with-memory-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row on collectCombinedStatements")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns on collectCombinedStatements, received %d", expectedNumDatums)
		}

		var statementFingerprintID uint64
		if statementFingerprintID, err = sqlstatsutil.DatumToUint64(row[0]); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		var txnFingerprintID uint64
		txnFingerprintDatums := tree.MustBeDArray(row[1])
		txnFingerprintIDs := make([]appstatspb.TransactionFingerprintID, 0, txnFingerprintDatums.Array.Len())
		for _, idDatum := range txnFingerprintDatums.Array {
			if txnFingerprintID, err = sqlstatsutil.DatumToUint64(idDatum); err != nil {
				return nil, srverrors.ServerError(ctx, err)
			}
			txnFingerprintIDs = append(txnFingerprintIDs, appstatspb.TransactionFingerprintID(txnFingerprintID))
		}

		app := string(tree.MustBeDString(row[2]))

		aggregatedTs := tree.MustBeDTimestampTZ(row[3]).Time
		distSQLCount := int64(*row[4].(*tree.DInt))
		fullScanCount := int64(*row[5].(*tree.DInt))
		failedCount := int64(*row[6].(*tree.DInt))
		query := string(tree.MustBeDString(row[7]))
		querySummary := string(tree.MustBeDString(row[8]))
		databases := string(tree.MustBeDString(row[9]))

		metadata := appstatspb.CollectedStatementStatistics{
			Key: appstatspb.StatementStatisticsKey{
				App:          app,
				DistSQL:      distSQLCount > 0,
				FullScan:     fullScanCount > 0,
				Failed:       failedCount > 0,
				Query:        query,
				QuerySummary: querySummary,
				Database:     databases,
			},
		}

		var stats appstatspb.StatementStatistics
		statsJSON := tree.MustBeDJSON(row[10]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &stats); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData:      metadata.Key,
				AggregatedTs: aggregatedTs,
			},
			ID:                appstatspb.StmtFingerprintID(statementFingerprintID),
			Stats:             stats,
			TxnFingerprintIDs: txnFingerprintIDs,
		}

		statements = append(statements, stmt)
	}

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return statements, nil
}

func getIterator(
	ctx context.Context,
	ie *sql.InternalExecutor,
	queryFormat string,
	table string,
	queryInfo string,
	whereClause string,
	args []interface{},
	aostClause string,
	orderAndLimit string,
) (isql.Rows, error) {

	query := fmt.Sprintf(
		queryFormat,
		table,
		whereClause,
		aostClause,
		orderAndLimit)
	opName := fmt.Sprintf(`console-combined-stmts-%s`, queryInfo)

	it, err := ie.QueryIteratorEx(ctx, opName, nil,
		sessiondata.NodeUserSessionDataOverride, query, args...)
	if err != nil {
		return it, srverrors.ServerError(ctx, err)
	}

	return it, nil
}

func collectCombinedTransactions(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	orderAndLimit string,
	testingKnobs *sqlstats.TestingKnobs,
	activityTableHasAllData bool,
	tableSuffix string,
) ([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, error) {
	aostClause := testingKnobs.GetAOSTClause()
	const expectedNumDatums = 5
	const queryFormat = `
SELECT *
FROM (SELECT app_name,
             max(aggregated_ts)                                           AS aggregated_ts,
             fingerprint_id,
             max(metadata),
             crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics
      FROM %s %s
      GROUP BY
          app_name,
          fingerprint_id) %s
%s`

	var it isql.Rows
	var err error
	if activityTableHasAllData {
		it, err = getIterator(
			ctx,
			ie,
			queryFormat,
			"crdb_internal.transaction_activity",
			"combined-txns-activity-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	defer func() {
		err = closeIterator(it, err)
	}()

	// If there are no results from the activity table, retrieve the data from the persisted table.
	if it == nil || !it.HasResults() {
		if it != nil {
			err = closeIterator(it, err)
		}
		it, err = getIterator(
			ctx,
			ie,
			queryFormat,
			"crdb_internal.transaction_statistics_persisted"+tableSuffix,
			"combined-txns-persisted-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		it, err = getIterator(
			ctx,
			ie,
			queryFormat,
			"crdb_internal.transaction_statistics",
			"combined-txns-with-memory-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row on collectCombinedTransactions")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns on collectCombinedTransactions, received %d", expectedNumDatums, row.Len())
		}

		app := string(tree.MustBeDString(row[0]))

		aggregatedTs := tree.MustBeDTimestampTZ(row[1]).Time
		fingerprintID, err := sqlstatsutil.DatumToUint64(row[2])
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		var metadata appstatspb.CollectedTransactionStatistics
		metadataJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeTxnStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[4]).JSON
		if err = sqlstatsutil.DecodeTxnStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		txnStats := serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			StatsData: appstatspb.CollectedTransactionStatistics{
				StatementFingerprintIDs:  metadata.StatementFingerprintIDs,
				App:                      app,
				Stats:                    metadata.Stats,
				AggregatedTs:             aggregatedTs,
				TransactionFingerprintID: appstatspb.TransactionFingerprintID(fingerprintID),
			},
		}

		transactions = append(transactions, txnStats)
	}

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return transactions, nil
}

func collectStmtsForTxns(
	ctx context.Context,
	ie *sql.InternalExecutor,
	req *serverpb.CombinedStatementsStatsRequest,
	transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
	testingKnobs *sqlstats.TestingKnobs,
	activityTableHasAllData bool,
	tableSuffix string,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {

	whereClause, args := buildWhereClauseForStmtsByTxn(req, transactions, testingKnobs)

	const queryFormat = `
SELECT fingerprint_id,
       transaction_fingerprint_id,
       crdb_internal.merge_stats_metadata(array_agg(metadata))    AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
       app_name
FROM %s %s
GROUP BY
    fingerprint_id,
    transaction_fingerprint_id,
    app_name
`

	const expectedNumDatums = 5
	var it isql.Rows
	var err error

	if activityTableHasAllData {
		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-activity-for-txn", nil,
			sessiondata.NodeUserSessionDataOverride, fmt.Sprintf(`
SELECT fingerprint_id,
       transaction_fingerprint_id,
       crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata))   AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
       app_name
FROM crdb_internal.statement_activity %s
GROUP BY
    fingerprint_id,
    transaction_fingerprint_id,
    app_name`, whereClause),
			args...)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the activity table, retrieve the data from the persisted table.
	var query string
	if it == nil || !it.HasResults() {
		if it != nil {
			err = closeIterator(it, err)
			if err != nil {
				return nil, srverrors.ServerError(ctx, err)
			}
		}
		query = fmt.Sprintf(
			queryFormat,
			"crdb_internal.statement_statistics_persisted"+tableSuffix,
			whereClause)
		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-for-txn", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)

		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		query = fmt.Sprintf(queryFormat, "crdb_internal.statement_statistics", whereClause)

		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-with-memory-for-txn", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)

		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	defer func() {
		closeErr := it.Close()
		if closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row on collectStmtsForTxns")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d on collectStmtsForTxns", expectedNumDatums)
		}

		var statementFingerprintID uint64
		if statementFingerprintID, err = sqlstatsutil.DatumToUint64(row[0]); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		var txnFingerprintID uint64
		if txnFingerprintID, err = sqlstatsutil.DatumToUint64(row[1]); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		var metadata appstatspb.CollectedStatementStatistics
		metadataJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		metadata.Key.TransactionFingerprintID = appstatspb.TransactionFingerprintID(txnFingerprintID)

		statsJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		app := string(tree.MustBeDString(row[4]))
		metadata.Key.App = app

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: metadata.Key,
			},
			ID:    appstatspb.StmtFingerprintID(statementFingerprintID),
			Stats: metadata.Stats,
		}

		statements = append(statements, stmt)

	}

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return statements, nil
}

func (s *statusServer) StatementDetails(
	ctx context.Context, req *serverpb.StatementDetailsRequest,
) (*serverpb.StatementDetailsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	return getStatementDetails(
		ctx,
		req,
		s.internalExecutor,
		s.st,
		s.sqlServer.execCfg.SQLStatsTestingKnobs)
}

func getStatementDetails(
	ctx context.Context,
	req *serverpb.StatementDetailsRequest,
	ie *sql.InternalExecutor,
	settings *cluster.Settings,
	testingKnobs *sqlstats.TestingKnobs,
) (*serverpb.StatementDetailsResponse, error) {
	limit := SQLStatsResponseMax.Get(&settings.SV)
	showInternal := SQLStatsShowInternal.Get(&settings.SV)
	whereClause, args, err := getStatementDetailsQueryClausesAndArgs(req, testingKnobs, showInternal)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// Used for mixed cluster version, where we need to use the persisted view with _v22_2.
	tableSuffix := ""
	if !settings.Version.IsActive(ctx, clusterversion.V23_1AddSQLStatsComputedIndexes) {
		tableSuffix = "_v22_2"
	}
	// Check if the activity tables have data within the selected period.
	activityHasData := false
	reqStartTime := getTimeFromSeconds(req.Start)
	if settings.Version.IsActive(ctx, clusterversion.V23_1AddSystemActivityTables) {
		activityHasData, err = activityTablesHaveFullData(
			ctx,
			ie,
			settings,
			testingKnobs,
			reqStartTime,
			1,
			serverpb.StatsSortOptions_SERVICE_LAT, //Order is not used on this endpoint, so any value can be passed here.
		)
		if err != nil {
			log.Errorf(ctx, "Error on getStatementDetails: %s", err)
		}
	}

	statementTotal, err := getTotalStatementDetails(ctx, ie, whereClause, args, activityHasData, tableSuffix)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	statementStatisticsPerAggregatedTs, err := getStatementDetailsPerAggregatedTs(
		ctx,
		ie,
		whereClause,
		args,
		limit,
		activityHasData,
		tableSuffix)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	statementStatisticsPerPlanHash, err := getStatementDetailsPerPlanHash(
		ctx,
		ie,
		whereClause,
		args,
		limit,
		activityHasData,
		tableSuffix)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// At this point the counts on statementTotal.metadata have the count for how many times we saw that value
	// as a row, and not the count of executions for each value.
	// The values on statementStatisticsPerPlanHash.Metadata.*Count have the correct count,
	// since the metadata is unique per plan hash.
	// Update the statementTotal.Metadata.*Count with the counts from statementStatisticsPerPlanHash.keyData.
	statementTotal.Metadata.DistSQLCount = 0
	statementTotal.Metadata.FailedCount = 0
	statementTotal.Metadata.FullScanCount = 0
	statementTotal.Metadata.VecCount = 0
	statementTotal.Metadata.TotalCount = 0
	for _, planStats := range statementStatisticsPerPlanHash {
		statementTotal.Metadata.DistSQLCount += planStats.Metadata.DistSQLCount
		statementTotal.Metadata.FailedCount += planStats.Metadata.FailedCount
		statementTotal.Metadata.FullScanCount += planStats.Metadata.FullScanCount
		statementTotal.Metadata.VecCount += planStats.Metadata.VecCount
		statementTotal.Metadata.TotalCount += planStats.Metadata.TotalCount
	}

	response := &serverpb.StatementDetailsResponse{
		Statement:                          statementTotal,
		StatementStatisticsPerAggregatedTs: statementStatisticsPerAggregatedTs,
		StatementStatisticsPerPlanHash:     statementStatisticsPerPlanHash,
		InternalAppNamePrefix:              catconstants.InternalAppNamePrefix,
	}

	return response, nil
}

// getStatementDetailsQueryClausesAndArgs returns whereClause and its arguments.
// The whereClause will be in the format `WHERE A = $1 AND B = $2` and
// args will return the list of arguments in order that will replace the actual values.
func getStatementDetailsQueryClausesAndArgs(
	req *serverpb.StatementDetailsRequest, testingKnobs *sqlstats.TestingKnobs, showInternal bool,
) (whereClause string, args []interface{}, err error) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())

	fingerprintID, err := strconv.ParseUint(req.FingerprintId, 10, 64)
	if err != nil {
		return "", nil, err
	}

	args = append(args, sqlstatsutil.EncodeUint64ToBytes(fingerprintID))
	buffer.WriteString(fmt.Sprintf(" WHERE fingerprint_id = $%d", len(args)))

	if !showInternal {
		// Filter out internal statements by app name.
		buffer.WriteString(fmt.Sprintf(" AND app_name NOT LIKE '%s%%'", catconstants.InternalAppNamePrefix))
	}

	// Statements are grouped ignoring the app name in the Statements/Transactions page, so when
	// calling for the Statement Details endpoint, this value can be empty or a list of app names.
	if len(req.AppNames) > 0 {
		if !(len(req.AppNames) == 1 && req.AppNames[0] == "") {
			hasInternal := false
			buffer.WriteString(" AND (")
			for i, app := range req.AppNames {
				if app == "(unset)" {
					app = ""
				}
				if strings.Contains(app, catconstants.InternalAppNamePrefix) {
					hasInternal = true
				}
				if i != 0 {
					args = append(args, app)
					buffer.WriteString(fmt.Sprintf(" OR app_name = $%d", len(args)))
				} else {
					args = append(args, app)
					buffer.WriteString(fmt.Sprintf(" app_name = $%d", len(args)))
				}
			}
			if hasInternal {
				buffer.WriteString(fmt.Sprintf(" OR app_name LIKE '%s%%'", catconstants.InternalAppNamePrefix))
			}
			buffer.WriteString(" )")
		}
	}

	start := getTimeFromSeconds(req.Start)
	if start != nil {
		args = append(args, *start)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}
	end := getTimeFromSeconds(req.End)
	if end != nil {
		args = append(args, *end)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}
	whereClause = buffer.String()

	return whereClause, args, nil
}

// getTotalStatementDetails return all the statistics for the selected statement combined.
func getTotalStatementDetails(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	activityTableHasAllData bool,
	tableSuffix string,
) (serverpb.StatementDetailsResponse_CollectedStatementSummary, error) {
	const expectedNumDatums = 4
	var statement serverpb.StatementDetailsResponse_CollectedStatementSummary
	const queryFormat = `
SELECT crdb_internal.merge_stats_metadata(array_agg(metadata))    AS metadata,
       array_agg(app_name)                                        AS app_names,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
       encode(fingerprint_id, 'hex')                              AS fingerprint_id
FROM %s %s
GROUP BY
    fingerprint_id
LIMIT 1`

	var row tree.Datums
	var err error

	if activityTableHasAllData {
		row, err = ie.QueryRowEx(ctx, "combined-stmts-activity-details-total", nil,
			sessiondata.NodeUserSessionDataOverride, fmt.Sprintf(`
SELECT crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata)) AS metadata,
       array_agg(app_name)                                               AS app_names,
       crdb_internal.merge_statement_stats(array_agg(statistics))        AS statistics,
       encode(fingerprint_id, 'hex')                                     AS fingerprint_id
FROM crdb_internal.statement_activity %s
GROUP BY
    fingerprint_id
LIMIT 1`, whereClause), args...)
		if err != nil {
			return statement, srverrors.ServerError(ctx, err)
		}
	}
	// If there are no results from the activity table, retrieve the data from the persisted table.
	if row == nil || row.Len() == 0 {
		row, err = ie.QueryRowEx(ctx, "combined-stmts-persisted-details-total", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(
				queryFormat,
				"crdb_internal.statement_statistics_persisted"+tableSuffix,
				whereClause), args...)
		if err != nil {
			return statement, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if row.Len() == 0 {
		row, err = ie.QueryRowEx(ctx, "combined-stmts-details-total-with-memory", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(queryFormat, "crdb_internal.statement_statistics", whereClause), args...)
		if err != nil {
			return statement, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results in-memory, return empty statement object.
	if row.Len() == 0 {
		return statement, nil
	}
	if row.Len() != expectedNumDatums {
		return statement, srverrors.ServerError(ctx, errors.Newf(
			"expected %d columns on getTotalStatementDetails, received %d", expectedNumDatums))
	}

	var statistics appstatspb.CollectedStatementStatistics
	var aggregatedMetadata appstatspb.AggregatedStatementMetadata
	metadataJSON := tree.MustBeDJSON(row[0]).JSON

	if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
		return statement, srverrors.ServerError(ctx, err)
	}

	apps := tree.MustBeDArray(row[1])
	var appNames []string
	for _, s := range apps.Array {
		appNames = util.CombineUnique(appNames, []string{string(tree.MustBeDString(s))})
	}
	aggregatedMetadata.AppNames = appNames

	statsJSON := tree.MustBeDJSON(row[2]).JSON
	if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &statistics.Stats); err != nil {
		return statement, srverrors.ServerError(ctx, err)
	}

	aggregatedMetadata.FormattedQuery = aggregatedMetadata.Query
	aggregatedMetadata.FingerprintID = string(tree.MustBeDString(row[3]))

	statement = serverpb.StatementDetailsResponse_CollectedStatementSummary{
		Metadata: aggregatedMetadata,
		Stats:    statistics.Stats,
	}

	return statement, nil
}

// getStatementDetailsPerAggregatedTs returns the list of statements
// per aggregated timestamp, not using the columns plan hash as
// part of the key on the grouping.
func getStatementDetailsPerAggregatedTs(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	limit int64,
	activityTableHasAllData bool,
	tableSuffix string,
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs, error) {
	const expectedNumDatums = 3
	const queryFormat = `
SELECT aggregated_ts,
       crdb_internal.merge_stats_metadata(array_agg(metadata))    AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
FROM %s %s
GROUP BY
    aggregated_ts
ORDER BY aggregated_ts ASC
LIMIT $%d`
	var it isql.Rows
	var err error
	defer func() {
		err = closeIterator(it, err)
	}()
	args = append(args, limit)

	if activityTableHasAllData {
		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-activity-details-by-aggregated-timestamp", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`
SELECT aggregated_ts,
       crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata)) AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
FROM crdb_internal.statement_activity %s
GROUP BY
    aggregated_ts
ORDER BY aggregated_ts ASC
LIMIT $%d`, whereClause, len(args)),
			args...)

		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the activity table, retrieve the data from the persisted table.
	var query string
	if it == nil || !it.HasResults() {
		if it != nil {
			err = closeIterator(it, err)
		}
		query = fmt.Sprintf(
			queryFormat,
			"crdb_internal.statement_statistics_persisted"+tableSuffix,
			whereClause,
			len(args))

		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-details-by-aggregated-timestamp", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)

		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		query = fmt.Sprintf(queryFormat, "crdb_internal.statement_statistics", whereClause, len(args))
		it, err = ie.QueryIteratorEx(ctx, "console-combined-stmts-details-by-aggregated-timestamp-with-memory", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	var statements []serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row on getStatementDetailsPerAggregatedTs")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns on getStatementDetailsPerAggregatedTs, received %d", expectedNumDatums)
		}

		aggregatedTs := tree.MustBeDTimestampTZ(row[0]).Time

		var metadata appstatspb.CollectedStatementStatistics
		var aggregatedMetadata appstatspb.AggregatedStatementMetadata
		metadataJSON := tree.MustBeDJSON(row[1]).JSON
		if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs{
			AggregatedTs: aggregatedTs,
			Stats:        metadata.Stats,
			Metadata:     aggregatedMetadata,
		}

		statements = append(statements, stmt)
	}
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return statements, nil
}

// getExplainPlanFromGist decode the Explain Plan from a Plan Gist.
func getExplainPlanFromGist(ctx context.Context, ie *sql.InternalExecutor, planGist string) string {
	planError := "Error collecting Explain Plan."
	var args []interface{}

	const query = `SELECT crdb_internal.decode_plan_gist($1)`
	args = append(args, planGist)

	it, err := ie.QueryIteratorEx(ctx, "console-combined-stmts-details-get-explain-plan", nil,
		sessiondata.NodeUserSessionDataOverride, query, args...)

	if err != nil {
		return planError
	}

	defer func() {
		err = closeIterator(it, err)
	}()

	var explainPlan []string
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return planError
		}
		explainPlanLine := string(tree.MustBeDString(row[0]))
		explainPlan = append(explainPlan, explainPlanLine)
	}
	if err != nil {
		return planError
	}

	return strings.Join(explainPlan, "\n")
}

func getIdxAndTableName(ctx context.Context, ie *sql.InternalExecutor, indexInfo string) string {
	var args []interface{}
	idxInfoArr := strings.Split(indexInfo, "@")
	tableID, err := strconv.ParseInt(idxInfoArr[0], 10, 64)
	if err != nil {
		return indexInfo
	}
	indexID, err := strconv.ParseInt(idxInfoArr[1], 10, 64)
	if err != nil {
		return indexInfo
	}
	args = append(args, tableID)
	args = append(args, indexID)

	row, err := ie.QueryRowEx(ctx,
		"combined-stmts-details-get-index-and-table-names", nil,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT descriptor_name, index_name FROM crdb_internal.table_indexes WHERE descriptor_id = $1 AND index_id = $2`,
		args...)
	if err != nil {
		return indexInfo
	}
	if row == nil {
		// Value being used on the UI for checks.
		return "dropped"
	}
	tableName := tree.MustBeDString(row[0])
	indexName := tree.MustBeDString(row[1])
	return fmt.Sprintf("%s@%s", tableName, indexName)
}

// getStatementDetailsPerPlanHash returns the list of statements
// per plan hash, not using the columns aggregated timestamp as
// part of the key on the grouping.
func getStatementDetailsPerPlanHash(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	limit int64,
	activityTableHasAllData bool,
	tableSuffix string,
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash, error) {
	expectedNumDatums := 5
	const queryFormat = `
SELECT plan_hash,
       (statistics -> 'statistics' -> 'planGists' ->> 0)          AS plan_gist,
       crdb_internal.merge_stats_metadata(array_agg(metadata))    AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
       index_recommendations
FROM %s %s
GROUP BY
    plan_hash,
    plan_gist,
    index_recommendations
LIMIT $%d`

	args = append(args, limit)
	var err error
	// We will have 1 open iterator at a time. For the deferred close operation, we will
	// only close the iterator if there were no errors creating the iterator. Closing an
	// iterator that returned an error on creation will cause a nil pointer deref.
	var it isql.Rows
	var iterErr error
	defer func() {
		if iterErr == nil {
			err = closeIterator(it, err)
		}
	}()

	if activityTableHasAllData {
		it, iterErr = ie.QueryIteratorEx(ctx, "console-combined-stmts-activity-details-by-plan-hash", nil,
			sessiondata.NodeUserSessionDataOverride, fmt.Sprintf(`
SELECT plan_hash,
       (statistics -> 'statistics' -> 'planGists' ->> 0)                 AS plan_gist,
       crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata)) AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics))        AS statistics,
       index_recommendations
FROM crdb_internal.statement_activity %s
GROUP BY
    plan_hash,
    plan_gist,
    index_recommendations
LIMIT $%d`, whereClause, len(args)), args...)
		if iterErr != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the activity table, retrieve the data from the persisted table.
	var query string
	if it == nil || !it.HasResults() {
		if it != nil {
			err = closeIterator(it, err)
		}
		query = fmt.Sprintf(
			queryFormat,
			"crdb_internal.statement_statistics_persisted"+tableSuffix,
			whereClause,
			len(args))
		it, iterErr = ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-details-by-plan-hash", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
		if iterErr != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		query = fmt.Sprintf(queryFormat, "crdb_internal.statement_statistics", whereClause, len(args))
		it, iterErr = ie.QueryIteratorEx(ctx, "console-combined-stmts-details-by-plan-hash-with-memory", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
		if iterErr != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	var statements []serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row on getStatementDetailsPerPlanHash")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns on getStatementDetailsPerPlanHash, received %d", expectedNumDatums)
		}

		var planHash uint64
		if planHash, err = sqlstatsutil.DatumToUint64(row[0]); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		planGist := string(tree.MustBeDStringOrDNull(row[1]))
		var explainPlan string
		if planGist != "" {
			explainPlan = getExplainPlanFromGist(ctx, ie, planGist)
		}

		var metadata appstatspb.CollectedStatementStatistics
		var aggregatedMetadata appstatspb.AggregatedStatementMetadata
		metadataJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}

		recommendations := tree.MustBeDArray(row[4])
		var idxRecommendations []string
		for _, s := range recommendations.Array {
			idxRecommendations = util.CombineUnique(idxRecommendations, []string{string(tree.MustBeDString(s))})
		}

		// A metadata is unique for each plan, meaning if any of the counts are greater than zero,
		// we can update the value of each count with the execution count of this plan hash to
		// have the correct count of each metric.
		if aggregatedMetadata.DistSQLCount > 0 {
			aggregatedMetadata.DistSQLCount = metadata.Stats.Count
		}
		if aggregatedMetadata.FailedCount > 0 {
			aggregatedMetadata.FailedCount = metadata.Stats.Count
		}
		if aggregatedMetadata.FullScanCount > 0 {
			aggregatedMetadata.FullScanCount = metadata.Stats.Count
		}
		if aggregatedMetadata.VecCount > 0 {
			aggregatedMetadata.VecCount = metadata.Stats.Count
		}
		aggregatedMetadata.TotalCount = metadata.Stats.Count

		var indexes []string
		for _, idx := range metadata.Stats.Indexes {
			indexes = append(indexes, getIdxAndTableName(ctx, ie, idx))
		}
		metadata.Stats.Indexes = indexes

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash{
			ExplainPlan:          explainPlan,
			PlanHash:             planHash,
			Stats:                metadata.Stats,
			Metadata:             aggregatedMetadata,
			IndexRecommendations: idxRecommendations,
		}

		statements = append(statements, stmt)
	}
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return statements, nil
}
