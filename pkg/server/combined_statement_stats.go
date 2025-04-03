// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// Table sources.
	CrdbInternalStmtStatsCombined  = "crdb_internal.statement_statistics"
	CrdbInternalStmtStatsPersisted = "crdb_internal.statement_statistics_persisted"
	CrdbInternalStmtStatsCached    = "crdb_internal.statement_activity"
	CrdbInternalTxnStatsCombined   = "crdb_internal.transaction_statistics"
	CrdbInternalTxnStatsPersisted  = "crdb_internal.transaction_statistics_persisted"
	CrdbInternalTxnStatsCached     = "crdb_internal.transaction_activity"

	// Sorts
	sortSvcLatDesc         = `(statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT DESC`
	sortCPUTimeDesc        = `(statistics -> 'execution_statistics' -> 'cpuSQLNanos' ->> 'mean')::FLOAT DESC`
	sortExecCountDesc      = `(statistics -> 'statistics' ->> 'cnt')::INT DESC`
	sortContentionTimeDesc = `(statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::FLOAT DESC`
	sortPCTRuntimeDesc     = `((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
                         (statistics -> 'statistics' ->> 'cnt')::FLOAT) DESC`
	sortLatencyInfoMinDesc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'min')::FLOAT DESC`
	sortLatencyInfoMaxDesc = `(statistics -> 'statistics' -> 'latencyInfo' ->> 'max')::FLOAT DESC`
	sortRowsProcessedDesc  = `((statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::FLOAT + 
												 (statistics -> 'statistics' -> 'rowsWritten' ->> 'mean')::FLOAT) DESC`
	sortMaxMemoryDesc = `(statistics -> 'execution_statistics' -> 'maxMemUsage' ->> 'mean')::FLOAT DESC`
	sortNetworkDesc   = `(statistics -> 'execution_statistics' -> 'networkBytes' ->> 'mean')::FLOAT DESC`
	sortRetriesDesc   = `(statistics -> 'statistics' ->> 'maxRetries')::INT DESC`
	sortLastExecDesc  = `(statistics -> 'statistics' ->> 'lastExecAt') DESC`
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
		s.sqlServer.pgServer.SQLServer.GetLocalSQLStatsProvider(),
		s.internalExecutor,
		s.st,
		s.sqlServer.execCfg.SQLStatsTestingKnobs)
}

type statementStatsRunner struct {
	stmtSourceTable string
	txnSourceTable  string
	ie              *sql.InternalExecutor
	testingKnobs    *sqlstats.TestingKnobs
}

func getCombinedStatementStats(
	ctx context.Context,
	req *serverpb.CombinedStatementsStatsRequest,
	statsProvider *sslocal.SQLStats,
	ie *sql.InternalExecutor,
	settings *cluster.Settings,
	testingKnobs *sqlstats.TestingKnobs,
) (*serverpb.StatementsResponse, error) {
	var err error
	showInternal := SQLStatsShowInternal.Get(&settings.SV)
	whereClause, orderAndLimit, args := getCombinedStatementsQueryClausesAndArgs(
		req, testingKnobs, showInternal, settings)

	// Check if the activity tables contains all the data required for the selected period from the request.
	activityHasAllData := false
	reqStartTime := getTimeFromSeconds(req.Start)
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

	stmtsRunTime, txnsRunTime, oldestDate, stmtSourceTable, txnSourceTable, err := getSourceStatsInfo(
		ctx,
		req,
		ie,
		testingKnobs,
		activityHasAllData,
		showInternal,
	)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	runner := &statementStatsRunner{
		stmtSourceTable: stmtSourceTable,
		txnSourceTable:  txnSourceTable,
		ie:              ie,
		testingKnobs:    testingKnobs,
	}

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics

	if req.FetchMode == nil || req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		transactions, err = runner.collectCombinedTransactions(
			ctx,
			whereClause,
			args,
			orderAndLimit,
		)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	if req.FetchMode != nil && req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		// If we're fetching for txns, the client still expects statement stats for
		// stmts in the txns response.
		statements, err = runner.collectStmtsForTxns(
			ctx,
			req,
			transactions,
		)
	} else {
		statements, err = runner.collectCombinedStatements(
			ctx,
			whereClause,
			args,
			orderAndLimit,
			settings,
		)
	}

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

	// Top Activity table doesn't store internal data.
	if SQLStatsShowInternal.Get(&settings.SV) {
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

	getRuntime := func(table string, createQuery func(tableName string) string) (float32, error) {

		queryToGetClusterTotalRunTime := createQuery(table)
		it, err := ie.QueryIteratorEx(
			ctx,
			redact.Sprintf(`console-combined-stmts-%s-total-runtime`, table),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			queryToGetClusterTotalRunTime, args...)

		if err != nil {
			return 0, err
		}

		defer func() {
			err = closeIterator(it, err)
		}()

		ok, err := it.Next(ctx)
		if err != nil {
			return 0, err
		}

		// It's possible the table is empty. Just return 0.
		if !ok {
			return 0, nil
		}

		var row tree.Datums
		if row = it.Cur(); row == nil {
			return 0, errors.New("unexpected null row on getSourceStatsInfo.getRuntime")
		}

		return float32(tree.MustBeDFloat(row[0])), nil
	}

	getOldestDate := func(table string) (*time.Time, error) {
		it, err := ie.QueryIteratorEx(
			ctx,
			redact.Sprintf(`console-combined-stmts-%s-oldest_date`, table),
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

	createActivityTableQuery := func(table string) string {
		return fmt.Sprintf(`
SELECT COALESCE(sum(total_latency), 0) from (SELECT aggregated_ts, 
        max(execution_total_cluster_seconds) as total_latency
FROM %s %s GROUP BY aggregated_ts) %s`, table, whereClause, testingKnobs.GetAOSTClause())
	}

	createStatsTableQuery := func(table string) string {
		return fmt.Sprintf(`
SELECT COALESCE(
          sum(
             (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
             (statistics -> 'statistics' ->> 'cnt')::FLOAT
          ),
       0)
FROM %s %s`, table, whereClause)
	}
	// We return statement info for both req modes (statements only and transactions only),
	// since statements are also returned for transactions only mode.
	// Note that the stmts query for txns can't use the activity table.
	// We shouldn't ever run into situations where fetchmode isn't specified, but if for some
	// reason we are fetching both stmt and txns overview info, the stmts source table returned will
	// describe what is used for the stmts overview query and not stmts for txns. In a future commit,
	// we will swap to using one source table for all stmts returned in a request.
	stmtsRuntime = 0
	if activityTableHasAllData && (req.FetchMode == nil || req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_StmtStatsOnly) {
		stmtSourceTable = CrdbInternalStmtStatsCached
		stmtsRuntime, err = getRuntime(stmtSourceTable, createActivityTableQuery)
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
		stmtSourceTable = CrdbInternalStmtStatsPersisted
		stmtsRuntime, err = getRuntime(stmtSourceTable, createStatsTableQuery)
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
		stmtSourceTable = CrdbInternalStmtStatsCombined
		stmtsRuntime, err = getRuntime(stmtSourceTable, createStatsTableQuery)
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
			txnSourceTable = CrdbInternalTxnStatsCached
			txnsRuntime, err = getRuntime(txnSourceTable, createActivityTableQuery)
			if err != nil {
				return 0, 0, nil, stmtSourceTable, txnSourceTable, err
			}
		}
		// If there are no results from the activity table, retrieve the data from the persisted table.
		if txnsRuntime == 0 {
			txnSourceTable = CrdbInternalTxnStatsPersisted
			txnsRuntime, err = getRuntime(txnSourceTable, createStatsTableQuery)
			if err != nil {
				return 0, 0, nil, stmtSourceTable, txnSourceTable, err
			}
		}
		// If there are no results from the persisted table, retrieve the data from the combined view
		// with data in-memory.
		if txnsRuntime == 0 {
			txnSourceTable = CrdbInternalTxnStatsCombined
			txnsRuntime, err = getRuntime(txnSourceTable, createStatsTableQuery)
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
		serverpb.StatsSortOptions_CONTENTION_TIME,
		serverpb.StatsSortOptions_PCT_RUNTIME:
		return true
	}
	return false
}

func getStmtColumnFromSortOption(sort serverpb.StatsSortOptions) string {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT:
		return sortSvcLatDesc
	case serverpb.StatsSortOptions_CPU_TIME:
		return sortCPUTimeDesc
	case serverpb.StatsSortOptions_EXECUTION_COUNT:
		return sortExecCountDesc
	case serverpb.StatsSortOptions_CONTENTION_TIME:
		return sortContentionTimeDesc
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
	case serverpb.StatsSortOptions_PCT_RUNTIME:
		return sortPCTRuntimeDesc
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

func (r *statementStatsRunner) collectCombinedStatements(
	ctx context.Context,
	whereClause string,
	args []interface{},
	orderAndLimit string,
	settings *cluster.Settings,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {
	aostClause := r.testingKnobs.GetAOSTClause()
	const expectedNumDatums = 9
	const queryFormat = `
SELECT 
    fingerprint_id,
    app_name,
    aggregated_ts,
    COALESCE(CAST(metadata -> 'distSQLCount' AS INT), 0)  AS distSQLCount,
    COALESCE(CAST(metadata -> 'fullScanCount' AS INT), 0) AS fullScanCount,
    metadata ->> 'query'                                  AS query,
    metadata ->> 'querySummary'                           AS querySummary,
    (SELECT string_agg(elem::text, ',') 
    FROM json_array_elements_text(metadata->'db') AS elem) AS databases,
    statistics
FROM (SELECT fingerprint_id,
             app_name,
             max(aggregated_ts)                           AS aggregated_ts,
             merge_stats_metadata(metadata)               AS metadata,
             merge_statement_stats(statistics)            AS statistics
      FROM %s %s
      GROUP BY
          fingerprint_id,
          app_name) %s
%s`
	metadataAggFn := mergeAggStmtMetadataColumnLatest
	activityQuery := strings.Join([]string{`
SELECT 
    fingerprint_id,
    app_name,
    aggregated_ts,
    COALESCE(CAST(metadata -> 'distSQLCount' AS INT), 0)  AS distSQLCount,
    COALESCE(CAST(metadata -> 'fullScanCount' AS INT), 0) AS fullScanCount,
    metadata ->> 'query'                                  AS query,
    metadata ->> 'querySummary'                           AS querySummary,
    (SELECT string_agg(elem::text, ',') 
    FROM json_array_elements_text(metadata->'db') AS elem) AS databases,
    statistics
FROM (SELECT fingerprint_id,
             app_name,
             max(aggregated_ts)                                     AS aggregated_ts,
             `, metadataAggFn, ` AS metadata,
             merge_statement_stats(statistics)                      AS statistics
      FROM %s %s
      GROUP BY
          fingerprint_id,
          app_name) %s
%s`}, "")

	var it isql.Rows
	var err error
	defer func() {
		err = closeIterator(it, err)
	}()

	switch r.stmtSourceTable {
	case CrdbInternalStmtStatsCached:
		it, err = getIterator(
			ctx,
			r.ie,
			// The statement activity table has aggregated metadata.
			activityQuery,
			CrdbInternalStmtStatsCached,
			"activity-by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
	case CrdbInternalStmtStatsPersisted, CrdbInternalStmtStatsCombined:
		it, err = getIterator(
			ctx,
			r.ie,
			queryFormat,
			r.stmtSourceTable,
			"by-interval",
			whereClause,
			args,
			aostClause,
			orderAndLimit)
	default:
		return nil, errors.Newf("combined statements: unknown source table: %s", r.stmtSourceTable)

	}
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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

		app := string(tree.MustBeDString(row[1]))

		aggregatedTs := tree.MustBeDTimestampTZ(row[2]).Time
		distSQLCount := int64(*row[3].(*tree.DInt))
		fullScanCount := int64(*row[4].(*tree.DInt))
		query := string(tree.MustBeDString(row[5]))
		querySummary := string(tree.MustBeDString(row[6]))
		databases := string(tree.MustBeDString(row[7]))

		metadata := appstatspb.CollectedStatementStatistics{
			Key: appstatspb.StatementStatisticsKey{
				App:          app,
				DistSQL:      distSQLCount > 0,
				FullScan:     fullScanCount > 0,
				Query:        query,
				QuerySummary: querySummary,
				Database:     databases,
			},
		}

		var stats appstatspb.StatementStatistics
		statsJSON := tree.MustBeDJSON(row[8]).JSON
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
			TxnFingerprintIDs: []appstatspb.TransactionFingerprintID{appstatspb.InvalidTransactionFingerprintID},
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
	opName := redact.Sprintf(`console-combined-stmts-%s`, queryInfo)

	it, err := ie.QueryIteratorEx(ctx, opName, nil,
		sessiondata.NodeUserSessionDataOverride, query, args...)
	if err != nil {
		return it, srverrors.ServerError(ctx, err)
	}

	return it, nil
}

func (r *statementStatsRunner) collectCombinedTransactions(
	ctx context.Context, whereClause string, args []interface{}, orderAndLimit string,
) ([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, error) {
	aostClause := r.testingKnobs.GetAOSTClause()
	const expectedNumDatums = 5
	const queryFormat = `
SELECT *
FROM (SELECT app_name,
             max(aggregated_ts)                   AS aggregated_ts,
             fingerprint_id,
             max(metadata),
             merge_transaction_stats(statistics)  AS statistics
      FROM %s %s
      GROUP BY
          app_name,
          fingerprint_id) %s
%s`

	var it isql.Rows
	var err error

	it, err = getIterator(
		ctx,
		r.ie,
		queryFormat,
		r.txnSourceTable,
		"collect-combined-transactions-by-interval",
		whereClause,
		args,
		aostClause,
		orderAndLimit)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	defer func() {
		err = closeIterator(it, err)
	}()

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

// This does not use the activity tables because the statement information is
// aggregated to remove the transaction fingerprint id to keep the size of the
// statement_activity manageable when the transactions can have over 1k+ statement ids.
func (r *statementStatsRunner) collectStmtsForTxns(
	ctx context.Context,
	req *serverpb.CombinedStatementsStatsRequest,
	transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {

	whereClause, args := buildWhereClauseForStmtsByTxn(req, transactions, r.testingKnobs)

	const queryFormat = `
SELECT fingerprint_id,
       transaction_fingerprint_id,
       merge_stats_metadata(metadata)    AS metadata,
       merge_statement_stats(statistics) AS statistics,
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

	if r.stmtSourceTable == CrdbInternalStmtStatsCombined {
		query := fmt.Sprintf(queryFormat, CrdbInternalStmtStatsCombined, whereClause)

		it, err = r.ie.QueryIteratorEx(ctx, "console-combined-stmts-with-memory-for-txn", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
	} else {
		query := fmt.Sprintf(
			queryFormat,
			CrdbInternalStmtStatsPersisted,
			whereClause)
		it, err = r.ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-for-txn", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
	}
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
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
