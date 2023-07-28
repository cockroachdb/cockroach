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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	CrdbInternalStmtStatsCombined        = "crdb_internal.statement_statistics"
	CrdbInternalStmtStatsPersisted       = "crdb_internal.statement_statistics_persisted"
	crdbInternalStmtStatsPersisted_V22_2 = "crdb_internal.statement_statistics_persisted_v22_2"
	CrdbInternalStmtStatsCached          = "crdb_internal.statement_activity"
	CrdbInternalTxnStatsCombined         = "crdb_internal.transaction_statistics"
	CrdbInternalTxnStatsPersisted        = "crdb_internal.transaction_statistics_persisted"
	CrdbInternalTxnStatsPersisted_V22_2  = "crdb_internal.transaction_statistics_persisted_v22_2"
	CrdbInternalTxnStatsCached           = "crdb_internal.transaction_activity"

	// Sorts
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

// This struct allows us to specify an app name and query
// for each table.
type sqlStatsQuery struct {
	query     func() string
	tableName string
	appName   string
}

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

type sqlStatsRespBuilder struct {
	st           *cluster.Settings
	testingKnobs *sqlstats.TestingKnobs
	ie           *sql.InternalExecutor

	// Surface internal statements.
	showInternal bool

	limit     int64
	statsType serverpb.StatementsRequest_FetchMode
	sort      serverpb.StatsSortOptions
	// Derived from request.
	startTime *time.Time
	endTime   *time.Time

	sqlStatsProvider sqlstats.Provider

	// The tables we should service the request from.
	// This will be determined and set while building the response.
	stmtsTableSource string
	txnsTableSource  string

	oldestEntryTs *time.Time
}

func newSqlStatsRespBuilder(
	ie *sql.InternalExecutor,
	req *serverpb.CombinedStatementsStatsRequest,
	sqlStatsProvider sqlstats.Provider,
	testingKnobs *sqlstats.TestingKnobs,
	st *cluster.Settings,
) sqlStatsRespBuilder {
	showInternal := SQLStatsShowInternal.Get(&st.SV)
	limit := req.Limit
	if limit == 0 {
		limit = SQLStatsResponseMax.Get(&st.SV)
	}

	// It's possible no fetch mode is provided. Just retrieve both stmts and txns.
	statsType := serverpb.StatementsRequest_StmtAndTxnStats
	// Default sort is by service latency.
	sort := serverpb.StatsSortOptions_SERVICE_LAT
	if req.FetchMode != nil {
		switch req.FetchMode.StatsType {
		case serverpb.CombinedStatementsStatsRequest_StmtStatsOnly:
			statsType = serverpb.StatementsRequest_StmtStatsOnly
		case serverpb.CombinedStatementsStatsRequest_TxnStatsOnly:
			statsType = serverpb.StatementsRequest_TxnStatsOnly
		}

		sort = req.FetchMode.Sort
	}

	return sqlStatsRespBuilder{
		ie:               ie,
		statsType:        statsType,
		showInternal:     showInternal,
		startTime:        getTimeFromSeconds(req.Start),
		endTime:          getTimeFromSeconds(req.End),
		sqlStatsProvider: sqlStatsProvider,
		testingKnobs:     testingKnobs,
		st:               st,
		limit:            limit,
		sort:             sort,
	}
}

func (s *statusServer) CombinedStatementStats(
	ctx context.Context, req *serverpb.CombinedStatementsStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	rb := newSqlStatsRespBuilder(
		s.internalExecutor,
		req,
		s.sqlServer.pgServer.SQLServer.GetSQLStatsProvider(),
		s.sqlServer.execCfg.SQLStatsTestingKnobs,
		s.st,
	)

	if err := rb.initSetSourceTables(ctx); err != nil {
		return nil, err
	}

	var err error
	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics

	whereClause, orderAndLimit, args := rb.getCombinedStatementsQueryClausesAndArgs()

	if rb.statsType != serverpb.StatementsRequest_StmtStatsOnly {
		transactions, err = rb.collectCombinedTransactions(
			ctx,
			whereClause,
			args,
			orderAndLimit)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	if rb.statsType == serverpb.StatementsRequest_TxnStatsOnly {
		// If we're fetching for txns, the client still expects statement stats for
		// stmts in the txns response.
		statements, err = rb.collectStmtsForTxns(ctx, transactions)
	} else {
		// If we're fetching both stmts and txns, we won't explicitly fetch the stmts
		// for a txn like in the above. Instead we'll assume it's likely that they will
		// appear in the general stmt stats query. The fetching of both stats types
		// ins't expected to occur often, as the UI will always provide a fetch mode as
		// of 22.2.

		statements, err = rb.collectCombinedStatements(ctx, whereClause, args, orderAndLimit)
	}

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	stmtsRunTime, txnsRunTime, err := rb.getSqlTotalRuntimes(ctx)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	response := &serverpb.StatementsResponse{
		Statements:            statements,
		Transactions:          transactions,
		LastReset:             rb.sqlStatsProvider.GetLastReset(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
		StmtsTotalRuntimeSecs: stmtsRunTime,
		TxnsTotalRuntimeSecs:  txnsRunTime,
		// Debug information.
		OldestAggregatedTsReturned: rb.oldestEntryTs,
		StmtsSourceTable:           rb.stmtsTableSource,
		TxnsSourceTable:            rb.txnsTableSource,
	}

	return response, nil
}

func (rb *sqlStatsRespBuilder) activityTablesHaveFullData(
	ctx context.Context,
) (result bool, err error) {

	if !StatsActivityUIEnabled.Get(&rb.st.SV) {
		return false, nil
	}

	limit := rb.limit
	order := rb.sort
	if (limit > 0 && !isLimitOnActivityTable(limit)) || !isSortOptionOnActivityTable(order) {
		return false, nil
	}

	startTime := rb.startTime
	if startTime == nil {
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
	it, err := rb.ie.QueryIteratorEx(
		ctx,
		"console-combined-stmts-activity-min-ts",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(queryWithPlaceholders, zeroDate.Format("2006-01-02 15:04:05.00"), rb.testingKnobs.GetAOSTClause()))

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

	hasData := !minAggregatedTs.IsZero() && (startTime.After(minAggregatedTs) || startTime.Equal(minAggregatedTs))
	return hasData, nil
}

// initSetSourceTables sets the source table and also fills the oldest entry in
// the source table for the response builder, which is returned for debugging.
// This must be called before the builder is usable.
func (rb *sqlStatsRespBuilder) initSetSourceTables(ctx context.Context) error {
	var args []interface{}
	startTime := rb.startTime
	endTime := rb.endTime

	var buffer strings.Builder
	buffer.WriteString(" WHERE true")

	if startTime != nil {
		args = append(args, *startTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}

	if endTime != nil {
		args = append(args, *endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	if !rb.showInternal {
		// Filter out internal statements by app name.
		buffer.WriteString(fmt.Sprintf(
			" AND app_name NOT LIKE '%s%%' AND app_name NOT LIKE '%s%%'",
			catconstants.InternalAppNamePrefix,
			catconstants.DelegatedAppNamePrefix))
	}

	whereClause := buffer.String()

	// isValidTable will return true if the provided source table can be used to
	// service the request.
	// We'll attempt to find the oldest entry in the requested timeframe.
	// If the table we're looking at has no results, we'll move on to the
	// next one until we find a non-nil time.
	// We can use this function for both stmts and txns tables since the columns
	// used in the queries are the same across all tables.
	isValidSourceTable := func(table string) bool {
		it, err := rb.ie.QueryIteratorEx(
			ctx,
			fmt.Sprintf(`console-combined-stmts-%s-oldest_date`, table),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`
SELECT min(aggregated_ts)
FROM %s %s`, table, whereClause), args...)

		if err != nil {
			return false
		}

		defer func() {
			err = closeIterator(it, err)
		}()

		if ok, err := it.Next(ctx); err != nil || !ok {
			return false
		}

		var row tree.Datums
		if row = it.Cur(); row == nil || row[0] == tree.DNull {
			return false
		}

		oldestTs := tree.MustBeDTimestampTZ(row[0]).Time
		rb.oldestEntryTs = &oldestTs

		return true
	}

	needTxnTable := rb.statsType != serverpb.StatementsRequest_StmtStatsOnly

	// If we don't find anything in the system tables, default to virtual tables.
	rb.stmtsTableSource = CrdbInternalStmtStatsCombined
	if needTxnTable {
		rb.txnsTableSource = CrdbInternalTxnStatsCombined
	}

	hasAllData, err := rb.activityTablesHaveFullData(ctx)
	// Apparently we only validate the stmt activity table to
	// determine whether we can also use the txn activity table.
	if err == nil && hasAllData && isValidSourceTable(CrdbInternalStmtStatsCached) {
		rb.stmtsTableSource = CrdbInternalStmtStatsCached
		if needTxnTable {
			rb.txnsTableSource = CrdbInternalTxnStatsCached
		}
		return nil
	}

	// Used for mixed cluster version, where we need to use the persisted view with _v22_2.
	isLatestClusterVersion := rb.st.Version.IsActive(ctx, clusterversion.V23_1AddSQLStatsComputedIndexes)

	// No cached data. See if persisted table has data.
	if isValidSourceTable(CrdbInternalStmtStatsPersisted) {
		if isLatestClusterVersion {
			rb.stmtsTableSource = CrdbInternalStmtStatsPersisted
		} else {
			rb.stmtsTableSource = crdbInternalStmtStatsPersisted_V22_2
		}
	}

	if needTxnTable && isValidSourceTable(CrdbInternalTxnStatsPersisted) {
		if isLatestClusterVersion {
			rb.txnsTableSource = CrdbInternalTxnStatsPersisted
		} else {
			rb.txnsTableSource = CrdbInternalTxnStatsPersisted_V22_2
		}
	}

	return nil
}

func (rb *sqlStatsRespBuilder) getSqlTotalRuntimes(
	ctx context.Context,
) (stmtsRunTime float32, txnsRunTime float32, err error) {
	var args []interface{}
	var buffer strings.Builder

	buffer.WriteString(" WHERE true")

	if rb.startTime != nil {
		args = append(args, *rb.startTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}

	if rb.endTime != nil {
		args = append(args, *rb.endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	whereClause := buffer.String()

	getRunTimeForTable := func(table string) (float32, error) {
		query := ""
		if table == CrdbInternalStmtStatsCached || table == CrdbInternalTxnStatsCached {
			query = fmt.Sprintf(`
SELECT COALESCE(
         max(execution_total_cluster_seconds),
       0)
FROM %s %s`, table, whereClause)
		} else {
			query = fmt.Sprintf(`
SELECT COALESCE(
          sum(
             (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
             (statistics -> 'statistics' ->> 'cnt')::FLOAT
          ),
       0)
FROM %s %s`, table, whereClause)
		}

		it, err := rb.ie.QueryIteratorEx(
			ctx,
			fmt.Sprintf(`console-combined-stmts-%s-total-runtime`, table),
			nil, sessiondata.NodeUserSessionDataOverride,
			query, args...)

		if err != nil {
			return 0, err
		}

		defer func() {
			err = closeIterator(it, err)
		}()

		if ok, err := it.Next(ctx); err != nil {
			return 0, err
		} else if !ok {
			// It's possible the table is empty. Just return 0.
			return 0, nil
		}

		var row tree.Datums
		if row = it.Cur(); row == nil {
			return 0, errors.Newf("unexpected null row on getSourceStatsInfo.getRuntime-%s", table)
		}

		return float32(tree.MustBeDFloat(row[0])), nil
	}

	if rb.statsType != serverpb.StatementsRequest_StmtStatsOnly {
		if txnsRunTime, err = getRunTimeForTable(rb.txnsTableSource); err != nil {
			return stmtsRunTime, txnsRunTime, err
		}
	}

	if stmtsRunTime, err = getRunTimeForTable(rb.stmtsTableSource); err != nil {
		return stmtsRunTime, txnsRunTime, err
	}

	return stmtsRunTime, txnsRunTime, nil
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
func (rb *sqlStatsRespBuilder) buildWhereClauseForStmtsByTxn(
	transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
) (whereClause string, args []interface{}) {
	var buffer strings.Builder
	buffer.WriteString(rb.testingKnobs.GetAOSTClause())

	buffer.WriteString(" WHERE true")

	// Add start and end filters from request.
	if rb.startTime != nil {
		args = append(args, *rb.startTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts >= $%d", len(args)))
	}

	if rb.endTime != nil {
		args = append(args, *rb.endTime)
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
func (rb *sqlStatsRespBuilder) getCombinedStatementsQueryClausesAndArgs() (
	whereClause string,
	orderAndLimitClause string,
	args []interface{},
) {
	var buffer strings.Builder
	buffer.WriteString(rb.testingKnobs.GetAOSTClause())

	if rb.showInternal {
		buffer.WriteString(" WHERE true")
	} else {
		// Filter out internal statements by app name.
		buffer.WriteString(fmt.Sprintf(
			" WHERE app_name NOT LIKE '%s%%' AND app_name NOT LIKE '%s%%'",
			catconstants.InternalAppNamePrefix,
			catconstants.DelegatedAppNamePrefix))
	}

	// Add start and end filters from request.
	if rb.startTime != nil {
		buffer.WriteString(" AND aggregated_ts >= $1")
		args = append(args, *rb.startTime)
	}

	if rb.endTime != nil {
		args = append(args, *rb.endTime)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}

	// Add LIMIT from request.
	limit := rb.limit
	args = append(args, limit)

	// Determine sort column.
	var col string
	if rb.statsType == serverpb.StatementsRequest_StmtAndTxnStats {
		col = "fingerprint_id"
	} else if rb.statsType == serverpb.StatementsRequest_StmtStatsOnly {
		col = getStmtColumnFromSortOption(rb.sort)
	} else {
		col = getTxnColumnFromSortOption(rb.sort)
	}

	orderAndLimitClause = fmt.Sprintf(` ORDER BY %s LIMIT $%d`, col, len(args))

	return buffer.String(), orderAndLimitClause, args
}

func (rb *sqlStatsRespBuilder) collectCombinedStatements(
	ctx context.Context, whereClause string, args []interface{}, orderAndLimit string,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {
	aostClause := rb.testingKnobs.GetAOSTClause()
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

	queryFormatFunc := func() string { return queryFormat }

	queries := map[string]*sqlStatsQuery{
		CrdbInternalStmtStatsCached: {
			query: func() string {
				return `
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
%s`
			},
			tableName: CrdbInternalStmtStatsCached,
			appName:   "combined-stmts-activity-by-interval",
		},
		CrdbInternalStmtStatsPersisted: {
			query:     queryFormatFunc,
			tableName: CrdbInternalStmtStatsPersisted,
			appName:   "combined-stmts-persisted-by-interval",
		},
		crdbInternalStmtStatsPersisted_V22_2: {
			query:     queryFormatFunc,
			tableName: crdbInternalStmtStatsPersisted_V22_2,
			appName:   "combined-stmts-persisted-by-interval",
		},
		CrdbInternalStmtStatsCombined: {
			query:     queryFormatFunc,
			tableName: CrdbInternalStmtStatsCombined,
			appName:   "combined-stmts-with-memory-by-interval",
		},
	}

	q := queries[rb.stmtsTableSource]

	it, err := getIterator(ctx,
		rb.ie, q.query(), q.tableName, q.appName, whereClause, args, aostClause, orderAndLimit)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	defer func() {
		err = closeIterator(it, err)
	}()

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

func (rb *sqlStatsRespBuilder) collectCombinedTransactions(
	ctx context.Context, whereClause string, args []interface{}, orderAndLimit string,
) (transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, err error) {
	aostClause := rb.testingKnobs.GetAOSTClause()
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

	queryFormatFunc := func() string {
		return queryFormat
	}

	queries := map[string]*sqlStatsQuery{
		CrdbInternalTxnStatsCached: {
			query:     queryFormatFunc,
			tableName: CrdbInternalTxnStatsCached,
			appName:   "combined-txns-activity-by-interval",
		},
		CrdbInternalTxnStatsPersisted: {
			query:     queryFormatFunc,
			tableName: CrdbInternalTxnStatsPersisted,
			appName:   "combined-txns-persisted-by-interval",
		},
		CrdbInternalTxnStatsPersisted_V22_2: {
			query:     queryFormatFunc,
			tableName: CrdbInternalTxnStatsPersisted_V22_2,
			appName:   "combined-txns-persisted-by-interval",
		},
		CrdbInternalTxnStatsCombined: {
			query:     queryFormatFunc,
			tableName: CrdbInternalTxnStatsCombined,
			appName:   "combined-txns-with-memory-by-interval",
		},
	}

	q := queries[rb.txnsTableSource]

	it, err := getIterator(ctx, rb.ie, q.query(), q.tableName, q.appName, whereClause, args, aostClause, orderAndLimit)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	defer func() {
		err = closeIterator(it, err)
	}()

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

func (rb *sqlStatsRespBuilder) collectStmtsForTxns(
	ctx context.Context,
	transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
) (statements []serverpb.StatementsResponse_CollectedStatementStatistics, err error) {

	whereClause, args := rb.buildWhereClauseForStmtsByTxn(transactions)

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

	queries := map[string]*sqlStatsQuery{
		CrdbInternalStmtStatsCached: {
			query: func() string {
				return fmt.Sprintf(`
SELECT fingerprint_id,
       transaction_fingerprint_id,
       crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata))   AS metadata,
       crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
       app_name
FROM crdb_internal.statement_activity %s
GROUP BY
    fingerprint_id,
    transaction_fingerprint_id,
    app_name`, whereClause)
			},
			tableName: CrdbInternalStmtStatsCached,
			appName:   "console-combined-stmts-activity-for-txn",
		},
		CrdbInternalStmtStatsPersisted: {
			query: func() string {
				return fmt.Sprintf(queryFormat, CrdbInternalStmtStatsPersisted, whereClause)
			},
			tableName: CrdbInternalStmtStatsPersisted,
			appName:   "console-combined-stms-persisted-for-txn",
		},
		crdbInternalStmtStatsPersisted_V22_2: {
			query: func() string {
				return fmt.Sprintf(queryFormat, crdbInternalStmtStatsPersisted_V22_2, whereClause)
			},
			tableName: crdbInternalStmtStatsPersisted_V22_2,
			appName:   "console-combined-stms-persisted-for-txn",
		},
		CrdbInternalStmtStatsCombined: {
			query: func() string {
				return fmt.Sprintf(queryFormat, CrdbInternalStmtStatsCombined, whereClause)
			},
			tableName: CrdbInternalStmtStatsCombined,
			appName:   "console-conbined-stms-with-memory-for-txn",
		},
	}

	q := queries[rb.stmtsTableSource]

	it, err := rb.ie.QueryIteratorEx(ctx, q.appName, nil,
		sessiondata.NodeUserSessionDataOverride, q.query(), args...)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	defer func() {
		err = closeIterator(it, err)
	}()

	const expectedNumDatums = 5
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
