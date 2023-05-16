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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util"
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

func (s *statusServer) CombinedStatementStats(
	ctx context.Context, req *serverpb.CombinedStatementsStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
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
	showInternal := SQLStatsShowInternal.Get(&settings.SV)
	whereClause, orderAndLimit, args := getCombinedStatementsQueryClausesAndArgs(
		req, testingKnobs, showInternal, settings)

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
	var err error

	if req.FetchMode == nil || req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		transactions, err = collectCombinedTransactions(ctx, ie, whereClause, args, orderAndLimit, testingKnobs)
		if err != nil {
			return nil, serverError(ctx, err)
		}
	}

	if req.FetchMode != nil && req.FetchMode.StatsType == serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		// Change the whereClause for the statements to those matching the txn_fingerprint_ids in the
		// transactions response that are within the desired interval. We also don't need the order and
		// limit anymore.
		orderAndLimit = ""
		whereClause, args = buildWhereClauseForStmtsByTxn(req, transactions, testingKnobs)
	}

	statements, err = collectCombinedStatements(ctx, ie, whereClause, args, orderAndLimit, testingKnobs)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	stmtsRunTime, txnsRunTime, err := getTotalRuntimeSecs(ctx, req, ie, testingKnobs)

	if err != nil {
		return nil, serverError(ctx, err)
	}

	response := &serverpb.StatementsResponse{
		Statements:            statements,
		Transactions:          transactions,
		LastReset:             statsProvider.GetLastReset(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
		StmtsTotalRuntimeSecs: stmtsRunTime,
		TxnsTotalRuntimeSecs:  txnsRunTime,
	}

	return response, nil
}

func getTotalRuntimeSecs(
	ctx context.Context,
	req *serverpb.CombinedStatementsStatsRequest,
	ie *sql.InternalExecutor,
	testingKnobs *sqlstats.TestingKnobs,
) (stmtsRuntime float32, txnsRuntime float32, err error) {
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

	queryWithPlaceholders := `
SELECT
COALESCE(
  sum(
    (statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
    (statistics-> 'statistics' ->> 'cnt')::FLOAT
  )
, 0)
FROM crdb_internal.%s_statistics_persisted
%s
`

	getRuntime := func(table string) (float32, error) {
		it, err := ie.QueryIteratorEx(
			ctx,
			fmt.Sprintf(`%s-total-runtime`, table),
			nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(queryWithPlaceholders, table, whereClause),
			args...)

		if err != nil {
			return 0, err
		}

		defer func() {
			closeErr := it.Close()
			if closeErr != nil {
				err = errors.CombineErrors(err, closeErr)
			}
		}()

		ok, err := it.Next(ctx)
		if err != nil {
			return 0, err
		}

		if !ok {
			return 0, errors.New("expected one row but got none")
		}

		var row tree.Datums
		if row = it.Cur(); row == nil {
			return 0, errors.New("unexpected null row")
		}

		return float32(tree.MustBeDFloat(row[0])), nil

	}

	if req.FetchMode == nil || req.FetchMode.StatsType != serverpb.CombinedStatementsStatsRequest_TxnStatsOnly {
		stmtsRuntime, err = getRuntime("statement")
		if err != nil {
			return 0, 0, err
		}
	}

	if req.FetchMode == nil || req.FetchMode.StatsType != serverpb.CombinedStatementsStatsRequest_StmtStatsOnly {
		txnsRuntime, err = getRuntime("transaction")
		if err != nil {
			return 0, 0, err
		}
	}

	return stmtsRuntime, txnsRuntime, err
}

// Common stmt and txn columns to sort on.
const (
	sortSvcLatDesc         = `(statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT DESC`
	sortExecCountDesc      = `(statistics -> 'statistics' ->> 'cnt')::INT DESC`
	sortContentionTimeDesc = `(statistics -> 'execution_statistics' -> 'contentionTime' ->> 'mean')::FLOAT DESC`
	sortPCTRuntimeDesc     = `((statistics -> 'statistics' -> 'svcLat' ->> 'mean')::FLOAT *
                         (statistics -> 'statistics' ->> 'cnt')::FLOAT) DESC`
)

func getStmtColumnFromSortOption(sort serverpb.StatsSortOptions) string {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT:
		return sortSvcLatDesc
	case serverpb.StatsSortOptions_EXECUTION_COUNT:
		return sortExecCountDesc
	case serverpb.StatsSortOptions_CONTENTION_TIME:
		return sortContentionTimeDesc
	default:
		return sortSvcLatDesc
	}
}

func getTxnColumnFromSortOption(sort serverpb.StatsSortOptions) string {
	switch sort {
	case serverpb.StatsSortOptions_SERVICE_LAT:
		return sortSvcLatDesc
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
		buffer.WriteString(fmt.Sprintf(" WHERE app_name NOT LIKE '%s%%'", catconstants.InternalAppNamePrefix))
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
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {
	aostClause := testingKnobs.GetAOSTClause()
	query := fmt.Sprintf(`
SELECT * FROM (
SELECT
    fingerprint_id,
    transaction_fingerprint_id,
    app_name,
    max(aggregated_ts) as aggregated_ts,
    metadata,
    crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
FROM crdb_internal.statement_statistics_persisted %s
GROUP BY
    fingerprint_id,
    transaction_fingerprint_id,
    app_name,
    metadata
) %s
%s`, whereClause, aostClause, orderAndLimit)

	const expectedNumDatums = 6

	it, err := ie.QueryIteratorEx(ctx, "combined-stmts-by-interval", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return nil, serverError(ctx, err)
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
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums)
		}

		var statementFingerprintID uint64
		if statementFingerprintID, err = sqlstatsutil.DatumToUint64(row[0]); err != nil {
			return nil, serverError(ctx, err)
		}

		var transactionFingerprintID uint64
		if transactionFingerprintID, err = sqlstatsutil.DatumToUint64(row[1]); err != nil {
			return nil, serverError(ctx, err)
		}

		app := string(tree.MustBeDString(row[2]))
		aggregatedTs := tree.MustBeDTimestampTZ(row[3]).Time

		var metadata roachpb.CollectedStatementStatistics
		metadataJSON := tree.MustBeDJSON(row[4]).JSON
		if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, serverError(ctx, err)
		}

		metadata.Key.App = app
		metadata.Key.TransactionFingerprintID =
			roachpb.TransactionFingerprintID(transactionFingerprintID)

		statsJSON := tree.MustBeDJSON(row[5]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
		}

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData:      metadata.Key,
				AggregatedTs: aggregatedTs,
			},
			ID:    roachpb.StmtFingerprintID(statementFingerprintID),
			Stats: metadata.Stats,
		}

		statements = append(statements, stmt)

	}

	if err != nil {
		return nil, serverError(ctx, err)
	}

	return statements, nil
}

func collectCombinedTransactions(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	orderAndLimit string,
	testingKnobs *sqlstats.TestingKnobs,
) ([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, error) {
	aostClause := testingKnobs.GetAOSTClause()

	query := fmt.Sprintf(`
SELECT * FROM (
SELECT
    app_name,
    max(aggregated_ts) as aggregated_ts,
    fingerprint_id,
    metadata,
    crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics
FROM crdb_internal.transaction_statistics_persisted %s
GROUP BY
    app_name,
    fingerprint_id,
    metadata
) %s
%s`, whereClause, aostClause, orderAndLimit)

	const expectedNumDatums = 5

	it, err := ie.QueryIteratorEx(ctx, "combined-txns-by-interval", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return nil, serverError(ctx, err)
	}

	defer func() {
		closeErr := it.Close()
		if closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums, row.Len())
		}

		app := string(tree.MustBeDString(row[0]))
		aggregatedTs := tree.MustBeDTimestampTZ(row[1]).Time
		fingerprintID, err := sqlstatsutil.DatumToUint64(row[2])
		if err != nil {
			return nil, serverError(ctx, err)
		}

		var metadata roachpb.CollectedTransactionStatistics
		metadataJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeTxnStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, serverError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[4]).JSON
		if err = sqlstatsutil.DecodeTxnStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
		}

		txnStats := serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			StatsData: roachpb.CollectedTransactionStatistics{
				StatementFingerprintIDs:  metadata.StatementFingerprintIDs,
				App:                      app,
				Stats:                    metadata.Stats,
				AggregatedTs:             aggregatedTs,
				TransactionFingerprintID: roachpb.TransactionFingerprintID(fingerprintID),
			},
		}

		transactions = append(transactions, txnStats)
	}

	if err != nil {
		return nil, serverError(ctx, err)
	}

	return transactions, nil
}

func (s *statusServer) StatementDetails(
	ctx context.Context, req *serverpb.StatementDetailsRequest,
) (*serverpb.StatementDetailsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
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
		return nil, serverError(ctx, err)
	}

	statementTotal, err := getTotalStatementDetails(ctx, ie, whereClause, args)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	statementStatisticsPerAggregatedTs, err := getStatementDetailsPerAggregatedTs(ctx, ie, whereClause, args, limit)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	statementStatisticsPerPlanHash, err := getStatementDetailsPerPlanHash(ctx, ie, whereClause, args, limit)
	if err != nil {
		return nil, serverError(ctx, err)
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
	ctx context.Context, ie *sql.InternalExecutor, whereClause string, args []interface{},
) (serverpb.StatementDetailsResponse_CollectedStatementSummary, error) {
	query := fmt.Sprintf(
		`SELECT
				crdb_internal.merge_stats_metadata(array_agg(metadata)) AS metadata,
				array_agg(app_name) as app_names,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
		FROM crdb_internal.statement_statistics_persisted %s
		GROUP BY
				fingerprint_id
		LIMIT 1`, whereClause)

	const expectedNumDatums = 3
	var statement serverpb.StatementDetailsResponse_CollectedStatementSummary

	row, err := ie.QueryRowEx(ctx, "combined-stmts-details-total", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return statement, serverError(ctx, err)
	}
	if len(row) == 0 {
		return statement, nil
	}
	if row.Len() != expectedNumDatums {
		return statement, serverError(ctx, errors.Newf("expected %d columns, received %d", expectedNumDatums))
	}

	var statistics roachpb.CollectedStatementStatistics
	var aggregatedMetadata roachpb.AggregatedStatementMetadata
	metadataJSON := tree.MustBeDJSON(row[0]).JSON

	if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
		return statement, serverError(ctx, err)
	}

	apps := tree.MustBeDArray(row[1])
	var appNames []string
	for _, s := range apps.Array {
		appNames = util.CombineUniqueString(appNames, []string{string(tree.MustBeDString(s))})
	}
	aggregatedMetadata.AppNames = appNames

	statsJSON := tree.MustBeDJSON(row[2]).JSON
	if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &statistics.Stats); err != nil {
		return statement, serverError(ctx, err)
	}

	aggregatedMetadata.FormattedQuery = aggregatedMetadata.Query

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
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs, error) {
	query := fmt.Sprintf(
		`SELECT
				aggregated_ts,
				crdb_internal.merge_stats_metadata(array_agg(metadata)) AS metadata,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
		FROM crdb_internal.statement_statistics_persisted %s
		GROUP BY
				aggregated_ts
		ORDER BY aggregated_ts ASC
		LIMIT $%d`, whereClause, len(args)+1)

	args = append(args, limit)
	const expectedNumDatums = 3

	it, err := ie.QueryIteratorEx(ctx, "combined-stmts-details-by-aggregated-timestamp", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return nil, serverError(ctx, err)
	}

	defer func() {
		closeErr := it.Close()
		if closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	var statements []serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums)
		}

		aggregatedTs := tree.MustBeDTimestampTZ(row[0]).Time

		var metadata roachpb.CollectedStatementStatistics
		var aggregatedMetadata roachpb.AggregatedStatementMetadata
		metadataJSON := tree.MustBeDJSON(row[1]).JSON
		if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
			return nil, serverError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
		}

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs{
			AggregatedTs: aggregatedTs,
			Stats:        metadata.Stats,
			Metadata:     aggregatedMetadata,
		}

		statements = append(statements, stmt)
	}
	if err != nil {
		return nil, serverError(ctx, err)
	}

	return statements, nil
}

// getExplainPlanFromGist decode the Explain Plan from a Plan Gist.
func getExplainPlanFromGist(ctx context.Context, ie *sql.InternalExecutor, planGist string) string {
	planError := "Error collecting Explain Plan."
	var args []interface{}

	query := `SELECT crdb_internal.decode_plan_gist($1)`
	args = append(args, planGist)

	it, err := ie.QueryIteratorEx(ctx, "combined-stmts-details-get-explain-plan", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return planError
	}

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

// getStatementDetailsPerPlanHash returns the list of statements
// per plan hash, not using the columns aggregated timestamp as
// part of the key on the grouping.
func getStatementDetailsPerPlanHash(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	limit int64,
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash, error) {
	query := fmt.Sprintf(
		`SELECT
				plan_hash,
				(statistics -> 'statistics' -> 'planGists'->>0) as plan_gist,
				crdb_internal.merge_stats_metadata(array_agg(metadata)) AS metadata,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics
		FROM crdb_internal.statement_statistics_persisted %s
		GROUP BY
				plan_hash,
				plan_gist
		LIMIT $%d`, whereClause, len(args)+1)

	args = append(args, limit)
	const expectedNumDatums = 4

	it, err := ie.QueryIteratorEx(ctx, "combined-stmts-details-by-plan-hash", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query, args...)

	if err != nil {
		return nil, serverError(ctx, err)
	}

	defer func() {
		closeErr := it.Close()
		if closeErr != nil {
			err = errors.CombineErrors(err, closeErr)
		}
	}()

	var statements []serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums)
		}

		var planHash uint64
		if planHash, err = sqlstatsutil.DatumToUint64(row[0]); err != nil {
			return nil, serverError(ctx, err)
		}
		planGist := string(tree.MustBeDStringOrDNull(row[1]))
		var explainPlan string
		if planGist != "" {
			explainPlan = getExplainPlanFromGist(ctx, ie, planGist)
		}

		var metadata roachpb.CollectedStatementStatistics
		var aggregatedMetadata roachpb.AggregatedStatementMetadata
		metadataJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeAggregatedMetadataJSON(metadataJSON, &aggregatedMetadata); err != nil {
			return nil, serverError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
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

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash{
			ExplainPlan: explainPlan,
			PlanHash:    planHash,
			Stats:       metadata.Stats,
			Metadata:    aggregatedMetadata,
		}

		statements = append(statements, stmt)
	}
	if err != nil {
		return nil, serverError(ctx, err)
	}

	return statements, nil
}
