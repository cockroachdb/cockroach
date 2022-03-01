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
	startTime := getTimeFromSeconds(req.Start)
	endTime := getTimeFromSeconds(req.End)
	limit := SQLStatsResponseMax.Get(&settings.SV)
	whereClause, orderAndLimit, args := getCombinedStatementsQueryClausesAndArgs(startTime, endTime, limit, testingKnobs)
	statements, err := collectCombinedStatements(ctx, ie, whereClause, args, orderAndLimit)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	transactions, err := collectCombinedTransactions(ctx, ie, whereClause, args, orderAndLimit)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	response := &serverpb.StatementsResponse{
		Statements:            statements,
		Transactions:          transactions,
		LastReset:             statsProvider.GetLastReset(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}

	return response, nil
}

// getCombinedStatementsQueryClausesAndArgs returns:
// - where clause (filtering by name and aggregates_ts when defined)
// - order and limit clause
// - args that will replace the clauses above
// The whereClause will be in the format `WHERE A = $1 AND B = $2` and
// args will return the list of arguments in order that will replace the actual values.
func getCombinedStatementsQueryClausesAndArgs(
	start, end *time.Time, limit int64, testingKnobs *sqlstats.TestingKnobs,
) (whereClause string, orderAndLimitClause string, args []interface{}) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())

	// Filter out internal statements by app name.
	buffer.WriteString(fmt.Sprintf(" WHERE app_name NOT LIKE '%s%%'", catconstants.InternalAppNamePrefix))

	if start != nil {
		buffer.WriteString(" AND aggregated_ts >= $1")
		args = append(args, *start)
	}

	if end != nil {
		args = append(args, *end)
		buffer.WriteString(fmt.Sprintf(" AND aggregated_ts <= $%d", len(args)))
	}
	args = append(args, limit)
	orderAndLimitClause = fmt.Sprintf(` ORDER BY aggregated_ts DESC LIMIT $%d`, len(args))

	return buffer.String(), orderAndLimitClause, args
}

func collectCombinedStatements(
	ctx context.Context,
	ie *sql.InternalExecutor,
	whereClause string,
	args []interface{},
	orderAndLimit string,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {

	query := fmt.Sprintf(
		`SELECT
				fingerprint_id,
				transaction_fingerprint_id,
				app_name,
				max(aggregated_ts) as aggregated_ts,
				metadata,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
				max(sampled_plan) AS sampled_plan,
				aggregation_interval
		FROM crdb_internal.statement_statistics %s
		GROUP BY
				fingerprint_id,
				transaction_fingerprint_id,
				app_name,
				metadata,
				aggregation_interval
		%s`, whereClause, orderAndLimit)

	const expectedNumDatums = 8

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

		planJSON := tree.MustBeDJSON(row[6]).JSON
		plan, err := sqlstatsutil.JSONToExplainTreePlanNode(planJSON)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		metadata.Stats.SensitiveInfo.MostRecentPlanDescription = *plan

		aggInterval := tree.MustBeDInterval(row[7]).Duration

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData:             metadata.Key,
				AggregatedTs:        aggregatedTs,
				AggregationInterval: time.Duration(aggInterval.Nanos()),
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
) ([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, error) {

	query := fmt.Sprintf(
		`SELECT
				app_name,
				max(aggregated_ts) as aggregated_ts,
				fingerprint_id,
				metadata,
				crdb_internal.merge_transaction_stats(array_agg(statistics)) AS statistics,
				aggregation_interval
			FROM crdb_internal.transaction_statistics %s
			GROUP BY
				app_name,
				fingerprint_id,
				metadata,
				aggregation_interval
			%s`, whereClause, orderAndLimit)

	const expectedNumDatums = 6

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

		aggInterval := tree.MustBeDInterval(row[5]).Duration

		txnStats := serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			StatsData: roachpb.CollectedTransactionStatistics{
				StatementFingerprintIDs:  metadata.StatementFingerprintIDs,
				App:                      app,
				Stats:                    metadata.Stats,
				AggregatedTs:             aggregatedTs,
				AggregationInterval:      time.Duration(aggInterval.Nanos()),
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
	whereClause, args, err := getStatementDetailsQueryClausesAndArgs(req, testingKnobs)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	statementTotal, err := getTotalStatementDetails(ctx, ie, whereClause, args)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	statementsPerAggregatedTs, err := getStatementDetailsPerAggregatedTs(ctx, ie, whereClause, args, limit)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	statementsPerPlanHash, err := getStatementDetailsPerPlanHash(ctx, ie, whereClause, args, limit)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	response := &serverpb.StatementDetailsResponse{
		Statement:                 statementTotal,
		StatementsPerAggregatedTs: statementsPerAggregatedTs,
		StatementsPerPlanHash:     statementsPerPlanHash,
		InternalAppNamePrefix:     catconstants.InternalAppNamePrefix,
	}

	return response, nil
}

// getStatementDetailsQueryClausesAndArgs returns whereClause and its arguments.
// The whereClause will be in the format `WHERE A = $1 AND B = $2` and
// args will return the list of arguments in order that will replace the actual values.
func getStatementDetailsQueryClausesAndArgs(
	req *serverpb.StatementDetailsRequest, testingKnobs *sqlstats.TestingKnobs,
) (whereClause string, args []interface{}, err error) {
	var buffer strings.Builder
	buffer.WriteString(testingKnobs.GetAOSTClause())

	fingerprintID, err := strconv.ParseUint(req.FingerprintId, 10, 64)
	if err != nil {
		return "", nil, err
	}
	args = append(args, strconv.FormatUint(fingerprintID, 16))
	buffer.WriteString(fmt.Sprintf(" WHERE encode(fingerprint_id, 'hex') = $%d", len(args)))

	// Filter out internal statements by app name.
	buffer.WriteString(fmt.Sprintf(" AND app_name NOT LIKE '%s%%'", catconstants.InternalAppNamePrefix))

	// Statements are grouped ignoring the app name in the Statements/Transactions page, so when
	// calling for the Statement Details endpoint, this value can be empty or a list of app names.
	if len(req.AppNames) > 0 {
		if !(len(req.AppNames) == 1 && req.AppNames[0] == "") {
			buffer.WriteString(" AND (")
			for i, app := range req.AppNames {
				if i != 0 {
					args = append(args, app)
					buffer.WriteString(fmt.Sprintf(" OR app_name = $%d", len(args)))
				} else {
					args = append(args, app)
					buffer.WriteString(fmt.Sprintf(" app_name = $%d", len(args)))
				}
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

// getTotalStatementDetails return all the statistics for the selectec statement combined.
func getTotalStatementDetails(
	ctx context.Context, ie *sql.InternalExecutor, whereClause string, args []interface{},
) (serverpb.StatementDetailsResponse_CollectedStatementSummary, error) {
	query := fmt.Sprintf(
		`SELECT
				metadata,
				aggregation_interval,
				prettify_statement(metadata ->> 'query', %d, %d, %d) as query,
				array_agg(app_name) as app_names,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
				max(sampled_plan) as sampled_plan
		FROM crdb_internal.statement_statistics %s
		GROUP BY
				metadata,
				aggregation_interval
		LIMIT 1`, tree.ConsoleLineWidth, tree.PrettyAlignAndDeindent, tree.UpperCase, whereClause)

	const expectedNumDatums = 6
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
	metadataJSON := tree.MustBeDJSON(row[0]).JSON
	if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJSON, &statistics); err != nil {
		return statement, serverError(ctx, err)
	}

	aggInterval := tree.MustBeDInterval(row[1]).Duration
	queryPrettify := string(tree.MustBeDString(row[2]))

	apps := tree.MustBeDArray(row[3])
	var appNames []string
	for _, s := range apps.Array {
		appNames = util.CombineUniqueString(appNames, []string{string(tree.MustBeDString(s))})
	}

	statsJSON := tree.MustBeDJSON(row[4]).JSON
	if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &statistics.Stats); err != nil {
		return statement, serverError(ctx, err)
	}

	planJSON := tree.MustBeDJSON(row[5]).JSON
	plan, err := sqlstatsutil.JSONToExplainTreePlanNode(planJSON)
	if err != nil {
		return statement, serverError(ctx, err)
	}
	statistics.Stats.SensitiveInfo.MostRecentPlanDescription = *plan

	statement = serverpb.StatementDetailsResponse_CollectedStatementSummary{
		KeyData:             statistics.Key,
		FormattedQuery:      queryPrettify,
		AppNames:            appNames,
		AggregationInterval: time.Duration(aggInterval.Nanos()),
		Stats:               statistics.Stats,
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
				metadata,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
				max(sampled_plan) as sampled_plan,
				aggregation_interval
		FROM crdb_internal.statement_statistics %s
		GROUP BY
				aggregated_ts,
				metadata,
				aggregation_interval
		LIMIT $%d`, whereClause, len(args)+1)

	args = append(args, limit)
	const expectedNumDatums = 5

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
		metadataJSON := tree.MustBeDJSON(row[1]).JSON
		if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, serverError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
		}

		planJSON := tree.MustBeDJSON(row[3]).JSON
		plan, err := sqlstatsutil.JSONToExplainTreePlanNode(planJSON)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		metadata.Stats.SensitiveInfo.MostRecentPlanDescription = *plan

		aggInterval := tree.MustBeDInterval(row[4]).Duration

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs{
			AggregatedTs:        aggregatedTs,
			AggregationInterval: time.Duration(aggInterval.Nanos()),
			Stats:               metadata.Stats,
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
				metadata,
				crdb_internal.merge_statement_stats(array_agg(statistics)) AS statistics,
				max(sampled_plan) as sampled_plan,
				aggregation_interval
		FROM crdb_internal.statement_statistics %s
		GROUP BY
				plan_hash,
				plan_gist,
				metadata,
				aggregation_interval
		LIMIT $%d`, whereClause, len(args)+1)

	args = append(args, limit)
	const expectedNumDatums = 6

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
		planGist := string(tree.MustBeDString(row[1]))
		explainPlan := getExplainPlanFromGist(ctx, ie, planGist)

		var metadata roachpb.CollectedStatementStatistics
		metadataJSON := tree.MustBeDJSON(row[2]).JSON
		if err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJSON, &metadata); err != nil {
			return nil, serverError(ctx, err)
		}

		statsJSON := tree.MustBeDJSON(row[3]).JSON
		if err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJSON, &metadata.Stats); err != nil {
			return nil, serverError(ctx, err)
		}

		planJSON := tree.MustBeDJSON(row[4]).JSON
		plan, err := sqlstatsutil.JSONToExplainTreePlanNode(planJSON)
		if err != nil {
			return nil, serverError(ctx, err)
		}
		metadata.Stats.SensitiveInfo.MostRecentPlanDescription = *plan

		aggInterval := tree.MustBeDInterval(row[5]).Duration

		stmt := serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash{
			AggregationInterval: time.Duration(aggInterval.Nanos()),
			ExplainPlan:         explainPlan,
			PlanHash:            planHash,
			Stats:               metadata.Stats,
		}

		statements = append(statements, stmt)
	}
	if err != nil {
		return nil, serverError(ctx, err)
	}

	return statements, nil
}
