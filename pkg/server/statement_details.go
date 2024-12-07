// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
	"github.com/cockroachdb/errors"
)

const (
	mergeAggStmtMetadataColumnLatest = "merge_aggregated_stmt_metadata(metadata)"
	mergeAggStmtMetadata_V23_2       = "crdb_internal.merge_aggregated_stmt_metadata(array_agg(metadata))"
)

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

// statementDetailsRespBuilder stores values needed to build queries
// to retrieve the statement details.
// We'll pursue a larger refactor later, but this struct will
// suffice in the short term to allow us to stop sending the
// same values across multiple functions.
// We will not refactor the repetitive logic occurring in choosing the
// source table just yet.
type statementDetailsRespBuilder struct {
	ie                          *sql.InternalExecutor
	mergeAggStmtMetadataColExpr string
	whereClause                 string
	qargs                       []interface{}
	limit                       int64
	activityHasData             bool
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

	// Check if the activity tables have data within the selected period.
	reqStartTime := getTimeFromSeconds(req.Start)
	activityHasData, err := activityTablesHaveFullData(
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

	// This expression is used to merge the metadata column from statement
	// activity table.
	mergeAggStmtMetadataColExpr := mergeAggStmtMetadataColumnLatest

	rb := statementDetailsRespBuilder{
		ie:                          ie,
		mergeAggStmtMetadataColExpr: mergeAggStmtMetadataColExpr,
		whereClause:                 whereClause,
		qargs:                       args,
		limit:                       limit,
		activityHasData:             activityHasData,
	}

	statementTotal, err := rb.getTotalStatementDetails(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	statementStatisticsPerAggregatedTs, err := rb.getStatementDetailsPerAggregatedTs(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	statementStatisticsPerPlanHash, err := rb.getStatementDetailsPerPlanHash(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// At this point the counts on statementTotal.metadata have the count for how many times we saw that value
	// as a row, and not the count of executions for each value.
	// The values on statementStatisticsPerPlanHash.Metadata.*Count have the correct count,
	// since the metadata is unique per plan hash.
	// Update the statementTotal.Metadata.*Count with the counts from statementStatisticsPerPlanHash.keyData.
	statementTotal.Metadata.DistSQLCount = 0
	statementTotal.Metadata.FullScanCount = 0
	statementTotal.Metadata.VecCount = 0
	statementTotal.Metadata.TotalCount = 0
	for _, planStats := range statementStatisticsPerPlanHash {
		statementTotal.Metadata.DistSQLCount += planStats.Metadata.DistSQLCount
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
func (rb *statementDetailsRespBuilder) getTotalStatementDetails(
	ctx context.Context,
) (serverpb.StatementDetailsResponse_CollectedStatementSummary, error) {
	const expectedNumDatums = 4
	var statement serverpb.StatementDetailsResponse_CollectedStatementSummary
	const queryFormat = `
SELECT merge_stats_metadata(metadata)    AS metadata,
       array_agg(app_name)               AS app_names,
       merge_statement_stats(statistics) AS statistics,
       encode(fingerprint_id, 'hex')     AS fingerprint_id
FROM %s %s
GROUP BY
    fingerprint_id
LIMIT 1`

	var row tree.Datums
	var err error

	if rb.activityHasData {
		row, err = rb.ie.QueryRowEx(ctx, "combined-stmts-activity-details-total", nil,
			sessiondata.NodeUserSessionDataOverride, fmt.Sprintf(`
SELECT %s                                  AS metadata,
       array_agg(app_name)                 AS app_names,
       merge_statement_stats(statistics)   AS statistics,
       encode(fingerprint_id, 'hex')       AS fingerprint_id
FROM crdb_internal.statement_activity %s
GROUP BY
    fingerprint_id
LIMIT 1`, rb.mergeAggStmtMetadataColExpr, rb.whereClause), rb.qargs...)
		if err != nil {
			return statement, srverrors.ServerError(ctx, err)
		}
	}
	// If there are no results from the activity table, retrieve the data from the persisted table.
	if row == nil || row.Len() == 0 {
		row, err = rb.ie.QueryRowEx(ctx, "combined-stmts-persisted-details-total", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(
				queryFormat,
				CrdbInternalStmtStatsPersisted,
				rb.whereClause), rb.qargs...)
		if err != nil {
			return statement, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if row.Len() == 0 {
		row, err = rb.ie.QueryRowEx(ctx, "combined-stmts-details-total-with-memory", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(queryFormat, CrdbInternalStmtStatsCombined, rb.whereClause), rb.qargs...)
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
func (rb *statementDetailsRespBuilder) getStatementDetailsPerAggregatedTs(
	ctx context.Context,
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByAggregatedTs, error) {
	const expectedNumDatums = 3
	const queryFormat = `
SELECT aggregated_ts,
       merge_stats_metadata(metadata)    AS metadata,
       merge_statement_stats(statistics) AS statistics
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
	args := rb.qargs[:]
	args = append(args, rb.limit)

	if rb.activityHasData {
		it, err = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-activity-details-by-aggregated-timestamp", nil,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`
SELECT aggregated_ts,
       %s                                  AS metadata,
       merge_statement_stats(statistics)   AS statistics
FROM crdb_internal.statement_activity %s
GROUP BY
    aggregated_ts
ORDER BY aggregated_ts ASC
LIMIT $%d`, rb.mergeAggStmtMetadataColExpr, rb.whereClause, len(args)),
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
			CrdbInternalStmtStatsPersisted,
			rb.whereClause,
			len(args))

		it, err = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-details-by-aggregated-timestamp", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)

		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		query = fmt.Sprintf(queryFormat, CrdbInternalStmtStatsCombined, rb.whereClause, len(args))
		it, err = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-details-by-aggregated-timestamp-with-memory", nil,
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
func (rb *statementDetailsRespBuilder) getStatementDetailsPerPlanHash(
	ctx context.Context,
) ([]serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash, error) {
	expectedNumDatums := 5
	const queryFormat = `
SELECT plan_hash,
       (statistics -> 'statistics' -> 'planGists' ->> 0)   AS plan_gist,
       merge_stats_metadata(metadata)                      AS metadata,
       merge_statement_stats(statistics)                   AS statistics,
       index_recommendations
FROM %s %s
GROUP BY
    plan_hash,
    plan_gist,
    index_recommendations
LIMIT $%d`

	args := rb.qargs[:]
	args = append(args, rb.limit)
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

	if rb.activityHasData {
		it, iterErr = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-activity-details-by-plan-hash", nil,
			sessiondata.NodeUserSessionDataOverride, fmt.Sprintf(`
SELECT plan_hash,
       (statistics -> 'statistics' -> 'planGists' ->> 0)   AS plan_gist,
       %s                                                  AS metadata,
       merge_statement_stats(statistics)                   AS statistics,
       index_recommendations
FROM crdb_internal.statement_activity %s
GROUP BY
    plan_hash,
    plan_gist,
    index_recommendations
LIMIT $%d`, rb.mergeAggStmtMetadataColExpr, rb.whereClause, len(args)), args...)
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
			"crdb_internal.statement_statistics_persisted",
			rb.whereClause,
			len(args))
		it, iterErr = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-persisted-details-by-plan-hash", nil,
			sessiondata.NodeUserSessionDataOverride, query, args...)
		if iterErr != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
	}

	// If there are no results from the persisted table, retrieve the data from the combined view
	// with data in-memory.
	if !it.HasResults() {
		err = closeIterator(it, err)
		query = fmt.Sprintf(queryFormat, CrdbInternalStmtStatsCombined, rb.whereClause, len(args))
		it, iterErr = rb.ie.QueryIteratorEx(ctx, "console-combined-stmts-details-by-plan-hash-with-memory", nil,
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
			explainPlan = getExplainPlanFromGist(ctx, rb.ie, planGist)
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
		if aggregatedMetadata.FullScanCount > 0 {
			aggregatedMetadata.FullScanCount = metadata.Stats.Count
		}
		if aggregatedMetadata.VecCount > 0 {
			aggregatedMetadata.VecCount = metadata.Stats.Count
		}
		aggregatedMetadata.TotalCount = metadata.Stats.Count

		var indexes []string
		for _, idx := range metadata.Stats.Indexes {
			indexes = append(indexes, getIdxAndTableName(ctx, rb.ie, idx))
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
