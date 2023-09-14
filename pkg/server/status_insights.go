package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// StmtInsightsDetails represents the structure of details column
// in system.statement_execution_insights table.
// Reminder for future maintainers: keep this struct backward compatible.
// Do not remove/rename already existing fields.
type StmtInsightsDetails struct {
	RowsRead    int64 `json:"rows_read"`
	RowsWritten int64 `json:"rows_written"`
}

func (d *StmtInsightsDetails) ToJSON() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

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
		StatementInsights: make([]*insights.Statement, 0),
	}

	// check if all information is available from persisted storage.
	// select count(end_time) from statement_execution_insights where end_time >= req.end_time
	// if count > 0 then we have all records persisted, do select

	// Query persisted statement insights
	persistedResp, err := s.QueryPersistedStatementInsights(ctx, req)
	if err != nil {
		return nil, err
	}
	for _, stmtInsight := range persistedResp.StatementInsights {
		resp.StatementInsights = append(resp.StatementInsights, stmtInsight)
	}
	// Request in-memory statement insights in case persisted data is incomplete.
	inMemoryInsights, err := s.ListExecutionInsights(ctx, &serverpb.ListExecutionInsightsRequest{WithContentionInfo: true})
	if err != nil {
		return nil, err
	}
	for _, insight := range inMemoryInsights.Insights {
		for _, stmt := range insight.Statements {
			if req.StatementID != nil && stmt.ID != *req.StatementID {
				continue
			}
			if req.StmtFingerprintID != appstatspb.StmtFingerprintID(0) && stmt.FingerprintID != req.StmtFingerprintID {
				continue
			}
			if req.StartTime != nil && req.EndTime != nil &&
				!timeutil.IsOverlappingTimeRanges(*req.StartTime, *req.EndTime, stmt.StartTime, stmt.EndTime) {
				continue
			}
			resp.StatementInsights = append(resp.StatementInsights, stmt)
		}
	}
	return resp, nil
}

// QueryPersistedStatementInsights returns statement insights from the storage
// that satisfy provided filters in req.
func (s *statusServer) QueryPersistedStatementInsights(
	ctx context.Context,
	req *serverpb.StatementExecutionInsightsRequest,
) (*serverpb.StatementExecutionInsightsResponse, error) {
	resp := serverpb.StatementExecutionInsightsResponse{}
	iter, err := queryStmtInsightsIterator(ctx, s.internalExecutor, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = iter.Close()
	}()
	for ok, innerErr := iter.Next(ctx); ok; ok, err = iter.Next(ctx) {
		if innerErr != nil {
			return nil, innerErr
		}
		row := iter.Cur()
		if row == nil {
			return nil, errors.New("unexpected null row on statement_execution_insights")
		}
		stmtInsight, err := decodeStmtInsight(row)
		if err != nil {
			return nil, err
		}
		resp.StatementInsights = append(resp.StatementInsights, stmtInsight)
	}
	return &resp, nil
}

func queryStmtInsightsIterator(
	ctx context.Context,
	ie *sql.InternalExecutor,
	req *serverpb.StatementExecutionInsightsRequest,
) (isql.Rows, error) {
	query := strings.Builder{}
	args := make([]interface{}, 0)
	query.WriteString(`
		SELECT
			statement_id,
			statement_fingerprint_id,
			problem,
			causes,
			query,
			status,
			start_time,
			end_time,
			full_scan,
			database_name,
			plan_gist,
			retries,
			last_retry_reason,
			execution_node_ids,
			index_recommendations,
			cpu_sql_nanos,
			error_code,
			service_lat_seconds,
			contention_time,
			contention_info,
			details
		FROM system.statement_execution_insights
		WHERE true
		`)
	if req.StatementID != nil {
		args = append(args, req.StatementID)
		query.WriteString(fmt.Sprintf("AND statement_id = $%d", len(args)))
	}
	if req.StmtFingerprintID > 0 {
		args = append(args, req.StatementID)
		query.WriteString(fmt.Sprintf("AND stmt_fingerprint_id = $%d", len(args)))
	}
	if req.StartTime != nil {
		args = append(args, req.StartTime)
		query.WriteString(fmt.Sprintf("AND start_time = $%d", len(args)))
	}
	if req.EndTime != nil {
		args = append(args, req.EndTime)
		query.WriteString(fmt.Sprintf("AND end_time = $%d", len(args)))
	}
	return ie.QueryIteratorEx(
		ctx,
		"stmt-exec-insights",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		query.String(),
		args...,
	)
}

// decodeStmtInsight maps data returned as result from queryStmtInsightsIterator func to
// insights.Statement struct. It tightly depends on the query defined in
// queryStmtInsightsIterator func and has to be updated if query is changed.
func decodeStmtInsight(datum tree.Datums) (*insights.Statement, error) {
	stmtID, err := clusterunique.IDFromString(string(tree.MustBeDString(datum[0]))) // statement_id
	if err != nil {
		return nil, err
	}

	stmtFingerprintID, err := appstatspb.DecodeFingerprintID[appstatspb.StmtFingerprintID]([]byte(tree.MustBeDBytes(datum[1]))) // statement_fingerprint_id
	if err != nil {
		return nil, err
	}

	problem := insights.Problem(insights.Problem_value[string(tree.MustBeDString(datum[2]))]) // problem
	dCauses := tree.MustBeDArray(datum[3]).Array                                              // causes
	causes := make([]insights.Cause, dCauses.Len())
	for i, d := range dCauses {
		causes[i] = insights.Cause(insights.Cause_value[string(tree.MustBeDString(d))])
	}
	stmtQuery := string(tree.MustBeDString(datum[4]))                                                              // query
	stmtStatus := insights.Statement_Status(insights.Statement_Status_value[string(tree.MustBeDString(datum[5]))]) // status
	// TODO: startTime and endTime should be tree.MustBeDTimestampTZ - timezone!
	startTime := tree.MustBeDTimestamp(datum[6]).Time // start_time
	var endTime time.Time
	if dEndTime, ok := tree.AsDTimestamp(datum[7]); ok { // end_time
		endTime = dEndTime.Time
	}
	fullScan := bool(tree.MustBeDBool(datum[8]))      // full_scan
	dbName := string(tree.MustBeDString(datum[9]))    // database_name
	planGist := string(tree.MustBeDString(datum[10])) // plan_gist
	retries := int64(tree.MustBeDInt(datum[11]))      // retries
	retryReason, _ := tree.AsDString(datum[12])       // last_retry_reason
	execNodes := tree.MustBeDArray(datum[13])         // nodes
	nodes := make([]int64, execNodes.Len())
	for i, n := range execNodes.Array {
		nodes[i] = int64(tree.MustBeDInt(n))
	}
	dIndexRecommendations := tree.MustBeDArray(datum[14]) // index_recommendations
	indexRecommendations := make([]string, dIndexRecommendations.Len())
	for i, d := range dIndexRecommendations.Array {
		indexRecommendations[i] = string(tree.MustBeDString(d))
	}
	cpuUsageNanos := int64(tree.MustBeDInt(datum[15]))         // cpu_sql_nanos
	errorCode, _ := tree.AsDString(datum[16])                  // error_code
	serviceLatSeconds := float64(tree.MustBeDFloat(datum[17])) // service_lat_seconds
	var contentionTime time.Duration
	if c, ok := tree.AsDInterval(datum[18]); ok { // contention_time
		if durationInSeconds, ok := c.AsInt64(); ok {
			contentionTime = time.Duration(durationInSeconds) * time.Second
		}
	}
	var contentionInfo insights.ContentionInfo
	if dContentionInfo, ok := tree.AsDJSON(datum[19]); ok { // contention_info
		if err = json.Unmarshal([]byte(dContentionInfo.JSON.String()), &contentionInfo); err != nil {
			return nil, err
		}
	}
	var details StmtInsightsDetails
	if dDetails, ok := tree.AsDJSON(datum[20]); ok { // details
		if err = json.Unmarshal([]byte(dDetails.JSON.String()), &details); err != nil {
			return nil, err
		}
	}
	stmtInsight := insights.Statement{
		ID:                   stmtID,
		FingerprintID:        *stmtFingerprintID,
		LatencyInSeconds:     serviceLatSeconds,
		Query:                stmtQuery,
		Status:               stmtStatus,
		StartTime:            startTime,
		EndTime:              endTime,
		FullScan:             fullScan,
		Database:             dbName,
		PlanGist:             planGist,
		RowsRead:             details.RowsRead,
		RowsWritten:          details.RowsWritten,
		Retries:              retries,
		AutoRetryReason:      string(retryReason),
		Nodes:                nodes,
		Contention:           &contentionTime,
		IndexRecommendations: indexRecommendations,
		Problem:              problem,
		Causes:               causes,
		CPUSQLNanos:          cpuUsageNanos,
		ErrorCode:            string(errorCode),
		ContentionInfo:       &contentionInfo,
	}
	return &stmtInsight, nil
}

// PersistStmtInsight persists statement insights into
// system.statement_execution_insights table.
// TODO: find better place where this function should live.
func PersistStmtInsight(
	ctx context.Context,
	ie *sql.InternalExecutor,
	s *insights.Statement,
) (int, error) {
	txnFingerprintID := appstatspb.TransactionFingerprintID(1234)
	causes := make([]string, len(s.Causes))
	for i, cause := range s.Causes {
		causes[i] = cause.String()
	}
	nodes := make([]int, len(s.Nodes))
	for i, node := range s.Nodes {
		nodes[i] = int(node)
	}
	details := StmtInsightsDetails{
		RowsRead:    s.RowsRead,
		RowsWritten: s.RowsWritten,
	}
	detailsJSON, err := details.ToJSON()
	if err != nil {
		return 0, err
	}

	contentionInfo, err := s.ContentionInfo.ToJSON()
	if err != nil {
		return 0, err
	}

	// TODO: insights.Statement proto message doesn't include some fields
	// that are required for `system.statement_execution_insights` table.
	// 1) extend insights.Statement message to include them
	// 2) replace dummy/hardcoded values with real one.
	args := []interface{}{
		"session_id",               // session_id
		uuid.FastMakeV4().String(), // transaction_id
		txnFingerprintID.Encode(),  // transaction_fingerprint_id
		s.ID.String(),              // statement_id
		s.FingerprintID.Encode(),   // statement_fingerprint_id
		s.Problem.String(),         // problem
		causes,                     // causes
		s.Query,                    // query
		s.Status.String(),          // status
		s.StartTime,                // start_time
		s.EndTime,                  // end_time
		s.FullScan,                 // full_scan
		"user_name",                // user_name
		"app_name",                 // app_name
		s.Database,                 // database_name
		s.PlanGist,                 // plan_gist
		s.Retries,                  // retries
		s.AutoRetryReason,          // last_retry_reason
		nodes,                      // execution_node_ids
		s.IndexRecommendations,     // index_recommendations
		true,                       // implicit_txn
		s.CPUSQLNanos,              // cpu_sql_nanos
		s.ErrorCode,                // error_code
		s.LatencyInSeconds,         // service_lat_seconds
		0,                          // retry_lat_seconds
		0,                          // commit_lat_seconds
		0,                          // idle_lat_seconds
		s.Contention.Seconds(),     // contention_time
		contentionInfo,             // contention_info
		detailsJSON,                // details
	}
	stmt := `
	INSERT INTO system.statement_execution_insights VALUES 
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
	 $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)`
	return ie.ExecEx(
		ctx,
		"insert-stmt-exec-insight",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		stmt,
		args...,
	)
}
