// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// Query parameter keys, matching the conventions used in api_v2_constants.go.
const (
	sortByKey          = "sortBy"
	sortOrderKey       = "sortOrder"
	pageNumKey         = "pageNum"
	pageSizeKey        = "pageSize"
	searchKey          = "search"
	appNameKey         = "appName"
	databaseKey        = "database"
	excludeInternalKey = "excludeInternal"

	defaultPageSize = 20
	defaultPageNum  = 1
	maxPageSize     = 100
)

// PaginatedResponse wraps any result set with pagination metadata.
// The JSON shape matches the PaginatedResponse used by the database
// metadata endpoints in the server package.
type PaginatedResponse[T any] struct {
	Results        T              `json:"results"`
	PaginationInfo paginationInfo `json:"pagination_info"`
}

type paginationInfo struct {
	TotalResults int64 `json:"total_results"`
	PageSize     int   `json:"page_size"`
	PageNum      int   `json:"page_num"`
}

// StatementRow represents a single row of aggregated statement statistics.
type StatementRow struct {
	FingerprintID        string  `json:"fingerprint_id"`
	ExecutionCount       int64   `json:"execution_count"`
	SvcLatMean           float64 `json:"svc_lat_mean"`
	SvcLatStdDev         float64 `json:"svc_lat_stddev"`
	CPUSQLNanosMean      float64 `json:"cpu_sql_nanos_mean"`
	CPUSQLNanosStdDev    float64 `json:"cpu_sql_nanos_stddev"`
	ContentionTimeMean   float64 `json:"contention_time_mean"`
	ContentionTimeStdDev float64 `json:"contention_time_stddev"`
	KVCPUTimeNanosMean   float64 `json:"kv_cpu_time_nanos_mean"`
	KVCPUTimeNanosStdDev float64 `json:"kv_cpu_time_nanos_stddev"`
	AdmWaitTimeMean      float64 `json:"admission_wait_time_mean"`
	AdmWaitTimeStdDev    float64 `json:"admission_wait_time_stddev"`
	RowsReadMean         float64 `json:"rows_read_mean"`
	RowsWrittenMean      float64 `json:"rows_written_mean"`
	BytesReadMean        float64 `json:"bytes_read_mean"`
	BytesReadStdDev      float64 `json:"bytes_read_stddev"`
	MaxRetries           int64   `json:"max_retries"`
	AppName              string  `json:"app_name"`
	PctOfTotalRuntime    float64 `json:"pct_of_total_runtime"`
	Query                string  `json:"query"`
	QuerySummary         string  `json:"query_summary"`
	Database             string  `json:"database"`
}

// TransactionRow represents a single row of aggregated transaction statistics.
type TransactionRow struct {
	FingerprintID        string   `json:"fingerprint_id"`
	ExecutionCount       int64    `json:"execution_count"`
	SvcLatMean           float64  `json:"svc_lat_mean"`
	SvcLatStdDev         float64  `json:"svc_lat_stddev"`
	CPUSQLNanosMean      float64  `json:"cpu_sql_nanos_mean"`
	CPUSQLNanosStdDev    float64  `json:"cpu_sql_nanos_stddev"`
	ContentionTimeMean   float64  `json:"contention_time_mean"`
	ContentionTimeStdDev float64  `json:"contention_time_stddev"`
	KVCPUTimeNanosMean   float64  `json:"kv_cpu_time_nanos_mean"`
	KVCPUTimeNanosStdDev float64  `json:"kv_cpu_time_nanos_stddev"`
	AdmWaitTimeMean      float64  `json:"admission_wait_time_mean"`
	AdmWaitTimeStdDev    float64  `json:"admission_wait_time_stddev"`
	RowsReadMean         float64  `json:"rows_read_mean"`
	RowsWrittenMean      float64  `json:"rows_written_mean"`
	BytesReadMean        float64  `json:"bytes_read_mean"`
	BytesReadStdDev      float64  `json:"bytes_read_stddev"`
	MaxRetries           int64    `json:"max_retries"`
	CommitLatMean        float64  `json:"commit_lat_mean"`
	CommitLatStdDev      float64  `json:"commit_lat_stddev"`
	AppName              string   `json:"app_name"`
	PctOfTotalRuntime    float64  `json:"pct_of_total_runtime"`
	QuerySummaries       []string `json:"query_summaries"`
}

// stmtSortColumns maps API sort column names to SQL expressions for statements.
var stmtSortColumns = map[string]string{
	"execution_count":      "cnt",
	"svc_lat_mean":         "svc_lat_mean",
	"cpu_sql_nanos_mean":   "cpu_sql_nanos_mean",
	"contention_time_mean": "contention_time_mean",
	"kv_cpu_time_mean":     "kv_cpu_time_nanos_mean",
	"adm_wait_time_mean":   "admission_wait_time_mean",
	"pct_of_total_runtime": "pct_of_total_runtime",
	"rows_read_mean":       "rows_read_mean",
	"rows_written_mean":    "rows_written_mean",
	"bytes_read_mean":      "bytes_read_mean",
	"max_retries":          "max_retries",
}

// txnSortColumns maps API sort column names to SQL expressions for
// transactions.
var txnSortColumns = map[string]string{
	"execution_count":      "cnt",
	"svc_lat_mean":         "svc_lat_mean",
	"cpu_sql_nanos_mean":   "cpu_sql_nanos_mean",
	"contention_time_mean": "contention_time_mean",
	"kv_cpu_time_mean":     "kv_cpu_time_nanos_mean",
	"adm_wait_time_mean":   "admission_wait_time_mean",
	"pct_of_total_runtime": "pct_of_total_runtime",
	"rows_read_mean":       "rows_read_mean",
	"rows_written_mean":    "rows_written_mean",
	"bytes_read_mean":      "bytes_read_mean",
	"max_retries":          "max_retries",
	"commit_lat_mean":      "commit_lat_mean",
}

// validateSortOrderValue validates the sort order value and returns the
// SQL sort order value and a boolean indicating if the value is valid.
// If the value is empty, it defaults to "DESC".
func validateSortOrderValue(sortOrder string) (string, bool) {
	toUpper := strings.ToUpper(sortOrder)
	switch toUpper {
	case "":
		return "DESC", true
	case "ASC", "DESC":
		return toUpper, true
	default:
		return "", false
	}
}

// defaultTimeRange is the fallback time window used when clients omit the
// start/end query parameters. Without a bound the queries would do a full
// table scan of the statistics tables, which under write-heavy workloads
// causes severe read-write contention and can hang the cluster.
const defaultTimeRange = 1 * time.Hour

// parseTimeRange extracts start and end timestamps from query params.
// Both are expected as integer seconds since the Unix epoch. If neither
// is provided, the range defaults to the last defaultTimeRange.
func parseTimeRange(r *http.Request) (start, end time.Time, err error) {
	if s := r.URL.Query().Get("start"); s != "" {
		sec, parseErr := strconv.ParseInt(s, 10, 64)
		if parseErr != nil {
			return time.Time{}, time.Time{},
				fmt.Errorf("invalid start time: expected seconds since epoch: %w", parseErr)
		}
		start = time.Unix(sec, 0).UTC()
	}
	if e := r.URL.Query().Get("end"); e != "" {
		sec, parseErr := strconv.ParseInt(e, 10, 64)
		if parseErr != nil {
			return time.Time{}, time.Time{},
				fmt.Errorf("invalid end time: expected seconds since epoch: %w", parseErr)
		}
		end = time.Unix(sec, 0).UTC()
	}
	// Default to the last hour when no time range is specified, to avoid
	// unbounded full-table scans on hot statistics tables.
	if start.IsZero() && end.IsZero() {
		end = time.Now().UTC()
		start = end.Add(-defaultTimeRange)
	}
	return start, end, nil
}

// safeStdDev computes sqrt(sumSq/count - mean^2), returning 0 if the value
// would be negative (due to floating-point imprecision) or if count is 0.
func safeStdDev(sumSq, count, mean float64) float64 {
	if count == 0 {
		return 0
	}
	variance := sumSq/count - mean*mean
	if variance < 0 {
		return 0
	}
	return math.Sqrt(variance)
}

// GetStatements returns paginated, sorted, aggregated statement statistics.
//
// Query Parameters:
//   - start: seconds since Unix epoch for the beginning of the time range.
//   - end: seconds since Unix epoch for the end of the time range.
//   - pageSize: number of results per page (default 20, max 100).
//   - pageNum: 1-based page number (default 1).
//   - sortBy: column to sort by (default "svc_lat_mean").
//   - sortOrder: "ASC" or "DESC" (default "DESC").
//   - search: case-insensitive substring match against the statement
//     fingerprint text.
//   - appName: exact match on the application name.
//   - database: exact match on the database name (from system.statements).
//   - excludeInternal: when "false", include internal app names (default
//     true, i.e. internal statements are excluded).
//
// @Summary Get aggregated statement statistics
// @Description Returns paginated, sorted statement statistics aggregated by
// @Description fingerprint and app_name for the given time range.
// @Tags SQL Activity
// @Produce json
// @Param start query int false "Start time (seconds since epoch)"
// @Param end query int false "End time (seconds since epoch)"
// @Param pageSize query int false "Page size" default(20)
// @Param pageNum query int false "Page number" default(1)
// @Param sortBy query string false "Sort column" default(svc_lat_mean)
// @Param sortOrder query string false "Sort order" default(DESC)
// @Param search query string false "Search fingerprint text"
// @Param appName query string false "Application name filter"
// @Param database query string false "Database name filter"
// @Param excludeInternal query string false "Exclude internal app names" default(true)
// @Success 200 {object} PaginatedResponse[[]StatementRow]
// @Failure 400 {object} ErrorResponse "Bad request"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /statements [get]
func (api *ApiV2DBConsole) GetStatements(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queryValues := r.URL.Query()
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageNumKey),
			http.StatusBadRequest)
		return
	}
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageSizeKey),
			http.StatusBadRequest)
		return
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if pageNum <= 0 {
		pageNum = defaultPageNum
	}

	offset := (pageNum - 1) * pageSize

	sortByQs := queryValues.Get(sortByKey)
	var sortBy string
	if col, ok := stmtSortColumns[sortByQs]; ok {
		sortBy = col
	} else {
		sortBy = "svc_lat_mean"
	}

	var sortOrder string
	if sortByQs != "" {
		sortOrder, _ = validateSortOrderValue(queryValues.Get(sortOrderKey))
	} else {
		sortOrder = "DESC"
	}

	search := queryValues.Get(searchKey)
	appName := queryValues.Get(appNameKey)
	database := queryValues.Get(databaseKey)
	excludeInternal := queryValues.Get(excludeInternalKey) != "false"

	// Build the count query. Two copies of queryFilters are needed because
	// filter methods are stateful (they append to args and advance the
	// placeholder index).
	countFilters := newQueryFilters(start, end, search, appName, database, excludeInternal)
	countTimeWhere := countFilters.timeWhereClause()
	countAppFilter := countFilters.appNameFilter("ss")

	countInternalFilter := countFilters.internalFilter("ss")

	var countQuery string
	if countFilters.hasJoinFilter() {
		// When searching or filtering by database, join system.statements.
		countSearchFilter := countFilters.searchFilter()
		countDBFilter := countFilters.databaseFilter()
		countQuery = fmt.Sprintf(`
SELECT count(*) FROM (
  SELECT ss.fingerprint_id
  FROM system.statement_statistics ss
  JOIN system.statements q ON q.fingerprint_id = ss.fingerprint_id
  WHERE %s%s%s%s%s
  GROUP BY ss.fingerprint_id, ss.app_name
) sub
AS OF SYSTEM TIME follower_read_timestamp()`, countTimeWhere, countInternalFilter, countAppFilter, countSearchFilter, countDBFilter)
	} else {
		countQuery = fmt.Sprintf(`
SELECT count(*) FROM (
  SELECT ss.fingerprint_id
  FROM system.statement_statistics ss
  WHERE %s%s%s
  GROUP BY ss.fingerprint_id, ss.app_name
) sub
AS OF SYSTEM TIME follower_read_timestamp()`, countTimeWhere, countInternalFilter, countAppFilter)
	}

	// Build the data query with its own filter set.
	dataFilters := newQueryFilters(start, end, search, appName, database, excludeInternal)
	dataTimeWhere := dataFilters.timeWhereClause()
	dataInternalFilter := dataFilters.internalFilter("ss")
	dataAppFilter := dataFilters.appNameFilter("ss")
	dataSearchFilter := dataFilters.searchFilter()
	dataDBFilter := dataFilters.databaseFilter()

	// Main data query. All aggregate columns come from the covering index
	// (stmt_fp_ts_cov_counts) since they are STORED columns in that index,
	// and the WHERE clause matches the partial index predicate.
	// The JOIN with system.statements fetches query text and summary without
	// touching the statistics JSONB column. When a search filter is active
	// the LEFT JOIN effectively becomes an INNER JOIN via the WHERE clause.
	dataQuery := fmt.Sprintf(`
SELECT
  encode(ss.fingerprint_id, 'hex') AS fingerprint_id,
  SUM(ss.execution_count)::INT8 AS cnt,
  SUM(ss.svc_lat_sum) / NULLIF(SUM(ss.execution_count::FLOAT8), 0)
    AS svc_lat_mean,
  SUM(ss.cpu_sql_nanos_sum) / NULLIF(SUM(ss.exec_sample_count::FLOAT8), 0)
    AS cpu_sql_nanos_mean,
  SUM(ss.contention_time_sum) / NULLIF(SUM(ss.exec_sample_count::FLOAT8), 0)
    AS contention_time_mean,
  SUM(ss.kv_cpu_time_nanos_sum) / NULLIF(SUM(ss.execution_count::FLOAT8), 0)
    AS kv_cpu_time_nanos_mean,
  SUM(ss.admission_wait_time_sum) / NULLIF(SUM(ss.exec_sample_count::FLOAT8), 0)
    AS admission_wait_time_mean,
  SUM(ss.svc_lat_sum_sq) AS svc_lat_sum_sq,
  SUM(ss.cpu_sql_nanos_sum_sq) AS cpu_sql_nanos_sum_sq,
  SUM(ss.contention_time_sum_sq) AS contention_time_sum_sq,
  SUM(ss.kv_cpu_time_nanos_sum_sq) AS kv_cpu_time_nanos_sum_sq,
  SUM(ss.admission_wait_time_sum_sq) AS admission_wait_time_sum_sq,
  SUM(ss.execution_count::FLOAT8) AS total_exec_count,
  SUM(ss.exec_sample_count::FLOAT8) AS total_sample_count,
  SUM(ss.rows_read_sum) / NULLIF(SUM(ss.execution_count::FLOAT8), 0)
    AS rows_read_mean,
  SUM(ss.rows_written_sum) / NULLIF(SUM(ss.execution_count::FLOAT8), 0)
    AS rows_written_mean,
  SUM(ss.bytes_read_sum) / NULLIF(SUM(ss.execution_count::FLOAT8), 0)
    AS bytes_read_mean,
  SUM(ss.bytes_read_sum_sq) AS bytes_read_sum_sq,
  MAX(ss.max_retries)::INT8 AS max_retries,
  ss.app_name,
  SUM(ss.svc_lat_sum) / NULLIF(SUM(SUM(ss.svc_lat_sum)) OVER (), 0)
    AS pct_of_total_runtime,
  COALESCE(q.fingerprint, '') AS query,
  COALESCE(q.summary, '') AS query_summary,
  COALESCE(q.db, '') AS database
FROM system.statement_statistics ss
LEFT JOIN system.statements q ON q.fingerprint_id = ss.fingerprint_id
AS OF SYSTEM TIME follower_read_timestamp()
WHERE %s%s%s%s%s
GROUP BY ss.fingerprint_id, ss.app_name, q.fingerprint, q.summary, q.db
ORDER BY %s %s
LIMIT %d OFFSET %d`,
		dataTimeWhere, dataInternalFilter, dataAppFilter, dataSearchFilter, dataDBFilter,
		sortBy, sortOrder,
		pageSize, offset)

	ie := api.InternalDB.Executor()

	// Execute count query.
	countRow, err := ie.QueryRowEx(
		ctx,
		"dbconsole-stmt-stats-count",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		countQuery, countFilters.args...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var totalCount int64
	if countRow != nil && len(countRow) > 0 {
		totalCount = int64(tree.MustBeDInt(countRow[0]))
	}

	// Execute data query.
	it, err := ie.QueryIteratorEx(
		ctx,
		"dbconsole-stmt-stats",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		dataQuery, dataFilters.args...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	defer func() { _ = it.Close() }()

	statements := make([]StatementRow, 0, pageSize)
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
		if !ok {
			break
		}
		row := it.Cur()
		// Column indices match the SELECT order above.
		fpID := string(tree.MustBeDString(row[0]))
		cnt := int64(tree.MustBeDInt(row[1]))
		svcLatMean := float64(tree.MustBeDFloat(row[2]))
		cpuMean := float64(tree.MustBeDFloat(row[3]))
		contentionMean := float64(tree.MustBeDFloat(row[4]))
		kvCPUMean := float64(tree.MustBeDFloat(row[5]))
		admWaitMean := float64(tree.MustBeDFloat(row[6]))
		svcLatSumSq := float64(tree.MustBeDFloat(row[7]))
		cpuSumSq := float64(tree.MustBeDFloat(row[8]))
		contentionSumSq := float64(tree.MustBeDFloat(row[9]))
		kvCPUSumSq := float64(tree.MustBeDFloat(row[10]))
		admWaitSumSq := float64(tree.MustBeDFloat(row[11]))
		totalExecCount := float64(tree.MustBeDFloat(row[12]))
		totalSampleCount := float64(tree.MustBeDFloat(row[13]))
		rowsReadMean := float64(tree.MustBeDFloat(row[14]))
		rowsWrittenMean := float64(tree.MustBeDFloat(row[15]))
		bytesReadMean := float64(tree.MustBeDFloat(row[16]))
		bytesReadSumSq := float64(tree.MustBeDFloat(row[17]))
		maxRetries := int64(tree.MustBeDInt(row[18]))
		appName := string(tree.MustBeDString(row[19]))
		pctRuntime := float64(tree.MustBeDFloat(row[20]))
		query := string(tree.MustBeDString(row[21]))
		querySummary := string(tree.MustBeDString(row[22]))
		database := string(tree.MustBeDString(row[23]))

		statements = append(statements, StatementRow{
			FingerprintID:        fpID,
			ExecutionCount:       cnt,
			SvcLatMean:           svcLatMean,
			SvcLatStdDev:         safeStdDev(svcLatSumSq, totalExecCount, svcLatMean),
			CPUSQLNanosMean:      cpuMean,
			CPUSQLNanosStdDev:    safeStdDev(cpuSumSq, totalSampleCount, cpuMean),
			ContentionTimeMean:   contentionMean,
			ContentionTimeStdDev: safeStdDev(contentionSumSq, totalSampleCount, contentionMean),
			KVCPUTimeNanosMean:   kvCPUMean,
			KVCPUTimeNanosStdDev: safeStdDev(kvCPUSumSq, totalExecCount, kvCPUMean),
			AdmWaitTimeMean:      admWaitMean,
			AdmWaitTimeStdDev:    safeStdDev(admWaitSumSq, totalSampleCount, admWaitMean),
			RowsReadMean:         rowsReadMean,
			RowsWrittenMean:      rowsWrittenMean,
			BytesReadMean:        bytesReadMean,
			BytesReadStdDev:      safeStdDev(bytesReadSumSq, totalExecCount, bytesReadMean),
			MaxRetries:           maxRetries,
			AppName:              appName,
			PctOfTotalRuntime:    pctRuntime,
			Query:                query,
			QuerySummary:         querySummary,
			Database:             database,
		})
	}

	resp := PaginatedResponse[[]StatementRow]{
		Results: statements,
		PaginationInfo: paginationInfo{
			TotalResults: totalCount,
			PageSize:     pageSize,
			PageNum:      pageNum,
		},
	}
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

// GetTransactions returns paginated, sorted, aggregated transaction statistics.
//
// Query Parameters:
//   - start: seconds since Unix epoch for the beginning of the time range.
//   - end: seconds since Unix epoch for the end of the time range.
//   - pageSize: number of results per page (default 20, max 100).
//   - pageNum: 1-based page number (default 1).
//   - sortBy: column to sort by (default "svc_lat_mean").
//   - sortOrder: "ASC" or "DESC" (default "DESC").
//   - appName: exact match on the application name.
//   - excludeInternal: when "false", include internal app names (default
//     true, i.e. internal transactions are excluded).
//
// @Summary Get aggregated transaction statistics
// @Description Returns paginated, sorted transaction statistics aggregated by
// @Description fingerprint and app_name for the given time range.
// @Tags SQL Activity
// @Produce json
// @Param start query int false "Start time (seconds since epoch)"
// @Param end query int false "End time (seconds since epoch)"
// @Param pageSize query int false "Page size" default(20)
// @Param pageNum query int false "Page number" default(1)
// @Param sortBy query string false "Sort column" default(svc_lat_mean)
// @Param sortOrder query string false "Sort order" default(DESC)
// @Param appName query string false "Application name filter"
// @Param excludeInternal query string false "Exclude internal app names" default(true)
// @Success 200 {object} PaginatedResponse[[]TransactionRow]
// @Failure 400 {object} ErrorResponse "Bad request"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /transactions [get]
func (api *ApiV2DBConsole) GetTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	start, end, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queryValues := r.URL.Query()
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageNumKey),
			http.StatusBadRequest)
		return
	}
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageSizeKey),
			http.StatusBadRequest)
		return
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	if pageNum <= 0 {
		pageNum = defaultPageNum
	}

	offset := (pageNum - 1) * pageSize

	sortByQs := queryValues.Get(sortByKey)
	var sortBy string
	if col, ok := txnSortColumns[sortByQs]; ok {
		sortBy = col
	} else {
		sortBy = "svc_lat_mean"
	}

	var sortOrder string
	if sortByQs != "" {
		sortOrder, _ = validateSortOrderValue(queryValues.Get(sortOrderKey))
	} else {
		sortOrder = "DESC"
	}

	appName := queryValues.Get(appNameKey)
	excludeInternal := queryValues.Get(excludeInternalKey) != "false"

	countFilters := newQueryFilters(start, end, "", appName, "" /* database */, excludeInternal)
	whereClause := countFilters.timeWhereClause()
	countInternalFilter := countFilters.internalFilter("ts")
	countAppFilter := countFilters.appNameFilter("ts")

	countQuery := fmt.Sprintf(`
SELECT count(*) FROM (
  SELECT ts.fingerprint_id
  FROM system.transaction_statistics ts
  WHERE %s%s%s
  GROUP BY ts.fingerprint_id, ts.app_name
) sub
AS OF SYSTEM TIME follower_read_timestamp()`, whereClause, countInternalFilter, countAppFilter)

	// Need a separate filter set for the data query since filter methods
	// are stateful.
	dataFilters := newQueryFilters(start, end, "", appName, "" /* database */, excludeInternal)
	dataWhereClause := dataFilters.timeWhereClause()
	dataInternalFilter := dataFilters.internalFilter("ts")
	dataAppFilter := dataFilters.appNameFilter("ts")

	dataQuery := fmt.Sprintf(`
SELECT
  encode(ts.fingerprint_id, 'hex') AS fingerprint_id,
  SUM(ts.execution_count)::INT8 AS cnt,
  SUM(ts.svc_lat_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS svc_lat_mean,
  SUM(ts.cpu_sql_nanos_sum) / NULLIF(SUM(ts.exec_sample_count::FLOAT8), 0)
    AS cpu_sql_nanos_mean,
  SUM(ts.contention_time_sum) / NULLIF(SUM(ts.exec_sample_count::FLOAT8), 0)
    AS contention_time_mean,
  SUM(ts.kv_cpu_time_nanos_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS kv_cpu_time_nanos_mean,
  SUM(ts.admission_wait_time_sum) / NULLIF(SUM(ts.exec_sample_count::FLOAT8), 0)
    AS admission_wait_time_mean,
  SUM(ts.svc_lat_sum_sq) AS svc_lat_sum_sq,
  SUM(ts.cpu_sql_nanos_sum_sq) AS cpu_sql_nanos_sum_sq,
  SUM(ts.contention_time_sum_sq) AS contention_time_sum_sq,
  SUM(ts.kv_cpu_time_nanos_sum_sq) AS kv_cpu_time_nanos_sum_sq,
  SUM(ts.admission_wait_time_sum_sq) AS admission_wait_time_sum_sq,
  SUM(ts.execution_count::FLOAT8) AS total_exec_count,
  SUM(ts.exec_sample_count::FLOAT8) AS total_sample_count,
  SUM(ts.rows_read_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS rows_read_mean,
  SUM(ts.rows_written_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS rows_written_mean,
  SUM(ts.bytes_read_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS bytes_read_mean,
  SUM(ts.bytes_read_sum_sq) AS bytes_read_sum_sq,
  MAX(ts.max_retries)::INT8 AS max_retries,
  SUM(ts.commit_lat_sum) / NULLIF(SUM(ts.execution_count::FLOAT8), 0)
    AS commit_lat_mean,
  SUM(ts.commit_lat_sum_sq) AS commit_lat_sum_sq,
  ts.app_name,
  SUM(ts.svc_lat_sum) / NULLIF(SUM(SUM(ts.svc_lat_sum)) OVER (), 0)
    AS pct_of_total_runtime
FROM system.transaction_statistics ts
  AS OF SYSTEM TIME follower_read_timestamp()
WHERE %s%s%s
GROUP BY ts.fingerprint_id, ts.app_name
ORDER BY %s %s
LIMIT %d OFFSET %d`,
		dataWhereClause, dataInternalFilter, dataAppFilter,
		sortBy, sortOrder,
		pageSize, offset)

	ie := api.InternalDB.Executor()

	countRow, err := ie.QueryRowEx(
		ctx,
		"dbconsole-txn-stats-count",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		countQuery, countFilters.args...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var totalCount int64
	if countRow != nil && len(countRow) > 0 {
		totalCount = int64(tree.MustBeDInt(countRow[0]))
	}

	it, err := ie.QueryIteratorEx(
		ctx,
		"dbconsole-txn-stats",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		dataQuery, dataFilters.args...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	defer func() { _ = it.Close() }()

	transactions := make([]TransactionRow, 0, pageSize)
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
		if !ok {
			break
		}
		row := it.Cur()
		fpID := string(tree.MustBeDString(row[0]))
		cnt := int64(tree.MustBeDInt(row[1]))
		svcLatMean := float64(tree.MustBeDFloat(row[2]))
		cpuMean := float64(tree.MustBeDFloat(row[3]))
		contentionMean := float64(tree.MustBeDFloat(row[4]))
		kvCPUMean := float64(tree.MustBeDFloat(row[5]))
		admWaitMean := float64(tree.MustBeDFloat(row[6]))
		svcLatSumSq := float64(tree.MustBeDFloat(row[7]))
		cpuSumSq := float64(tree.MustBeDFloat(row[8]))
		contentionSumSq := float64(tree.MustBeDFloat(row[9]))
		kvCPUSumSq := float64(tree.MustBeDFloat(row[10]))
		admWaitSumSq := float64(tree.MustBeDFloat(row[11]))
		totalExecCount := float64(tree.MustBeDFloat(row[12]))
		totalSampleCount := float64(tree.MustBeDFloat(row[13]))
		rowsReadMean := float64(tree.MustBeDFloat(row[14]))
		rowsWrittenMean := float64(tree.MustBeDFloat(row[15]))
		bytesReadMean := float64(tree.MustBeDFloat(row[16]))
		bytesReadSumSq := float64(tree.MustBeDFloat(row[17]))
		maxRetries := int64(tree.MustBeDInt(row[18]))
		commitLatMean := float64(tree.MustBeDFloat(row[19]))
		commitLatSumSq := float64(tree.MustBeDFloat(row[20]))
		appName := string(tree.MustBeDString(row[21]))
		pctRuntime := float64(tree.MustBeDFloat(row[22]))

		transactions = append(transactions, TransactionRow{
			FingerprintID:        fpID,
			ExecutionCount:       cnt,
			SvcLatMean:           svcLatMean,
			SvcLatStdDev:         safeStdDev(svcLatSumSq, totalExecCount, svcLatMean),
			CPUSQLNanosMean:      cpuMean,
			CPUSQLNanosStdDev:    safeStdDev(cpuSumSq, totalSampleCount, cpuMean),
			ContentionTimeMean:   contentionMean,
			ContentionTimeStdDev: safeStdDev(contentionSumSq, totalSampleCount, contentionMean),
			KVCPUTimeNanosMean:   kvCPUMean,
			KVCPUTimeNanosStdDev: safeStdDev(kvCPUSumSq, totalExecCount, kvCPUMean),
			AdmWaitTimeMean:      admWaitMean,
			AdmWaitTimeStdDev:    safeStdDev(admWaitSumSq, totalSampleCount, admWaitMean),
			RowsReadMean:         rowsReadMean,
			RowsWrittenMean:      rowsWrittenMean,
			BytesReadMean:        bytesReadMean,
			BytesReadStdDev:      safeStdDev(bytesReadSumSq, totalExecCount, bytesReadMean),
			MaxRetries:           maxRetries,
			CommitLatMean:        commitLatMean,
			CommitLatStdDev:      safeStdDev(commitLatSumSq, totalExecCount, commitLatMean),
			AppName:              appName,
			PctOfTotalRuntime:    pctRuntime,
		})
	}

	// Fetch statement query summaries for each transaction in the page.
	// This is a separate query to avoid degrading the covering index
	// performance of the main aggregation (metadata is not in the covering
	// index). We look up one arbitrary row per transaction fingerprint_id
	// to get the stmtFingerprintIDs from metadata, unnest them, and join
	// to system.statements.
	if len(transactions) > 0 {
		txnFPIDs := make([]string, len(transactions))
		txnIndexByFPID := make(map[string][]int, len(transactions))
		for i, txn := range transactions {
			txnFPIDs[i] = txn.FingerprintID
			txnIndexByFPID[txn.FingerprintID] = append(
				txnIndexByFPID[txn.FingerprintID], i,
			)
		}

		// Build a VALUES list of decoded transaction fingerprint IDs for the
		// batch lookup. Each txnFPID is a hex string from our own query output,
		// so it is safe to interpolate via decode().
		var valuesBuilder strings.Builder
		for i, fpID := range txnFPIDs {
			if i > 0 {
				valuesBuilder.WriteString(", ")
			}
			fmt.Fprintf(&valuesBuilder, "(decode('%s', 'hex'))", fpID)
		}

		// For each transaction fingerprint, pick one row to get the metadata
		// JSONB, unnest stmtFingerprintIDs, and join system.statements to get
		// the query summaries.
		var summInternalFilter string
		if excludeInternal {
			summInternalFilter = "\n    AND ts.app_name NOT LIKE '$ internal%%'"
		}
		summaryQuery := fmt.Sprintf(`
SELECT
  encode(sub.txn_fp, 'hex') AS txn_fingerprint_id,
  array_agg(DISTINCT q.summary) AS query_summaries
FROM (
  SELECT
    ts.fingerprint_id AS txn_fp,
    jsonb_array_elements_text(ts.metadata->'stmtFingerprintIDs')
      AS stmt_fp_hex
  FROM system.transaction_statistics ts
  AS OF SYSTEM TIME follower_read_timestamp()
  WHERE ts.fingerprint_id IN (SELECT column1 FROM (VALUES %s) AS v)%s
  GROUP BY ts.fingerprint_id, ts.metadata->'stmtFingerprintIDs'
) sub
JOIN system.statements q
  ON q.fingerprint_id = decode(sub.stmt_fp_hex, 'hex')
GROUP BY sub.txn_fp`, valuesBuilder.String(), summInternalFilter)

		summIt, summErr := ie.QueryIteratorEx(
			ctx,
			"dbconsole-txn-stmt-summaries",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			summaryQuery,
		)
		if summErr != nil {
			srverrors.APIV2InternalError(ctx, summErr, w)
			return
		}
		defer func() { _ = summIt.Close() }()

		for {
			ok, err := summIt.Next(ctx)
			if err != nil {
				srverrors.APIV2InternalError(ctx, err, w)
				return
			}
			if !ok {
				break
			}
			summRow := summIt.Cur()
			txnFPID := string(tree.MustBeDString(summRow[0]))
			summariesArr := tree.MustBeDArray(summRow[1])
			summaries := make([]string, 0, summariesArr.Len())
			for _, d := range summariesArr.Array {
				if d != tree.DNull {
					summaries = append(summaries, string(tree.MustBeDString(d)))
				}
			}
			if indices, ok := txnIndexByFPID[txnFPID]; ok {
				for _, idx := range indices {
					transactions[idx].QuerySummaries = summaries
				}
			}
		}
	}

	resp := PaginatedResponse[[]TransactionRow]{
		Results: transactions,
		PaginationInfo: paginationInfo{
			TotalResults: totalCount,
			PageSize:     pageSize,
			PageNum:      pageNum,
		},
	}
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

// queryFilters holds the parameterized filter state for building SQL queries.
// It tracks placeholder indices so that time range and search filters can
// share a single args slice.
type queryFilters struct {
	conditions      []string
	args            []interface{}
	nextArgIdx      int
	search          string
	appName         string
	database        string
	excludeInternal bool
}

// newQueryFilters builds filters from the provided time range and optional
// search, app name, database, and internal-exclusion values.
func newQueryFilters(
	start, end time.Time, search, appName, database string, excludeInternal bool,
) queryFilters {
	qf := queryFilters{
		nextArgIdx:      1,
		search:          search,
		appName:         appName,
		database:        database,
		excludeInternal: excludeInternal,
	}

	if !start.IsZero() {
		qf.conditions = append(qf.conditions,
			fmt.Sprintf("aggregated_ts >= $%d", qf.nextArgIdx))
		qf.args = append(qf.args, start)
		qf.nextArgIdx++
	}
	if !end.IsZero() {
		qf.conditions = append(qf.conditions,
			fmt.Sprintf("aggregated_ts < $%d", qf.nextArgIdx))
		qf.args = append(qf.args, end)
		qf.nextArgIdx++
	}

	return qf
}

// timeWhereClause returns the time-range portion of a WHERE clause.
// If no time filters exist it returns "true".
func (qf *queryFilters) timeWhereClause() string {
	if len(qf.conditions) == 0 {
		return "true"
	}
	return strings.Join(qf.conditions, " AND ")
}

// internalFilter returns a SQL fragment that excludes internal app names
// when excludeInternal is true. The alias is the table alias (e.g. "ss"
// or "ts"). Returns empty string when internal statements should be
// included.
func (qf *queryFilters) internalFilter(alias string) string {
	if !qf.excludeInternal {
		return ""
	}
	return fmt.Sprintf(" AND %s.app_name NOT LIKE '$ internal%%%%'", alias)
}

// searchFilter returns a parameterized ILIKE condition for the search term
// (e.g. "AND q.fingerprint ILIKE $3") and appends the wrapped search value
// to the args slice. Returns empty string if no search is active.
func (qf *queryFilters) searchFilter() string {
	if qf.search == "" {
		return ""
	}
	clause := fmt.Sprintf(" AND q.fingerprint ILIKE $%d", qf.nextArgIdx)
	qf.args = append(qf.args, fmt.Sprintf("%%%s%%", qf.search))
	qf.nextArgIdx++
	return clause
}

// hasSearch reports whether a search filter is active.
func (qf *queryFilters) hasSearch() bool {
	return qf.search != ""
}

// appNameFilter returns a parameterized equality condition for app_name
// (e.g. " AND ss.app_name = $3") using the given table alias, and appends
// the value to the args slice. Returns empty string if no app name filter
// is active.
func (qf *queryFilters) appNameFilter(alias string) string {
	if qf.appName == "" {
		return ""
	}
	clause := fmt.Sprintf(" AND %s.app_name = $%d", alias, qf.nextArgIdx)
	qf.args = append(qf.args, qf.appName)
	qf.nextArgIdx++
	return clause
}

// databaseFilter returns a parameterized equality condition for the database
// column on system.statements (e.g. " AND q.db = $4") and appends the value
// to the args slice. Returns empty string if no database filter is active.
func (qf *queryFilters) databaseFilter() string {
	if qf.database == "" {
		return ""
	}
	clause := fmt.Sprintf(" AND q.db = $%d", qf.nextArgIdx)
	qf.args = append(qf.args, qf.database)
	qf.nextArgIdx++
	return clause
}

// hasJoinFilter reports whether any filter requires a JOIN to
// system.statements (search or database).
func (qf *queryFilters) hasJoinFilter() bool {
	return qf.search != "" || qf.database != ""
}
