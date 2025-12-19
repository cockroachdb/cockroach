// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

const (
	statsTopKCountKey = "topKCount"
	statsTopKColKey   = "topKCol"
	statsStartTime    = "startTime"
	statsEndTime      = "endTime"
	statsDb           = "db"
	statsAppName      = "appName"
)

func (a *apiV2Server) GetStatementActivities(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)

	// Check for view activity or view activity redacted permission

	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	queryValues := r.URL.Query()
	sortByQs := queryValues.Get(sortByKey)
	var sortOrder string
	if sortByQs != "" {
		var ok bool
		sortOrder, ok = validateSortOrderValue(queryValues.Get(sortOrderKey))
		if !ok {
			http.Error(w, "invalid sort key value", http.StatusBadRequest)
			return
		}
	}

	topKCount, err := apiutil.GetIntQueryStringVal(queryValues, statsTopKCountKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", statsTopKCountKey),
			http.StatusBadRequest)
		return
	}
	topKCol := queryValues.Get(statsTopKColKey)

	startTimeSec, err := apiutil.GetIntQueryStringVal(queryValues, statsStartTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", statsStartTime),
			http.StatusBadRequest)
		return
	}

	endTimeSec, err := apiutil.GetIntQueryStringVal(queryValues, statsEndTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", statsEndTime),
			http.StatusBadRequest)
		return
	}
	// TODO: Validate that startTime is before endTime
	startTime := getTimeFromSeconds(int64(startTimeSec))
	endTime := getTimeFromSeconds(int64(endTimeSec))

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

	if pageNum <= 0 {
		pageNum = defaultPageNum
	}
	offset := (pageNum - 1) * pageSize

	filter := StatementActivityFilters{
		sortBy:    sortByQs,
		sortOrder: sortOrder,
		topKCount: topKCount,
		topKCol:   topKCol,
		pageSize:  pageSize,
		offset:    offset,
		startTime: startTime,
		endTime:   endTime,
	}
	resp, err := queryStatementStats(ctx, a.sqlServer.internalExecutor, filter)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

func queryStatementStats(
	ctx context.Context, db isql.Executor, filter StatementActivityFilters,
) (GetStatementActivitiesResponse, error) {

	sortBy := filter.sortBy
	sortOrder := filter.sortOrder
	topKCount := filter.topKCount
	topKCol := filter.topKCol
	pageSize := filter.pageSize
	offset := filter.offset
	startTime := filter.startTime
	endTime := filter.endTime
	var resp GetStatementActivitiesResponse
	var args []interface{}
	var whereStatement = "WHERE app_name NOT LIKE '$ internal%%'"
	args = append(args, topKCount, pageSize, offset)

	if startTime != nil {
		args = append(args, *startTime)
		whereStatement = fmt.Sprintf("%s AND aggregated_ts >= $%d", whereStatement, len(args))
	}

	if endTime != nil {
		args = append(args, *endTime)
		whereStatement = fmt.Sprintf("%s AND aggregated_ts < $%d", whereStatement, len(args))
	}

	query := fmt.Sprintf(`
SELECT *, count(*) OVER() as total_row_count FROM (
  SELECT
    encode(fingerprint_id, 'hex') AS fingerprint_id,
    sum(execution_count)::INT as cnt,
    SUM(svc_lat_sum) / NULLIF(SUM(execution_count::FLOAT8), 0) AS svc_lat_mean,
    SUM(cpu_sql_nanos_sum) / NULLIF(SUM(exec_sample_count::FLOAT8), 0) AS cpu_sql_nanos_mean,
    SUM(contention_time_sum) / NULLIF(SUM(exec_sample_count::FLOAT8), 0) AS contention_time_mean,
    SUM(kv_cpu_time_nanos_sum) / NULLIF(SUM(execution_count::FLOAT8), 0) AS kv_cpu_time_nanos_mean,
    SQRT((SUM(svc_lat_sum_sq) / NULLIF(SUM(execution_count::FLOAT8), 0)) - POWER(SUM(svc_lat_sum) / NULLIF(SUM(execution_count::FLOAT8), 0),2) ) AS svc_lat_stddev,
    SQRT((SUM(cpu_sql_nanos_sum_sq) / NULLIF(SUM(exec_sample_count::FLOAT8), 0)) - POWER(SUM(cpu_sql_nanos_sum) / NULLIF(SUM(exec_sample_count::FLOAT8), 0), 2) ) AS cpu_sql_nanos_stddev,
    SQRT((SUM(contention_time_sum_sq) / NULLIF(SUM(exec_sample_count::FLOAT8), 0)) - POWER(SUM(contention_time_sum) / NULLIF(SUM(exec_sample_count::FLOAT8), 0),2) ) AS contention_time_stddev,
    SQRT((SUM(kv_cpu_time_nanos_sum_sq) / NULLIF(SUM(execution_count::FLOAT8), 0)) - POWER(SUM(kv_cpu_time_nanos_sum) / NULLIF(SUM(execution_count::FLOAT8), 0), 2)) AS kv_cpu_time_nanos_stddev,
    max(metadata)->>'query' as query,
    max(metadata)->>'querySummary' as query_summary
  
  FROM system.statement_statistics
  %s
  GROUP BY fingerprint_id
  ORDER BY %s DESC
  LIMIT $1
)
ORDER BY %s %s
LIMIT $2
OFFSET $3
`, whereStatement, topKCol, sortBy, sortOrder)

	rows, err := db.QueryBufferedEx(
		ctx,
		"get-statement-activities",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
		query, args...,
	)
	if err != nil {
		return resp, err
	}

	var results []AggregatedStatementActivity
	var totalRowCount int64
	for _, row := range rows {
		if totalRowCount == 0 && row[12] != tree.DNull {
			totalRowCount = int64(tree.MustBeDInt(row[12]))
		}

		activity := AggregatedStatementActivity{
			FingerprintID: string(tree.MustBeDString(row[0])),
		}

		if row[1] != tree.DNull {
			activity.ExecutionCount = int64(tree.MustBeDInt(row[1]))
		}
		if row[2] != tree.DNull {
			activity.ServiceLatencyMean = float64(tree.MustBeDFloat(row[2]))
		}
		if row[3] != tree.DNull {
			activity.SQLCPUMeanNanos = float64(tree.MustBeDFloat(row[3]))
		}
		if row[4] != tree.DNull {
			activity.ContentionTimeMean = float64(tree.MustBeDFloat(row[4]))
		}
		if row[5] != tree.DNull {
			activity.KVCPUMeanNanos = float64(tree.MustBeDFloat(row[5]))
		}
		if row[6] != tree.DNull {
			activity.ServiceLatencyStdDev = float64(tree.MustBeDFloat(row[6]))
		}
		if row[7] != tree.DNull {
			activity.SQLCPUStdDev = float64(tree.MustBeDFloat(row[7]))
		}
		if row[8] != tree.DNull {
			activity.ContentionTimeStdDev = float64(tree.MustBeDFloat(row[8]))
		}
		if row[9] != tree.DNull {
			activity.KVCPUStdDev = float64(tree.MustBeDFloat(row[9]))
		}
		if row[10] != tree.DNull {
			activity.Query = string(tree.MustBeDString(row[10]))
		}
		if row[11] != tree.DNull {
			activity.QuerySummary = string(tree.MustBeDString(row[11]))
		}

		results = append(results, activity)
	}

	resp = GetStatementActivitiesResponse{
		Results: results,
		PaginationInfo: paginationInfo{
			TotalResults: totalRowCount,
			PageSize:     pageSize,
			PageNum:      (offset / pageSize) + 1,
		},
	}

	return resp, nil
}

type GetStatementActivitiesResponse = PaginatedResponse[[]AggregatedStatementActivity]

type AggregatedStatementActivity struct {
	FingerprintID        string  `json:"fingerprint_id"`
	Query                string  `json:"query"`
	QuerySummary         string  `json:"query_summary"`
	ExecutionCount       int64   `json:"execution_count"`
	ServiceLatencyMean   float64 `json:"service_latency_mean"`
	ServiceLatencyStdDev float64 `json:"service_latency_stddev"`
	SQLCPUMeanNanos      float64 `json:"sql_cpu_mean_nanos"`
	SQLCPUStdDev         float64 `json:"sql_cpu_stddev"`
	ContentionTimeMean   float64 `json:"contention_time_mean"`
	ContentionTimeStdDev float64 `json:"contention_time_stddev"`
	KVCPUMeanNanos       float64 `json:"kv_cpu_mean_nanos"`
	KVCPUStdDev          float64 `json:"kv_cpu_stddev"`
}

type StatementActivityFilters struct {
	appName   string
	db        string
	sortBy    string
	sortOrder string
	topKCount int
	topKCol   string
	pageSize  int
	offset    int
	startTime *time.Time
	endTime   *time.Time
}
