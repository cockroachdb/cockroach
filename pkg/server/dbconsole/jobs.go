// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
)

// JobInfo contains summarized job information for the UI.
// Note: descriptor_ids from the protobuf JobResponse is intentionally omitted
// because the DB Console UI does not use it.
type JobInfo struct {
	// ID is the unique identifier for the job. Serialized as a string to avoid
	// JavaScript integer precision loss for values exceeding 2^53.
	ID int64 `json:"id,string"`
	// Type is the type of the job (e.g. "BACKUP", "RESTORE").
	Type string `json:"type"`
	// Description is a human-readable description of the job.
	Description string `json:"description"`
	// Statement is the SQL statement that created the job.
	Statement string `json:"statement"`
	// Username is the user who created the job.
	Username string `json:"username"`
	// Status is the current status of the job.
	Status string `json:"status"`
	// RunningStatus provides additional detail on the job's running state.
	RunningStatus string `json:"running_status"`
	// Created is the ISO 8601 timestamp when the job was created.
	Created string `json:"created"`
	// Started is the ISO 8601 timestamp when the job started executing.
	Started string `json:"started"`
	// Finished is the ISO 8601 timestamp when the job finished.
	Finished string `json:"finished"`
	// Modified is the ISO 8601 timestamp when the job was last modified.
	Modified string `json:"modified"`
	// FractionCompleted is the fraction of the job that has been completed.
	FractionCompleted float32 `json:"fraction_completed"`
	// Error is the error message if the job failed.
	Error string `json:"error"`
	// HighwaterTimestamp is the highwater timestamp as an ISO 8601 string.
	HighwaterTimestamp string `json:"highwater_timestamp"`
	// HighwaterDecimal is the highwater timestamp in decimal form for use
	// with AS OF SYSTEM TIME.
	HighwaterDecimal string `json:"highwater_decimal"`
	// LastRun is the ISO 8601 timestamp of the last execution attempt.
	LastRun string `json:"last_run"`
	// NextRun is the ISO 8601 timestamp of the next scheduled execution.
	NextRun string `json:"next_run"`
	// NumRuns is the number of times the job has been executed.
	NumRuns int64 `json:"num_runs"`
	// CoordinatorID is the node ID coordinating the job.
	CoordinatorID int64 `json:"coordinator_id"`
	// ExecutionFailures is a log of execution failures.
	ExecutionFailures []JobExecutionFailure `json:"execution_failures"`
	// Messages contains informational messages associated with the job.
	Messages []JobMessageInfo `json:"messages"`
}

// JobExecutionFailure represents a single execution failure of a job.
type JobExecutionFailure struct {
	// Status is the status of the job during the failed execution.
	Status string `json:"status"`
	// Start is the ISO 8601 timestamp when the execution started.
	Start string `json:"start"`
	// End is the ISO 8601 timestamp when the error occurred.
	End string `json:"end"`
	// Error is the error message.
	Error string `json:"error"`
}

// JobMessageInfo represents an informational message associated with a job.
type JobMessageInfo struct {
	// Kind is the kind of message.
	Kind string `json:"kind"`
	// Timestamp is the ISO 8601 timestamp of the message.
	Timestamp string `json:"timestamp"`
	// Message is the message text.
	Message string `json:"message"`
}

// JobsListResponse contains the list of jobs and metadata.
type JobsListResponse struct {
	// Jobs contains the list of jobs.
	Jobs []JobInfo `json:"jobs"`
	// EarliestRetainedTime is the earliest time for which job records are
	// retained, as an ISO 8601 timestamp.
	EarliestRetainedTime string `json:"earliest_retained_time"`
}

// GetJobs returns a list of jobs matching the provided filters.
// @Summary List jobs
// @Description Returns a list of jobs, optionally filtered by status, type, and limit.
// @Tags Jobs
// @Produce json
// @Param status query string false "Filter by job status"
// @Param type query int false "Filter by job type (int32 enum value)"
// @Param limit query int false "Maximum number of jobs to return"
// @Success 200 {object} JobsListResponse "Job list"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /jobs [get]
func (api *ApiV2DBConsole) GetJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	status := r.URL.Query().Get("status")

	var jobType jobspb.Type
	if typeStr := r.URL.Query().Get("type"); typeStr != "" {
		typeVal, err := strconv.Atoi(typeStr)
		if err != nil {
			apiutil.WriteJSONResponse(
				ctx, w, http.StatusBadRequest,
				ErrorResponse{Error: "invalid type parameter"},
			)
			return
		}
		jobType = jobspb.Type(typeVal)
	}

	var limit int32
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limitVal, err := strconv.Atoi(limitStr)
		if err != nil {
			apiutil.WriteJSONResponse(
				ctx, w, http.StatusBadRequest,
				ErrorResponse{Error: "invalid limit parameter"},
			)
			return
		}
		limit = int32(limitVal)
	}

	userName := authserver.UserFromHTTPAuthInfoContext(ctx)

	// Build the query. This replicates the logic from
	// BuildJobQueryFromRequest in admin.go, which cannot be imported here
	// due to circular dependency (server -> dbconsole -> server).
	q := safesql.NewQuery()
	q.Append(`
SELECT job_id, job_type, description, statement, user_name, status,
       running_status, created, finished, modified,
       fraction_completed, high_water_timestamp, error, coordinator_id
  FROM crdb_internal.jobs
 WHERE true`)
	if status != "" {
		q.Append(" AND status = $", status)
	}
	if jobType != jobspb.TypeUnspecified {
		q.Append(" AND job_type = $", jobType.String())
	} else {
		// Don't show automatic jobs in the overview page.
		q.Append(" AND ( job_type NOT IN (")
		for idx, jt := range jobspb.AutomaticJobTypes {
			if idx != 0 {
				q.Append(", ")
			}
			q.Append("$", jt.String())
		}
		q.Append(" ) OR job_type IS NULL)")
	}
	q.Append(" ORDER BY created DESC")
	if limit > 0 {
		q.Append(" LIMIT $", tree.DInt(limit))
	}

	it, err := api.InternalDB.Executor().QueryIteratorEx(
		ctx, "dbconsole-jobs", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	defer func() { _ = it.Close() }()

	var jobsList []JobInfo
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
		if !ok {
			break
		}
		job, err := scanRowToJobInfo(it.Cur())
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
		jobsList = append(jobsList, job)
	}

	if jobsList == nil {
		jobsList = []JobInfo{}
	}

	result := JobsListResponse{
		Jobs: jobsList,
	}
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, result)
}

// GetJob returns a single job by ID.
// @Summary Get job
// @Description Returns a single job identified by its ID.
// @Tags Jobs
// @Produce json
// @Param job_id path int true "Job ID"
// @Success 200 {object} JobInfo "Job details"
// @Failure 400 {object} ErrorResponse "Invalid job ID"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /jobs/{job_id} [get]
func (api *ApiV2DBConsole) GetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	jobIDStr := mux.Vars(r)["job_id"]
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		apiutil.WriteJSONResponse(
			ctx, w, http.StatusBadRequest,
			ErrorResponse{Error: "invalid job_id parameter"},
		)
		return
	}

	userName := authserver.UserFromHTTPAuthInfoContext(ctx)

	const query = `
SELECT job_id, job_type, description, statement, user_name, status,
       running_status, created, finished, modified,
       fraction_completed, high_water_timestamp, error, coordinator_id
  FROM crdb_internal.jobs
 WHERE job_id = $1`

	row, _, err := api.InternalDB.Executor().QueryRowExWithCols(
		ctx, "dbconsole-job", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query, jobID,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	if row == nil {
		srverrors.APIV2InternalError(
			ctx,
			errors.Newf("could not get job for job_id %d; 0 rows returned", jobID),
			w,
		)
		return
	}

	result, err := scanRowToJobInfo(row)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	result.Messages = fetchJobMessages(ctx, jobID, api.InternalDB.Executor())
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, result)
}

// scanRowToJobInfo converts a database row from crdb_internal.jobs to a JobInfo.
// The row must contain exactly these columns in order:
//
//	job_id, job_type, description, statement, user_name, status,
//	running_status, created, finished, modified,
//	fraction_completed, high_water_timestamp, error, coordinator_id
func scanRowToJobInfo(row tree.Datums) (JobInfo, error) {
	if len(row) != 14 {
		return JobInfo{}, errors.Newf("expected 14 columns, got %d", len(row))
	}

	var job JobInfo

	// job_id (INT)
	job.ID = int64(tree.MustBeDInt(row[0]))

	// job_type (STRING)
	job.Type = string(tree.MustBeDString(row[1]))

	// description (STRING)
	job.Description = string(tree.MustBeDString(row[2]))

	// statement (STRING)
	job.Statement = string(tree.MustBeDString(row[3]))

	// user_name (STRING)
	job.Username = string(tree.MustBeDString(row[4]))

	// status (STRING)
	job.Status = string(tree.MustBeDString(row[5]))

	// running_status (STRING, nullable)
	if row[6] != tree.DNull {
		job.RunningStatus = string(tree.MustBeDString(row[6]))
	}

	// created (TIMESTAMPTZ)
	job.Created = datumToTimeStr(row[7])

	// finished (TIMESTAMPTZ, nullable)
	job.Finished = datumToTimeStr(row[8])

	// modified (TIMESTAMPTZ)
	job.Modified = datumToTimeStr(row[9])

	// fraction_completed (FLOAT, nullable)
	if row[10] != tree.DNull {
		job.FractionCompleted = float32(tree.MustBeDFloat(row[10]))
	}

	// high_water_timestamp (DECIMAL, nullable)
	if row[11] != tree.DNull {
		hwDecimal := &row[11].(*tree.DDecimal).Decimal
		hwTimestamp, err := hlc.DecimalToHLC(hwDecimal)
		if err != nil {
			return JobInfo{}, errors.Wrap(err, "highwater timestamp had unexpected format")
		}
		goTime := hwTimestamp.GoTime()
		job.HighwaterTimestamp = goTime.Format(time.RFC3339)
		job.HighwaterDecimal = hwDecimal.String()
	}

	// error (STRING)
	job.Error = string(tree.MustBeDString(row[12]))

	// coordinator_id (INT, nullable)
	if row[13] != tree.DNull {
		job.CoordinatorID = int64(tree.MustBeDInt(row[13]))
	}

	return job, nil
}

// datumToTimeStr converts a tree.Datum timestamp to an RFC3339 string.
// Returns an empty string if the datum is NULL or the zero time.
func datumToTimeStr(d tree.Datum) string {
	if d == tree.DNull {
		return ""
	}
	t := tree.MustBeDTimestampTZ(d).Time
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// fetchJobMessages retrieves informational messages associated with a job from
// the system.job_message table. Uses the node user since this is a system table
// and the caller has already verified access to the job.
func fetchJobMessages(ctx context.Context, jobID int64, executor isql.Executor) []JobMessageInfo {
	const msgQuery = `SELECT kind, written, message FROM system.job_message WHERE job_id = $1 ORDER BY written DESC`
	it, err := executor.QueryIteratorEx(
		ctx, "dbconsole-job-messages", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		msgQuery, jobID,
	)
	if err != nil {
		return []JobMessageInfo{
			{Kind: "error", Timestamp: formatTime(timeutil.Now()), Message: err.Error()},
		}
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			// If we can't close, at least we've read what we could.
		}
	}()

	var messages []JobMessageInfo
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return []JobMessageInfo{
				{Kind: "error", Timestamp: formatTime(timeutil.Now()), Message: err.Error()},
			}
		}
		if !ok {
			break
		}
		row := it.Cur()
		messages = append(messages, JobMessageInfo{
			Kind:      string(tree.MustBeDStringOrDNull(row[0])),
			Timestamp: datumToTimeStr(row[1]),
			Message:   string(tree.MustBeDStringOrDNull(row[2])),
		})
	}
	return messages
}

// formatTime formats a time.Time as an ISO 8601 string. Returns an empty string
// for zero values.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
