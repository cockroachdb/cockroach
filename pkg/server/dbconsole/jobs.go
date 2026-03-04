// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/gorilla/mux"
)

// CollectExecutionDetailsResponse indicates whether the request to collect
// job execution details was successful.
type CollectExecutionDetailsResponse struct {
	// Success is true if the collection request was submitted successfully.
	Success bool `json:"success"`
}

// CollectJobExecutionDetails triggers collection of execution details for a
// specific job.
//
// ---
// @Summary Collect job execution details
// @Description Triggers collection of execution details for the specified job.
// @Tags Jobs
// @Produce json
// @Param job_id path int true "Job ID"
// @Success 200 {object} CollectExecutionDetailsResponse
// @Failure 400 {string} string "Invalid job_id"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /jobs/{job_id}/collect-execution-details [post]
func (api *ApiV2DBConsole) CollectJobExecutionDetails(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	jobIDStr := mux.Vars(r)["job_id"]
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid job_id", http.StatusBadRequest)
		return
	}

	ie := api.InternalDB.Executor()
	_, err = ie.ExecEx(
		ctx, "dbconsole-collect-job-exec-details", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`SELECT crdb_internal.request_job_execution_details($1::INT)`,
		jobID,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, CollectExecutionDetailsResponse{
		Success: true,
	})
}
