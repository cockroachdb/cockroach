// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

func init() {
	RegisterFeature(&Feature{
		Name:        "jobs_manager",
		Title:       "Jobs Manager",
		Description: "Pause and resume active jobs",
		RoutePath:   "/feature/jobs-manager",
	})
}

// JobManagerJob represents a single job in the jobs manager response.
type JobManagerJob struct {
	JobID       int64  `json:"job_id"`
	JobType     string `json:"job_type"`
	Description string `json:"description"`
	Status      string `json:"status"`
	Created     string `json:"created"`
}

// JobsManagerJobsResponse is the response body for GET /jobs-manager/jobs.
type JobsManagerJobsResponse struct {
	Jobs []JobManagerJob `json:"jobs"`
}

// GetJobsManagerJobs lists running and paused jobs.
func (api *ApiV2DBConsole) GetJobsManagerJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	if !requireFeatureEnabled(ctx, "jobs_manager", &api.Settings.SV, w) {
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "jobs-manager-list", nil,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT job_id, job_type, description, status, created
		 FROM crdb_internal.jobs
		 WHERE status IN ('running', 'paused', 'pause-requested', 'resume-requested')
		 ORDER BY created DESC
		 LIMIT 50`,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	result := JobsManagerJobsResponse{
		Jobs: make([]JobManagerJob, 0, len(rows)),
	}
	for _, row := range rows {
		job := JobManagerJob{
			JobID:       int64(tree.MustBeDInt(row[0])),
			JobType:     string(tree.MustBeDString(row[1])),
			Description: string(tree.MustBeDString(row[2])),
			Status:      string(tree.MustBeDString(row[3])),
			Created: tree.AsStringWithFlags(
				row[4], tree.FmtBareStrings,
			),
		}
		result.Jobs = append(result.Jobs, job)
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, result)
}

// JobControlRequest is the request body for POST /jobs-manager/jobs/{id}.
type JobControlRequest struct {
	Action string `json:"action"` // "pause" or "resume"
}

// HandleJobsManagerControl pauses or resumes a job by ID.
func (api *ApiV2DBConsole) HandleJobsManagerControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	if !requireFeatureEnabled(ctx, "jobs_manager", &api.Settings.SV, w) {
		return
	}

	// Parse job ID from path: /jobs-manager/jobs/{id}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "jobs-manager" || parts[1] != "jobs" {
		http.NotFound(w, r)
		return
	}
	jobID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		http.Error(w, "invalid job ID", http.StatusBadRequest)
		return
	}

	var req JobControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	var stmt string
	switch req.Action {
	case "pause":
		stmt = fmt.Sprintf("PAUSE JOB %d", jobID)
	case "resume":
		stmt = fmt.Sprintf("RESUME JOB %d", jobID)
	default:
		http.Error(
			w, `action must be "pause" or "resume"`,
			http.StatusBadRequest,
		)
		return
	}

	ie := api.InternalDB.Executor()
	_, err = ie.ExecEx(
		ctx, "jobs-manager-control", nil,
		sessiondata.NodeUserSessionDataOverride, stmt,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}
