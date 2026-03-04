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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
)

// ScheduleInfo represents a single schedule.
type ScheduleInfo struct {
	// ID is the unique identifier for the schedule.
	ID int64 `json:"id"`
	// Label is the human-readable name of the schedule.
	Label string `json:"label"`
	// ScheduleStatus is the current status of the schedule.
	ScheduleStatus string `json:"schedule_status"`
	// NextRun is the timestamp of the next scheduled run.
	NextRun string `json:"next_run,omitempty"`
	// State is the schedule state.
	State string `json:"state,omitempty"`
	// Recurrence is the cron-style recurrence pattern.
	Recurrence string `json:"recurrence,omitempty"`
	// JobsRunning is the number of currently running jobs for this schedule.
	JobsRunning int64 `json:"jobs_running"`
	// Owner is the user who owns the schedule.
	Owner string `json:"owner"`
	// Created is the timestamp when the schedule was created.
	Created string `json:"created"`
	// Command is the schedule command payload.
	Command string `json:"command,omitempty"`
}

// SchedulesResponse contains the list of schedules.
type SchedulesResponse struct {
	// Schedules is the list of schedules.
	Schedules []ScheduleInfo `json:"schedules"`
}

// GetSchedules returns the list of schedules with optional filtering by
// status.
//
// ---
// @Summary List schedules
// @Description Returns the list of schedules, optionally filtered by status.
// @Tags Schedules
// @Produce json
// @Param status query string false "Filter by schedule status"
// @Param limit query int false "Maximum number of results"
// @Success 200 {object} SchedulesResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /schedules [get]
func (api *ApiV2DBConsole) GetSchedules(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	query := safesql.NewQuery()
	query.Append(
		`WITH s AS (SHOW SCHEDULES)
     SELECT id, label, schedule_status, next_run, state,
            recurrence, num_running, owner, created, command
     FROM s`,
	)

	status := r.URL.Query().Get("status")
	if status != "" {
		query.Append(` WHERE schedule_status = $`, status)
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
		query.Append(` LIMIT $`, limit)
	}

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-schedules", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	schedules := make([]ScheduleInfo, 0, len(rows))
	for _, row := range rows {
		schedules = append(schedules, scanScheduleRow(row))
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, SchedulesResponse{
		Schedules: schedules,
	})
}

// GetSchedule returns details for a specific schedule.
//
// ---
// @Summary Get schedule
// @Description Returns details for a specific schedule.
// @Tags Schedules
// @Produce json
// @Param schedule_id path int true "Schedule ID"
// @Success 200 {object} ScheduleInfo
// @Failure 400 {string} string "Invalid schedule_id"
// @Failure 404 {string} string "Schedule not found"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /schedules/{schedule_id} [get]
func (api *ApiV2DBConsole) GetSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	scheduleIDStr := mux.Vars(r)["schedule_id"]
	scheduleID, err := strconv.ParseInt(scheduleIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid schedule_id", http.StatusBadRequest)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-schedule", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`WITH s AS (SHOW SCHEDULES)
     SELECT id, label, schedule_status, next_run, state,
            recurrence, num_running, owner, created, command
     FROM s
     WHERE id = $1`,
		scheduleID,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	if len(rows) == 0 {
		http.Error(w, "schedule not found", http.StatusNotFound)
		return
	}

	schedule := scanScheduleRow(rows[0])
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, schedule)
}

// scanScheduleRow extracts a ScheduleInfo from a result row with columns:
// id, label, schedule_status, next_run, state, recurrence, num_running,
// owner, created, command.
func scanScheduleRow(row tree.Datums) ScheduleInfo {
	s := ScheduleInfo{
		ID:             int64(tree.MustBeDInt(row[0])),
		Label:          string(tree.MustBeDString(row[1])),
		ScheduleStatus: string(tree.MustBeDString(row[2])),
	}
	if row[3] != tree.DNull {
		s.NextRun = tree.AsStringWithFlags(row[3], tree.FmtBareStrings)
	}
	if row[4] != tree.DNull {
		s.State = string(tree.MustBeDString(row[4]))
	}
	if row[5] != tree.DNull {
		s.Recurrence = string(tree.MustBeDString(row[5]))
	}
	s.JobsRunning = int64(tree.MustBeDInt(row[6]))
	s.Owner = string(tree.MustBeDString(row[7]))
	s.Created = tree.AsStringWithFlags(row[8], tree.FmtBareStrings)
	if row[9] != tree.DNull {
		s.Command = string(tree.MustBeDString(row[9]))
	}
	return s
}
