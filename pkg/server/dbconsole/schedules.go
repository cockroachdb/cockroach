// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
)

// ScheduleInfo contains summarized schedule information for the UI.
type ScheduleInfo struct {
	// ID is the unique identifier for the schedule. Serialized as a string to
	// avoid JavaScript integer precision loss for values exceeding 2^53.
	ID int64 `json:"id,string"`
	// Label is the name of the schedule.
	Label string `json:"label"`
	// Status is the schedule status (ACTIVE or PAUSED).
	Status string `json:"status"`
	// NextRun is the next scheduled run time in ISO 8601 format, empty if paused.
	NextRun string `json:"next_run"`
	// State is the schedule state from the schedule_state protobuf.
	State string `json:"state"`
	// Recurrence is the cron expression or NEVER.
	Recurrence string `json:"recurrence"`
	// JobsRunning is the number of currently running jobs for this schedule.
	JobsRunning int64 `json:"jobs_running"`
	// Owner is the user who owns this schedule.
	Owner string `json:"owner"`
	// Created is the schedule creation time in ISO 8601 format.
	Created string `json:"created"`
	// Command is the JSON representation of the schedule's execution arguments.
	Command string `json:"command"`
}

// SchedulesListResponse contains schedules for the schedules list page.
type SchedulesListResponse struct {
	// Schedules contains information for all matching schedules.
	Schedules []ScheduleInfo `json:"schedules"`
}

// GetSchedules returns schedule information for the schedules list page.
// @Summary List schedules
// @Description Returns schedule information, optionally filtered by status.
// @Tags Schedules
// @Produce json
// @Param status query string false "Filter by status (ACTIVE or PAUSED)"
// @Param limit query int false "Maximum number of schedules to return"
// @Success 200 {object} SchedulesListResponse "Schedule list"
// @Failure 400 {object} ErrorResponse "Invalid limit"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /schedules [get]
func (api *ApiV2DBConsole) GetSchedules(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit, err := apiutil.GetIntQueryStringVal(r.URL.Query(), "limit")
	if err != nil {
		apiutil.WriteJSONResponse(ctx, w, http.StatusBadRequest, ErrorResponse{
			Error: "invalid limit parameter",
		})
		return
	}

	userName := authserver.UserFromHTTPAuthInfoContext(ctx)

	query := safesql.NewQuery()
	query.Append(
		`WITH s AS (SHOW SCHEDULES)
SELECT id, label, schedule_status, next_run, state,
       recurrence, jobsrunning, owner, created, command
FROM s`,
	)

	statusFilter := strings.ToUpper(r.URL.Query().Get("status"))
	if statusFilter != "" {
		if statusFilter != "ACTIVE" && statusFilter != "PAUSED" {
			apiutil.WriteJSONResponse(ctx, w, http.StatusBadRequest, ErrorResponse{
				Error: "invalid status parameter, must be ACTIVE or PAUSED",
			})
			return
		}
		query.Append(` WHERE schedule_status = $`, statusFilter)
	}

	query.Append(` ORDER BY created DESC`)

	if limit > 0 {
		query.Append(` LIMIT $`, limit)
	}

	if queryErrs := query.Errors(); len(queryErrs) > 0 {
		srverrors.APIV2InternalError(ctx, queryErrs[0], w)
		return
	}

	rows, err := api.InternalDB.Executor().QueryBufferedEx(
		ctx, "dbconsole-schedules", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	schedules := make([]ScheduleInfo, 0, len(rows))
	for _, row := range rows {
		info, err := scanScheduleRow(row)
		if err != nil {
			srverrors.APIV2InternalError(ctx, err, w)
			return
		}
		schedules = append(schedules, info)
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, SchedulesListResponse{
		Schedules: schedules,
	})
}

// GetSchedule returns information for a single schedule.
// @Summary Get schedule
// @Description Returns information for a single schedule by ID.
// @Tags Schedules
// @Produce json
// @Param schedule_id path int true "Schedule ID"
// @Success 200 {object} ScheduleInfo "Schedule info"
// @Failure 400 {object} ErrorResponse "Invalid schedule ID"
// @Failure 404 {object} ErrorResponse "Schedule not found"
// @Failure 500 {object} ErrorResponse "Internal error"
// @Security ApiKeyAuth
// @Router /schedules/{schedule_id} [get]
func (api *ApiV2DBConsole) GetSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	scheduleIDStr := mux.Vars(r)["schedule_id"]
	scheduleID, err := strconv.ParseInt(scheduleIDStr, 10, 64)
	if err != nil {
		apiutil.WriteJSONResponse(ctx, w, http.StatusBadRequest, ErrorResponse{
			Error: "invalid schedule_id",
		})
		return
	}

	userName := authserver.UserFromHTTPAuthInfoContext(ctx)

	rows, err := api.InternalDB.Executor().QueryBufferedEx(
		ctx, "dbconsole-schedule", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		`WITH s AS (SHOW SCHEDULES)
SELECT id, label, schedule_status, next_run, state,
       recurrence, jobsrunning, owner, created, command
FROM s
WHERE id = $1`,
		scheduleID,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	if len(rows) == 0 {
		apiutil.WriteJSONResponse(ctx, w, http.StatusNotFound, ErrorResponse{
			Error: "schedule not found",
		})
		return
	}

	info, err := scanScheduleRow(rows[0])
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, info)
}

// scanScheduleRow converts a tree.Datums row from the schedules query into a
// ScheduleInfo struct.
func scanScheduleRow(row tree.Datums) (ScheduleInfo, error) {
	if len(row) != 10 {
		return ScheduleInfo{}, errors.Newf("expected 10 columns, got %d", len(row))
	}

	info := ScheduleInfo{
		ID:          int64(tree.MustBeDInt(row[0])),
		Label:       string(tree.MustBeDString(row[1])),
		Status:      string(tree.MustBeDString(row[2])),
		Recurrence:  string(tree.MustBeDString(row[5])),
		JobsRunning: int64(tree.MustBeDInt(row[6])),
		Owner:       string(tree.MustBeDString(row[7])),
	}

	// next_run: nullable timestamp.
	if row[3] != tree.DNull {
		ts, ok := row[3].(*tree.DTimestampTZ)
		if !ok {
			return ScheduleInfo{}, errors.Newf(
				"expected DTimestampTZ for next_run, got %T", row[3])
		}
		info.NextRun = ts.Time.Format(time.RFC3339)
	}

	// state: nullable text field (extracted via ->> which returns DString).
	if row[4] != tree.DNull {
		info.State = tree.AsStringWithFlags(row[4], tree.FmtBareStrings)
	}

	// created: non-nullable timestamp.
	createdTS, ok := row[8].(*tree.DTimestampTZ)
	if !ok {
		return ScheduleInfo{}, errors.Newf(
			"expected DTimestampTZ for created, got %T", row[8])
	}
	info.Created = createdTS.Time.Format(time.RFC3339)

	// command: nullable JSONB field (the delegate strips @type from execution args).
	if row[9] != tree.DNull {
		cmdJSON, ok := row[9].(*tree.DJSON)
		if !ok {
			return ScheduleInfo{}, errors.Newf(
				"expected DJSON for command, got %T", row[9])
		}
		info.Command = cmdJSON.JSON.String()
	}

	return info, nil
}
