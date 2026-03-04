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
)

const defaultEventsLimit = 1000

// EventInfo represents a single event from the system event log.
type EventInfo struct {
	// Timestamp is the time the event occurred.
	Timestamp string `json:"timestamp"`
	// EventType is the type of event (e.g. "create_database").
	EventType string `json:"event_type"`
	// ReportingID is the node ID that reported the event.
	ReportingID int64 `json:"reporting_id"`
	// Info is the JSON-encoded event details.
	Info string `json:"info"`
	// UniqueID is the unique identifier for this event.
	UniqueID []byte `json:"unique_id"`
}

// EventsResponse contains the list of events.
type EventsResponse struct {
	// Events is the list of event log entries.
	Events []EventInfo `json:"events"`
}

// GetEvents returns events from the system event log with optional filtering
// by type and pagination.
//
// ---
// @Summary Get events
// @Description Returns events from the system event log.
// @Tags Events
// @Produce json
// @Param type query string false "Filter by event type"
// @Param limit query int false "Maximum number of results (default 1000)"
// @Param offset query int false "Offset for pagination"
// @Success 200 {object} EventsResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /events [get]
func (api *ApiV2DBConsole) GetEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	limit := defaultEventsLimit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	var offset int
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		var err error
		offset, err = strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			http.Error(w, "invalid offset parameter", http.StatusBadRequest)
			return
		}
	}

	eventType := r.URL.Query().Get("type")

	query := safesql.NewQuery()
	query.Append(
		`SELECT timestamp, "eventType", "reportingID", info, "uniqueID"
     FROM system.eventlog`,
	)
	if eventType != "" {
		query.Append(` WHERE "eventType" = $`, eventType)
	}
	query.Append(` ORDER BY timestamp DESC`)
	query.Append(` LIMIT $`, limit)
	if offset > 0 {
		query.Append(` OFFSET $`, offset)
	}

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-events", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	events := make([]EventInfo, 0, len(rows))
	for _, row := range rows {
		var info string
		if row[3] != tree.DNull {
			info = string(tree.MustBeDString(row[3]))
		}
		var uniqueID []byte
		if row[4] != tree.DNull {
			uniqueID = []byte(tree.MustBeDBytes(row[4]))
		}
		events = append(events, EventInfo{
			Timestamp:   tree.AsStringWithFlags(row[0], tree.FmtBareStrings),
			EventType:   string(tree.MustBeDString(row[1])),
			ReportingID: int64(tree.MustBeDInt(row[2])),
			Info:        info,
			UniqueID:    uniqueID,
		})
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, EventsResponse{
		Events: events,
	})
}
