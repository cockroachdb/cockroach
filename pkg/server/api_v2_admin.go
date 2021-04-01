// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Response for listUsers.
//
// swagger:model usersResponse
type usersResponse struct {
	serverpb.UsersResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// swagger:operation GET /users/ listUsers
//
// List users
//
// List SQL users on this cluster.
//
// ---
// parameters:
// - name: limit
//   type: integer
//   in: query
//   description: Maximum number of results to return in this call.
//   required: false
// - name: offset
//   type: integer
//   in: query
//   description: Continuation token for results after a past limited run.
//   required: false
// produces:
// - application/json
// responses:
//   "200":
//     description: Users response
//     schema:
//       "$ref": "#/definitions/usersResponse"
func (a *apiV2Server) listUsers(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := security.MakeSQLUsernameFromPreNormalizedString(
		ctx.Value(webSessionUserKey{}).(string))
	ctx = a.admin.server.AnnotateCtx(ctx)

	query := `SELECT username FROM system.users WHERE "isRole" = false ORDER BY username`
	qargs := []interface{}{}
	if limit > 0 {
		query += " LIMIT $"
		qargs = append(qargs, limit)
		if offset > 0 {
			query += " OFFSET $"
			qargs = append(qargs, offset)
		}
	}
	it, err := a.admin.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-users", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: username},
		query, qargs...
	)
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}

	var resp usersResponse
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(tree.MustBeDString(row[0]))})
	}
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	if limit > 0 && len(resp.Users) == limit {
		resp.Next = offset + limit
	}
	writeJSONResponse(ctx, w, 200, resp)
}

// Response for listEvents.
//
// swagger:model eventsResponse
type eventsResponse struct {
	serverpb.EventsResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// swagger:operation GET /events/ listEvents
//
// List events
//
// Lists the latest event log entries, in descending order.
//
// ---
// parameters:
// - name: type
//   type: string
//   in: query
//   description: Type of events to filter for (e.g. "create_table")
//   required: false
// - name: targetID
//   type: integer
//   in: query
//   description: Filter for events with this targetID.
//   required: false
// - name: limit
//   type: integer
//   in: query
//   description: Maximum number of results to return in this call.
//   required: false
// - name: offset
//   type: integer
//   in: query
//   description: Continuation token for results after a past limited run.
//   required: false
// produces:
// - application/json
// responses:
//   "200":
//     description: Events response
//     schema:
//       "$ref": "#/definitions/eventsResponse"
func (a *apiV2Server) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := security.MakeSQLUsernameFromPreNormalizedString(
		ctx.Value(webSessionUserKey{}).(string))
	ctx = a.admin.server.AnnotateCtx(ctx)
	queryValues := r.URL.Query()

	req := &serverpb.EventsRequest{}
	if typ := queryValues.Get("type"); len(typ) > 0 {
		req.Type = typ
	}
	if targetID := queryValues.Get("targetID"); len(targetID) > 0 {
		if targetIDInt, err := strconv.ParseInt(targetID, 10, 64); err == nil {
			req.TargetId = targetIDInt
		}
	}

	var resp eventsResponse
	eventsResp, err := a.admin.eventsHelper(
		ctx, req, username, limit, offset, true /* redactEvents */)
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	resp.EventsResponse = *eventsResp
	if limit > 0 && len(resp.Events) == limit {
		resp.Next = offset + limit
	}
	writeJSONResponse(ctx, w, 200, resp)
}

// Response for listDatabases.
//
// swagger:model databasesResponse
type databasesResponse struct {
	serverpb.DatabasesResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// swagger:operation GET /databases/ listDatabases
//
// List databases
//
// Lists all databases on this cluster.
//
// ---
// parameters:
// - name: limit
//   type: integer
//   in: query
//   description: Maximum number of results to return in this call.
//   required: false
// - name: offset
//   type: integer
//   in: query
//   description: Continuation token for results after a past limited run.
//   required: false
// produces:
// - application/json
// responses:
//   "200":
//     description: Databases response
//     schema:
//       "$ref": "#/definitions/databasesResponse"
func (a *apiV2Server) listDatabases(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := security.MakeSQLUsernameFromPreNormalizedString(
		ctx.Value(webSessionUserKey{}).(string))
	ctx = a.admin.server.AnnotateCtx(ctx)

	var resp databasesResponse
	req := &serverpb.DatabasesRequest{}
	dbsResp, err := a.admin.databasesHelper(ctx, req, username, limit, offset)
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	var databases interface{}
	databases, resp.Next = simplePaginate(dbsResp.Databases, limit, offset)
	resp.Databases = databases.([]string)
	writeJSONResponse(ctx, w, 200, resp)
}

// Response for databaseDetails.
//
// swagger:model databaseDetailsResponse
type databaseDetailsResponse struct {
	serverpb.DatabaseDetailsResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// swagger:operation GET /databases/{database}/ listDatabases
//
// Get database details
//
// Returns details about a table's grants, tables, and other info.
//
// ---
// parameters:
// - name: database
//   type: string
//   in: path
//   description: Name of database being looked up.
//   required: true
// - name: limit
//   type: integer
//   in: query
//   description: Maximum number of tables to return in this call.
//   required: false
// - name: offset
//   type: integer
//   in: query
//   description: Continuation token for results after a past limited run.
//   required: false
// produces:
// - application/json
// responses:
//   "200":
//     description: Database details response
//     schema:
//       "$ref": "#/definitions/databaseDetailsResponse"
//   "404":
//     description: Database not found
func (a *apiV2Server) databaseDetails(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := security.MakeSQLUsernameFromPreNormalizedString(
		ctx.Value(webSessionUserKey{}).(string))
	ctx = a.admin.server.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.DatabaseDetailsRequest{
		Database: pathVars["database_name"],
	}

	var resp databaseDetailsResponse
	dbDetailsResp, err := a.admin.databaseDetailsHelper(ctx, req, username, limit, offset)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			http.Error(w, "database not found", http.StatusNotFound)
		} else {
			apiV2InternalError(ctx, err, w)
		}
		return
	}
	resp.DatabaseDetailsResponse = *dbDetailsResp
	if limit > 0 && len(resp.TableNames) == limit {
		resp.Next = limit
	}
	writeJSONResponse(ctx, w, 200, resp)
}

// Response for tableDetails.
//
// swagger:model tableDetailsResponse
type tableDetailsResponse	serverpb.TableDetailsResponse

// swagger:operation GET /databases/{database}/tables/{table}/ tableDetails
//
// Get table details
//
// Returns details about a table.
//
// ---
// parameters:
// - name: database
//   type: string
//   in: path
//   description: Name of database being looked up.
//   required: true
// - name: table
//   type: string
//   in: path
//   description: Name of table being looked up.
//   required: true
// produces:
// - application/json
// responses:
//   "200":
//     description: Database details response
//     schema:
//       "$ref": "#/definitions/tableDetailsResponse"
//   "404":
//     description: Database or table not found
func (a *apiV2Server) tableDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	username := security.MakeSQLUsernameFromPreNormalizedString(
		ctx.Value(webSessionUserKey{}).(string))
	ctx = a.admin.server.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.TableDetailsRequest{
		Database: pathVars["database_name"],
		Table:    pathVars["table_name"],
	}

	resp, err := a.admin.tableDetailsHelper(ctx, req, username)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			http.Error(w, "database or table not found", http.StatusNotFound)
		} else {
			apiV2InternalError(ctx, err, w)
		}
		return
	}
	writeJSONResponse(ctx, w, 200, tableDetailsResponse(*resp))
}
