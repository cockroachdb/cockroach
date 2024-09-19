// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Response for listUsers.
type usersResponse struct {
	serverpb.UsersResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// # List users
//
// List SQL users on this cluster.
//
// ---
// parameters:
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Users response
//	  schema:
//	    "$ref": "#/definitions/usersResponse"
func (a *apiV2Server) listUsers(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)

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
	it, err := a.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-users", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: username},
		query, qargs...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	var resp usersResponse
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(tree.MustBeDString(row[0]))})
	}
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	if limit > 0 && len(resp.Users) >= limit {
		resp.Next = offset + len(resp.Users)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for listEvents.
type eventsResponse struct {
	serverpb.EventsResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// # List events
//
// Lists the latest event log entries, in descending order.
//
// ---
// parameters:
//   - name: type
//     type: string
//     in: query
//     description: Type of events to filter for (e.g. "create_table"). Only one
//     event type can be specified at a time.

//	required: false
//	- name: limit
//	  type: integer
//	  in: query
//	  description: Maximum number of results to return in this call.
//	  required: false
//	- name: offset
//	  type: integer
//	  in: query
//	  description: Continuation token for results after a past limited run.
//	  required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Events response
//	  schema:
//	    "$ref": "#/definitions/eventsResponse"
func (a *apiV2Server) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)
	queryValues := r.URL.Query()

	req := &serverpb.EventsRequest{}
	if typ := queryValues.Get("type"); len(typ) > 0 {
		req.Type = typ
	}

	var resp eventsResponse
	eventsResp, err := a.admin.eventsHelper(
		ctx, req, username, limit, offset, true /* redactEvents */)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	resp.EventsResponse = *eventsResp
	if limit > 0 && len(resp.Events) >= limit {
		resp.Next = offset + len(resp.Events)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for listDatabases.
type databasesResponse struct {
	serverpb.DatabasesResponse

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// # List databases
//
// Lists all databases on this cluster.
//
// ---
// parameters:
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Databases response
//	  schema:
//	    "$ref": "#/definitions/databasesResponse"
func (a *apiV2Server) listDatabases(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	ctx := r.Context()
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)

	var resp databasesResponse
	req := &serverpb.DatabasesRequest{}
	dbsResp, err := a.admin.databasesHelper(ctx, req, username, limit, offset)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var databases interface{}
	databases, resp.Next = simplePaginate(dbsResp.Databases, limit, offset)
	resp.Databases = databases.([]string)
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for databaseDetails.
type databaseDetailsResponse struct {
	// DescriptorID is an identifier used to uniquely identify this database.
	DescriptorID int64 `json:"descriptor_id,omitempty"`
}

// # Get database descriptor ID
//
// Returns the database's descriptor ID.
//
// ---
// parameters:
//   - name: database
//     type: string
//     in: path
//     description: Name of database being looked up.
//     required: true
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Database details response
//	  schema:
//	    "$ref": "#/definitions/databaseDetailsResponse"
//	"404":
//	  description: Database not found
func (a *apiV2Server) databaseDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.DatabaseDetailsRequest{
		Database: pathVars["database_name"],
	}
	// TODO(bilal): Find a way to cleanly surface zone config info (returned by
	// getMiscDatabaseDetails) without leaking any sensitive info like
	// SubZoneSpans.
	dbDetailsResp, err := a.admin.getMiscDatabaseDetails(ctx, req, username, nil /* resp */)
	if err != nil {
		if status.Code(err) == codes.NotFound || isNotFoundError(err) {
			http.Error(w, "database not found", http.StatusNotFound)
		} else {
			srverrors.APIV2InternalError(ctx, err, w)
		}
		return
	}
	resp := databaseDetailsResponse{
		DescriptorID: dbDetailsResp.DescriptorID,
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for databaseGrants.
type databaseGrantsResponse struct {
	// Grants are the privileges granted to users on this database.
	Grants []serverpb.DatabaseDetailsResponse_Grant `json:"grants"`

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// # Lists grants on a database
//
// Returns grants on a database. Grants are the privileges granted to users
// on this database.
//
// ---
// parameters:
//   - name: database
//     type: string
//     in: path
//     description: Name of the database being looked up.
//     required: true
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of grants to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Database grants response
//	  schema:
//	    "$ref": "#/definitions/databaseGrantsResponse"
//	"404":
//	  description: Database not found
func (a *apiV2Server) databaseGrants(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := getSimplePaginationValues(r)
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.DatabaseDetailsRequest{
		Database: pathVars["database_name"],
	}
	grants, err := a.admin.getDatabaseGrants(ctx, req, username, limit, offset)
	if err != nil {
		if status.Code(err) == codes.NotFound || isNotFoundError(err) {
			http.Error(w, "database not found", http.StatusNotFound)
		} else {
			srverrors.APIV2InternalError(ctx, err, w)
		}
		return
	}
	resp := databaseGrantsResponse{Grants: grants}
	if limit > 0 && len(grants) >= limit {
		resp.Next = offset + len(grants)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for databaseTables.
type databaseTablesResponse struct {
	// TableNames contains the names of all tables in this database. Note that
	// all responses will be schema-qualified (schema.table) and that every schema
	// or table that contains a "sql unsafe character" such as uppercase letters
	// or dots will be surrounded with double quotes, such as "naughty schema".table.
	TableNames []string `json:"table_names,omitempty"`

	// The continuation token, for use in the next paginated call in the `offset`
	// parameter.
	Next int `json:"next,omitempty"`
}

// # Lists tables on a database
//
// Lists names of all tables in the database. The names of all responses will
// be schema-qualified.
//
// ---
// parameters:
//   - name: database
//     type: string
//     in: path
//     description: Name of the database being looked up.
//     required: true
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of tables to return in this call.
//     required: false
//   - name: offset
//     type: integer
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Database tables response
//	  schema:
//	    "$ref": "#/definitions/databaseTablesResponse"
//	"404":
//	  description: Database not found
func (a *apiV2Server) databaseTables(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := getSimplePaginationValues(r)
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.DatabaseDetailsRequest{
		Database: pathVars["database_name"],
	}
	tables, err := a.admin.getDatabaseTables(ctx, req, username, limit, offset)
	if err != nil {
		if status.Code(err) == codes.NotFound || isNotFoundError(err) {
			http.Error(w, "database not found", http.StatusNotFound)
		} else {
			srverrors.APIV2InternalError(ctx, err, w)
		}
		return
	}
	resp := databaseTablesResponse{TableNames: tables}
	if limit > 0 && len(tables) >= limit {
		resp.Next = offset + len(tables)
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// Response for tableDetails.
type tableDetailsResponse serverpb.TableDetailsResponse

// # Get table details
//
// Returns details about a table.
//
// ---
// parameters:
//   - name: database
//     type: string
//     in: path
//     description: Name of the database being looked up.
//     required: true
//   - name: table
//     type: string
//     in: path
//     description: Name of table being looked up. Table may be
//     schema-qualified (schema.table) and each name component that contains
//     sql unsafe characters such as . or uppercase letters must be surrounded
//     in double quotes like "naughty schema".table.
//     required: true
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Database details response
//	  schema:
//	    "$ref": "#/definitions/tableDetailsResponse"
//	"404":
//	  description: Database or table not found
func (a *apiV2Server) tableDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	username := authserver.UserFromHTTPAuthInfoContext(ctx)
	ctx = a.sqlServer.AnnotateCtx(ctx)
	pathVars := mux.Vars(r)
	req := &serverpb.TableDetailsRequest{
		Database: pathVars["database_name"],
		Table:    pathVars["table_name"],
	}

	resp, err := a.admin.tableDetailsHelper(ctx, req, username)
	if err != nil {
		if status.Code(err) == codes.NotFound || isNotFoundError(err) {
			http.Error(w, "database or table not found", http.StatusNotFound)
		} else {
			srverrors.APIV2InternalError(ctx, err, w)
		}
		return
	}
	apiutil.WriteJSONResponse(ctx, w, 200, tableDetailsResponse(*resp))
}
