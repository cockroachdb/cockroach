// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gorilla/mux"
)

const (
	granteeSortKey string = "grantee"
	privSortKey    string = "privilege"
)

type grantRecord struct {
	Grantee   string `json:"grantee"`
	Privilege string `json:"privilege"`
}

type databaseGrantsResponseWithPagination struct {
	PaginatedResponse[[]grantRecord]
	Name string `json:"name"`
}

// getDatabaseGrants returns a paginated response of grants on the  database with the provided id.
//
// ---
// parameters:
//
//   - name: database_id
//     type: integer
//     description: The ID of the database to get grants for.
//     in: path
//
//   - name: pageNum
//     type: integer
//     description: The page number to retrieve.
//     in: query
//     required: false
//
//   - name: pageSize
//     type: integer
//     description: The number of results to return per page.
//     in: query
//     required: false
//
//   - name: sortBy
//     type: string
//     description: The column to sort by.
//     in: query
//     required: false
//
//   - name: sortOrder
//     type: string
//     description: The order to sort by. Must be either "asc" or "desc", case insensitive.
//     in: query
//     required: false
//
// produces:
// - application/json: databaseGrantsResponseWithPagination
//
// responses:
//
//	"200":
//		description: A paginated response of grants on the provided database.
//	"404":
//		description: The db does not exist.
//	"400":
//		description: The request is malformed.
//	"500":
//		description: An internal server error occurred.
func (a *apiV2Server) getDatabaseGrants(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate request parameters.
	pathVars := mux.Vars(r)
	dbId, err := strconv.Atoi(pathVars[dbIdPathVar])
	if err != nil {
		http.Error(w, "invalid database id", http.StatusBadRequest)
		return
	}
	queryValues := r.URL.Query()
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset := 0
	if pageNum > 0 {
		offset = (pageNum - 1) * pageSize
	}
	sortBy := getValidGrantsSortParam(queryValues.Get(sortByKey))
	var sortOrder string
	if sortBy != "" {
		var ok bool
		sortOrder, ok = validateSortOrderValue(queryValues.Get(sortOrderKey))
		if !ok {
			http.Error(w, "invalid sort order", http.StatusBadRequest)
			return
		}
	}

	// Get the database grants.
	resp, err := getDatabaseGrantsResponseWithPagination(
		ctx,
		a.sqlServer.internalExecutor,
		dbId, sqlUser,
		pageSize,
		offset,
		sortBy,
		sortOrder)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	if resp.Name == "" {
		http.Error(w, "database does not exist", http.StatusNotFound)
		return
	}

	resp.PaginationInfo.PageNum = pageNum
	resp.PaginationInfo.PageSize = pageSize

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

// grantsVisibleToUser reports whether sqlUser can see any grant rows for the
// object targeted by probeQuery. It distinguishes an empty trailing page (an
// accessible object paged past its last grant) from an object the user cannot
// see, so callers can 404 only the latter and keep it indistinguishable from a
// non-existent id.
//
// it is the exhausted grants iterator; it is closed before the probe runs
// because the internal executor runs one statement at a time. The caller's
// deferred Close is then a safe no-op.
func grantsVisibleToUser(
	ctx context.Context,
	ie *sql.InternalExecutor,
	it isql.Rows,
	sqlUser username.SQLUsername,
	opName redact.RedactableString,
	probeQuery string,
	qargs ...interface{},
) (bool, error) {
	if err := it.Close(); err != nil {
		return false, err
	}
	row, err := ie.QueryRowEx(
		ctx, opName, nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser}, probeQuery, qargs...)
	if err != nil {
		return false, err
	}
	return row != nil, nil
}

func getDatabaseGrantsResponseWithPagination(
	ctx context.Context,
	ie *sql.InternalExecutor,
	dbId int,
	sqlUser username.SQLUsername,
	limit int,
	offset int,
	sortBy string,
	sortOrder string,
) (resp databaseGrantsResponseWithPagination, retErr error) {
	row, err := ie.QueryRowEx(
		ctx, "get-database-name-by-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
SELECT name FROM system.namespace
WHERE "parentID" = 0 AND "parentSchemaID" = 0 AND id = $1`, dbId)
	if err != nil {
		return resp, err
	}

	if row == nil || row[0] == tree.DNull {
		// Database id does not exist.
		return resp, nil
	}

	dbName := string(*row[0].(*tree.DString))
	resp.Name = dbName

	escDbName := tree.NameStringP(&dbName)

	query := safesql.NewQuery()
	query.Append(fmt.Sprintf(`
SELECT grantee, privilege_type, count(*) OVER() as total_row_count
FROM %s.crdb_internal.cluster_database_privileges`, escDbName))

	if sortBy != "" {
		query.Append(fmt.Sprintf(" ORDER BY %s %s", sortBy, sortOrder))
	}

	// Pagination arguments.
	if limit > 0 {
		query.Append(" LIMIT $", limit)
	}
	if offset > 0 {
		query.Append(" OFFSET $", offset)
	}

	resp.Results = make([]grantRecord, 0)
	it, err := ie.QueryIteratorEx(ctx, "get-database-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		return resp, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return resp, err
	}
	if !ok {
		// Zero rows means either the user cannot see the database or an accessible
		// database was paged past its last grant. Only the former must 404, to stay
		// indistinguishable from a non-existent id. At offset 0 there is no earlier
		// page, so zero rows already implies no access; past offset 0, probe the
		// first page to tell the two apart.
		if offset > 0 {
			visible, err := grantsVisibleToUser(ctx, ie, it, sqlUser,
				"get-database-grants-visibility",
				fmt.Sprintf(
					`SELECT 1 FROM %s.crdb_internal.cluster_database_privileges LIMIT 1`, escDbName))
			if err != nil {
				return resp, err
			}
			if visible {
				// Empty trailing page for a visible database: keep the name, 200.
				return resp, nil
			}
		}
		resp.Name = ""
		return resp, nil
	}

	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		if err != nil {
			return resp, err
		}
		grant := grantRecord{}
		row := it.Cur()
		if resp.PaginationInfo.TotalResults == 0 {
			resp.PaginationInfo.TotalResults = int64(tree.MustBeDInt(row[2]))
		}
		if err := scanner.Scan(row, "grantee", &grant.Grantee); err != nil {
			return resp, err
		}
		if err := scanner.Scan(row, "privilege_type", &grant.Privilege); err != nil {
			return resp, err
		}
		resp.Results = append(resp.Results, grant)
	}
	return resp, nil
}

// getTableGrants returns a paginated response of grants for the provided table.
//
// ---
// parameters:
//
//   - name: table_id
//     type: integer
//     description: The ID of the table to get grants for.
//     in: path
//
//   - name: pageNum
//     type: integer
//     description: The page number to retrieve.
//     in: query
//     required: false
//
//   - name: pageSize
//     type: integer
//     description: The number of results to return per page.
//     in: query
//     required: false
//
//   - name: sortBy
//     type: string
//     description: The column to sort by.
//     in: query
//     required: false
//
//   - name: sortOrder
//     type: string
//     description: The order to sort by. Must be either "asc" or "desc", case insensitive.
//     in: query
//     required: false
//
// produces:
// - application/json: tableGrantsResponseWithPagination
//
// responses:
//
//	"200":
//		description: A paginated response of grants on the provided table.
//	"404":
//		description: The table does not exist.
//	"400":
//		description: The request is malformed.
//	"500":
//		description: An internal server error occurred.
func (a *apiV2Server) getTableGrants(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate request parameters.
	pathVars := mux.Vars(r)
	tableId, err := strconv.Atoi(pathVars[tableIdPathVar])
	if err != nil {
		http.Error(w, "invalid database id", http.StatusBadRequest)
		return
	}
	queryValues := r.URL.Query()
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset := 0
	if pageNum > 0 {
		offset = (pageNum - 1) * pageSize
	}
	sortBy := getValidGrantsSortParam(queryValues.Get(sortByKey))
	var sortOrder string
	if sortBy != "" {
		var ok bool
		sortOrder, ok = validateSortOrderValue(queryValues.Get(sortOrderKey))
		if !ok {
			http.Error(w, "invalid sort order", http.StatusBadRequest)
			return
		}
	}

	// Get the table grants.
	resp, err := getTableGrantsResponseWithPagination(
		ctx,
		a.sqlServer.internalExecutor,
		tableId,
		sqlUser,
		pageSize,
		offset,
		sortBy,
		sortOrder)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	if resp.TableName == "" {
		http.Error(w, "table does not exist", http.StatusNotFound)
		return
	}

	resp.PaginationInfo.PageNum = pageNum
	resp.PaginationInfo.PageSize = pageSize

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

type tableGrantsResponseWithPagination struct {
	PaginatedResponse[[]grantRecord]
	DatabaseName string `json:"database_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
}

func getTableGrantsResponseWithPagination(
	ctx context.Context,
	ie *sql.InternalExecutor,
	tableId int,
	sqlUser username.SQLUsername,
	limit int,
	offset int,
	sortBy string,
	sortOrder string,
) (resp tableGrantsResponseWithPagination, retErr error) {
	row, err := ie.QueryRowEx(
		ctx, "get-table-name-by-id", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
SELECT
   t.name AS table_name,
   sc.name AS schema_name,
   db.name AS db_name
FROM system.namespace t
   JOIN system.namespace sc ON t."parentSchemaID" = sc.id
   JOIN system.namespace db on t."parentID" = db.id
WHERE t.id = $1
`, tableId)
	if err != nil {
		return resp, err
	}

	if row == nil || row[0] == tree.DNull {
		// Table id does not exist.
		return resp, nil
	}

	resp.TableName = string(*row[0].(*tree.DString))
	resp.SchemaName = string(*row[1].(*tree.DString))
	resp.DatabaseName = string(*row[2].(*tree.DString))
	escDbName := tree.NameStringP(&resp.DatabaseName)

	query := safesql.NewQuery()
	query.Append(fmt.Sprintf(`
SELECT grantee, privilege_type, count(*) OVER() as total_row_count
FROM %s.information_schema.table_privileges
WHERE table_name = $ AND table_schema = $`, escDbName),
		resp.TableName, resp.SchemaName)

	if sortBy != "" {
		query.Append(fmt.Sprintf(" ORDER BY %s %s", sortBy, sortOrder))
	}

	// Pagination arguments.
	if limit > 0 {
		query.Append(" LIMIT $", limit)
	}
	if offset > 0 {
		query.Append(" OFFSET $", offset)
	}

	resp.Results = make([]grantRecord, 0)
	it, err := ie.QueryIteratorEx(ctx, "get-table-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		return resp, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return resp, err
	}
	if !ok {
		// Zero rows means either the user cannot see the table or an accessible
		// table was paged past its last grant. Only the former must 404, to stay
		// indistinguishable from a non-existent id. At offset 0 there is no earlier
		// page, so zero rows already implies no access; past offset 0, probe the
		// first page to tell the two apart.
		if offset > 0 {
			visible, err := grantsVisibleToUser(ctx, ie, it, sqlUser,
				"get-table-grants-visibility",
				fmt.Sprintf(
					`SELECT 1 FROM %s.information_schema.table_privileges
WHERE table_name = $1 AND table_schema = $2 LIMIT 1`, escDbName),
				resp.TableName, resp.SchemaName)
			if err != nil {
				return resp, err
			}
			if visible {
				// Empty trailing page for a visible table: keep the names, 200.
				return resp, nil
			}
		}
		resp.TableName = ""
		resp.SchemaName = ""
		resp.DatabaseName = ""
		return resp, nil
	}

	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		if err != nil {
			return resp, err
		}
		grant := grantRecord{}
		row := it.Cur()
		if resp.PaginationInfo.TotalResults == 0 {
			resp.PaginationInfo.TotalResults = int64(tree.MustBeDInt(row[2]))
		}
		if err := scanner.Scan(row, "grantee", &grant.Grantee); err != nil {
			return resp, err
		}
		if err := scanner.Scan(row, "privilege_type", &grant.Privilege); err != nil {
			return resp, err
		}
		resp.Results = append(resp.Results, grant)
	}
	return resp, nil
}

// getValidGrantsSortParam returns a valid sort parameter for the grants
// query based on the provided sortBy value. If the sortBy value is not
// valid, an empty string is returned.
func getValidGrantsSortParam(sortBy string) string {
	switch sortBy {
	case granteeSortKey:
		return "grantee"
	case privSortKey:
		return "privilege_type"
	default:
		return ""
	}
}
