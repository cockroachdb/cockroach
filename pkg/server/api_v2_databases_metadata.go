// Copyright 2024 The Cockroach Authors.
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
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
)

const (
	dbIdKey         = "dbId"
	nameKey         = "name"
	sortByKey       = "sortBy"
	sortOrderKey    = "sortOrder"
	pageNumKey      = "pageNum"
	pageSizeKey     = "pageSize"
	storeIdKey      = "storeId"
	defaultPageSize = 10
	defaultPageNum  = 1
)

// GetTableMetadata returns a paginated response of table metadata and statistics. This is not a live view of
// the table data but instead is cached data that had been precomputed at an earlier time.
//
// The user making the request will receive table metadata based on the CONNECT database grant and admin privilege.
// If the user provides a database id that they are not authorized for, they will receive an empty response. Similarly,
// if the user does not provide a database id, the result set will only include databases in which they have the
// previously mentioned authorizations.
//
// ---
// parameters:
//
//   - name: dbId
//     type: integer
//     description: The id of the database to fetch table metadata.
//     in: query
//     required: false
//
//   - name: name
//     type: string
//     description: a string which is used to match table and schema names against. This string is tokenized by "." and
//     all the tokens are matched against both schema and table name. For a table to match this parameter, each token
//     must match either the schema name or the table name.
//     in: query
//     required: false
//
//   - name: sortBy
//     type: string
//     description: Which column to sort by. This currently supports: "replicationSize", "ranges", "liveData",
//     "columns", "indexes", "lastUpdated". If a non supported value is provided, it will be ignored.
//     in: query
//     required: false
//
//   - name: sortOrder
//     type: string
//     description: The direction in which to order the sortBy column by. Supports either "asc" or "desc". If a non
//     supported value is provided, it will be ignored.
//     in: query
//     required: false
//
//   - name: pageSize
//     type: string
//     description: The size of the page of the result set to return.
//     in: query
//     required: false
//
//   - name: pageNum
//     type: string
//     description: The page number of the result set to return.
//     in: query
//     required: false
//
//   - name: storeId
//     type: integer
//     description: The id of the store to filter tables by. If the table contains data the store, it will be included
//     in the result set. Multiple storeId query parameters are support. If multiple are provided, a table will be
//     included in the result set if it contains data in at least one of the stores.
//     in: query
//     required: false
//
// produces:
// - application/json
//
// responses:
//
//	"200":
//	  description: A paginated response of tableMetadata results.
//	"422":
//		description: An UnprocessableEntity error if dbId, storeId, pageNum, or pageKey are not int values
func (a *apiV2Server) GetTableMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)
	// TODO (kyle): build http method handling directly into route registration
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	queryValues := r.URL.Query()
	dbId, err := apiutil.GetIntQueryStringVal(queryValues, dbIdKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", dbIdKey),
			http.StatusUnprocessableEntity)
		return
	}
	tableName := queryValues.Get(nameKey)
	sortByQs := queryValues.Get(sortByKey)
	sortOrderQS := queryValues.Get(sortOrderKey)
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageNumKey),
			http.StatusUnprocessableEntity)
		return
	}
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageSizeKey),
			http.StatusUnprocessableEntity)
		return
	}

	storeIds, err := apiutil.GetIntQueryStringVals(queryValues, storeIdKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", storeIdKey),
			http.StatusUnprocessableEntity)
		return
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	if pageNum <= 0 {
		pageNum = defaultPageNum
	}

	offset := (pageNum - 1) * pageSize

	var sortBy string
	switch sortByQs {
	case "name":
		sortBy = "(schema_name, table_name)"
	case "replicationSize":
		sortBy = "replication_size_bytes"
	case "ranges":
		sortBy = "total_ranges"
	case "liveData":
		sortBy = "perc_live_data"
	case "columns":
		sortBy = "total_columns"
	case "indexes":
		sortBy = "total_indexes"
	case "lastUpdated":
		sortBy = "last_updated"
	}

	var sortOrder string
	if sortBy != "" {
		sortOrder = "ASC"
		if sortOrderQS == "desc" {
			sortOrder = "DESC"
		}
	}

	var nameFilters []string
	if tableName != "" {
		tokenized := strings.Split(tableName, ".")
		for _, token := range tokenized {
			nameFilters = append(nameFilters, fmt.Sprintf("%%%s%%", token))
		}
	}

	tmd, totalRowCount, err := a.getTableMetadata(ctx, sqlUser, dbId, nameFilters, storeIds, sortBy, sortOrder,
		pageSize, offset)

	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	resp := PaginatedResponse[[]tableMetadata]{
		Results: tmd,
		PaginationInfo: paginationInfo{
			TotalResults: totalRowCount,
			PageSize:     pageSize,
			PageNum:      pageNum,
		},
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)

}

func (a *apiV2Server) getTableMetadata(
	ctx context.Context,
	sqlUser username.SQLUsername,
	dbId int,
	nameFilters []string,
	storeIds []int,
	sortBy string,
	sortOrder string,
	limit int,
	offset int,
) (tms []tableMetadata, totalRowCount int64, retErr error) {
	sqlUserStr := sqlUser.Normalized()
	query := safesql.NewQuery()
	// Base query fetches from system.table_metadata, but only returns a metadata for tables
	// in which the sql user has the `CONNECT` database privilege for or if the sql user is an
	// admin.
	query.Append(`SELECT
  		tbm.db_id,
  		tbm.db_name,
  		tbm.table_id,
  		tbm.schema_name,
			tbm.table_name,
			tbm.replication_size_bytes, 
			tbm.total_ranges, 
			tbm.total_columns, 
			tbm.total_indexes, 
			tbm.perc_live_data,
			tbm.total_live_data_bytes,
			tbm.total_data_bytes,
			tbm.store_ids, 
			COALESCE(tbm.last_update_error, '') as last_update_error,
			tbm.last_updated,
			count(*) OVER() as total_row_count
		FROM system.table_metadata tbm
		LEFT JOIN system.role_members rm ON rm.role = 'admin' AND member = $
		WHERE (rm.role = 'admin' OR tbm.db_name in (
	  			SELECT cdp.database_name
	  			FROM "".crdb_internal.cluster_database_privileges cdp
	  			WHERE grantee = $
	  			AND privilege_type = 'CONNECT'
	  		))
		`, sqlUserStr, sqlUserStr)

	// Add filter for db id if one  is provided
	if dbId > 0 {
		query.Append("AND tbm.db_id = $ ", dbId)
	}

	// If name filters are provided, filter on those. For each name filter,
	// we check against both the schema name and the table name. Each name
	// filter must match either the schema name or table name.
	if len(nameFilters) > 0 {
		query.Append("AND (")
		qs := make([]string, 0, len(nameFilters))
		qa := make([]interface{}, 0, len(nameFilters)*2)
		for _, nameFilter := range nameFilters {
			qs = append(qs, "(tbm.schema_name ILIKE $ OR tbm.table_name ILIKE $)")
			qa = append(qa, nameFilter, nameFilter)
		}
		query.Append(strings.Join(qs, " AND "), qa...)
		query.Append(") ")
	}

	// If store ids are provided, at least one of the store
	// ids must exist in the store_ids array.
	if len(storeIds) > 0 {
		query.Append("AND ( ")
		for i, storeId := range storeIds {
			query.Append("tbm.store_ids @> ARRAY[$] ", storeId)
			if i < len(storeIds)-1 {
				query.Append("OR ")
			}
		}
		query.Append(") ")
	}

	orderBy := ""
	if sortBy != "" {
		orderBy = fmt.Sprintf("%s %s,", sortBy, sortOrder)
	}

	query.Append(fmt.Sprintf("ORDER BY %s table_id %s ", orderBy, sortOrder))
	query.Append("LIMIT $ ", limit)
	query.Append("OFFSET $ ", offset)

	it, err := a.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "get-table-metadata", nil, /* txn */
		sessiondata.InternalExecutorOverride{},
		// We only want to show the grants on the database.
		query.String(), query.QueryArguments()...,
	)

	if err != nil {
		return nil, totalRowCount, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, totalRowCount, err
	}

	setTotalRowCount := true
	if ok {
		// If ok == false, the query returned 0 rows.
		scanner := makeResultScanner(it.Types())
		for ; ok; ok, err = it.Next(ctx) {
			var tmd tableMetadata
			row := it.Cur()
			if setTotalRowCount {
				if err := scanner.Scan(row, "total_row_count", &totalRowCount); err != nil {
					return nil, totalRowCount, err
				}
				setTotalRowCount = false
			}
			if err := scanner.Scan(row, "db_id", &tmd.DbId); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "db_name", &tmd.DbName); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "table_id", &tmd.TableId); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "schema_name", &tmd.SchemaName); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "table_name", &tmd.TableName); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "store_ids", &tmd.StoreIds); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "replication_size_bytes", &tmd.ReplicationSizeBytes); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "total_ranges", &tmd.RangeCount); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "total_columns", &tmd.ColumnCount); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "total_indexes", &tmd.IndexCount); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "perc_live_data", &tmd.PercentLiveData); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "total_live_data_bytes", &tmd.TotalLiveDataBytes); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "total_data_bytes", &tmd.TotalDataBytes); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "last_update_error", &tmd.LastUpdateError); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "last_updated", &tmd.LastUpdated); err != nil {
				return nil, totalRowCount, err
			}
			tms = append(tms, tmd)
		}
		if err != nil {
			return nil, 0, err
		}
	}

	return tms, totalRowCount, nil
}

// GetDBMetadata returns a paginated response of database metadata and statistics. This is not a live view of
// the database data but instead is cached data that had been precomputed at an earlier time.
//
// The user making the request will receive database metadata based on the CONNECT database grant and admin privilege.
//
// ---
// parameters:
//
//   - name: name
//     type: string
//     description: a string which is used to match database name against.
//     in: query
//     required: false
//
//   - name: sortBy
//     type: string
//     description: Which column to sort by. This currently supports: "name", "size", "tableCount", and "lastUpdated".
//     If a non supported value is provided, it will be ignored.
//     in: query
//     required: false
//
//   - name: sortOrder
//     type: string
//     description: The direction in which to order the sortBy column by. Supports either "asc" or "desc". If a non
//     supported value is provided, it will be ignored.
//     in: query
//     required: false
//
//   - name: pageSize
//     type: string
//     description: The size of the page of the result set to return.
//     in: query
//     required: false
//
//   - name: pageNum
//     type: string
//     description: The page number of the result set to return.
//     in: query
//     required: false
//
//   - name: storeId
//     type: integer
//     description: The id of the store to filter databases by. If the database has at least one table in it that
//     contains data in the store, it will be included in the result set. Multiple storeId query parameters are support.
//     If multiple are provided, a database will be included in the result set if it contains >0 tables data in at least
//     one of the stores.
//     in: query
//     required: false
//
// produces:
// - application/json
//
// responses:
//
//	"200":
//	  description: A paginated response of dbMetadata results.
func (a *apiV2Server) GetDBMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	queryValues := r.URL.Query()

	dbName := queryValues.Get(nameKey)
	sortByQs := queryValues.Get(sortByKey)
	sortOrderQs := queryValues.Get(sortOrderKey)
	pageNum, err := apiutil.GetIntQueryStringVal(queryValues, pageNumKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageNumKey),
			http.StatusUnprocessableEntity)
		return
	}
	pageSize, err := apiutil.GetIntQueryStringVal(queryValues, pageSizeKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", pageSizeKey),
			http.StatusUnprocessableEntity)
		return
	}

	storeIds, err := apiutil.GetIntQueryStringVals(queryValues, storeIdKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid query param value for: %s", storeIdKey),
			http.StatusUnprocessableEntity)
		return
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	if pageNum <= 0 {
		pageNum = defaultPageNum
	}

	offset := (pageNum - 1) * pageSize

	var sortBy string
	switch sortByQs {
	case "name":
		sortBy = "db_name"
	case "size":
		sortBy = "size_bytes"
	case "tableCount":
		sortBy = "table_count"
	case "lastUpdated":
		sortBy = "last_updated"
	}

	var sortOrder string
	if sortByQs != "" {
		sortOrder = "ASC"
		if sortOrderQs == "desc" {
			sortOrder = "DESC"
		}
	}

	var dbNameFilter string
	if dbName != "" {
		dbNameFilter = fmt.Sprintf("%%%s%%", dbName)
	}

	dbm, totalRowCount, err := a.getDBMetadata(ctx, sqlUser, dbNameFilter, storeIds, sortBy, sortOrder, pageSize, offset)

	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	resp := PaginatedResponse[[]dbMetadata]{
		Results: dbm,
		PaginationInfo: paginationInfo{
			TotalResults: totalRowCount,
			PageSize:     pageSize,
			PageNum:      pageNum,
		},
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)

}

func (a *apiV2Server) getDBMetadata(
	ctx context.Context,
	sqlUser username.SQLUsername,
	dbName string,
	storeIds []int,
	sortBy string,
	sortOrder string,
	limit int,
	offset int,
) (dbms []dbMetadata, totalRowCount int64, retErr error) {
	sqlUserStr := sqlUser.Normalized()
	dbms = make([]dbMetadata, 0)
	query := safesql.NewQuery()

	// Base query aggregates table metadata by db_id. It joins on a subquery which flattens
	// and deduplicates all store ids for tables in a database into a single array. This query
	// will only return databases that the provided sql user has CONNECT privileges to. If they
	// are an admin, they have access to all databases.
	query.Append(`SELECT
		tbm.db_id,
		tbm.db_name,
		sum(tbm.replication_size_bytes):: INT as size_bytes,
		count(tbm.table_id) as table_count,
		max(tbm.last_updated) as last_updated,
		s.store_ids,
		count(*) OVER() as total_row_count
		FROM system.table_metadata tbm
		JOIN crdb_internal.databases dbs ON dbs.id = tbm.db_id
		LEFT JOIN system.role_members rm ON rm.role = 'admin' AND member = $
		JOIN (
			SELECT db_id, array_agg(DISTINCT unnested_ids) as store_ids
			FROM system.table_metadata, unnest(store_ids) as unnested_ids
			GROUP BY db_id
		) s ON s.db_id = tbm.db_id
		WHERE (rm.role = 'admin' OR dbs.name in (
			SELECT cdp.database_name
			FROM "".crdb_internal.cluster_database_privileges cdp
			WHERE grantee = $
			AND privilege_type = 'CONNECT'
		))
`, sqlUserStr, sqlUserStr)

	if dbName != "" {
		query.Append("AND db_name ILIKE $ ", dbName)
	}

	// If store ids are provided, at least one of the store
	// ids must exist in the store_ids array.
	if len(storeIds) > 0 {
		query.Append("AND ( ")
		for i, storeId := range storeIds {
			query.Append("tbm.store_ids @> ARRAY[$] ", storeId)
			if i < len(storeIds)-1 {
				query.Append("OR ")
			}
		}
		query.Append(") ")
	}

	orderBy := ""
	if sortBy != "" {
		orderBy = fmt.Sprintf("%s %s,", sortBy, sortOrder)
	}

	query.Append("GROUP BY tbm.db_id, tbm.db_name, s.store_ids ")
	query.Append(fmt.Sprintf("ORDER BY %s db_id %s ", orderBy, sortOrder))
	query.Append("LIMIT $ ", limit)
	query.Append("OFFSET $ ", offset)

	it, err := a.admin.internalExecutor.QueryIteratorEx(
		ctx, "get-database-metadata", nil, /* txn */
		sessiondata.InternalExecutorOverride{},
		query.String(), query.QueryArguments()...,
	)

	if err != nil {
		return nil, totalRowCount, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, totalRowCount, err
	}

	setTotalRowCount := true
	if ok {
		// If ok == false, the query returned 0 rows.
		scanner := makeResultScanner(it.Types())
		for ; ok; ok, err = it.Next(ctx) {
			var dbm dbMetadata
			row := it.Cur()
			if setTotalRowCount {
				if err := scanner.Scan(row, "total_row_count", &totalRowCount); err != nil {
					return nil, totalRowCount, err
				}
				setTotalRowCount = false
			}
			if err := scanner.Scan(row, "db_id", &dbm.DbId); err != nil {
				return nil, 0, err
			}
			if err := scanner.Scan(row, "db_name", &dbm.DbName); err != nil {
				return nil, 0, err
			}
			if err := scanner.Scan(row, "size_bytes", &dbm.SizeBytes); err != nil {
				return nil, 0, err
			}
			if err := scanner.Scan(row, "table_count", &dbm.TableCount); err != nil {
				return nil, 0, err
			}
			if err := scanner.Scan(row, "store_ids", &dbm.StoreIds); err != nil {
				return nil, totalRowCount, err
			}
			if err := scanner.Scan(row, "last_updated", &dbm.LastUpdated); err != nil {
				return nil, 0, err
			}
			dbms = append(dbms, dbm)
		}
		if err != nil {
			return nil, 0, err
		}
	}

	return dbms, totalRowCount, nil
}

// TableMetadataJob routes to the necessary receiver based on the http method of the request. Requires
// The user making the request must have the CONNECT database grant on at least one database and the VIEWJOB system
// privilege, or admin privilege.
// ---
// produces:
// - application/json
//
// responses:
//
//	"200":
//	  description: A tmUpdateJobStatusResponse
//	"404":
//	  description: Not found if the user doesn't have the correct authorizations
func (a *apiV2Server) TableMetadataJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	authorized, err := a.updateTableMetadataJobAuthorized(ctx, sqlUser)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	if !authorized {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	var resp interface{}
	switch r.Method {
	case http.MethodGet:
		resp, err = a.getTableMetadataUpdateJobStatus(ctx)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

// getTableMetadataUpdateJobStatus gets the status of the table metadata update job. The requesting user
// must have the CONNECT privilege to at least one database on the cluster, or the admin role. If the user
// doesn't have the necessary authorization, an "empty" response will be returned.
func (a *apiV2Server) getTableMetadataUpdateJobStatus(
	ctx context.Context,
) (jobStatus tmUpdateJobStatusResponse, retErr error) {
	query := safesql.NewQuery()
	query.Append(`
	SELECT 
	  TIMESTAMPTZ 'epoch' + (progress->>'modifiedMicros' || ' microseconds')::interval as last_modified,
	  coalesce((progress->>'fractionCompleted')::FLOAT, 0) as fraction_completed,
	  (progress->'tableMetadataCache'->>'lastCompletedTime')::TIMESTAMPTZ as last_completed_time,
	  (progress->'tableMetadataCache'->>'lastStartTime')::TIMESTAMPTZ as last_start_time,
	  coalesce(progress->'tableMetadataCache'->>'status', 'NOT_RUNNING') as status
	FROM (
		SELECT crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', progress) as progress
		FROM crdb_internal.system_jobs
		WHERE id = $
	)
`, jobs.UpdateTableMetadataCacheJobID)

	it, err := a.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "get-tableMetadataUpdateJob-status", nil, /* txn */
		sessiondata.InternalExecutorOverride{},
		query.String(), query.QueryArguments()...,
	)

	if err != nil {
		return jobStatus, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return jobStatus, err
	}

	if ok {
		// If ok == false, the query returned 0 rows.
		row := it.Cur()
		scanner := makeResultScanner(it.Types())
		if err = scanner.Scan(row, "fraction_completed", &jobStatus.Progress); err != nil {
			return jobStatus, err
		}
		if err = scanner.Scan(row, "last_modified", &jobStatus.LastUpdatedTime); err != nil {
			return jobStatus, err
		}
		if err = scanner.Scan(row, "last_start_time", &jobStatus.LastStartTime); err != nil {
			return jobStatus, err
		}
		if err = scanner.Scan(row, "last_completed_time", &jobStatus.LastCompletedTime); err != nil {
			return jobStatus, err
		}
		if err = scanner.Scan(row, "status", &jobStatus.CurrentStatus); err != nil {
			return jobStatus, err
		}
	}
	return jobStatus, nil
}

func (a *apiV2Server) updateTableMetadataJobAuthorized(
	ctx context.Context, sqlUser username.SQLUsername,
) (isAuthorized bool, retErr error) {
	query := safesql.NewQuery()
	sqlUserStr := sqlUser.Normalized()
	query.Append(`
	SELECT count(*)
	FROM (
	  SELECT 1 FROM system.role_members WHERE member = $ AND role = 'admin'
		UNION
		SELECT 1 
		FROM "".crdb_internal.cluster_database_privileges cdp
	 	WHERE cdp.grantee = $ 
	 	AND cdp.privilege_type = 'CONNECT' 
	)
`, sqlUserStr, sqlUserStr)

	it, err := a.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "check-updatejob-authorized", nil, /* txn */
		sessiondata.InternalExecutorOverride{},
		query.String(), query.QueryArguments()...,
	)

	if err != nil {
		return false, err
	}

	defer func(it isql.Rows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return false, err
	}

	if ok {
		// If ok == false, the query returned 0 rows.
		row := it.Cur()
		scanner := makeResultScanner(it.Types())
		var count int64
		if err = scanner.Scan(row, "count", &count); err != nil {
			return false, err
		}
		return count > 0, nil
	}
	return false, nil

}

type PaginatedResponse[T any] struct {
	Results        T              `json:"results"`
	PaginationInfo paginationInfo `json:"paginationInfo"`
}

type paginationInfo struct {
	TotalResults int64 `json:"total_results"`
	PageSize     int   `json:"page_size"`
	PageNum      int   `json:"page_num"`
}

type tableMetadata struct {
	DbId                 int64     `json:"db_id"`
	DbName               string    `json:"db_name"`
	TableId              int64     `json:"table_id"`
	SchemaName           string    `json:"schema_name"`
	TableName            string    `json:"table_name"`
	ReplicationSizeBytes int64     `json:"replication_size_bytes"`
	RangeCount           int64     `json:"range_count"`
	ColumnCount          int64     `json:"column_count"`
	IndexCount           int64     `json:"index_count"`
	PercentLiveData      float32   `json:"percent_live_data"`
	TotalLiveDataBytes   int64     `json:"total_live_data_bytes"`
	TotalDataBytes       int64     `json:"total_data_bytes"`
	StoreIds             []int64   `json:"store_ids"`
	LastUpdateError      string    `json:"last_update_error,omitempty"`
	LastUpdated          time.Time `json:"last_updated"`
}

type dbMetadata struct {
	DbId        int64     `json:"db_id,omitempty"`
	DbName      string    `json:"db_name,omitempty"`
	SizeBytes   int64     `json:"size_bytes,omitempty"`
	TableCount  int64     `json:"table_count,omitempty"`
	StoreIds    []int64   `json:"store_ids"`
	LastUpdated time.Time `json:"last_updated"`
}

type tmUpdateJobStatusResponse struct {
	CurrentStatus     string     `json:"current_status"`
	Progress          float32    `json:"progress"`
	LastStartTime     *time.Time `json:"last_start_time"`
	LastCompletedTime *time.Time `json:"last_completed_time"`
	LastUpdatedTime   *time.Time `json:"last_updated_time"`
}
