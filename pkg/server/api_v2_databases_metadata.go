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
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
)

const (
	dbIdPath        = "database_id"
	dbNameKey       = "dbName"
	tableNameKey    = "tableName"
	sortByKey       = "sortBy"
	sortOrderKey    = "sortOrder"
	pageNumKey      = "pageNum"
	pageSizeKey     = "pageSize"
	storeIdKey      = "storeId"
	defaultPageSize = 10
	defaultPageNum  = 1
)

func (a *apiV2Server) DbTablesMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = a.sqlServer.AnnotateCtx(ctx)
	// TODO (kyle): build http method handling directly into route registration
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO (kyle): add permission / role checks
	queryValues := r.URL.Query()
	qs := &dbTablesMDQueryStringArgs{
		tableName: apiutil.GetQueryStringVal(queryValues, tableNameKey),
		storeIds:  apiutil.GetIntQueryStringVals(queryValues, storeIdKey),
		sortBy:    apiutil.GetQueryStringVal(queryValues, sortByKey),
		sortOrder: apiutil.GetQueryStringVal(queryValues, sortOrderKey),
		pageNum:   apiutil.GetIntQueryStringVal(queryValues, pageNumKey),
		pageSize:  apiutil.GetIntQueryStringVal(queryValues, pageSizeKey),
	}
	pathVars := mux.Vars(r)
	dbId, err := strconv.Atoi(pathVars[dbIdPath])
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
	}

	pageSize := defaultPageSize
	if qs.pageSize.Exists && qs.pageSize.Value > 0 {
		pageSize = qs.pageSize.Value
	}
	pageNum := defaultPageNum
	if qs.pageNum.Exists && qs.pageNum.Value > 0 {
		pageNum = qs.pageNum.Value
	}

	offset := (pageNum - 1) * pageSize

	var sortBy string
	var sortOrder string
	if qs.sortBy.Exists {
		switch qs.sortBy.Value {
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

		switch qs.sortOrder.Value {
		case "asc":
			sortOrder = "ASC"
		case "desc":
			sortOrder = "DESC"
		default:
			sortOrder = "ASC"
		}

	}

	var tableNameFilter string
	if qs.tableName.Exists {
		tableNameFilter = fmt.Sprintf("%%%s%%", qs.tableName.Value)
	}

	tmd, totalRowCount, err := a.getTableMetadata(ctx, dbId, tableNameFilter, qs.storeIds.Value, sortBy, sortOrder,
		pageSize, offset)

	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	resp := dbTableMetadataResponse{
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
	dbId int,
	tableName string,
	storeIds []int,
	sortBy string,
	sortOrder string,
	limit int,
	offset int,
) (tms []tableMetadata, totalRowCount int64, retErr error) {
	tms = make([]tableMetadata, 0)

	query := safesql.NewQuery()
	query.Append(`SELECT 
  		table_id, 
			table_name, 
			replication_size_bytes, 
			total_ranges, 
			total_columns, 
			total_indexes, 
			perc_live_data,
			total_live_data_bytes,
			total_data_bytes,
			store_ids, 
			COALESCE(last_update_error, '') as last_update_error,
			last_updated,
			count(*) OVER() as total_row_count
		FROM system.table_metadata
		WHERE db_id = $`, dbId)

	if tableName != "" {
		query.Append("AND table_name ILIKE $", tableName)
	}

	if len(storeIds) > 0 {
		query.Append("AND ( ")
		for i, storeId := range storeIds {
			query.Append("store_ids @> ARRAY[$] ", storeId)
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
	it, err := a.admin.internalExecutor.QueryIteratorEx(
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
			if err := scanner.Scan(row, "table_id", &tmd.TableId); err != nil {
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
		if err = maybeHandleNotFoundError(ctx, err); err != nil {
			return nil, totalRowCount, err
		}
	}

	return tms, totalRowCount, nil
}

func (a *apiV2Server) DbsMetadata(w http.ResponseWriter, r *http.Request) {
	//ctx := r.Context()
	//ctx = a.sqlServer.AnnotateCtx(ctx)
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
}

type dbTablesMDQueryStringArgs struct {
	tableName apiutil.QueryStringVal[string]
	storeIds  apiutil.QueryStringVal[[]int]
	sortBy    apiutil.QueryStringVal[string]
	sortOrder apiutil.QueryStringVal[string]
	pageNum   apiutil.QueryStringVal[int]
	pageSize  apiutil.QueryStringVal[int]
}

type dbTableMetadataResponse struct {
	Results        []tableMetadata `json:"results,omitempty"`
	PaginationInfo paginationInfo  `json:"paginationInfo,omitempty"`
}

type paginationInfo struct {
	TotalResults int64
	PageSize     int
	PageNum      int
}

type tableMetadata struct {
	TableId              int64     `json:"table_id,omitempty"`
	TableName            string    `json:"table_name,omitempty"`
	ReplicationSizeBytes int64     `json:"replication_size_bytes,omitempty"`
	RangeCount           int64     `json:"range_count,omitempty"`
	ColumnCount          int64     `json:"column_count,omitempty"`
	IndexCount           int64     `json:"index_count,omitempty"`
	PercentLiveData      float32   `json:"percent_live_data,omitempty"`
	TotalLiveDataBytes   int64     `json:"total_live_data_bytes,omitempty"`
	TotalDataBytes       int64     `json:"total_data_bytes,omitempty"`
	StoreIds             []int64   `json:"store_ids,omitempty"`
	LastUpdateError      string    `json:"last_update_error,omitempty"`
	LastUpdated          time.Time `json:"last_updated"`
}
