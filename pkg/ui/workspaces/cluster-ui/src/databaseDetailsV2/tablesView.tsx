// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import { Link } from "react-router-dom";

import { useNodeStatuses } from "src/api";
import {
  TableMetadataRequest,
  TableSortOption,
  useTableMetadata,
} from "src/api/databases/getTableMetadataApi";
import { NodeRegionsSelector } from "src/components/nodeRegionsSelector/nodeRegionsSelector";
import { RegionNodesLabel } from "src/components/regionNodesLabel";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { Search } from "src/sharedFromCloud/search";
import {
  SortDirection,
  Table,
  TableChangeFn,
  TableColumnProps,
} from "src/sharedFromCloud/table";
import useTable, { TableParams } from "src/sharedFromCloud/useTable";
import { StoreID } from "src/types/clusterTypes";
import { Bytes, EncodeDatabaseTableUri } from "src/util";

import { TableColName } from "./constants";
import { TableRow } from "./types";
import { rawTableMetadataToRows } from "./utils";

const COLUMNS: (TableColumnProps<TableRow> & { sortKey?: TableSortOption })[] =
  [
    {
      title: TableColName.NAME,
      width: "15%",
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (t: TableRow) => {
        // This linking is just temporary. We'll need to update it to the correct path
        // using db ID and table ID once we have the table details page.
        const encodedDBPath = EncodeDatabaseTableUri(t.dbName, t.name);
        return <Link to={encodedDBPath}>{t.qualifiedNameWithSchema}</Link>;
      },
      sortKey: TableSortOption.NAME,
    },
    {
      title: TableColName.REPLICATION_SIZE,
      width: "fit-content",
      sorter: (a, b) => a.replicationSizeBytes - b.replicationSizeBytes,
      render: (t: TableRow) => {
        return Bytes(t.replicationSizeBytes);
      },
      sortKey: TableSortOption.REPLICATION_SIZE,
    },
    {
      title: TableColName.RANGE_COUNT,
      width: "fit-content",
      sorter: true,
      render: (t: TableRow) => {
        return t.rangeCount;
      },
      sortKey: TableSortOption.RANGES,
    },
    {
      title: TableColName.COLUMN_COUNT,
      width: "fit-content",
      sorter: true,
      render: (t: TableRow) => {
        return t.columnCount;
      },
      sortKey: TableSortOption.COLUMNS,
    },
    {
      title: TableColName.NODE_REGIONS,
      width: "20%",
      render: (t: TableRow) => (
        <div>
          {Object.entries(t.nodesByRegion ?? {}).map(([region, nodes]) => (
            <RegionNodesLabel
              key={region}
              nodes={nodes}
              region={{ label: region, code: region }}
            />
          ))}
        </div>
      ),
    },
    {
      title: TableColName.LIVE_DATA_PERCENTAGE,
      sorter: true,
      width: "fit-content",
      sortKey: TableSortOption.LIVE_DATA,
      render: (t: TableRow) => {
        return (
          <div>
            <div>{t.liveDataPercentage * 100}%</div>
            <div>
              {Bytes(t.liveDataBytes)} / {Bytes(t.totalDataBytes)}
            </div>
          </div>
        );
      },
    },
    {
      title: TableColName.STATS_LAST_UPDATED,
      sorter: true,
      render: (t: TableRow) => {
        return t.statsLastUpdated.format("YYYY-MM-DD HH:mm:ss");
      },
    },
  ];

const createTableMetadataRequestFromParams = (
  dbID: string,
  params: TableParams,
): TableMetadataRequest => {
  return {
    pagination: {
      pageSize: params.pagination.pageSize,
      pageNum: params.pagination.page,
    },
    sortBy: params.sort?.field ?? "name",
    sortOrder: params.sort?.order,
    dbId: parseInt(dbID, 10),
    storeIds: params.filters.storeIDs.map(sid => parseInt(sid, 10) as StoreID),
    name: params.search,
  };
};

const initialParams: TableParams = {
  filters: { storeIDs: [] as string[] },
  pagination: {
    page: 1,
    pageSize: 10,
  },
  search: "",
  sort: {
    field: TableSortOption.NAME,
    order: "asc",
  },
};

export const TablesPageV2 = () => {
  const { params, setFilters, setSort, setSearch, setPagination } = useTable({
    initial: initialParams,
  });

  // Get db id from the URL.
  const { dbID } = useRouteParams();
  const { data, error, isLoading } = useTableMetadata(
    createTableMetadataRequestFromParams(dbID, params),
  );
  const nodesResp = useNodeStatuses();
  const paginationState = data?.pagination_info;

  const onNodeRegionsChange = (storeIDs: StoreID[]) => {
    setFilters({
      storeIDs: storeIDs.map(sid => sid.toString()),
    });
  };

  const tableData = useMemo(
    () =>
      rawTableMetadataToRows(data?.results ?? [], {
        nodeIDToRegion: nodesResp.nodeIDToRegion,
        storeIDToNodeID: nodesResp.storeIDToNodeID,
        isLoading: nodesResp.isLoading,
      }),
    [data, nodesResp],
  );

  const onTableChange: TableChangeFn<TableRow> = (pagination, sorter) => {
    setPagination({ page: pagination.current, pageSize: pagination.pageSize });
    if (sorter) {
      const colKey = sorter.columnKey;
      if (typeof colKey !== "number") {
        // CockroachDB table component sets the col idx as the column key.
        return;
      }
      setSort({
        field: COLUMNS[colKey].sortKey,
        order: sorter.order === "descend" ? "desc" : "asc",
      });
    }
  };

  const nodeRegionsValue = params.filters.storeIDs.map(
    sid => parseInt(sid, 10) as StoreID,
  );

  const sort = params.sort;
  const colsWithSort = useMemo(
    () =>
      COLUMNS.map((col, i) => {
        const colInd = COLUMNS.findIndex(c => c.sortKey === sort.field);
        const sortOrder: SortDirection =
          sort?.order === "desc" ? "descend" : "ascend";
        return {
          ...col,
          sortOrder: colInd === i && col.sorter ? sortOrder : null,
        };
      }),
    [sort],
  );

  return (
    <>
      <PageSection>
        <PageConfig>
          <PageConfigItem>
            <Search
              defaultValue={params.search}
              placeholder="Search tables"
              onSubmit={setSearch}
            />
          </PageConfigItem>
          <PageConfigItem minWidth={"200px"}>
            <NodeRegionsSelector
              value={nodeRegionsValue}
              onChange={onNodeRegionsChange}
            />
          </PageConfigItem>
        </PageConfig>
      </PageSection>
      <PageSection>
        <Loading page="TablesV2" loading={isLoading} error={error}>
          <PageCount
            page={params.pagination.page}
            pageSize={params.pagination.pageSize}
            total={data?.pagination_info?.total_results ?? 0}
            entity="tables"
          />
          <Table
            columns={colsWithSort}
            dataSource={tableData}
            pagination={{
              size: "small",
              current: params.pagination.page,
              pageSize: params.pagination.pageSize,
              showSizeChanger: false,
              position: ["bottomCenter"],
              total: paginationState?.total_results,
            }}
            onChange={onTableChange}
          />
        </Loading>
      </PageSection>
    </>
  );
};
