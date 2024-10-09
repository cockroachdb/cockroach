// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tag } from "antd";
import React, { useMemo } from "react";
import { Link } from "react-router-dom";

import { useNodeStatuses } from "src/api";
import {
  ListTableMetadataRequest,
  TableSortOption,
  useTableMetadata,
} from "src/api/databases/getTableMetadataApi";
import { NodeRegionsSelector } from "src/components/nodeRegionsSelector/nodeRegionsSelector";
import { RegionNodesLabel } from "src/components/regionNodesLabel";
import { TableMetadataJobControl } from "src/components/tableMetadataLastUpdated/tableMetadataJobControl";
import { Tooltip } from "src/components/tooltip";
import { AUTO_STATS_COLLECTION_HELP } from "src/constants/tooltipMessages";
import { useRouteParams } from "src/hooks/useRouteParams";
import { PageSection } from "src/layouts";
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
import { Timestamp } from "src/timestamp";
import { StoreID } from "src/types/clusterTypes";
import { Bytes, DATE_WITH_SECONDS_FORMAT_24_TZ, tabAttr } from "src/util";

import { TableColName } from "./constants";
import { TableRow } from "./types";
import { tableMetadataToRows } from "./utils";

const COLUMNS: (TableColumnProps<TableRow> & { sortKey?: TableSortOption })[] =
  [
    {
      title: (
        <Tooltip title={"The name of the table."}>{TableColName.NAME}</Tooltip>
      ),
      width: "15%",
      sorter: (a, b) => a.tableName.localeCompare(b.tableName),
      render: (t: TableRow) => {
        return (
          <Link to={`/table/${t.tableId}`}>{t.qualifiedNameWithSchema}</Link>
        );
      },
      sortKey: TableSortOption.NAME,
    },
    {
      title: (
        <Tooltip
          title={
            "The approximate compressed total disk size across all replicas of the table."
          }
        >
          {TableColName.REPLICATION_SIZE}
        </Tooltip>
      ),
      width: "fit-content",
      sorter: (a, b) => a.replicationSizeBytes - b.replicationSizeBytes,
      render: (t: TableRow) => {
        return Bytes(t.replicationSizeBytes);
      },
      sortKey: TableSortOption.REPLICATION_SIZE,
    },
    {
      title: (
        <Tooltip title={"The number of ranges the table."}>
          {TableColName.RANGE_COUNT}
        </Tooltip>
      ),
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
      title: (
        <Tooltip title={"Regions/Nodes on which the table's data is stored."}>
          {TableColName.NODE_REGIONS}
        </Tooltip>
      ),
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
      title: (
        <Tooltip
          title={
            "The percentage of total uncompressed logical data that has not been modified (updated or deleted)."
          }
        >
          {TableColName.LIVE_DATA_PERCENTAGE}
        </Tooltip>
      ),
      sorter: true,
      width: "fit-content",
      sortKey: TableSortOption.LIVE_DATA,
      render: (t: TableRow) => {
        return (
          <div>
            <div>{t.percentLiveData * 100}%</div>
            <div>
              {Bytes(t.totalLiveDataBytes)} / {Bytes(t.totalDataBytes)}
            </div>
          </div>
        );
      },
    },
    {
      title: (
        <Tooltip title={AUTO_STATS_COLLECTION_HELP}>
          {TableColName.AUTO_STATS_ENABLED}
        </Tooltip>
      ),
      sorter: true,
      render: (t: TableRow) => {
        const type = t.autoStatsEnabled ? "success" : "error";
        const text = t.autoStatsEnabled ? "ENABLED" : "DISABLED";
        return <Tag color={type}>{text}</Tag>;
      },
    },
    {
      title: (
        <Tooltip
          title={
            "The last time table statistics used by the SQL optimizer were updated."
          }
        >
          {TableColName.STATS_LAST_UPDATED}
        </Tooltip>
      ),
      sorter: true,
      render: (t: TableRow) => (
        <Timestamp
          time={t.statsLastUpdated}
          format={DATE_WITH_SECONDS_FORMAT_24_TZ}
          fallback={"Never"}
        />
      ),
    },
  ];

const createTableMetadataRequestFromParams = (
  dbID: string,
  params: TableParams,
): ListTableMetadataRequest => {
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

const ignoreParams = [tabAttr];

export const TablesPageV2 = () => {
  const { params, setFilters, setSort, setSearch, setPagination } = useTable({
    initial: initialParams,
    paramsToIgnore: ignoreParams,
  });

  // Get db id from the URL.
  const { dbID } = useRouteParams();
  const { data, error, isLoading, refreshTables } = useTableMetadata(
    createTableMetadataRequestFromParams(dbID, params),
  );
  const nodesResp = useNodeStatuses();

  const onNodeRegionsChange = (storeIDs: StoreID[]) => {
    setFilters({
      storeIDs: storeIDs.map(sid => sid.toString()),
    });
  };

  const tableList = data?.results;
  const tableData = useMemo(
    () =>
      tableMetadataToRows(tableList ?? [], {
        nodeIDToRegion: nodesResp.nodeIDToRegion,
        storeIDToNodeID: nodesResp.storeIDToNodeID,
        isLoading: nodesResp.isLoading,
      }),
    [
      tableList,
      nodesResp.nodeIDToRegion,
      nodesResp.storeIDToNodeID,
      nodesResp.isLoading,
    ],
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
        <PageCount
          page={params.pagination.page}
          pageSize={params.pagination.pageSize}
          total={data?.pagination.totalResults ?? 0}
          entity="tables"
        />
        <Table
          loading={isLoading}
          error={error}
          actionButton={
            <TableMetadataJobControl onDataUpdated={refreshTables} />
          }
          columns={colsWithSort}
          dataSource={tableData}
          pagination={{
            size: "small",
            current: params.pagination.page,
            pageSize: params.pagination.pageSize,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: data?.pagination.totalResults,
          }}
          onChange={onTableChange}
        />
      </PageSection>
    </>
  );
};
