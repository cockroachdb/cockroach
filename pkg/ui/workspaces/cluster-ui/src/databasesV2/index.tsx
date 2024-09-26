// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Skeleton } from "antd";
import React, { useMemo } from "react";
import { Link } from "react-router-dom";

import {
  DatabaseMetadataRequest,
  DatabaseSortOptions,
  useDatabaseMetadata,
} from "src/api/databases/getDatabaseMetadataApi";
import { RegionNodesLabel } from "src/components/regionNodesLabel";
import { PageLayout, PageSection } from "src/layouts";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import { Search } from "src/sharedFromCloud/search";
import {
  SortDirection,
  Table,
  TableChangeFn,
  TableColumnProps,
} from "src/sharedFromCloud/table";
import useTable, { TableParams } from "src/sharedFromCloud/useTable";
import { Bytes } from "src/util";

import { useNodeStatuses } from "../api";
import { NodeRegionsSelector } from "../components/nodeRegionsSelector/nodeRegionsSelector";
import { StoreID } from "../types/clusterTypes";

import { DatabaseColName } from "./constants";
import { DatabaseRow } from "./databaseTypes";
import { rawDatabaseMetadataToDatabaseRows } from "./utils";

const COLUMNS: (TableColumnProps<DatabaseRow> & {
  sortKey?: DatabaseSortOptions;
})[] = [
  {
    title: DatabaseColName.NAME,
    sorter: (a, b) => a.name.localeCompare(b.name),
    sortKey: DatabaseSortOptions.NAME,
    render: (db: DatabaseRow) => {
      return <Link to={`/v2/databases/${db.id}`}>{db.name}</Link>;
    },
  },
  {
    title: DatabaseColName.SIZE,
    sortKey: DatabaseSortOptions.REPLICATION_SIZE,
    sorter: (a, b) => a.approximateDiskSizeBytes - b.approximateDiskSizeBytes,
    render: (db: DatabaseRow) => {
      return Bytes(db.approximateDiskSizeBytes);
    },
  },
  {
    title: DatabaseColName.TABLE_COUNT,
    sortKey: DatabaseSortOptions.TABLE_COUNT,
    sorter: true,
    render: (db: DatabaseRow) => {
      return db.tableCount;
    },
  },
  {
    title: DatabaseColName.RANGE_COUNT,
    sortKey: DatabaseSortOptions.RANGES,
    sorter: true,
    render: (db: DatabaseRow) => {
      return db.rangeCount;
    },
  },
  {
    title: DatabaseColName.NODE_REGIONS,
    render: (db: DatabaseRow) => (
      <Skeleton loading={db.nodesByRegion.isLoading}>
        <div>
          {Object.entries(db.nodesByRegion?.data).map(([region, nodes]) => (
            <RegionNodesLabel
              key={region}
              nodes={nodes}
              region={{ label: region, code: region }}
            />
          ))}
        </div>
      </Skeleton>
    ),
  },
  {
    title: "Schema insights",
    render: (db: DatabaseRow) => {
      return db.schemaInsightsCount;
    },
  },
];

const initialParams = {
  filters: {
    storeIDs: [] as string[],
  },
  pagination: {
    page: 1,
    pageSize: 10,
  },
  search: "",
  sort: {
    field: "name",
    order: "asc" as const,
  },
};

const createDatabaseMetadataRequestFromParams = (
  params: TableParams,
): DatabaseMetadataRequest => {
  return {
    pagination: {
      pageSize: params.pagination.pageSize,
      pageNum: params.pagination?.page,
    },
    sortBy: params.sort?.field ?? "name",
    sortOrder: params.sort?.order ?? "asc",
    name: params.search,
    storeIds: params.filters.storeIDs.map(sid => parseInt(sid, 10)),
  };
};

export const DatabasesPageV2 = () => {
  const { params, setFilters, setSort, setSearch, setPagination } = useTable({
    initial: initialParams,
  });

  const { data, error, isLoading } = useDatabaseMetadata(
    createDatabaseMetadataRequestFromParams(params),
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
      rawDatabaseMetadataToDatabaseRows(data?.results ?? [], {
        nodeIDToRegion: nodesResp.nodeIDToRegion,
        storeIDToNodeID: nodesResp.storeIDToNodeID,
        isLoading: nodesResp.isLoading,
      }),
    [data, nodesResp],
  );

  const onTableChange: TableChangeFn<DatabaseRow> = (pagination, sorter) => {
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

  const sort = params.sort;
  const colsWithSort = useMemo(
    () =>
      COLUMNS.map(col => {
        const sortOrder: SortDirection =
          sort?.order === "desc" ? "descend" : "ascend";
        return {
          ...col,
          sortOrder:
            sort.field === col.sortKey && col.sorter ? sortOrder : null,
        };
      }),
    [sort],
  );

  const nodeRegionsValue = params.filters.storeIDs.map(
    sid => parseInt(sid, 10) as StoreID,
  );

  return (
    <PageLayout>
      <PageHeader title="Databases" />
      <PageSection>
        <PageConfig>
          <PageConfigItem>
            <Search placeholder="Search databases" onSubmit={setSearch} />
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
        <Loading page="Databases overview" loading={isLoading} error={error}>
          <PageCount
            page={params.pagination.page}
            pageSize={params.pagination.pageSize}
            total={paginationState?.total_results}
            entity="databases"
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
    </PageLayout>
  );
};
