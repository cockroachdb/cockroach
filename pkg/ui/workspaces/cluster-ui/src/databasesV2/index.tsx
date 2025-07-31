// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Row } from "antd";
import React, { useContext, useMemo } from "react";
import { Redirect } from "react-router";
import { Link } from "react-router-dom";

import { useNodeStatuses } from "src/api";
import { useClusterSettings } from "src/api/clusterSettingsApi";
import {
  DatabaseMetadataRequest,
  DatabaseSortOptions,
  useDatabaseMetadata,
} from "src/api/databases/getDatabaseMetadataApi";
import { NodeRegionsSelector } from "src/components/nodeRegionsSelector/nodeRegionsSelector";
import { RegionNodesLabel } from "src/components/regionNodesLabel";
import { TableMetadataJobControl } from "src/components/tableMetadataLastUpdated/tableMetadataJobControl";
import { Tooltip } from "src/components/tooltip";
import { AUTO_STATS_COLLECTION_HELP } from "src/components/tooltipMessages";
import { ClusterDetailsContext, CockroachCloudContext } from "src/contexts";
import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { BooleanSetting } from "src/settings";
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
import { StoreID } from "src/types/clusterTypes";
import { Bytes } from "src/util";

import { DatabaseColName } from "./constants";
import { DatabaseRow } from "./databaseTypes";
import { rawDatabaseMetadataToDatabaseRows } from "./utils";

const AUTO_STATS_ENABLED_CS = "sql.stats.automatic_collection.enabled";

const COLUMNS: (TableColumnProps<DatabaseRow> & {
  sortKey?: DatabaseSortOptions;
  hideIfTenant?: boolean;
})[] = [
  {
    title: (
      <Tooltip title={"The name of the database."}>
        {DatabaseColName.NAME}
      </Tooltip>
    ),
    sorter: (a, b) => a.name.localeCompare(b.name),
    sortKey: DatabaseSortOptions.NAME,
    render: (db: DatabaseRow) => {
      return <Link to={`/databases/${db.id}`}>{db.name}</Link>;
    },
  },
  {
    title: (
      <Tooltip
        title={
          "The approximate total disk size across all table replicas in the database."
        }
      >
        {DatabaseColName.SIZE}
      </Tooltip>
    ),
    sortKey: DatabaseSortOptions.REPLICATION_SIZE,
    sorter: (a, b) => a.approximateDiskSizeBytes - b.approximateDiskSizeBytes,
    align: "right",
    render: (db: DatabaseRow) => {
      return Bytes(db.approximateDiskSizeBytes);
    },
  },
  {
    title: (
      <Tooltip title={"The total number of tables in the database."}>
        {DatabaseColName.TABLE_COUNT}
      </Tooltip>
    ),
    sortKey: DatabaseSortOptions.TABLE_COUNT,
    sorter: true,
    align: "right",
    render: (db: DatabaseRow) => {
      return db.tableCount;
    },
  },
  {
    title: (
      <Tooltip
        title={"Regions/Nodes on which the database tables are located."}
      >
        {DatabaseColName.NODE_REGIONS}
      </Tooltip>
    ),
    hideIfTenant: true,
    width: "fit-content",
    render: (db: DatabaseRow) => (
      <RegionNodesLabel
        loading={db.nodesByRegion.isLoading}
        nodesByRegion={db.nodesByRegion?.data}
      />
    ),
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
  const clusterDetails = useContext(ClusterDetailsContext);
  const isTenant = clusterDetails.isTenant;
  const isCloud = useContext(CockroachCloudContext);
  const { params, setFilters, setSort, setSearch, setPagination } = useTable({
    initial: initialParams,
  });
  const { data, error, isLoading, refreshDatabases } = useDatabaseMetadata(
    createDatabaseMetadataRequestFromParams(params),
  );
  const nodesResp = useNodeStatuses();
  const { settingValues, isLoading: settingsLoading } = useClusterSettings({
    names: [AUTO_STATS_ENABLED_CS],
  });

  const onNodeRegionsChange = (storeIDs: StoreID[]) => {
    setFilters({
      storeIDs: storeIDs.map(sid => sid.toString()),
    });
  };

  const tableData = useMemo(
    () =>
      rawDatabaseMetadataToDatabaseRows(data?.results ?? [], {
        nodeStatusByID: nodesResp.nodeStatusByID,
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
      COLUMNS.filter(c => !isTenant || !c.hideIfTenant).map(col => {
        const sortOrder: SortDirection =
          sort?.order === "desc" ? "descend" : "ascend";
        return {
          ...col,
          sortOrder:
            sort.field === col.sortKey && col.sorter ? sortOrder : null,
        };
      }),
    [sort, isTenant],
  );

  const nodeRegionsValue = params.filters.storeIDs.map(
    sid => parseInt(sid, 10) as StoreID,
  );

  // 409 conflict - this error code arises when the CRDB version
  // is not compatible with the APIs used by this page.
  // v2 Databases page is only supported for finalized CRDB versions >= 24.3.
  // We can remove this check when the cluster-ui version moves past the 24.3 release.
  if (!isCloud && error?.status === 409) {
    return <Redirect to="/legacy/databases" />;
  }

  return (
    <PageLayout>
      <PageHeader
        title="Databases"
        actions={
          !settingsLoading && (
            <BooleanSetting
              text={"Auto stats collection"}
              enabled={settingValues[AUTO_STATS_ENABLED_CS]?.value === "true"}
              tooltipText={AUTO_STATS_COLLECTION_HELP}
            />
          )
        }
      />
      <PageConfig>
        <PageConfigItem>
          <Search placeholder="Search databases" onSubmit={setSearch} />
        </PageConfigItem>
        {!isTenant && (
          <PageConfigItem minWidth={"200px"}>
            <NodeRegionsSelector
              value={nodeRegionsValue}
              onChange={onNodeRegionsChange}
            />
          </PageConfigItem>
        )}
      </PageConfig>
      <PageSection>
        <Row align={"middle"} justify={"space-between"}>
          <PageCount
            page={params.pagination.page}
            pageSize={params.pagination.pageSize}
            total={data?.pagination.totalResults ?? 0}
            entity="databases"
          />
          <TableMetadataJobControl onJobComplete={refreshDatabases} />
        </Row>
        <Table
          loading={isLoading}
          error={error}
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
    </PageLayout>
  );
};
