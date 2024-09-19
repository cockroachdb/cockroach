// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import Select, { OptionsType } from "react-select";

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
import { ReactSelectOption } from "src/types/selectTypes";
import { Bytes, EncodeDatabaseUri } from "src/util";

import { DatabaseColName } from "./constants";
import { DatabaseRow } from "./databaseTypes";
import {
  getColTitleFromSortKey,
  getSortKeyFromColTitle,
  rawDatabaseMetadataToDatabaseRows,
} from "./utils";

const mockRegionOptions = [
  { label: "US East (N. Virginia)", value: "us-east-1" },
  { label: "US East (Ohio)", value: "us-east-2" },
];

const COLUMNS: TableColumnProps<DatabaseRow>[] = [
  {
    title: DatabaseColName.NAME,
    sorter: true,
    render: (db: DatabaseRow) => {
      const encodedDBPath = EncodeDatabaseUri(db.name);
      // TODO (xzhang: For CC we have to use `${location.pathname}/${db.name}`
      return <Link to={encodedDBPath}>{db.name}</Link>;
    },
  },
  {
    title: DatabaseColName.SIZE,
    sorter: true,
    render: (db: DatabaseRow) => {
      return Bytes(db.approximateDiskSizeBytes);
    },
  },
  {
    title: DatabaseColName.TABLE_COUNT,
    sorter: false,
    render: (db: DatabaseRow) => {
      return db.tableCount;
    },
  },
  {
    title: DatabaseColName.RANGE_COUNT,
    sorter: true,
    render: (db: DatabaseRow) => {
      return db.rangeCount;
    },
  },
  {
    title: DatabaseColName.NODE_REGIONS,
    render: (db: DatabaseRow) => (
      <div>
        {Object.entries(db.nodesByRegion ?? {}).map(([region, nodes]) => (
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
  };
};

export const DatabasesPageV2 = () => {
  const { params, setSort, setSearch, setPagination } = useTable({
    initial: initialParams,
  });

  const { data, error, isLoading } = useDatabaseMetadata(
    createDatabaseMetadataRequestFromParams(params),
  );

  const paginationState = data?.pagination_info;

  const [nodeRegions, setNodeRegions] = useState<ReactSelectOption[]>([]);
  const onNodeRegionsChange = (selected: OptionsType<ReactSelectOption>) => {
    setNodeRegions((selected ?? []).map(v => v));
  };

  const tableData = useMemo(
    () => rawDatabaseMetadataToDatabaseRows(data?.results ?? []),
    [data],
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
        field: getSortKeyFromColTitle(COLUMNS[colKey].title as DatabaseColName),
        order: sorter.order === "descend" ? "desc" : "asc",
      });
    }
  };

  const sort = params.sort;
  const colsWithSort = useMemo(
    () =>
      COLUMNS.map((col, i) => {
        const title = getColTitleFromSortKey(sort.field as DatabaseSortOptions);
        const colInd = COLUMNS.findIndex(c => c.title === title);
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
    <PageLayout>
      <PageHeader title="Databases" />
      <PageSection>
        <PageConfig>
          <PageConfigItem>
            <Search placeholder="Search databases" onSubmit={setSearch} />
          </PageConfigItem>
          <PageConfigItem minWidth={"200px"}>
            <Select
              placeholder={"Regions"}
              name="nodeRegions"
              options={mockRegionOptions}
              clearable={true}
              isMulti
              value={nodeRegions}
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
