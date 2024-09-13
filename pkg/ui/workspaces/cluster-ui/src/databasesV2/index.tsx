// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Space } from "antd";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import Select, { OptionsType } from "react-select";

import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import { Search } from "src/sharedFromCloud/search";
import { ReactSelectOption } from "src/types/selectTypes";

import { Table, TableColumnProps } from "../sharedFromCloud/table";
import useTable from "../sharedFromCloud/useTable";
import { Bytes, EncodeDatabaseUri } from "../util";

import { DatabaseRow } from "./databaseTypes";

const mockRegionOptions = [
  { label: "US East (N. Virginia)", value: "us-east-1" },
  { label: "US East (Ohio)", value: "us-east-2" },
];

const mockData: DatabaseRow[] = new Array(20).fill(1).map((_, i) => ({
  name: `myDB-${i}`,
  id: i,
  approximateDiskSizeMiB: i * 100,
  tableCount: i,
  rangeCount: i,
  nodesByRegion: {
    [mockRegionOptions[0].value]: [1, 2],
    [mockRegionOptions[1].value]: [3],
  },
  schemaInsightsCount: i,
  key: i.toString(),
}));

const filters = {};

const initialParams = {
  filters,
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

const columns: TableColumnProps<DatabaseRow>[] = [
  {
    title: "Name",
    sorter: true,
    render: (db: DatabaseRow) => {
      const encodedDBPath = EncodeDatabaseUri(db.name);
      // TODO (xzhang: For CC we have to use `${location.pathname}/${db.name}`
      return <Link to={encodedDBPath}>{db.name}</Link>;
    },
  },
  {
    title: "Size",
    sorter: true,
    render: (db: DatabaseRow) => {
      return Bytes(db.approximateDiskSizeMiB);
    },
  },
  {
    title: "Tables",
    sorter: true,
    render: (db: DatabaseRow) => {
      return db.tableCount;
    },
  },
  {
    title: "Ranges",
    sorter: true,
    render: (db: DatabaseRow) => {
      return db.rangeCount;
    },
  },
  {
    title: "Regions / Nodes",
    render: (db: DatabaseRow) => (
      <Space direction="vertical">
        {db.nodesByRegion &&
          Object.keys(db.nodesByRegion).map(
            region => `${region}: ${db.nodesByRegion[region].length}`,
          )}
      </Space>
    ),
  },
  {
    title: "Schema insights",
    render: (db: DatabaseRow) => {
      return db.schemaInsightsCount;
    },
  },
];

export const DatabasesPageV2 = () => {
  const { setSearch } = useTable({
    initial: initialParams,
  });
  const data = mockData;

  const [nodeRegions, setNodeRegions] = useState<ReactSelectOption[]>([]);
  const onNodeRegionsChange = (selected: OptionsType<ReactSelectOption>) => {
    setNodeRegions((selected ?? []).map(v => v));
  };

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
        <PageCount
          page={1}
          pageSize={10}
          total={data.length}
          entity="databases"
        />
        <Table
          columns={columns}
          dataSource={data}
          pagination={{
            size: "small",
            current: 1,
            pageSize: 10,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: data.length,
          }}
          onChange={(_pagination, _sorter) => {}}
        />
      </PageSection>
    </PageLayout>
  );
};
