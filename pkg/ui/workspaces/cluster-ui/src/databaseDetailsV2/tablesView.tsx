// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Badge, BadgeIntent } from "@cockroachlabs/ui-components";
import moment from "moment-timezone";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import Select, { OptionsType } from "react-select";

import { RegionNodesLabel } from "src/components/regionNodesLabel";
import { PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { Search } from "src/sharedFromCloud/search";
import { Table, TableColumnProps } from "src/sharedFromCloud/table";
import useTable from "src/sharedFromCloud/useTable";
import { NodeID } from "src/types/clusterTypes";
import { ReactSelectOption } from "src/types/selectTypes";
import { Bytes, EncodeDatabaseTableUri } from "src/util";

import { TableColName } from "./constants";
import { TableRow } from "./types";

const mockRegionOptions = [
  { label: "US East (N. Virginia)", value: "us-east-1" },
  { label: "US East (Ohio)", value: "us-east-2" },
];

const mockLastUpdated = moment.utc();
const mockData: TableRow[] = new Array(20).fill(1).map((_, i) => ({
  name: `myDB-${i}`,
  qualifiedNameWithSchema: `public.table-${i}`,
  dbName: `myDB-${i}`,
  dbID: i,
  replicationSizeBytes: i * 100,
  rangeCount: i,
  columnCount: i,
  nodesByRegion:
    i % 2 === 0
      ? {
          [mockRegionOptions[0].value]: [1, 2] as NodeID[],
          [mockRegionOptions[1].value]: [3] as NodeID[],
        }
      : null,
  liveDataPercentage: 1,
  liveDataBytes: i * 100,
  totalDataBytes: i * 100,
  autoStatsCollectionEnabled: i % 2 === 0,
  statsLastUpdated: mockLastUpdated,
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

const columns: TableColumnProps<TableRow>[] = [
  {
    title: TableColName.NAME,
    width: "15%",
    sorter: true,
    render: (t: TableRow) => {
      // This linking is just temporary. We'll need to update it to the correct path
      // using db ID and table ID once we have the table details page.
      const encodedDBPath = EncodeDatabaseTableUri(t.dbName, t.name);
      return <Link to={encodedDBPath}>{t.qualifiedNameWithSchema}</Link>;
    },
  },
  {
    title: TableColName.REPLICATION_SIZE,
    width: "fit-content",
    sorter: true,
    render: (t: TableRow) => {
      return Bytes(t.replicationSizeBytes);
    },
  },
  {
    title: TableColName.RANGE_COUNT,
    width: "fit-content",
    sorter: true,
    render: (t: TableRow) => {
      return t.rangeCount;
    },
  },
  {
    title: TableColName.COLUMN_COUNT,
    width: "fit-content",
    sorter: true,
    render: (t: TableRow) => {
      return t.columnCount;
    },
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
    title: TableColName.AUTO_STATS_COLLECTION,
    sorter: true,
    render: (t: TableRow) => {
      let intent: BadgeIntent = "success";
      let text = "Enabled";
      if (!t.autoStatsCollectionEnabled) {
        intent = "warning";
        text = "Disabled";
      }
      return (
        <Badge intent={intent} transformCase={"uppercase"}>
          {text}
        </Badge>
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

export const TablesPageV2 = () => {
  const { params, setSearch } = useTable({
    initial: initialParams,
  });
  const data = mockData;

  const [nodeRegions, setNodeRegions] = useState<ReactSelectOption<string>[]>(
    [],
  );
  const onNodeRegionsChange = (
    selected: OptionsType<ReactSelectOption<string>>,
  ) => {
    setNodeRegions((selected ?? []).map(v => v));
  };

  return (
    <>
      <PageSection>
        <PageConfig>
          <PageConfigItem>
            <Search placeholder="Search tables" onSubmit={setSearch} />
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
          pageSize={params.pagination.pageSize}
          total={data.length}
          entity="tables"
        />
        <Table
          columns={columns}
          dataSource={data}
          pagination={{
            size: "small",
            current: params.pagination.page,
            pageSize: params.pagination.pageSize,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: data.length,
          }}
          onChange={(_pagination, _sorter) => {}}
        />
      </PageSection>
    </>
  );
};
