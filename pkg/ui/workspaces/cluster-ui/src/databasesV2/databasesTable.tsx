// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Table } from "antd";
import { ColumnsType } from "antd/es/table";
import React from "react";
import { Link } from "react-router-dom";

import { RegionNodesLabel } from "src/components/regionNodesLabel";

import { EncodeDatabaseUri } from "../util";

import { DatabaseRow } from "./databaseTypes";

const columns: ColumnsType<DatabaseRow> = [
  {
    title: "Name",
    key: "name",
    sorter: true,
    render: (_, db: DatabaseRow) => {
      const encodedDBPath = EncodeDatabaseUri(db.name);
      // TODO (xzhang: For CC we have to use `${location.pathname}/${db.name}`
      return <Link to={encodedDBPath}>{db.name}</Link>;
    },
  },
  {
    title: "Size",
    dataIndex: "approximateDiskSizeMiB",
    key: "size",
    sorter: true,
  },
  {
    title: "Tables",
    dataIndex: "tableCount",
    key: "tables",
    sorter: true,
  },
  {
    title: "Ranges",
    dataIndex: "rangeCount",
    key: "ranges",
    sorter: true,
  },
  {
    title: "Regions / Nodes",
    key: "regions",
    render: (db: DatabaseRow) => (
      <div>
        {Object.entries(db.nodesByRegion ?? []).map(([region, nodes]) => (
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
    dataIndex: "schemaInsightsCount",
    key: "schemaInsights",
  },
];

type DatabasesTableProps = {
  data: DatabaseRow[];
};

export const DatabasesTable: React.FC<DatabasesTableProps> = ({
  data = [],
}) => {
  return (
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
    />
  );
};
