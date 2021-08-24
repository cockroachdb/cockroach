// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import moment from "moment";
import {
  ColumnDescriptor,
  SortedTable,
  Pagination,
  ResultsPerPageLabel,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./hotRanges.module.styl";
import { cockroach } from "src/js/protos";

const PAGE_SIZE = 50;
const cx = classNames.bind(styles);

interface HotRangesTableProps {
  hotRangesList: cockroach.server.serverpb.HotRangesResponseV2.IHotRange[];
  lastUpdate?: string;
}

const HotRangesTable = ({ hotRangesList }: HotRangesTableProps) => {
  const [pagination, setPagination] = useState({
    pageSize: PAGE_SIZE,
    current: 1,
  });
  const [sortSetting, setSortSetting] = useState({
    ascending: true,
    columnTitle: null,
  });
  const getCurrentDateTime = () => {
    const nowUtc = moment.utc();
    return (
      nowUtc.format("MMM DD, YYYY") +
      " at " +
      nowUtc.format("h:mm A") +
      " (UTC)"
    );
  };

  if (hotRangesList.length === 0) {
    return <div>No hot ranges</div>;
  }
  const columns: ColumnDescriptor<
    cockroach.server.serverpb.HotRangesResponseV2.IHotRange
  >[] = [
    {
      name: "rangeId",
      title: (
        <Tooltip placement="bottom" title="Range ID">
          Range ID
        </Tooltip>
      ),
      cell: (val: cockroach.server.serverpb.HotRangesResponseV2.IHotRange) => (
        <Link to={`/reports/range/${val.range_id}`}>{val.range_id}</Link>
      ),
      sort: val => val.range_id,
    },
    {
      name: "qps",
      title: (
        <Tooltip placement="bottom" title="QPS">
          QPS
        </Tooltip>
      ),
      cell: val => <>{val.qps}</>,
      sort: val => val.qps,
    },
    {
      name: "nodes",
      title: (
        <Tooltip placement="bottom" title="Nodes">
          Nodes
        </Tooltip>
      ),
      cell: val => (
        <Link to={`/node/${val.replica_node_ids[0]}`}>
          {val.replica_node_ids.join(", ")}
        </Link>
      ),
      sort: val => val.replica_node_ids[0],
    },

    {
      name: "storeId",
      title: (
        <Tooltip placement="bottom" title="Store ID">
          Store ID
        </Tooltip>
      ),
      cell: val => <>{val.store_id}</>,
      sort: val => val.store_id,
    },
    {
      name: "leasholder",
      title: (
        <Tooltip placement="bottom" title="Leaseholder">
          Leaseholder
        </Tooltip>
      ),
      cell: val => <>{val.leaseholder_node_id}</>,
      sort: val => val.leaseholder_node_id,
    },
    {
      name: "database",
      title: (
        <Tooltip placement="bottom" title="Database">
          Database
        </Tooltip>
      ),
      cell: val => <>{val.database_name}</>,
      sort: val => val.database_name,
    },
    {
      name: "table",
      title: (
        <Tooltip placement="bottom" title="Table">
          Table
        </Tooltip>
      ),
      cell: val => (
        <Link to={`/database/${val.database_name}/table/${val.table_name}`}>
          {val.table_name}
        </Link>
      ),
      sort: val => val.table_name,
    },
    {
      name: "index",
      title: (
        <Tooltip placement="bottom" title="Index">
          Index
        </Tooltip>
      ),
      cell: val => <>{val.index_name}</>,
      sort: val => val.index_name,
    },
  ];

  return (
    <div>
      <div className={cx("hotranges-heading-container")}>
        <h4>
          <ResultsPerPageLabel
            pagination={{
              ...pagination,
              total: hotRangesList.length,
            }}
            pageName="hot ranges"
          />
        </h4>
        <div>Last update: {getCurrentDateTime()}</div>
      </div>
      <SortedTable
        data={hotRangesList}
        columns={columns}
        className={cx("hotranges-table")}
        sortSetting={sortSetting}
        onChangeSortSetting={(ss: SortSetting) =>
          setSortSetting({
            ascending: ss.ascending,
            columnTitle: ss.columnTitle,
          })
        }
        pagination={pagination}
      />
      <Pagination
        pageSize={PAGE_SIZE}
        current={pagination.current}
        total={hotRangesList.length}
        onChange={(page: number, pageSize?: number) =>
          setPagination({
            pageSize,
            current: page,
          })
        }
      />
    </div>
  );
};

export default HotRangesTable;
