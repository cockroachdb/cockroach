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
import {
  ColumnDescriptor,
  SortedTable,
  Pagination,
  ResultsPerPageLabel,
  SortSetting,
  Anchor,
  EmptyTable,
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import { round } from "lodash";
import styles from "./hotRanges.module.styl";
import { cockroach } from "src/js/protos";
import {
  performanceBestPracticesHotSpots,
  readsAndWritesOverviewPage,
  uiDebugPages,
} from "src/util/docs";
import emptyTableResultsImg from "assets/emptyState/empty-table-results.svg";

const PAGE_SIZE = 50;
const cx = classNames.bind(styles);

interface HotRangesTableProps {
  hotRangesList: cockroach.server.serverpb.HotRangesResponseV2.IHotRange[];
  lastUpdate?: string;
  nodeIdToLocalityMap: Map<number, string>;
  clearFilterContainer: React.ReactNode;
}

const HotRangesTable = ({
  hotRangesList,
  nodeIdToLocalityMap,
  lastUpdate,
  clearFilterContainer,
}: HotRangesTableProps) => {
  const [pagination, setPagination] = useState({
    pageSize: PAGE_SIZE,
    current: 1,
  });
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "qps",
  });

  const columns: ColumnDescriptor<
    cockroach.server.serverpb.HotRangesResponseV2.IHotRange
  >[] = [
    {
      name: "rangeId",
      title: (
        <Tooltip
          placement="bottom"
          title={
            <span>
              The internal ID of the hot range. Click the range ID to view the{" "}
              <Anchor href={uiDebugPages} className={cx("light-anchor")}>
                range report
              </Anchor>{" "}
              for this range.
            </span>
          }
        >
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
        <Tooltip
          placement="bottom"
          title="The total number of `SELECT`, `UPDATE`, `INSERT`, and `DELETE` queries
          executed per second on this range."
        >
          QPS
        </Tooltip>
      ),
      cell: val => <>{round(val.qps, 2)}</>,
      sort: val => val.qps,
    },
    {
      name: "nodes",
      title: (
        <Tooltip
          placement="bottom"
          title="The ID of each node where the range data is found."
        >
          Nodes
        </Tooltip>
      ),
      cell: val => (
        <>
          {val.replica_node_ids.map((nodeId, idx, arr) => (
            <Link to={`/node/${nodeId}`}>
              {nodeId}
              {idx < arr.length - 1 && ", "}
            </Link>
          ))}
        </>
      ),
      sort: val => val.replica_node_ids[0],
    },

    {
      name: "storeId",
      title: (
        <Tooltip
          placement="bottom"
          title="The internal ID of the store where the range data is found."
        >
          Store ID
        </Tooltip>
      ),
      cell: val => <>{val.store_id}</>,
      sort: val => val.store_id,
    },
    {
      name: "leasholder",
      title: (
        <Tooltip
          placement="bottom"
          title={
            <span>
              The internal ID of the node that has the{" "}
              <Anchor
                href={readsAndWritesOverviewPage}
                className={cx("light-anchor")}
              >
                range lease
              </Anchor>
              .
            </span>
          }
        >
          Leaseholder
        </Tooltip>
      ),
      cell: val => <>{val.leaseholder_node_id}</>,
      sort: val => val.leaseholder_node_id,
    },
    {
      name: "database",
      title: (
        <Tooltip
          placement="bottom"
          title="Name of the database where the range data is found."
        >
          Database
        </Tooltip>
      ),
      cell: val => <>{val.database_name}</>,
      sort: val => val.database_name,
    },
    {
      name: "table",
      title: (
        <Tooltip
          placement="bottom"
          title="Name of the table where the range data is found."
        >
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
        <Tooltip
          placement="bottom"
          title="Name of the index where the range data is indexed, if applicable."
        >
          Index
        </Tooltip>
      ),
      cell: val => <>{val.index_name}</>,
      sort: val => val.index_name,
    },
    {
      name: "locality",
      title: (
        <Tooltip
          placement="bottom"
          title="The locality of the node where the range data is found."
        >
          Locality
        </Tooltip>
      ),
      cell: val => <>{nodeIdToLocalityMap.get(val.node_id)}</>,
      sort: val => nodeIdToLocalityMap.get(val.node_id),
    },
  ];

  return (
    <div className="section">
      <div className={cx("hotranges-heading-container")}>
        <h4 className="cl-count-title">
          <ResultsPerPageLabel
            pagination={{
              ...pagination,
              total: hotRangesList.length,
            }}
            pageName="results"
          />
          {clearFilterContainer}
        </h4>
        <h4 className="cl-count-title">
          {lastUpdate && `Last update: ${lastUpdate}`}
        </h4>
      </div>
      <SortedTable
        data={hotRangesList}
        columns={columns}
        tableWrapperClassName={cx("hotranges-table")}
        sortSetting={sortSetting}
        onChangeSortSetting={(ss: SortSetting) =>
          setSortSetting({
            ascending: ss.ascending,
            columnTitle: ss.columnTitle,
          })
        }
        pagination={pagination}
        renderNoResult={
          <EmptyTable
            title="No hot ranges"
            icon={emptyTableResultsImg}
            footer={
              <Anchor href={performanceBestPracticesHotSpots} target="_blank">
                Learn more about hot ranges
              </Anchor>
            }
          />
        }
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
