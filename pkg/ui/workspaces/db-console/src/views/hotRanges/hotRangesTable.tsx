// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ColumnDescriptor,
  SortedTable,
  Pagination,
  ResultsPerPageLabel,
  SortSetting,
  Anchor,
  EmptyTable,
  util,
} from "@cockroachlabs/cluster-ui";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import round from "lodash/round";
import React, { useState } from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";

import emptyTableResultsImg from "assets/emptyState/empty-table-results.svg";
import { sortSettingLocalSetting } from "oss/src/redux/hotRanges";
import { AdminUIState } from "oss/src/redux/state";
import { cockroach } from "src/js/protos";
import {
  performanceBestPracticesHotSpots,
  readsAndWritesOverviewPage,
  uiDebugPages,
} from "src/util/docs";

import styles from "./hotRanges.module.styl";

const PAGE_SIZE = 50;
const cx = classNames.bind(styles);

interface HotRangesTableProps {
  hotRangesList: cockroach.server.serverpb.HotRangesResponseV2.IHotRange[];
  lastUpdate?: string;
  nodeIdToLocalityMap: Map<number, string>;
  clearFilterContainer: React.ReactNode;
  sortSetting?: SortSetting;
  onSortChange?: (ss: SortSetting) => void;
}

const HotRangesTable = ({
  hotRangesList,
  nodeIdToLocalityMap,
  lastUpdate,
  clearFilterContainer,
  sortSetting,
  onSortChange,
}: HotRangesTableProps) => {
  const [pagination, setPagination] = useState({
    pageSize: PAGE_SIZE,
    current: 1,
  });

  const columns: ColumnDescriptor<cockroach.server.serverpb.HotRangesResponseV2.IHotRange>[] =
    [
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
        cell: (
          val: cockroach.server.serverpb.HotRangesResponseV2.IHotRange,
        ) => <Link to={`/reports/range/${val.range_id}`}>{val.range_id}</Link>,
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
        name: "cpuPerSecond",
        title: (
          <Tooltip
            placement="bottom"
            title="The total CPU time per second used in processing this range."
          >
            CPU
          </Tooltip>
        ),
        cell: val => <>{util.Duration(val.cpu_time_per_second)}</>,
        sort: val => val.cpu_time_per_second,
      },
      {
        name: "writesPerSecond",
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of keys written per second on this range."
          >
            Write (keys)
          </Tooltip>
        ),
        cell: val => <>{round(val.writes_per_second, 2)}</>,
        sort: val => val.writes_per_second,
      },
      {
        name: "writeBytesPerSecond",
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of bytes written per second on this range."
          >
            Write (bytes)
          </Tooltip>
        ),
        cell: val => <>{util.Bytes(val.write_bytes_per_second)}</>,
        sort: val => val.write_bytes_per_second,
      },
      {
        name: "readsPerSecond",
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of keys read per second on this range."
          >
            Read (keys)
          </Tooltip>
        ),
        cell: val => <>{round(val.reads_per_second, 2)}</>,
        sort: val => val.reads_per_second,
      },
      {
        name: "readsBytesPerSecond",
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of bytes read per second on this range."
          >
            Read (bytes)
          </Tooltip>
        ),
        cell: val => <>{util.Bytes(val.read_bytes_per_second)}</>,
        sort: val => val.read_bytes_per_second,
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
              <Link to={`/node/${nodeId}`} key={nodeId}>
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
        cell: val => <>{val.databases.join(", ")}</>,
        sort: val => val.databases.join(", "),
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
        cell: val => val.tables.join(", "),
        sort: val => val.tables.join(", "),
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
        cell: val => <>{val.indexes.join(", ")}</>,
        sort: val => val.indexes.join(", "),
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
        onChangeSortSetting={(ss: SortSetting) => onSortChange(ss)}
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

const mapDispatchToProps = {
  onSortChange: (ss: SortSetting) =>
    sortSettingLocalSetting.set({
      ascending: ss.ascending,
      columnTitle: ss.columnTitle,
    }),
};

const mapStateToProps = (state: AdminUIState) => ({
  sortSetting: sortSettingLocalSetting.selector(state),
});

export default connect(mapStateToProps, mapDispatchToProps)(HotRangesTable);
