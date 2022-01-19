import React, { useState } from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import { cockroach } from "src/js/protos";
import {
  ColumnDescriptor,
  SortedTable,
  Pagination,
  ResultsPerPageLabel,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./hotRanges.module.styl";

const PAGE_SIZE = 50;
const cx = classNames.bind(styles);
type HotRange = cockroach.server.serverpb.HotRangesResponseV2.HotRange;
interface HotRangesTableProps {
  hotRangesList: HotRange[];
  lastUpdate: string;
}

const HotRangesTable = ({ hotRangesList, lastUpdate }: HotRangesTableProps) => {
  const [pagination, setPagination] = useState({
    pageSize: PAGE_SIZE,
    current: 1,
  });
  const [sortSetting, setSortSetting] = useState({
    ascending: true,
    columnTitle: null,
  });

  if (hotRangesList.length === 0) {
    return <div>No hot ranges</div>;
  }
  const columns: ColumnDescriptor<HotRange>[] = [
    {
      name: "rangeId",
      title: (
        <Tooltip placement="bottom" title="Range ID">
          Range ID
        </Tooltip>
      ),
      cell: (val: HotRange) => (
        <Link to={`/reports/range/${val.range_id}`}>{val.range_id}</Link>
      ),
      sort: (val: HotRange) => val.range_id,
    },
    {
      name: "qps",
      title: (
        <Tooltip placement="bottom" title="QPS">
          QPS
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{val.qps}</>,
      sort: (val: HotRange) => val.qps,
    },
    {
      name: "nodes",
      title: (
        <Tooltip placement="bottom" title="Nodes">
          Nodes
        </Tooltip>
      ),
      cell: (val: HotRange) => (
        <Link to={`/node/${val.replica_node_ids[0]}`}>
          {val.replica_node_ids.join(", ")}
        </Link>
      ),
      sort: (val: HotRange) => val.replica_node_ids[0],
    },
    {
      name: "leasholder",
      title: (
        <Tooltip placement="bottom" title="Leaseholder">
          Leaseholder
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{val.leaseholder_node_id}</>,
      sort: (val: HotRange) => val.leaseholder_node_id,
    },
    {
      name: "database",
      title: (
        <Tooltip placement="bottom" title="Database">
          Database
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{val.database_name}</>,
      sort: (val: HotRange) => val.database_name,
    },
    {
      name: "table",
      title: (
        <Tooltip placement="bottom" title="Table">
          Table
        </Tooltip>
      ),
      cell: (val: HotRange) => (
        <Link to={`/database/${val.database_name}/table/${val.table_name}`}>
          {val.table_name}
        </Link>
      ),
      sort: (val: HotRange) => val.table_name,
    },
    {
      name: "index",
      title: (
        <Tooltip placement="bottom" title="Index">
          Index
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{val.index_name}</>,
      sort: (val: HotRange) => val.index_name,
    },
  ];

  return (
    <div className={cx("hotranges-wrapper")}>
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
        <div>Last update: {lastUpdate}</div>
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
