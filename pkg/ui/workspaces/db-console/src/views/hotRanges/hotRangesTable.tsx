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
  Text
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./hotRanges.module.styl";

const PAGE_SIZE = 50;
const cx = classNames.bind(styles);
type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;
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
        <Text>
          Range ID
        </Text>
      ),
      cell: (val: HotRange) => (
        <Link to={`/reports/range/${val.range_id}`}>{val.range_id}</Link>
      ),
      sort: (val: HotRange) => val.range_id,
    },
    {
      name: "qps",
      title: (
        <Tooltip placement="bottom" title="The range throughput in queries per second (QPS).">
          QPS
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{Math.round(val.qps * 100) / 100}</>,
      sort: (val: HotRange) => val.qps,
    },
    {
      name: "nodes",
      title: (
        <Tooltip placement="bottom" title="The node(s) that contain a range.">
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
        <Tooltip placement="bottom" title="The node that contains the range's leasholder.">
          Leaseholder
        </Tooltip>
      ),
      cell: (val: HotRange) => <>{val.leaseholder_node_id}</>,
      sort: (val: HotRange) => val.leaseholder_node_id,
    },
    {
      name: "database",
      title: (
        <Text>
          Database
        </Text>
      ),
      cell: (val: HotRange) => <>{val.database_name}</>,
      sort: (val: HotRange) => val.database_name,
    },
    {
      name: "table",
      title: (
        <Text>
          Table
        </Text>
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
        <Text>
          Index
        </Text>
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
