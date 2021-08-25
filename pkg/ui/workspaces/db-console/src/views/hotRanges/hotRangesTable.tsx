import React, { useState } from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import moment from "moment";
import { HotRange } from "../../redux/hotRanges/hotRangesReducer";
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
interface HotRangesTableProps {
  hotRangesList: HotRange[];
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
  const columns: ColumnDescriptor<HotRange>[] = [
    {
      name: "rangeId",
      title: (
        <Tooltip placement="bottom" title="Range ID">
          Range ID
        </Tooltip>
      ),
      cell: (val: HotRange) => (
        <Link to={`/reports/range/${val.rangeId}`}>{val.rangeId}</Link>
      ),
      sort: (val) => val.rangeId,
    },
    {
      name: "qps",
      title: (
        <Tooltip placement="bottom" title="QPS">
          QPS
        </Tooltip>
      ),
      cell: (val) => <>{val.queriesPerSecond}</>,
      sort: (val) => val.queriesPerSecond,
    },
    {
      name: "nodes",
      title: (
        <Tooltip placement="bottom" title="Nodes">
          Nodes
        </Tooltip>
      ),
      cell: (val) => (
        <Link to={`/node/${val.nodeIds[0]}`}>{val.nodeIds.join(", ")}</Link>
      ),
      sort: (val) => val.nodeIds[0],
    },
    {
      name: "leasholder",
      title: (
        <Tooltip placement="bottom" title="Leaseholder">
          Leaseholder
        </Tooltip>
      ),
      cell: (val) => <>{val.leaseHolder}</>,
      sort: (val) => val.leaseHolder,
    },
    {
      name: "database",
      title: (
        <Tooltip placement="bottom" title="Database">
          Database
        </Tooltip>
      ),
      cell: (val) => <>{val.database}</>,
      sort: (val) => val.database,
    },
    {
      name: "table",
      title: (
        <Tooltip placement="bottom" title="Table">
          Table
        </Tooltip>
      ),
      cell: (val) => (
        <Link to={`/database/${val.database}/table/${val.table}`}>
          {val.table}
        </Link>
      ),
      sort: (val) => val.table,
    },
    {
      name: "index",
      title: (
        <Tooltip placement="bottom" title="Index">
          Index
        </Tooltip>
      ),
      cell: (val) => <>{val.index}</>,
      sort: (val) => val.index,
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
