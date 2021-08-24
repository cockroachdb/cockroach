import React, { useState } from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import { HotRange } from "../../redux/hotRanges/hotRangesReducer";
import {
  ColumnDescriptor,
  SortedTable,
} from "../shared/components/sortedtable";
import { Pagination, ResultsPerPageLabel } from "@cockroachlabs/cluster-ui";

const PAGE_SIZE = 50;

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
    sortKey: null,
  });
  if (hotRangesList.length === 0) {
    return <div>No hot ranges</div>;
  }
  const columns: ColumnDescriptor<HotRange>[] = [
    {
      title: (
        <Tooltip placement="bottom" title="Range ID">
          Range ID
        </Tooltip>
      ),
      cell: (val: HotRange) => (
        <Link to={`/database/${val.rangeId}`}>{val.rangeId}</Link>
      ),
      sort: (val) => val.rangeId,
    },
    {
      title: (
        <Tooltip placement="bottom" title="QPS">
          QPS
        </Tooltip>
      ),
      cell: (val) => <>{val.queriesPerSecond}</>,
      sort: (val) => val.queriesPerSecond,
    },
    {
      title: (
        <Tooltip placement="bottom" title="Nodes">
          Nodes
        </Tooltip>
      ),
      cell: (val) => (
        <Link to={`/database/${val.nodeIds}`}>{val.nodeIds.join(", ")}</Link>
      ),
      sort: (val) => val.nodeIds[0],
    },
    {
      title: (
        <Tooltip placement="bottom" title="Leaseholder">
          Leaseholder
        </Tooltip>
      ),
      cell: (val) => (
        <Link to={`/database/${val.leaseHolder}`}>{val.leaseHolder}</Link>
      ),
      sort: (val) => val.leaseHolder,
    },
    {
      title: (
        <Tooltip placement="bottom" title="Database">
          Database
        </Tooltip>
      ),
      cell: (val) => <>{val.database}</>,
      sort: (val) => val.database,
    },
    {
      title: (
        <Tooltip placement="bottom" title="Table">
          Table
        </Tooltip>
      ),
      cell: (val) => <Link to={`/database/${val.table}`}>{val.table}</Link>,
      sort: (val) => val.table,
    },
    {
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
      <div>
        <h4>
          <ResultsPerPageLabel
            pagination={{
              ...pagination,
              total: hotRangesList.length,
            }}
            pageName="hot ranges"
          />
        </h4>
      </div>
      <SortedTable
        data={hotRangesList}
        columns={columns}
        sortSetting={sortSetting}
        onChangeSortSetting={setSortSetting}
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
