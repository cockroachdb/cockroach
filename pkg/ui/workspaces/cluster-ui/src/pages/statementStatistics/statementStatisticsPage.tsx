// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDown } from "@cockroachlabs/icons";
import { Tooltip } from "@cockroachlabs/ui-components";
import { Dropdown, Menu } from "antd";
import React, { useMemo, useState, useCallback } from "react";
import { Link } from "react-router-dom";

import {
  StatementActivitiesRequest,
  useStatementActivities,
  StatementActivity,
  StatementActivitySortCol,
} from "src/api/sqlActivity";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { Tooltip as HeaderTooltip } from "src/components/tooltip";
import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import {
  Table,
  TableColumnProps,
  useKeyedData,
  SortDirection,
} from "src/sharedFromCloud/table";
import {
  TimeScale,
  TimeScaleDropdown,
  timeScale1hMinOptions,
  toDateRange,
} from "src/timeScaleDropdown";
import { Duration, HexStringToInt64String } from "src/util/format";

type KeyedStatementActivity = StatementActivity & { key: string };

type ColumnWithSort = TableColumnProps<KeyedStatementActivity> & {
  sortKey?: StatementActivitySortCol;
};

enum StatementColName {
  STATEMENT = "Statement",
  EXECUTION_COUNT = "Execution Count",
  PCT_RUNTIME = "% of Runtime",
  SERVICE_LATENCY = "Service Latency (mean)",
  SQL_CPU = "SQL CPU (mean)",
  CONTENTION = "Contention (mean)",
  KV_CPU = "KV CPU (mean)",
  ADMISSION_WAIT = "Admission Wait (mean)",
}

const COLUMNS: ColumnWithSort[] = [
  {
    title: (
      <HeaderTooltip title="The SQL statement fingerprint.">
        {StatementColName.STATEMENT}
      </HeaderTooltip>
    ),
    width: "40%",
    render: (row: KeyedStatementActivity) => {
      const fingerprintIdNum = HexStringToInt64String(row.fingerprintId);
      return (
        <Link to={`/statement/true/${fingerprintIdNum}`}>
          <Tooltip
            placement="bottom"
            content={
              <pre style={{ maxWidth: "500px", whiteSpace: "pre-wrap" }}>
                {row.query}
              </pre>
            }
          >
            <div className="cl-table-link__tooltip-hover-area">
              {row.querySummary}
            </div>
          </Tooltip>
        </Link>
      );
    },
  },
  {
    title: (
      <HeaderTooltip title="The total number of times this statement was executed.">
        {StatementColName.EXECUTION_COUNT}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.EXECUTION_COUNT,
    render: (row: KeyedStatementActivity) =>
      row.executionCount.toLocaleString(),
  },
  {
    title: (
      <HeaderTooltip title="The percentage of total runtime consumed by this statement.">
        {StatementColName.PCT_RUNTIME}
      </HeaderTooltip>
    ),
    align: "right",
    render: (row: KeyedStatementActivity) =>
      (row.pctOfTotalRuntime * 100).toFixed(2) + "%",
  },
  {
    title: (
      <HeaderTooltip title="The mean service latency for this statement.">
        {StatementColName.SERVICE_LATENCY}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.SERVICE_LATENCY,
    render: (row: KeyedStatementActivity) =>
      Duration(row.serviceLatencyMean * 1e9),
  },
  {
    title: (
      <HeaderTooltip title="The mean SQL CPU time for this statement.">
        {StatementColName.SQL_CPU}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.SQL_CPU,
    render: (row: KeyedStatementActivity) => Duration(row.sqlCpuMeanNanos),
  },
  {
    title: (
      <HeaderTooltip title="The mean contention time for this statement.">
        {StatementColName.CONTENTION}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.CONTENTION,
    render: (row: KeyedStatementActivity) =>
      Duration(row.contentionTimeMean * 1e9),
  },
  {
    title: (
      <HeaderTooltip title="The mean KV CPU time for this statement.">
        {StatementColName.KV_CPU}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.KV_CPU,
    render: (row: KeyedStatementActivity) => Duration(row.kvCpuMeanNanos),
  },
  {
    title: (
      <HeaderTooltip title="The mean admission wait time for this statement.">
        {StatementColName.ADMISSION_WAIT}
      </HeaderTooltip>
    ),
    align: "right",
    sorter: true,
    sortKey: StatementActivitySortCol.ADMISSION_WAIT,
    render: (row: KeyedStatementActivity) =>
      Duration(row.admissionWaitTimeMean),
  },
];

// Search criteria options
const topKOptions = [
  { value: 25, label: "25" },
  { value: 50, label: "50" },
  { value: 100, label: "100" },
  { value: 500, label: "500" },
];

const sortByOptions = [
  { value: StatementActivitySortCol.EXECUTION_COUNT, label: "Execution Count" },
  { value: StatementActivitySortCol.PCT_RUNTIME, label: "% of Runtime" },
  { value: StatementActivitySortCol.SERVICE_LATENCY, label: "Statement Time" },
  { value: StatementActivitySortCol.SQL_CPU, label: "SQL CPU Time" },
  { value: StatementActivitySortCol.CONTENTION, label: "Contention Time" },
  { value: StatementActivitySortCol.KV_CPU, label: "KV CPU Time" },
  { value: StatementActivitySortCol.ADMISSION_WAIT, label: "Admission Wait Time" },
];

const getSortByLabel = (value: StatementActivitySortCol): string => {
  const option = sortByOptions.find(opt => opt.value === value);
  return option?.label ?? "Execution Count";
};

const defaultTimeScale: TimeScale = {
  ...timeScale1hMinOptions["Past Hour"],
  key: "Past Hour",
  fixedWindowEnd: false,
};

// Pending search criteria (UI state before Apply is clicked)
type PendingCriteriaState = {
  topValue: number;
  byValue: StatementActivitySortCol;
  timeScale: TimeScale;
};

// Applied request state (all params that affect the API request)
type AppliedRequestState = {
  topKCount: number;
  topKCol: StatementActivitySortCol;
  startTime: number;
  endTime: number;
  page: number;
  pageSize: number;
  sortBy: StatementActivitySortCol;
  sortOrder: "asc" | "desc";
};

const defaultPendingCriteria: PendingCriteriaState = {
  topValue: 100,
  byValue: StatementActivitySortCol.EXECUTION_COUNT,
  timeScale: defaultTimeScale,
};

const createInitialAppliedState = (): AppliedRequestState => {
  const [start, end] = toDateRange(defaultTimeScale);
  return {
    topKCount: 100,
    topKCol: StatementActivitySortCol.EXECUTION_COUNT,
    startTime: Math.floor(start.unix()),
    endTime: Math.floor(end.unix()),
    page: 1,
    pageSize: 20,
    sortBy: StatementActivitySortCol.EXECUTION_COUNT,
    sortOrder: "desc",
  };
};

const createStatementActivitiesRequest = (
  appliedState: AppliedRequestState,
): StatementActivitiesRequest => {
  return {
    pagination: {
      pageSize: appliedState.pageSize,
      pageNum: appliedState.page,
    },
    sortBy: appliedState.sortBy,
    sortOrder: appliedState.sortOrder,
    topKCount: appliedState.topKCount,
    topKCol: appliedState.topKCol,
    startTime: appliedState.startTime,
    endTime: appliedState.endTime,
  };
};

// Convert antd sort order to our sort order
const antdSortOrderToSortOrder = (
  order: "ascend" | "descend" | null | undefined,
): "asc" | "desc" | undefined => {
  if (order === "ascend") return "asc";
  if (order === "descend") return "desc";
  return undefined;
};

// Convert our sort order to antd sort order
const sortOrderToAntdSortOrder = (
  order: "asc" | "desc" | undefined,
): SortDirection | undefined => {
  if (order === "asc") return "ascend";
  if (order === "desc") return "descend";
  return undefined;
};

export const StatementStatisticsPage = () => {
  // Pending search criteria (UI state before Apply is clicked)
  const [pendingCriteria, setPendingCriteria] = useState<PendingCriteriaState>(
    defaultPendingCriteria,
  );

  // Applied request state (all params that affect the API request - single source of truth)
  const [appliedState, setAppliedState] = useState<AppliedRequestState>(
    createInitialAppliedState,
  );

  const onChangeTop = useCallback((top: number) => {
    setPendingCriteria(prev => ({ ...prev, topValue: top }));
  }, []);

  const onChangeBy = useCallback((by: StatementActivitySortCol) => {
    setPendingCriteria(prev => ({ ...prev, byValue: by }));
  }, []);

  const onChangeTimeScale = useCallback((ts: TimeScale) => {
    setPendingCriteria(prev => ({ ...prev, timeScale: ts }));
  }, []);

  const onApply = useCallback(() => {
    const [start, end] = toDateRange(pendingCriteria.timeScale);
    // Single state update with all request parameters
    setAppliedState(prev => ({
      ...prev,
      topKCount: pendingCriteria.topValue,
      topKCol: pendingCriteria.byValue,
      startTime: Math.floor(start.unix()),
      endTime: Math.floor(end.unix()),
      page: 1, // Reset to page 1
      sortBy: pendingCriteria.byValue, // Sort by the "By" column
      sortOrder: "desc",
    }));
  }, [pendingCriteria]);

  const { data, error, isLoading } = useStatementActivities(
    createStatementActivitiesRequest(appliedState),
  );

  const tableData = useKeyedData(data?.results, "fingerprintId");

  // Add sortOrder to columns based on current sort state
  const columnsWithSort = useMemo(() => {
    return COLUMNS.map(col => {
      if (col.sortKey && col.sortKey === appliedState.sortBy) {
        return {
          ...col,
          sortOrder: sortOrderToAntdSortOrder(appliedState.sortOrder),
        };
      }
      return {
        ...col,
        sortOrder: undefined,
      };
    });
  }, [appliedState.sortBy, appliedState.sortOrder]);

  const onTableChange = (
    pagination: { current?: number; pageSize?: number },
    sorter: { columnKey?: React.Key; order?: "ascend" | "descend" | null },
  ) => {
    // Single state update for pagination and/or sorting
    setAppliedState(prev => {
      const newState = { ...prev };

      // Handle pagination
      if (pagination.current !== undefined) {
        newState.page = pagination.current;
      }
      if (pagination.pageSize !== undefined) {
        newState.pageSize = pagination.pageSize;
      }

      // Handle sorting
      if (sorter.columnKey !== undefined) {
        const columnIndex = Number(sorter.columnKey);
        const column = COLUMNS[columnIndex];
        if (column?.sortKey) {
          const newOrder = antdSortOrderToSortOrder(sorter.order);
          // When antd sends null (reset), toggle back to ascending
          newState.sortBy = column.sortKey;
          newState.sortOrder = newOrder ?? "asc";
        }
      }

      return newState;
    });
  };

  const menuTop = (
    <Menu
      onClick={e => {
        const top = Number(e.key);
        if (!isNaN(top)) {
          onChangeTop(top);
        }
      }}
    >
      {topKOptions.map(option => (
        <Menu.Item key={option.value}>{option.label}</Menu.Item>
      ))}
    </Menu>
  );

  const menuBy = (
    <Menu
      onClick={e => {
        const by = e.key as StatementActivitySortCol;
        if (Object.values(StatementActivitySortCol).includes(by)) {
          onChangeBy(by);
        }
      }}
    >
      {sortByOptions.map(option => (
        <Menu.Item key={option.value}>{option.label}</Menu.Item>
      ))}
    </Menu>
  );

  return (
    <PageLayout>
      <PageHeader title="Statement Statistics" />
      <PageSection>
        <div style={{ marginBottom: "16px" }}>
          <h5 className={commonStyles("base-heading")}>Search Criteria</h5>
          <PageConfig>
            <PageConfigItem>
              <span style={{ marginRight: "8px" }}>Top</span>
              <Dropdown overlay={menuTop} trigger={["click"]}>
                <div
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    padding: "4px 12px",
                    border: "1px solid #d9d9d9",
                    borderRadius: "4px",
                    cursor: "pointer",
                    minWidth: "60px",
                    justifyContent: "space-between",
                  }}
                >
                  <span>{pendingCriteria.topValue}</span>
                  <CaretDown style={{ marginLeft: "8px" }} />
                </div>
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <span style={{ marginRight: "8px" }}>By</span>
              <Dropdown overlay={menuBy} trigger={["click"]}>
                <div
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    padding: "4px 12px",
                    border: "1px solid #d9d9d9",
                    borderRadius: "4px",
                    cursor: "pointer",
                    minWidth: "140px",
                    justifyContent: "space-between",
                  }}
                >
                  <span>{getSortByLabel(pendingCriteria.byValue)}</span>
                  <CaretDown style={{ marginLeft: "8px" }} />
                </div>
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <span style={{ marginRight: "8px" }}>Time Range</span>
              <TimeScaleDropdown
                options={timeScale1hMinOptions}
                currentScale={pendingCriteria.timeScale}
                setTimeScale={onChangeTimeScale}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Button textAlign="center" onClick={onApply}>
                Apply
              </Button>
            </PageConfigItem>
          </PageConfig>
        </div>
        <PageCount
          page={appliedState.page}
          pageSize={appliedState.pageSize}
          total={data?.pagination?.totalResults ?? 0}
          entity="statements"
        />
        <Table<KeyedStatementActivity>
          loading={isLoading}
          error={error}
          columns={columnsWithSort}
          dataSource={tableData ?? []}
          pagination={{
            size: "small",
            current: appliedState.page,
            pageSize: appliedState.pageSize,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: data?.pagination?.totalResults ?? 0,
          }}
          onChange={onTableChange}
        />
      </PageSection>
    </PageLayout>
  );
};
