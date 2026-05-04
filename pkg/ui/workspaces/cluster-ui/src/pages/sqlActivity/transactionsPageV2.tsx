// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Switch } from "antd";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useMemo, useState } from "react";

import { useResetSQLStats } from "src/api/sqlStatsApi";
import {
  TransactionRow,
  TransactionSortOptions,
  TransactionsRequest,
  useSqlActivityTransactions,
} from "src/api/sqlActivityApi";
import { barChartFactory } from "src/barCharts/barChartFactory";
import { Tooltip } from "src/components/tooltip";
import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { ResultsPerPageLabel } from "src/pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
} from "src/queryFilter";
import ClearStats from "src/sqlActivity/clearStats";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import {
  TimeScale,
  TimeScaleDropdown,
  timeScale1hMinOptions,
  toRoundedDateRange,
} from "src/timeScaleDropdown";
import timeScaleStyles from "src/timeScaleDropdown/timeScale.module.scss";
import { commonStyles } from "src/common";
import {
  SortDirection,
  Table,
  TableChangeFn,
  TableColumnProps,
} from "src/sharedFromCloud/table";
import { Bytes, Count, Duration, PercentageCustom } from "src/util";

const timeScaleStylesCx = classNames.bind(timeScaleStyles);

// ---------------------------------------------------------------------------
// Bar chart factories for V2 TransactionRow
// ---------------------------------------------------------------------------
const svcLatBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "bar-chart__parse", value: d => d.svcLatMean }],
  v => Duration(v * 1e9),
  { name: "bar-chart__overall-dev", value: d => d.svcLatStddev },
);

const commitLatBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "bar-chart__parse", value: d => d.commitLatMean }],
  v => Duration(v * 1e9),
  { name: "bar-chart__overall-dev", value: d => d.commitLatStddev },
);

const cpuBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "cpu", value: d => d.cpuSqlNanosMean }],
  v => Duration(v),
  { name: "cpu-dev", value: d => d.cpuSqlNanosStddev },
);

const contentionBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "contention", value: d => d.contentionTimeMean }],
  v => Duration(v * 1e9),
  { name: "contention-dev", value: d => d.contentionTimeStddev },
);

const kvCpuBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "kv-cpu-time", value: d => d.kvCpuTimeNanosMean }],
  v => Duration(v),
  { name: "kv-cpu-time-dev", value: d => d.kvCpuTimeNanosStddev },
);

const admWaitBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "admission-wait-time", value: d => d.admissionWaitTimeMean }],
  v => Duration(v),
  { name: "admission-wait-time-dev", value: d => d.admissionWaitTimeStddev },
);

const bytesReadBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "bytes-read", value: d => d.bytesReadMean }],
  Bytes,
  { name: "bytes-read-dev", value: d => d.bytesReadStddev },
);

const countBarChart = barChartFactory<TransactionRow>(
  "grey",
  [{ name: "count-first-try", value: d => d.executionCount }],
  v => Count(v),
);

const retryBarChart = barChartFactory<TransactionRow>(
  "red",
  [{ name: "count-retry", value: d => d.maxRetries }],
  v => Count(v),
);

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------

const makeColumns = (
  rows: TransactionRow[],
): (TableColumnProps<TransactionRow> & {
  sortKey?: TransactionSortOptions;
})[] => {
  const countRenderer = countBarChart(rows);
  const svcLatRenderer = svcLatBarChart(rows);
  const commitLatRenderer = commitLatBarChart(rows);
  const cpuRenderer = cpuBarChart(rows);
  const contentionRenderer = contentionBarChart(rows);
  const kvCpuRenderer = kvCpuBarChart(rows);
  const admWaitRenderer = admWaitBarChart(rows);
  const bytesReadRenderer = bytesReadBarChart(rows);
  const retryRenderer = retryBarChart(rows);

  return [
    {
      title: (
        <Tooltip title="Statement fingerprints that make up this transaction.">
          Transaction
        </Tooltip>
      ),
      width: 400,
      render: (row: TransactionRow) => {
        const text =
          row.querySummaries?.length > 0
            ? row.querySummaries.join(" ; ")
            : row.fingerprintId;
        return (
          <div
            style={{
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              maxWidth: 400,
            }}
            title={text}
          >
            {text}
          </div>
        );
      },
    },
    {
      title: (
        <Tooltip title="Cumulative number of executions of this transaction fingerprint within the specified time interval.">
          Execution Count
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.EXECUTION_COUNT,
      sorter: true,
      align: "right" as const,
      render: countRenderer,
    },
    {
      title: (
        <Tooltip title="Average service latency per execution.">
          Service Latency
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.SVC_LAT_MEAN,
      sorter: true,
      align: "right" as const,
      render: svcLatRenderer,
    },
    {
      title: (
        <Tooltip title="Average commit latency per execution.">
          Commit Latency
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.COMMIT_LAT_MEAN,
      sorter: true,
      align: "right" as const,
      render: commitLatRenderer,
    },
    {
      title: (
        <Tooltip title="Average CPU time spent executing within the SQL layer per execution.">
          SQL CPU Time
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.CPU_SQL_NANOS_MEAN,
      sorter: true,
      align: "right" as const,
      render: cpuRenderer,
    },
    {
      title: (
        <Tooltip title="Average time spent waiting for locks and contention per execution.">
          Contention Time
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.CONTENTION_TIME_MEAN,
      sorter: true,
      align: "right" as const,
      render: contentionRenderer,
    },
    {
      title: (
        <Tooltip title="Average CPU time spent in the KV layer per execution.">
          KV CPU Time
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.KV_CPU_TIME_MEAN,
      sorter: true,
      align: "right" as const,
      render: kvCpuRenderer,
    },
    {
      title: (
        <Tooltip title="Average time spent waiting in admission control per execution.">
          Admission Wait Time
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.ADM_WAIT_TIME_MEAN,
      sorter: true,
      align: "right" as const,
      render: admWaitRenderer,
    },
    {
      title: (
        <Tooltip title="Average number of rows read per execution.">
          Rows Read
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.ROWS_READ_MEAN,
      sorter: true,
      align: "right" as const,
      render: (row: TransactionRow) => Count(row.rowsReadMean),
    },
    {
      title: (
        <Tooltip title="Average number of rows written per execution.">
          Rows Written
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.ROWS_WRITTEN_MEAN,
      sorter: true,
      align: "right" as const,
      render: (row: TransactionRow) => Count(row.rowsWrittenMean),
    },
    {
      title: (
        <Tooltip title="Average number of bytes read per execution.">
          Bytes Read
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.BYTES_READ_MEAN,
      sorter: true,
      align: "right" as const,
      render: bytesReadRenderer,
    },
    {
      title: (
        <Tooltip title="Maximum number of retries observed across all executions.">
          Max Retries
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.MAX_RETRIES,
      sorter: true,
      align: "right" as const,
      render: retryRenderer,
    },
    {
      title: (
        <Tooltip title="Percentage of total cluster service latency used by this transaction fingerprint.">
          % of Runtime
        </Tooltip>
      ),
      sortKey: TransactionSortOptions.PCT_OF_TOTAL_RUNTIME,
      sorter: true,
      align: "right" as const,
      render: (row: TransactionRow) =>
        PercentageCustom(row.pctOfTotalRuntime, 1, 2),
    },
    {
      title: "App Name",
      render: (row: TransactionRow) => row.appName || "(unset)",
    },
  ];
};

// ---------------------------------------------------------------------------
// Initial state
// ---------------------------------------------------------------------------
const defaultTimeScale: TimeScale = {
  ...timeScale1hMinOptions["Past Hour"],
  key: "Past Hour",
  fixedWindowEnd: false,
};

const PAGE_SIZE = 20;

const timeScaleToDateRange = (
  ts: TimeScale,
): { start: number; end: number } => {
  const [s, e] = toRoundedDateRange(ts);
  return { start: s.unix(), end: e.unix() };
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export const TransactionsPageV2 = () => {
  const [timeScale, setTimeScale] = useState<TimeScale>(defaultTimeScale);
  const [page, setPage] = useState(1);
  const [sortField, setSortField] = useState<string>(
    TransactionSortOptions.SVC_LAT_MEAN,
  );
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [filters, setFilters] = useState<Filters>(defaultFilters);
  const [excludeInternal, setExcludeInternal] = useState(true);
  const [requestTime] = useState(moment.utc());

  const { reset: resetSQLStats } = useResetSQLStats();

  const { start, end } = timeScaleToDateRange(timeScale);
  const req: TransactionsRequest = {
    start,
    end,
    pagination: { pageSize: PAGE_SIZE, pageNum: page },
    sortBy: sortField,
    sortOrder,
    appName: filters.app || undefined,
    excludeInternal,
  };

  const { data, error, isLoading } = useSqlActivityTransactions(req);

  const rows = data?.results ?? [];
  const totalResults = data?.pagination.totalResults ?? 0;

  const columns = useMemo(() => makeColumns(rows), [rows]);

  const sort = { field: sortField, order: sortOrder };
  const colsWithSort = useMemo(
    () =>
      columns.map(col => {
        const sd: SortDirection =
          sort.order === "desc" ? "descend" : "ascend";
        return {
          ...col,
          sortOrder:
            sort.field === col.sortKey && col.sorter ? sd : null,
        };
      }),
    [sort, columns],
  );

  const onTableChange: TableChangeFn<TransactionRow> = (
    pagination,
    sorter,
  ) => {
    setPage(pagination.current);
    if (sorter) {
      const colKey = sorter.columnKey;
      if (typeof colKey !== "number") return;
      const col = columns[colKey];
      if (col.sortKey) {
        setSortField(col.sortKey);
        setSortOrder(sorter.order === "descend" ? "desc" : "asc");
      }
    }
  };

  const onSubmitFilters = (f: Filters) => {
    setFilters(f);
    setPage(1);
  };

  const activeFilters = calculateActiveFilters(filters);

  return (
    <PageLayout>
      <PageSection>
        <h3 className={commonStyles("base-heading")}>Transactions (V2)</h3>
        <section style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <PageConfig>
            <PageConfigItem>
              <Filter
                onSubmitFilters={onSubmitFilters}
                appNames={[]}
                activeFilters={activeFilters}
                filters={filters}
                hideTimeLabel={true}
              />
            </PageConfigItem>
            <PageConfigItem>
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <Switch
                  checked={!excludeInternal}
                  onChange={(checked: boolean) => {
                    setExcludeInternal(!checked);
                    setPage(1);
                  }}
                />
                Show internal
              </label>
            </PageConfigItem>
          </PageConfig>
          <PageConfig>
            <PageConfigItem>
              <TimeScaleDropdown
                currentScale={timeScale}
                setTimeScale={setTimeScale}
                options={timeScale1hMinOptions}
              />
            </PageConfigItem>
          </PageConfig>
        </section>
        <section>
          <PageConfig>
            <PageConfigItem>
              <p className={timeScaleStylesCx("time-label")}>
                <TimeScaleLabel
                  timeScale={timeScale}
                  requestTime={requestTime}
                />
                {", "}
                <ResultsPerPageLabel
                  pagination={{
                    pageSize: PAGE_SIZE,
                    current: page,
                    total: totalResults,
                  }}
                  pageName="transactions"
                />
              </p>
            </PageConfigItem>
            <PageConfigItem
              className={`${commonStyles("separator")}`}
            >
              <ClearStats
                resetSQLStats={resetSQLStats}
                tooltipType="transaction"
              />
            </PageConfigItem>
          </PageConfig>
        </section>
        <Table
          loading={isLoading}
          error={error}
          columns={colsWithSort}
          dataSource={rows}
          dataTest="transactions-v2-table"
          pagination={{
            size: "small",
            current: page,
            pageSize: PAGE_SIZE,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: totalResults,
          }}
          onChange={onTableChange}
        />
      </PageSection>
    </PageLayout>
  );
};
