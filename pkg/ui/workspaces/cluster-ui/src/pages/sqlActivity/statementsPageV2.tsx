// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Switch } from "antd";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useHistory, useLocation } from "react-router-dom";

import {
  StatementRow,
  StatementSortOptions,
  SqlActivityStatementsRequest,
  useSqlActivityStatements,
} from "src/api/sqlActivityApi";
import { useResetSQLStats } from "src/api/sqlStatsApi";
import { barChartFactory } from "src/barCharts/barChartFactory";
import { Button } from "src/button";
import { commonStyles } from "src/common";
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
import { Search } from "src/search";
import {
  SortDirection,
  Table,
  TableChangeFn,
  TableColumnProps,
} from "src/sharedFromCloud/table";
import ClearStats from "src/sqlActivity/clearStats";
import {
  TimeScale,
  TimeScaleDropdown,
  timeScale1hMinOptions,
  toRoundedDateRange,
} from "src/timeScaleDropdown";
import timeScaleStyles from "src/timeScaleDropdown/timeScale.module.scss";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { Bytes, Count, Duration, PercentageCustom } from "src/util";

const timeScaleStylesCx = classNames.bind(timeScaleStyles);

// ---------------------------------------------------------------------------
// URL parameter helpers
// ---------------------------------------------------------------------------
const URL_KEYS = {
  sortBy: "sortBy",
  sortOrder: "sortOrder",
  page: "page",
  search: "q",
  app: "app",
  database: "db",
  start: "start",
  end: "end",
  excludeInternal: "excludeInternal",
} as const;

interface URLState {
  sortField: string;
  sortOrder: "asc" | "desc";
  page: number;
  search: string;
  app: string;
  database: string;
  start: number; // seconds since epoch
  end: number; // seconds since epoch
  excludeInternal: boolean;
}

const parseURLState = (searchStr: string): Partial<URLState> => {
  const p = new URLSearchParams(searchStr);
  const result: Partial<URLState> = {};
  if (p.has(URL_KEYS.sortBy)) result.sortField = p.get(URL_KEYS.sortBy);
  if (p.has(URL_KEYS.sortOrder)) {
    const v = p.get(URL_KEYS.sortOrder);
    if (v === "asc" || v === "desc") result.sortOrder = v;
  }
  if (p.has(URL_KEYS.page)) {
    const n = parseInt(p.get(URL_KEYS.page), 10);
    if (!isNaN(n) && n > 0) result.page = n;
  }
  if (p.has(URL_KEYS.search)) result.search = p.get(URL_KEYS.search);
  if (p.has(URL_KEYS.app)) result.app = p.get(URL_KEYS.app);
  if (p.has(URL_KEYS.database)) result.database = p.get(URL_KEYS.database);
  if (p.has(URL_KEYS.start)) {
    const n = parseInt(p.get(URL_KEYS.start), 10);
    if (!isNaN(n)) result.start = n;
  }
  if (p.has(URL_KEYS.end)) {
    const n = parseInt(p.get(URL_KEYS.end), 10);
    if (!isNaN(n)) result.end = n;
  }
  if (p.has(URL_KEYS.excludeInternal)) {
    result.excludeInternal = p.get(URL_KEYS.excludeInternal) !== "false";
  }
  return result;
};

const buildURLParams = (state: URLState): string => {
  const p = new URLSearchParams();
  p.set(URL_KEYS.sortBy, state.sortField);
  p.set(URL_KEYS.sortOrder, state.sortOrder);
  if (state.page > 1) p.set(URL_KEYS.page, state.page.toString());
  if (state.search) p.set(URL_KEYS.search, state.search);
  if (state.app) p.set(URL_KEYS.app, state.app);
  if (state.database) p.set(URL_KEYS.database, state.database);
  if (state.start) p.set(URL_KEYS.start, state.start.toString());
  if (state.end) p.set(URL_KEYS.end, state.end.toString());
  if (!state.excludeInternal) {
    p.set(URL_KEYS.excludeInternal, "false");
  }
  return p.toString();
};

// ---------------------------------------------------------------------------
// Bar chart factories for V2 StatementRow
// ---------------------------------------------------------------------------
const svcLatBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "bar-chart__parse", value: d => d.svcLatMean }],
  v => Duration(v * 1e9),
  { name: "bar-chart__overall-dev", value: d => d.svcLatStddev },
);

const cpuBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "cpu", value: d => d.cpuSqlNanosMean }],
  v => Duration(v),
  { name: "cpu-dev", value: d => d.cpuSqlNanosStddev },
);

const contentionBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "contention", value: d => d.contentionTimeMean }],
  v => Duration(v * 1e9),
  { name: "contention-dev", value: d => d.contentionTimeStddev },
);

const kvCpuBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "kv-cpu-time", value: d => d.kvCpuTimeNanosMean }],
  v => Duration(v),
  { name: "kv-cpu-time-dev", value: d => d.kvCpuTimeNanosStddev },
);

const admWaitBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "admission-wait-time", value: d => d.admissionWaitTimeMean }],
  v => Duration(v),
  { name: "admission-wait-time-dev", value: d => d.admissionWaitTimeStddev },
);

const bytesReadBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "bytes-read", value: d => d.bytesReadMean }],
  Bytes,
  { name: "bytes-read-dev", value: d => d.bytesReadStddev },
);

const countBarChart = barChartFactory<StatementRow>(
  "grey",
  [{ name: "count-first-try", value: d => d.executionCount }],
  v => Count(v),
);

const retryBarChart = barChartFactory<StatementRow>(
  "red",
  [{ name: "count-retry", value: d => d.maxRetries }],
  v => Count(v),
);

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------

const makeColumns = (
  rows: StatementRow[],
): (TableColumnProps<StatementRow> & {
  sortKey?: StatementSortOptions;
})[] => {
  const countRenderer = countBarChart(rows);
  const svcLatRenderer = svcLatBarChart(rows);
  const cpuRenderer = cpuBarChart(rows);
  const contentionRenderer = contentionBarChart(rows);
  const kvCpuRenderer = kvCpuBarChart(rows);
  const admWaitRenderer = admWaitBarChart(rows);
  const bytesReadRenderer = bytesReadBarChart(rows);
  const retryRenderer = retryBarChart(rows);

  return [
    {
      title: <Tooltip title="SQL statement fingerprint.">Statement</Tooltip>,
      width: 400,
      render: (row: StatementRow) => (
        <div
          style={{
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
            maxWidth: 400,
          }}
          title={row.query}
        >
          {row.querySummary || row.query}
        </div>
      ),
    },
    {
      title: (
        <Tooltip title="Cumulative number of executions of statements with this fingerprint within the specified time interval.">
          Execution Count
        </Tooltip>
      ),
      sortKey: StatementSortOptions.EXECUTION_COUNT,
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
      sortKey: StatementSortOptions.SVC_LAT_MEAN,
      sorter: true,
      align: "right" as const,
      render: svcLatRenderer,
    },
    {
      title: (
        <Tooltip title="Average CPU time spent executing within the SQL layer per execution.">
          SQL CPU Time
        </Tooltip>
      ),
      sortKey: StatementSortOptions.CPU_SQL_NANOS_MEAN,
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
      sortKey: StatementSortOptions.CONTENTION_TIME_MEAN,
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
      sortKey: StatementSortOptions.KV_CPU_TIME_MEAN,
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
      sortKey: StatementSortOptions.ADM_WAIT_TIME_MEAN,
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
      sortKey: StatementSortOptions.ROWS_READ_MEAN,
      sorter: true,
      align: "right" as const,
      render: (row: StatementRow) => Count(row.rowsReadMean),
    },
    {
      title: (
        <Tooltip title="Average number of rows written per execution.">
          Rows Written
        </Tooltip>
      ),
      sortKey: StatementSortOptions.ROWS_WRITTEN_MEAN,
      sorter: true,
      align: "right" as const,
      render: (row: StatementRow) => Count(row.rowsWrittenMean),
    },
    {
      title: (
        <Tooltip title="Average number of bytes read per execution.">
          Bytes Read
        </Tooltip>
      ),
      sortKey: StatementSortOptions.BYTES_READ_MEAN,
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
      sortKey: StatementSortOptions.MAX_RETRIES,
      sorter: true,
      align: "right" as const,
      render: retryRenderer,
    },
    {
      title: (
        <Tooltip title="Percentage of total cluster service latency used by this statement fingerprint.">
          % of Runtime
        </Tooltip>
      ),
      sortKey: StatementSortOptions.PCT_OF_TOTAL_RUNTIME,
      sorter: true,
      align: "right" as const,
      render: (row: StatementRow) =>
        PercentageCustom(row.pctOfTotalRuntime, 1, 2),
    },
    {
      title: "App Name",
      render: (row: StatementRow) => row.appName || "(unset)",
    },
    {
      title: "Database",
      render: (row: StatementRow) => row.database || "(unknown)",
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

// ---------------------------------------------------------------------------
// Helpers to convert TimeScale <-> ISO strings for the API
// ---------------------------------------------------------------------------
const timeScaleToDateRange = (
  ts: TimeScale,
): { start: number; end: number } => {
  const [s, e] = toRoundedDateRange(ts);
  return { start: s.unix(), end: e.unix() };
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export const StatementsPageV2 = () => {
  const history = useHistory();
  const location = useLocation();

  // Parse initial state from URL.
  const urlState = useMemo(
    () => parseURLState(location.search),
    // Only parse on mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  // If URL has start/end, build a fixed-window TimeScale from them.
  const initialTimeScale = useMemo((): TimeScale => {
    if (urlState.start && urlState.end) {
      const s = moment.unix(urlState.start).utc();
      const e = moment.unix(urlState.end).utc();
      if (s.isValid() && e.isValid()) {
        return {
          ...defaultTimeScale,
          key: "Custom",
          fixedWindowEnd: e,
          windowSize: moment.duration(e.diff(s)),
        };
      }
    }
    return defaultTimeScale;
  }, [urlState.start, urlState.end]);

  const [timeScale, setTimeScale] = useState<TimeScale>(initialTimeScale);
  const [page, setPage] = useState(urlState.page ?? 1);
  const [sortField, setSortField] = useState<string>(
    urlState.sortField ?? StatementSortOptions.SVC_LAT_MEAN,
  );
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">(
    urlState.sortOrder ?? "desc",
  );
  const [search, setSearch] = useState(urlState.search ?? "");
  const [filters, setFilters] = useState<Filters>({
    ...defaultFilters,
    app: urlState.app ?? "",
    database: urlState.database ?? "",
  });
  const [excludeInternal, setExcludeInternal] = useState(
    urlState.excludeInternal ?? true,
  );
  const [requestTime] = useState(moment.utc());

  const { reset: resetSQLStats } = useResetSQLStats();

  const { start, end } = timeScaleToDateRange(timeScale);

  // Sync state to URL via history.replace.
  useEffect(() => {
    const qs = buildURLParams({
      sortField,
      sortOrder,
      page,
      search,
      app: filters.app ?? "",
      database: filters.database ?? "",
      start,
      end,
      excludeInternal,
    });
    history.replace({ search: qs });
  }, [
    sortField,
    sortOrder,
    page,
    search,
    filters,
    start,
    end,
    excludeInternal,
    history,
  ]);

  const req: SqlActivityStatementsRequest = {
    start,
    end,
    pagination: { pageSize: PAGE_SIZE, pageNum: page },
    sortBy: sortField,
    sortOrder,
    search: search || undefined,
    appName: filters.app || undefined,
    database: filters.database || undefined,
    excludeInternal,
  };

  const { data, error, isLoading, refresh } = useSqlActivityStatements(req);

  const onApply = useCallback(() => {
    refresh();
  }, [refresh]);

  const rows = data?.results ?? [];
  const totalResults = data?.pagination.totalResults ?? 0;

  const columns = useMemo(() => makeColumns(data?.results), [data?.results]);

  const colsWithSort = useMemo(() => {
    const sort = { field: sortField, order: sortOrder };
    return columns.map(col => {
      const sd: SortDirection = sort.order === "desc" ? "descend" : "ascend";
      return {
        ...col,
        sortOrder: sort.field === col.sortKey && col.sorter ? sd : null,
      };
    });
  }, [columns, sortField, sortOrder]);

  const onTableChange: TableChangeFn<StatementRow> = (pagination, sorter) => {
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
        <h3 className={commonStyles("base-heading")}>Statements (V2)</h3>
        <section
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <PageConfig>
            <PageConfigItem>
              <Search
                placeholder="Search statements"
                onSubmit={(val: string) => {
                  setSearch(val);
                  setPage(1);
                }}
                defaultValue={search}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Filter
                onSubmitFilters={onSubmitFilters}
                appNames={[]}
                dbNames={[]}
                activeFilters={activeFilters}
                filters={filters}
                showDB={true}
                hideTimeLabel={true}
              />
            </PageConfigItem>
            <PageConfigItem>
              <label
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  cursor: "pointer",
                }}
              >
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
            <PageConfigItem>
              <Button type="secondary" onClick={onApply}>
                Apply
              </Button>
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
                  pageName="statements"
                />
              </p>
            </PageConfigItem>
            <PageConfigItem className={`${commonStyles("separator")}`}>
              <ClearStats
                resetSQLStats={resetSQLStats}
                tooltipType="statement"
              />
            </PageConfigItem>
          </PageConfig>
        </section>
        <Table
          loading={isLoading}
          error={error}
          columns={colsWithSort}
          dataSource={rows}
          dataTest="statements-v2-table"
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
