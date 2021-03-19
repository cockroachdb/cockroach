import React from "react";
import classNames from "classnames/bind";
import Long from "long";

import { FixLong, StatementSummary, StatementStatistics } from "src/util";
import {
  countBarChart,
  rowsReadBarChart,
  bytesReadBarChart,
  latencyBarChart,
  contentionBarChart,
  maxMemUsageBarChart,
  networkBytesBarChart,
  retryBarChart,
  workloadPctBarChart,
} from "src/barCharts";
import { ActivateDiagnosticsModalRef } from "src/statementsDiagnostics";
import { ColumnDescriptor, SortedTable } from "src/sortedtable";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  StatementTableTitle,
  StatementTableCell,
  NodeNames,
} from "./statementsTableContent";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
type ICollectedStatementStatistics = cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
import styles from "./statementsTable.module.scss";
const cx = classNames.bind(styles);
const longToInt = (d: number | Long) => Number(FixLong(d));

function makeCommonColumns(
  statements: AggregateStatistics[],
  totalWorkload: number,
): ColumnDescriptor<AggregateStatistics>[] {
  const defaultBarChartOptions = {
    classes: {
      root: cx("statements-table__col--bar-chart"),
      label: cx("statements-table__col--bar-chart__label"),
    },
  };
  const sampledExecStatsBarChartOptions = {
    classes: defaultBarChartOptions.classes,
    displayNoSamples: (d: ICollectedStatementStatistics) => {
      return longToInt(d.stats.exec_stats?.count) == 0;
    },
  };

  const countBar = countBarChart(statements, defaultBarChartOptions);
  const rowsReadBar = rowsReadBarChart(statements, defaultBarChartOptions);
  const bytesReadBar = bytesReadBarChart(statements, defaultBarChartOptions);
  const latencyBar = latencyBarChart(statements, defaultBarChartOptions);
  const contentionBar = contentionBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const maxMemUsageBar = maxMemUsageBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const networkBytesBar = networkBytesBarChart(
    statements,
    sampledExecStatsBarChartOptions,
  );
  const retryBar = retryBarChart(statements, defaultBarChartOptions);

  return [
    {
      name: "executionCount",
      title: StatementTableTitle.executionCount,
      className: cx("statements-table__col-count"),
      cell: countBar,
      sort: stmt => FixLong(Number(stmt.stats.count)),
    },
    {
      name: "rowsRead",
      title: StatementTableTitle.rowsRead,
      className: cx("statements-table__col-rows-read"),
      cell: rowsReadBar,
      sort: stmt => FixLong(Number(stmt.stats.rows_read.mean)),
    },
    {
      name: "bytesRead",
      title: StatementTableTitle.bytesRead,
      cell: bytesReadBar,
      sort: stmt => FixLong(Number(stmt.stats.bytes_read.mean)),
    },
    {
      name: "latency",
      title: StatementTableTitle.statementTime,
      className: cx("statements-table__col-latency"),
      cell: latencyBar,
      sort: stmt => stmt.stats.service_lat.mean,
    },
    {
      name: "contention",
      title: StatementTableTitle.contention,
      cell: contentionBar,
      sort: stmt => FixLong(Number(stmt.stats.exec_stats.contention_time.mean)),
    },
    {
      name: "maxMemoryUsage",
      title: StatementTableTitle.maxMemUsage,
      cell: maxMemUsageBar,
      sort: stmt => FixLong(Number(stmt.stats.exec_stats.max_mem_usage.mean)),
    },
    {
      name: "networkBytes",
      title: StatementTableTitle.networkBytes,
      cell: networkBytesBar,
      sort: stmt => FixLong(Number(stmt.stats.exec_stats.network_bytes.mean)),
    },
    {
      name: "retries",
      title: StatementTableTitle.retries,
      className: cx("statements-table__col-retries"),
      cell: retryBar,
      sort: stmt =>
        longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count),
    },
    {
      name: "workloadPct",
      title: StatementTableTitle.workloadPct,
      cell: workloadPctBarChart(
        statements,
        defaultBarChartOptions,
        totalWorkload,
      ),
      sort: stmt =>
        (stmt.stats.service_lat.mean * longToInt(stmt.stats.count)) /
        totalWorkload,
    },
  ];
}

export interface AggregateStatistics {
  // label is either shortStatement (StatementsPage) or nodeId (StatementDetails).
  label: string;
  implicitTxn: boolean;
  fullScan: boolean;
  stats: StatementStatistics;
  drawer?: boolean;
  firstCellBordered?: boolean;
  diagnosticsReports?: cockroach.server.serverpb.IStatementDiagnosticsReport[];
  // totalWorkload is the sum of service latency of all statements listed on the table.
  totalWorkload?: Long;
}

export class StatementsSortedTable extends SortedTable<AggregateStatistics> {}

export function shortStatement(summary: StatementSummary, original: string) {
  switch (summary.statement) {
    case "update":
      return "UPDATE " + summary.table;
    case "insert":
      return "INSERT INTO " + summary.table;
    case "select":
      return "SELECT FROM " + summary.table;
    case "delete":
      return "DELETE FROM " + summary.table;
    case "create":
      return "CREATE TABLE " + summary.table;
    case "set":
      return "SET " + summary.table;
    default:
      return original;
  }
}

export function makeStatementsColumns(
  statements: AggregateStatistics[],
  selectedApp: string,
  // totalWorkload is the sum of service latency of all statements listed on the table.
  totalWorkload: number,
  search?: string,
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>,
  onDiagnosticsDownload?: (report: IStatementDiagnosticsReport) => void,
): ColumnDescriptor<AggregateStatistics>[] {
  const columns: ColumnDescriptor<AggregateStatistics>[] = [
    {
      name: "statements",
      title: StatementTableTitle.statements,
      className: cx("cl-table__col-query-text"),
      cell: StatementTableCell.statements(search, selectedApp),
      sort: stmt => stmt.label,
    },
  ];
  columns.push(...makeCommonColumns(statements, totalWorkload));

  if (activateDiagnosticsRef) {
    const diagnosticsColumn: ColumnDescriptor<AggregateStatistics> = {
      name: "diagnostics",
      title: StatementTableTitle.diagnostics,
      cell: StatementTableCell.diagnostics(
        activateDiagnosticsRef,
        onDiagnosticsDownload,
      ),
      sort: stmt => {
        if (stmt.diagnosticsReports?.length > 0) {
          // Perform sorting by first diagnostics report as only
          // this one can be either in ready or waiting status.
          return stmt.diagnosticsReports[0].completed ? "READY" : "WAITING";
        }
        return null;
      },
      titleAlign: "right",
    };
    columns.push(diagnosticsColumn);
  }
  return columns;
}

export function makeNodesColumns(
  statements: AggregateStatistics[],
  nodeNames: NodeNames,
  totalWorkload: number,
): ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      name: "nodes",
      title: null,
      cell: StatementTableCell.nodeLink(nodeNames),
    },
  ];

  return original.concat(makeCommonColumns(statements, totalWorkload));
}
