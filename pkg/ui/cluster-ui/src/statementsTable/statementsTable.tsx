import React from "react";
import classNames from "classnames/bind";
import Long from "long";

import { FixLong, StatementSummary, StatementStatistics } from "src/util";
import {
  countBarChart,
  latencyBarChart,
  retryBarChart,
  rowsBarChart,
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

import styles from "./statementsTable.module.scss";
const cx = classNames.bind(styles);
const longToInt = (d: number | Long) => Number(FixLong(d));

function makeCommonColumns(
  statements: AggregateStatistics[],
): ColumnDescriptor<AggregateStatistics>[] {
  const countBar = countBarChart(statements, {
    classes: {
      root: cx("statements-table__col-count--bar-chart"),
      label: cx("statements-table__col-count--bar-chart__label"),
    },
  });
  const retryBar = retryBarChart(statements, {
    classes: {
      root: cx("statements-table__col-retries--bar-chart"),
      label: cx("statements-table__col-retries--bar-chart__label"),
    },
  });
  const rowsBar = rowsBarChart(statements, {
    classes: {
      root: cx("statements-table__col-rows--bar-chart"),
      label: cx("statements-table__col-rows--bar-chart__label"),
    },
  });
  const latencyBar = latencyBarChart(statements, {
    classes: {
      root: cx("statements-table__col-latency--bar-chart"),
    },
  });

  return [
    {
      name: "retries",
      title: StatementTableTitle.retries,
      className: cx("statements-table__col-retries"),
      cell: retryBar,
      sort: stmt =>
        longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count),
    },
    {
      name: "executionCount",
      title: StatementTableTitle.executionCount,
      className: cx("statements-table__col-count"),
      cell: countBar,
      sort: stmt => FixLong(Number(stmt.stats.count)),
    },
    {
      name: "rowsAffected",
      title: StatementTableTitle.rowsAffected,
      className: cx("statements-table__col-rows"),
      cell: rowsBar,
      sort: stmt => stmt.stats.num_rows.mean,
    },
    {
      name: "latency",
      title: StatementTableTitle.latency,
      className: cx("statements-table__col-latency"),
      cell: latencyBar,
      sort: stmt => stmt.stats.service_lat.mean,
    },
  ];
}

export interface AggregateStatistics {
  // label is either shortStatement (StatementsPage) or nodeId (StatementDetails).
  label: string;
  implicitTxn: boolean;
  stats: StatementStatistics;
  drawer?: boolean;
  firstCellBordered?: boolean;
  diagnosticsReports?: cockroach.server.serverpb.IStatementDiagnosticsReport[];
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
  search?: string,
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>,
  onDiagnosticsDownload?: (report: IStatementDiagnosticsReport) => void,
  onStatementClick?: (statement: string) => void,
): ColumnDescriptor<AggregateStatistics>[] {
  const columns: ColumnDescriptor<AggregateStatistics>[] = [
    {
      name: "statements",
      title: StatementTableTitle.statements,
      className: cx("cl-table__col-query-text"),
      cell: StatementTableCell.statements(
        search,
        selectedApp,
        onStatementClick,
      ),
      sort: stmt => stmt.label,
    },
    {
      name: "txtType",
      title: StatementTableTitle.txtType,
      className: cx("statements-table__col-time"),
      cell: stmt => (stmt.implicitTxn ? "Implicit" : "Explicit"),
      sort: stmt => (stmt.implicitTxn ? "Implicit" : "Explicit"),
    },
  ];
  columns.push(...makeCommonColumns(statements));

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
): ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      name: "nodes",
      title: null,
      cell: StatementTableCell.nodeLink(nodeNames),
    },
  ];

  return original.concat(makeCommonColumns(statements));
}
