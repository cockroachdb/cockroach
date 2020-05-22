// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import { StatementStatistics } from "src/util/appStats";
import { FixLong } from "src/util/fixLong";
import { StatementSummary } from "src/util/sql/summarize";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { countBarChart, latencyBarChart, retryBarChart, rowsBarChart } from "./barCharts";
import "./statements.styl";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import { ActivateDiagnosticsModalRef } from "./diagnostics/activateDiagnosticsModal";
import { StatementTableTitles, StatementTableCell, NodeNames } from "./statemetsTableContent";

const longToInt = (d: number | Long) => FixLong(d).toInt();

export interface AggregateStatistics {
  // label is either shortStatement (StatementsPage) or nodeId (StatementDetails).
  label: string;
  implicitTxn: boolean;
  stats: StatementStatistics;
  drawer?: boolean;
  firstCellBordered?: boolean;
  diagnosticsReport?: IStatementDiagnosticsReport;
}

export class StatementsSortedTable extends SortedTable<AggregateStatistics> {}

export function shortStatement(summary: StatementSummary, original: string) {
  switch (summary.statement) {
    case "update": return "UPDATE " + summary.table;
    case "insert": return "INSERT INTO " + summary.table;
    case "select": return "SELECT FROM " + summary.table;
    case "delete": return "DELETE FROM " + summary.table;
    case "create": return "CREATE TABLE " + summary.table;
    case "set": return "SET " + summary.table;
    default: return original;
  }
}

export function makeStatementsColumns(
  statements: AggregateStatistics[],
  selectedApp: string,
  search?: string,
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>,
): ColumnDescriptor<AggregateStatistics>[]  {

  const columns: ColumnDescriptor<AggregateStatistics>[] = [
    {
      title: StatementTableTitles.statements,
      className: "cl-table__col-query-text",
      cell: StatementTableCell.statements(search, selectedApp),
      sort: (stmt) => stmt.label,
    },
    {
      title: StatementTableTitles.txtType,
      className: "statements-table__col-time",
      cell: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
      sort: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
    },
  ];
  columns.push(...makeCommonColumns(statements));

  if (activateDiagnosticsRef) {
    const diagnosticsColumn: ColumnDescriptor<AggregateStatistics> = {
      title: StatementTableTitles.diagnostics,
      cell: StatementTableCell.diagnostics(activateDiagnosticsRef),
      sort: (stmt) => {
        if (stmt.diagnosticsReport) {
          return stmt.diagnosticsReport.completed ? "READY" : "WAITING FOR QUERY";
        }
        return null;
      },
    };
    columns.push(diagnosticsColumn);
  }
  return columns;
}

export function makeNodesColumns(statements: AggregateStatistics[], nodeNames: NodeNames)
    : ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      title: null,
      cell: StatementTableCell.nodeLink(nodeNames),
      // sort: (stmt) => stmt.label,
    },
  ];

  return original.concat(makeCommonColumns(statements));
}

function makeCommonColumns(statements: AggregateStatistics[])
    : ColumnDescriptor<AggregateStatistics>[] {
  const countBar = countBarChart(statements);
  const retryBar = retryBarChart(statements);
  const rowsBar = rowsBarChart(statements);
  const latencyBar = latencyBarChart(statements);

  return [
    {
      title: StatementTableTitles.retries,
      className: "statements-table__col-retries",
      cell: retryBar,
      sort: (stmt) => (longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count)),
    },
    {
      title: StatementTableTitles.executionCount,
      className: "statements-table__col-count",
      cell: countBar,
      sort: (stmt) => FixLong(stmt.stats.count).toInt(),
    },
    {
      title: StatementTableTitles.rowsAffected,
      className: "statements-table__col-rows",
      cell: rowsBar,
      sort: (stmt) => stmt.stats.num_rows.mean,
    },
    {
      title: StatementTableTitles.latency,
      className: "statements-table__col-latency",
      cell: latencyBar,
      sort: (stmt) => stmt.stats.service_lat.mean,
    },
  ];
}
