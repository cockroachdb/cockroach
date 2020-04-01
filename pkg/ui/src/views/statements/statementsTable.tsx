// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import getHighlightedText from "oss/src/util/highlightedText";
import React from "react";
import { Link } from "react-router-dom";

import { StatementStatistics } from "src/util/appStats";
import { FixLong } from "src/util/fixLong";
import { StatementSummary, summarize } from "src/util/sql/summarize";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { countBarChart, latencyBarChart, retryBarChart, rowsBarChart } from "./barCharts";
import { Anchor, Tooltip } from "src/components";
import "./statements.styl";
import { DiagnosticStatusBadge } from "./diagnostics/diagnosticStatusBadge";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import { ActivateDiagnosticsModalRef } from "./diagnostics/activateDiagnosticsModal";

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

function StatementLink(props: { statement: string, app: string, implicitTxn: boolean, search: string }) {
  const summary = summarize(props.statement);
  const base = props.app && props.app.length > 0 ? `/statements/${props.app}/${props.implicitTxn}` : `/statement/${props.implicitTxn}`;
  return (
    <Link to={ `${base}/${encodeURIComponent(props.statement)}` }>
      <div className="cl-table-link__tooltip">
        <Tooltip
          placement="bottom"
          title={<pre className="cl-table-link__description">
            { getHighlightedText(props.statement, props.search) }
          </pre>}
          overlayClassName="cl-table-link__statement-tooltip--fixed-width"
        >
          <div className="cl-table-link__tooltip-hover-area">
            { getHighlightedText(shortStatement(summary, props.statement), props.search, true) }
          </div>
        </Tooltip>
      </div>
    </Link>
  );
}

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
      title: "Statement",
      className: "cl-table__col-query-text",
      cell: (stmt) => (
        <StatementLink
          statement={ stmt.label }
          implicitTxn={ stmt.implicitTxn }
          search={search}
          app={ selectedApp }
        />
      ),
      sort: (stmt) => stmt.label,
    },
    {
      title: "Txn Type",
      className: "statements-table__col-time",
      cell: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
      sort: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
    },
  ];
  columns.push(...makeCommonColumns(statements));

  if (activateDiagnosticsRef) {
    const diagnosticsColumn: ColumnDescriptor<AggregateStatistics> = {
      title: "Diagnostics",
      cell: (stmt) => {
        if (stmt.diagnosticsReport) {
          return <DiagnosticStatusBadge status={stmt.diagnosticsReport.completed ? "READY" : "WAITING FOR QUERY"}/>;
        }
        return (
          <Anchor
            onClick={() => activateDiagnosticsRef?.current?.showModalFor(stmt.label)}
          >
            Activate
          </Anchor>
        );
      },
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

function NodeLink(props: { nodeId: string, nodeNames: { [nodeId: string]: string } }) {
  return (
    <Link to={ `/node/${props.nodeId}` }>
      <div className="node-name-tooltip__info-icon">{props.nodeNames[props.nodeId]}</div>
    </Link>
  );
}

export function makeNodesColumns(statements: AggregateStatistics[], nodeNames: { [nodeId: string]: string })
    : ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      title: null,
      cell: (stmt) => <NodeLink nodeId={stmt.label} nodeNames={ nodeNames } />,
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
      title: "Retries",
      className: "statements-table__col-retries",
      cell: retryBar,
      sort: (stmt) => (longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count)),
    },
    {
      title: "Execution Count",
      className: "statements-table__col-count",
      cell: countBar,
      sort: (stmt) => FixLong(stmt.stats.count).toInt(),
    },
    {
      title: "Rows Affected",
      className: "statements-table__col-rows",
      cell: rowsBar,
      sort: (stmt) => stmt.stats.num_rows.mean,
    },
    {
      title: "Latency",
      className: "statements-table__col-latency",
      cell: latencyBar,
      sort: (stmt) => stmt.stats.service_lat.mean,
    },
  ];
}
