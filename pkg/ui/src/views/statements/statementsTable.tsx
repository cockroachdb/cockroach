// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import getHighlightedText from "src/util/highlightedText";
import React from "react";
import { Link } from "react-router-dom";
import classNames from "classnames/bind";

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
import styles from "./statementsTable.module.styl";
import { statementsSql, transactionalPipelining, statementDiagnostics, statementsRetries, statementsTimeInterval } from "oss/src/util/docs";

const cx = classNames.bind(styles);
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
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"SQL statement "}
                <Anchor
                  href={statementsSql}
                  target="_blank"
                >
                  fingerprint.
                </Anchor>
              </p>
              <p>
                To view additional details of a SQL statement fingerprint, click this to open the Statement Details page.
              </p>
            </div>
          }
        >
          Statements
        </Tooltip>
      ),
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
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"Type of transaction (implicit or explicit). Explicit transactions refer to statements that are wrapped by "}
                <code>BEGIN</code>
                {" and "}
                <code>COMMIT</code>
                {" statements by the client. Explicit transactions employ "}
                <Anchor
                  href={transactionalPipelining}
                  target="_blank"
                >
                  transactional pipelining
                </Anchor>
                {" and therefore report latencies that do not account for replication."}
              </p>
              <p>
                For statements not in explicit transactions, CockroachDB wraps each statement in individual implicit transactions.
              </p>
            </div>
          }
        >
          TXN Type
        </Tooltip>
      ),
      className: "statements-table__col-time",
      cell: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
      sort: (stmt) => (stmt.implicitTxn ? "Implicit" : "Explicit"),
    },
  ];
  columns.push(...makeCommonColumns(statements));

  if (activateDiagnosticsRef) {
    const diagnosticsColumn: ColumnDescriptor<AggregateStatistics> = {
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"Option to activate "}
                <Anchor
                  href={statementDiagnostics}
                  target="_blank"
                >
                  diagnostics
                </Anchor>
                {" for each statement. If activated, this displays the status of diagnostics collection ("}
                <code>WAITING FOR QUERY</code>, <code>READY</code>, OR <code>ERROR</code>).
              </p>
            </div>
          }
        >
          Diagnostics
        </Tooltip>
      ),
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
  const countBar = countBarChart(
    statements,
    {
      classes: {
        root: cx("statements-table__col-count--bar-chart"),
        label: cx("statements-table__col-count--bar-chart__label"),
      },
    },
  );
  const retryBar = retryBarChart(
    statements,
    {
      classes: {
        root: cx("statements-table__col-retries--bar-chart"),
        label: cx("statements-table__col-retries--bar-chart__label"),
      },
    },
  );
  const rowsBar = rowsBarChart(
    statements,
    {
      classes: {
        root: cx("statements-table__col-rows--bar-chart"),
        label: cx("statements-table__col-rows--bar-chart__label"),
      },
    },
  );
  const latencyBar = latencyBarChart(
    statements,
    {
      classes: {
        root: cx("statements-table__col-latency--bar-chart"),
      },
    },
  );

  return [
    {
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"Cumulative number of "}
                <Anchor
                  href={statementsRetries}
                  target="_blank"
                >
                  retries
                </Anchor>
                {" of statements with this fingerprint within the last hour or specified time interval."}
              </p>
            </div>
          }
        >
          Retries
        </Tooltip>
      ),
      className: "statements-table__col-retries",
      cell: retryBar,
      sort: (stmt) => (longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count)),
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"Cumulative number of executions of statements with this fingerprint within the last hour or specified "}
                <Anchor
                  href={statementsTimeInterval}
                  target="_blank"
                >
                  time interval
                </Anchor>.
              </p>
              <p>
                {"The bar indicates the ratio of runtime success (gray) to "}
                <Anchor
                  href={statementsRetries}
                  target="_blank"
                >
                  retries
                </Anchor>
                {" (red) for the SQL statement fingerprint."}
              </p>
            </div>
          }
        >
          Execution Count
        </Tooltip>
      ),
      className: "statements-table__col-count",
      cell: countBar,
      sort: (stmt) => FixLong(stmt.stats.count).toInt(),
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                {"Average number of rows returned while executing statements with this fingerprint within the last hour or specified "}
                <Anchor
                  href={statementsTimeInterval}
                  target="_blank"
                >
                  time interval
                </Anchor>.
              </p>
              <p>
                The gray bar indicates the mean number of rows returned. The blue bar indicates one standard deviation from the mean.
              </p>
            </div>
          }
        >
          Rows Affected
        </Tooltip>
      ),
      className: "statements-table__col-rows",
      cell: rowsBar,
      sort: (stmt) => stmt.stats.num_rows.mean,
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title={
            <div className="tooltip__table--title">
              <p>
                Average service latency of statements with this fingerprint within the last hour or specified time interval.
              </p>
              <p>
                The gray bar indicates the mean latency. The blue bar indicates one standard deviation from the mean.
              </p>
            </div>
          }
        >
          Latency
        </Tooltip>
      ),
      className: "statements-table__col-latency",
      cell: latencyBar,
      sort: (stmt) => stmt.stats.service_lat.mean,
    },
  ];
}
