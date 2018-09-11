import React from "react";
import { Link } from "react-router";

import { FixLong } from "src/util/fixLong";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { StatementStatistics } from "src/util/appStats";
import { Duration } from "src/util/format";
import { summarize, StatementSummary } from "src/util/sql/summarize";

import { countBarChart, retryBarChart, rowsBarChart, latencyBarChart } from "./barCharts";

import "./statements.styl";

const longToInt = (d: number | Long) => FixLong(d).toInt();

export interface AggregateStatistics {
  label: string;
  stats: StatementStatistics;
}

export class StatementsSortedTable extends SortedTable<AggregateStatistics> {}

function StatementLink(props: { statement: string, app: string }) {
  const summary = summarize(props.statement);
  const base = props.app ? `/statements/${props.app}` : "/statement";

  return (
    <Link to={ `${base}/${encodeURIComponent(props.statement)}` }>
      <div className="statement__tooltip">
        <ToolTipWrapper text={ <pre style={{ whiteSpace: "pre-wrap" }}>{ props.statement }</pre> }>
          <div className="statement__tooltip-hover-area">
            { shortStatement(summary, props.statement) }
          </div>
        </ToolTipWrapper>
      </div>
    </Link>
  );
}

function shortStatement(summary: StatementSummary, original: string) {
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

function calculateCumulativeTime(stats: StatementStatistics) {
  const count = FixLong(stats.count).toInt();
  const latency = stats.service_lat.mean;

  return count * latency;
}

export function makeStatementsColumns(statements: AggregateStatistics[], selectedApp: string)
    : ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      title: "Statement",
      className: "statements-table__col-query-text",
      cell: (stmt) => <StatementLink statement={ stmt.label } app={ selectedApp } />,
      sort: (stmt) => stmt.label,
    },
  ];

  return original.concat(makeCommonColumns(statements));
}

function NodeLink(props: { nodeId: string, nodeNames: { [nodeId: string]: string } }) {
  return (
    <Link to={ `/node/${props.nodeId}` }>
      <div className="node-name__tooltip">
        <ToolTipWrapper text={props.nodeNames[props.nodeId]} short>
          <div className="node-name-tooltip__tooltip-hover-area">
            <div className="node-name-tooltip__info-icon">n{props.nodeId}</div>
          </div>
        </ToolTipWrapper>
      </div>
    </Link>
  );
}

export function makeNodesColumns(statements: AggregateStatistics[], nodeNames: { [nodeId: string]: string })
    : ColumnDescriptor<AggregateStatistics>[] {
  const original: ColumnDescriptor<AggregateStatistics>[] = [
    {
      title: (
        <React.Fragment>
          Node
          <div className="numeric-stats-table__tooltip">
            <ToolTipWrapper text="Statement statistics grouped by which node received the request for the statement.">
              <div className="numeric-stats-table__tooltip-hover-area">
                <div className="numeric-stats-table__info-icon">i</div>
              </div>
            </ToolTipWrapper>
          </div>
        </React.Fragment>
      ),
      cell: (stmt) => <NodeLink nodeId={ stmt.label } nodeNames={ nodeNames } />,
      sort: (stmt) => stmt.label,
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
      title: "Time",
      className: "statements-table__col-time",
      cell: (stmt) => Duration(calculateCumulativeTime(stmt.stats) * 1e9),
      sort: (stmt) => calculateCumulativeTime(stmt.stats),
    },
    {
      title: "Execution Count",
      className: "statements-table__col-count",
      cell: countBar,
      sort: (stmt) => FixLong(stmt.stats.count).toInt(),
    },
    {
      title: "Retries",
      className: "statements-table__col-retries",
      cell: retryBar,
      sort: (stmt) => (longToInt(stmt.stats.count) - longToInt(stmt.stats.first_attempt_count)),
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
