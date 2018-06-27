import d3 from "d3";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";
import { createSelector } from "reselect";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { refreshQueries } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { NumericStat, stdDev, combineStatementStats, flattenStatementStats, StatementStatistics, ExecutionStatistics } from "src/util/appStats";
import { statementAttr, appAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { Duration } from "src/util/format";
import { intersperse } from "src/util/intersperse";
import { Pick } from "src/util/pick";
import { SqlBox } from "src/views/shared/components/sql/box";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";

import { countBreakdown, rowsBreakdown, latencyBreakdown, approximify } from "./barCharts";

interface AggregateStatistics {
  statement: string;
  app: string[];
  distSQL: boolean[];
  failed: boolean[];
  stats: StatementStatistics;
}

function AppLink(props: { app: string }) {
  if (!props.app) {
    return <span className="app-name app-name__unset">(unset)</span>;
  }

  return (
    <Link className="app-name" to={ `/statements/${encodeURIComponent(props.app)}` }>
      { props.app }
    </Link>
  );
}

interface StatementDetailsOwnProps {
  statement: AggregateStatistics;
  refreshQueries: typeof refreshQueries;
}

type StatementDetailsProps = StatementDetailsOwnProps & RouterState;

interface NumericStatRow {
  name: string;
  value: NumericStat;
  bar?: () => {};
}

interface NumericStatTableProps {
  rows: NumericStatRow[];
  count: number;
  format: (v: number) => string;
}

class NumericStatTable extends React.Component<NumericStatTableProps> {
  static defaultProps = {
    format: (v: number) => `${v}`,
  };

  render() {
    return (
      <table className="numeric-stats-table">
        <thead>
          <tr className="numeric-stats-table__row--header">
            <th className="numeric-stats-table__cell" />
            <th className="numeric-stats-table__cell">Mean</th>
            <th className="numeric-stats-table__cell">Std. Dev.</th>
            <th className="numeric-stats-table__cell" />
          </tr>
        </thead>
        <tbody style={{ textAlign: "right" }}>
          {
            this.props.rows.map((row: NumericStatRow) => {
              return (
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>{ row.name }</th>
                  <td className="numeric-stats-table__cell">{ this.props.format(row.value.mean) }</td>
                  <td className="numeric-stats-table__cell">{ this.props.format(stdDev(row.value, this.props.count)) }</td>
                  <td className="numeric-stats-table__cell">{ row.bar ? row.bar() : null }</td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  }
}

class StatementDetails extends React.Component<StatementDetailsProps> {
  componentWillMount() {
    this.props.refreshQueries();
  }

  componentWillReceiveProps() {
    this.props.refreshQueries();
  }

  render() {
    return (
      <div>
        <Helmet>
          <title>
            { "Details | " + (this.props.params[appAttr] ? this.props.params[appAttr] + " App | " : "") + "Statements" }
          </title>
        </Helmet>
        <section className="section">
          <h2>Statement Details</h2>
          <Loading
            loading={_.isNil(this.props.statement)}
            className="loading-image loading-image__spinner"
            image={spinner}
          >
            { this.renderContent() }
          </Loading>
        </section>
      </div>
    );
  }

  renderContent() {
    if (!this.props.statement) {
      return null;
    }

    const { stats, statement, app, distSQL, failed } = this.props.statement;

    const count = FixLong(stats.count).toInt();
    const firstAttemptCount = FixLong(stats.first_attempt_count).toInt();

    const { firstAttemptsBarChart, retriesBarChart, maxRetriesBarChart } = countBreakdown(this.props.statement);
    const { rowsBarChart } = rowsBreakdown(this.props.statement);
    const { parseBarChart, planBarChart, runBarChart, overheadBarChart, overallBarChart } = latencyBreakdown(this.props.statement);

    return (
      <div className="content l-columns">
        <div className="l-columns__left">
          <section className="section">
            <SqlBox value={ statement } />
          </section>
          <section className="section">
            <h3>Execution Count</h3>
            <table className="numeric-stats-table">
              <tbody>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>First Attempts</th>
                  <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ firstAttemptCount }</td>
                  <td className="numeric-stats-table__cell">{ firstAttemptsBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Retries</th>
                  <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ count - firstAttemptCount }</td>
                  <td className="numeric-stats-table__cell">{ retriesBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Max Retries</th>
                  <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ FixLong(stats.max_retries).toInt() }</td>
                  <td className="numeric-stats-table__cell">{ maxRetriesBarChart() }</td>
                </tr>
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Total</th>
                  <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ count }</td>
                  <td className="numeric-stats-table__cell" />
                </tr>
              </tbody>
            </table>
          </section>
          <section className="section">
            <h3>Latency by Phase</h3>
            <NumericStatTable
              count={ count }
              format={ (v: number) => Duration(v * 1e9) }
              rows={[
                { name: "Parse", value: stats.parse_lat, bar: parseBarChart },
                { name: "Plan", value: stats.plan_lat, bar: planBarChart },
                { name: "Run", value: stats.run_lat, bar: runBarChart },
                { name: "Overhead", value: stats.overhead_lat, bar: overheadBarChart },
                { name: "Overall", value: stats.service_lat, bar: overallBarChart },
              ]}
            />
          </section>
          <section className="section">
            <h3>Row Count</h3>
            <NumericStatTable
              count={ count }
              format={ (v: number) => "" + (Math.round(v * 100) / 100) }
              rows={[
                { name: "Rows", value: stats.num_rows, bar: rowsBarChart },
              ]}
            />
          </section>
        </div>
        <div className="l-columns__right">
          <SummaryBar>
            <SummaryHeadlineStat
              title="Total Time"
              tooltip="Cumulative time spent servicing this statement."
              value={ count * stats.service_lat.mean }
              format={ v => Duration(v * 1e9) } />
            <SummaryHeadlineStat
              title="Execution Count"
              tooltip="Number of times this statement has executed."
              value={ count }
              format={ approximify } />
            <SummaryHeadlineStat
              title="Executed without Retry"
              tooltip="Portion of executions free of retries."
              value={ firstAttemptCount / count }
              format={ d3.format("%") } />
            <SummaryHeadlineStat
              title="Mean Service Latency"
              tooltip="Latency to parse, plan, and execute the statement."
              value={ stats.service_lat.mean }
              format={ v => Duration(v * 1e9) } />
            <SummaryHeadlineStat
              title="Mean Number of Rows"
              tooltip="The average number of rows returned or affected."
              value={ stats.num_rows.mean }
              format={ approximify } />
          </SummaryBar>
          <table className="numeric-stats-table">
            <tbody>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>App</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>
                  { intersperse<ReactNode>(app.map(a => <AppLink app={ a } key={ a } />), ", ") }
                </td>
              </tr>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Used DistSQL?</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ renderBools(distSQL) }</td>
              </tr>
              <tr className="numeric-stats-table__row--body">
                <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Failed?</th>
                <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ renderBools(failed) }</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}

function renderBools(bools: boolean[]) {
  if (bools.length === 0) {
    return "(unknown)";
  }
  if (bools.length === 1) {
    return bools[0] ? "Yes" : "No";
  }
  return "(both included)";
}

type QueriesState = Pick<AdminUIState, "cachedData", "queries">;

export const selectStatement = createSelector(
  (state: QueriesState) => state.cachedData.queries.data && state.cachedData.queries.data.queries,
  (_state: QueriesState, props: { params: { [key: string]: string } }) => props,
  (queries, props) => {
    if (!queries) {
      return null;
    }

    const statement = props.params[statementAttr];
    let app = props.params[appAttr];
    let predicate = (stmt: ExecutionStatistics) => stmt.statement === statement;

    if (app) {
        if (app === "(unset)") {
            app = "";
        }
        predicate = (stmt: ExecutionStatistics) => stmt.statement === statement && stmt.app === app;
    }

    const statements = flattenStatementStats(queries);
    const results = _.filter(statements, predicate);

    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      app: _.uniq(results.map(s => s.app)),
      distSQL: _.uniq(results.map(s => s.distSQL)),
      failed: _.uniq(results.map(s => s.failed)),
    };
  },
);

// tslint:disable-next-line:variable-name
const StatementDetailsConnected = connect(
  (state: AdminUIState, props: RouterState) => ({
    statement: selectStatement(state, props),
  }),
  {
    refreshQueries,
  },
)(StatementDetails);

export default StatementDetailsConnected;
