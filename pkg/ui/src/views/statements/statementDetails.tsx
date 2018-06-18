import d3 from "d3";
import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";
import { createSelector } from "reselect";

import { refreshQueries } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { statementAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { Duration } from "src/util/format";
import { SqlBox } from "src/views/shared/components/sql/box";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";
import * as protos from "src/js/protos";

import { countBarChart, latencyBarChart } from "./barCharts";

type StatementStatistics = protos.cockroach.sql.CollectedStatementStatistics$Properties;

interface StatementDetailsOwnProps {
  statement: StatementStatistics;
  refreshQueries: typeof refreshQueries;
}

type StatementDetailsProps = StatementDetailsOwnProps & RouterState;

interface NumericStat {
  mean?: number;
  squared_diffs?: number;
}

function variance(stat: NumericStat, count: number) {
  return stat.squared_diffs / (count - 1);
}

interface NumericStatRow {
  name: string;
  value: NumericStat;
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
            <th className="numeric-stats-table__cell">Variance</th>
          </tr>
        </thead>
        <tbody style={{ textAlign: "right" }}>
          {
            this.props.rows.map((row: NumericStatRow) => {
              return (
                <tr className="numeric-stats-table__row--body">
                  <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>{ row.name }</th>
                  <td className="numeric-stats-table__cell">{ this.props.format(row.value.mean) }</td>
                  <td className="numeric-stats-table__cell">{ this.props.format(variance(row.value, this.props.count)) }</td>
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
    if (!this.props.statement) {
      return "loading...";
    }

    const { stats } = this.props.statement;

    const count = FixLong(stats.count).toInt();
    const firstAttemptCount = FixLong(stats.first_attempt_count).toInt();

    const countBar = countBarChart();
    const latencyBar = latencyBarChart();

    return (
      <div>
        <Helmet>
          <title>{`Details | Statements`}</title>
        </Helmet>
        <section className="section">
          <h2>Statement Details</h2>
          <div className="content l-columns">
            <div className="l-columns__left">
              <SqlBox value={ this.props.statement.key.query } />
              <section className="section">
                <h3>Execution Count</h3>
                <div className="details-bar">{ countBar(this.props.statement) }</div>
                <table className="numeric-stats-table">
                  <tbody>
                    <tr className="numeric-stats-table__row--body">
                      <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>First Attempt Count</th>
                      <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ firstAttemptCount }</td>
                    </tr>
                    <tr className="numeric-stats-table__row--body">
                      <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Total Count</th>
                      <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ count }</td>
                    </tr>
                    <tr className="numeric-stats-table__row--body">
                      <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Cumulative Retries</th>
                      <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ count - firstAttemptCount }</td>
                    </tr>
                    <tr className="numeric-stats-table__row--body">
                      <th className="numeric-stats-table__cell" style={{ textAlign: "left" }}>Max Retries</th>
                      <td className="numeric-stats-table__cell" style={{ textAlign: "right" }}>{ FixLong(stats.max_retries).toInt() }</td>
                    </tr>
                  </tbody>
                </table>
              </section>
              <section className="section">
                <h3>Latency by Phase</h3>
                <div className="details-bar">{ latencyBar(this.props.statement) }</div>
                <NumericStatTable
                  count={ count }
                  format={ (v: number) => Duration(v * 1e9) }
                  rows={[
                    { name: "Overall", value: stats.service_lat },
                    { name: "Parse", value: stats.parse_lat },
                    { name: "Plan", value: stats.plan_lat },
                    { name: "Run", value: stats.run_lat },
                    { name: "Overhead", value: stats.overhead_lat },
                  ]}
                />
              </section>
              <section className="section">
                <h3>Row Count</h3>
                <NumericStatTable
                  count={ count }
                  format={ (v: number) => "" + Math.round(v) }
                  rows={[
                    { name: "Rows", value: stats.num_rows },
                  ]}
                />
              </section>
            </div>
            <div className="l-columns__right">
              <SummaryBar>
                <SummaryHeadlineStat
                  title="Execution Count"
                  tooltip="Number of times this statement has executed."
                  value={ count } />
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
                  value={ Math.round(stats.num_rows.mean) } />
              </SummaryBar>
            </div>
          </div>
        </section>
      </div>
    );
  }
}

const selectStatement = createSelector(
  (state: AdminUIState) => state.cachedData.queries.data && state.cachedData.queries.data.queries,
  (_state: AdminUIState, props: RouterState) => props.params[statementAttr],
  (haystack, needle) => haystack && _.find(haystack, hay => hay.key.query === needle),
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
