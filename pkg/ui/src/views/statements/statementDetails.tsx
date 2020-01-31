// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Icon, Row, Tabs } from "antd";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";
import { InjectedRouter, Params } from "react-router/lib/Router";
import { createSelector } from "reselect";
import { refreshStatements } from "src/redux/apiReducers";
import { nodeDisplayNameByIDSelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { combineStatementStats, ExecutionStatistics, flattenStatementStats, NumericStat, StatementStatistics, stdDev } from "src/util/appStats";
import { appAttr, implicitTxnAttr, statementAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { Duration } from "src/util/format";
import { intersperse } from "src/util/intersperse";
import { Pick } from "src/util/pick";
import Loading from "src/views/shared/components/loading";
import { SortSetting } from "src/views/shared/components/sortabletable";
import SqlBox from "src/views/shared/components/sql/box";
import { formatNumberForDisplay } from "src/views/shared/components/summaryBar";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { PlanView } from "src/views/statements/planView";
import { SummaryCard } from "../shared/components/summaryCard";
import { approximify, latencyBreakdown, longToInt, rowsBreakdown } from "./barCharts";
import { AggregateStatistics, makeNodesColumns, StatementsSortedTable } from "./statementsTable";

const { TabPane } = Tabs;

interface Fraction {
  numerator: number;
  denominator: number;
}

interface SingleStatementStatistics {
  statement: string;
  app: string[];
  distSQL: Fraction;
  opt: Fraction;
  implicit_txn: Fraction;
  failed: Fraction;
  node_id: number[];
  stats: StatementStatistics;
  byNode: AggregateStatistics[];
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
  statement: SingleStatementStatistics;
  statementsError: Error | null;
  nodeNames: { [nodeId: string]: string };
  refreshStatements: typeof refreshStatements;
  nodesSummary: NodesSummary;
  router: InjectedRouter;
}

type StatementDetailsProps = StatementDetailsOwnProps & RouterState;

interface StatementDetailsState {
  sortSetting: SortSetting;
}

interface NumericStatRow {
  name: string;
  value: NumericStat;
  bar?: () => ReactNode;
  summary?: boolean;
}

interface NumericStatTableProps {
  title?: string;
  description?: string;
  measure: string;
  rows: NumericStatRow[];
  count: number;
  format?: (v: number) => string;
}

class NumericStatTable extends React.Component<NumericStatTableProps> {
  static defaultProps = {
    format: (v: number) => `${v}`,
  };

  render() {
    const { rows } = this.props;
    return (
      <table className="sort-table statements-table">
        <thead>
          <tr className="sort-table__row sort-table__row--header">
            <th className="sort-table__cell sort-table__cell--header">Phase</th>
            <th className="sort-table__cell">Mean {this.props.measure}</th>
            <th className="sort-table__cell">Standard Deviation</th>
          </tr>
        </thead>
        <tbody>
          {
            rows.map((row: NumericStatRow) => {
              const classNames = "sort-table__row sort-table__row--body" +
                (row.summary ? " sort-table__row--summary" : "");
              return (
                <tr className={classNames}>
                  <th className="sort-table__cell sort-table__cell--header" style={{ textAlign: "left" }}>{ row.name }</th>
                  <td className="sort-table__cell">{ row.bar ? row.bar() : null }</td>
                  <td className="sort-table__cell sort-table__cell--active">{ this.props.format(stdDev(row.value, this.props.count)) }</td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  }
}

export class StatementDetails extends React.Component<StatementDetailsProps, StatementDetailsState> {

  constructor(props: StatementDetailsProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 5,  // Latency
        ascending: false,
      },
    };
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  componentWillMount() {
    this.props.refreshStatements();
  }

  componentWillReceiveProps() {
    this.props.refreshStatements();
  }

  prevPage = () => this.props.router.goBack();

  render() {
    return (
      <div>
        <Helmet title={ "Details | " + (this.props.params[appAttr] ? this.props.params[appAttr] + " App | " : "") + "Statements" } />
        <div className="section page--header">
          <div className="page--header__back-btn">
            <Icon type="arrow-left" /> <a onClick={this.prevPage}>Statements</a>
          </div>
          <h1 className="page--header__title">Statement Details</h1>
        </div>
        <section className="section section--container">
          <Loading
            loading={_.isNil(this.props.statement)}
            error={this.props.statementsError}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }

  renderContent = () => {
    if (!this.props.statement) {
      return null;
    }
    const { stats, statement, app, opt, failed, implicit_txn } = this.props.statement;

    if (!stats) {
      const sourceApp = this.props.params[appAttr];
      const listUrl = "/statements" + (sourceApp ? "/" + sourceApp : "");

      return (
        <React.Fragment>
          <section className="section">
            <SqlBox value={ statement } />
          </section>
          <section className="section">
            <h3>Unable to find statement</h3>
            There are no execution statistics for this statement.{" "}
            <Link className="back-link" to={ listUrl }>
              Back to Statements
            </Link>
          </section>
        </React.Fragment>
      );
    }

    const count = FixLong(stats.count).toInt();

    const { rowsBarChart } = rowsBreakdown(this.props.statement);
    const { parseBarChart, planBarChart, runBarChart, overheadBarChart, overallBarChart } = latencyBreakdown(this.props.statement);

    const totalCountBarChart = longToInt(this.props.statement.stats.count);
    const firstAttemptsBarChart = longToInt(this.props.statement.stats.first_attempt_count);
    const retriesBarChart = totalCountBarChart - firstAttemptsBarChart;
    const maxRetriesBarChart = longToInt(this.props.statement.stats.max_retries);

    const statsByNode = this.props.statement.byNode;
    const logicalPlan = stats.sensitive_info && stats.sensitive_info.most_recent_plan_description;
    const duration = (v: number) => Duration(v * 1e9);
    return (
      <Tabs defaultActiveKey="1" className="cockroach--tabs">
        <TabPane tab="Overview" key="1">
          <Row gutter={16}>
            <Col className="gutter-row" span={16}>
              <SqlBox value={ statement } />
            </Col>
            <Col className="gutter-row" span={8}>
              <SummaryCard>
                <Row>
                  <Col span={12}>
                    <div className="summary--card__counting">
                      <h3 className="summary--card__counting--value">{formatNumberForDisplay(count * stats.service_lat.mean, duration)}</h3>
                      <p className="summary--card__counting--label">Total Time</p>
                    </div>
                  </Col>
                  <Col span={12}>
                    <div className="summary--card__counting">
                      <h3 className="summary--card__counting--value">{formatNumberForDisplay(stats.service_lat.mean, duration)}</h3>
                      <p className="summary--card__counting--label">Mean Service Latency</p>
                    </div>
                  </Col>
                </Row>
                <p className="summary--card__divider"></p>
                <div className="summary--card__item" style={{ justifyContent: "flex-start" }}>
                  <h4 className="summary--card__item--label">App:</h4>
                  <p className="summary--card__item--value">{ intersperse<ReactNode>(app.map(a => <AppLink app={ a } key={ a } />), ", ") }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Transaction Type</h4>
                  <p className="summary--card__item--value">{ renderTransactionType(implicit_txn) }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Distributed execution?</h4>
                  <p className="summary--card__item--value">{ renderBools(opt) }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Used cost-based optimizer?</h4>
                  <p className="summary--card__item--value">{ renderBools(opt) }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Failed?</h4>
                  <p className="summary--card__item--value">{ renderBools(failed) }</p>
                </div>
              </SummaryCard>
              <SummaryCard>
                <h2 className="summary--card__title">Execution Count</h2>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">First Attempts</h4>
                  <p className="summary--card__item--value">{ firstAttemptsBarChart }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Retries</h4>
                  <p className="summary--card__item--value summary--card__item--value-red">{ retriesBarChart }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Max Retries</h4>
                  <p className="summary--card__item--value summary--card__item--value-red">{ maxRetriesBarChart }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Total</h4>
                  <p className="summary--card__item--value">{ totalCountBarChart }</p>
                </div>
                <p className="summary--card__divider"></p>
                <h2 className="summary--card__title">Rows Affected</h2>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Mean Rows</h4>
                  <p className="summary--card__item--value">{ rowsBarChart(true) }</p>
                </div>
                <div className="summary--card__item">
                  <h4 className="summary--card__item--label">Standard Deviation</h4>
                  <p className="summary--card__item--value">{ rowsBarChart() }</p>
                </div>
              </SummaryCard>
            </Col>
          </Row>
        </TabPane>
        <TabPane tab="Logical Plan" key="2">
          <SummaryCard>
            <PlanView
              title="Logical Plan"
              plan={logicalPlan}
            />
          </SummaryCard>
        </TabPane>
        <TabPane tab="Execution Stats" key="3">
          <SummaryCard>
            <h2 className="summary--card__title">
              Execution Latency By Phase
              <div className="numeric-stats-table__tooltip">
                <ToolTipWrapper text="The execution latency of this statement, broken down by phase.">
                  <div className="numeric-stats-table__tooltip-hover-area">
                    <div className="numeric-stats-table__info-icon">i</div>
                  </div>
                </ToolTipWrapper>
              </div>
            </h2>
            <NumericStatTable
              title="Phase"
              measure="Latency"
              count={ count }
              format={ (v: number) => Duration(v * 1e9) }
              rows={[
                { name: "Parse", value: stats.parse_lat, bar: parseBarChart },
                { name: "Plan", value: stats.plan_lat, bar: planBarChart },
                { name: "Run", value: stats.run_lat, bar: runBarChart },
                { name: "Overhead", value: stats.overhead_lat, bar: overheadBarChart },
                { name: "Overall", summary: true, value: stats.service_lat, bar: overallBarChart },
              ]}
            />
          </SummaryCard>
          <SummaryCard>
            <h2 className="summary--card__title">
              Stats By Node
              <div className="numeric-stats-table__tooltip">
                <ToolTipWrapper text="text">
                  <div className="numeric-stats-table__tooltip-hover-area">
                    <div className="numeric-stats-table__info-icon">i</div>
                  </div>
                </ToolTipWrapper>
              </div>
            </h2>
            <StatementsSortedTable
              className="statements-table"
              data={statsByNode}
              columns={makeNodesColumns(statsByNode, this.props.nodeNames)}
              sortSetting={this.state.sortSetting}
              onChangeSortSetting={this.changeSortSetting}
              firstCellBordered
            />
          </SummaryCard>
        </TabPane>
      </Tabs>
    );
  }
}

function renderTransactionType(implicitTxn: Fraction) {
  if (Number.isNaN(implicitTxn.numerator)) {
    return "(unknown)";
  }
  if (implicitTxn.numerator === 0) {
    return "Explicit";
  }
  if (implicitTxn.numerator === implicitTxn.denominator) {
    return "Implicit";
  }
  const fraction = approximify(implicitTxn.numerator) + " of " + approximify(implicitTxn.denominator);
  return `${fraction} were Implicit Txns`;
}

function renderBools(fraction: Fraction) {
  if (Number.isNaN(fraction.numerator)) {
    return "(unknown)";
  }
  if (fraction.numerator === 0) {
    return "No";
  }
  if (fraction.numerator === fraction.denominator) {
    return "Yes";
  }
  return approximify(fraction.numerator) + " of " + approximify(fraction.denominator);
}

type StatementsState = Pick<AdminUIState, "cachedData", "statements">;

interface StatementDetailsData {
  nodeId: number;
  implicitTxn: boolean;
  stats: StatementStatistics[];
}

function keyByNodeAndImplicitTxn(stmt: ExecutionStatistics): string {
  return stmt.node_id.toString() + stmt.implicit_txn;
}

function coalesceNodeStats(stats: ExecutionStatistics[]): AggregateStatistics[] {
  const byNodeAndImplicitTxn: { [nodeId: string]: StatementDetailsData } = {};

  stats.forEach(stmt => {
    const key = keyByNodeAndImplicitTxn(stmt);
    if (!(key in byNodeAndImplicitTxn)) {
      byNodeAndImplicitTxn[key] = {
        nodeId: stmt.node_id,
        implicitTxn: stmt.implicit_txn,
        stats: [],
      };
    }
    byNodeAndImplicitTxn[key].stats.push(stmt.stats);
  });

  return Object.keys(byNodeAndImplicitTxn).map(key => {
    const stmt = byNodeAndImplicitTxn[key];
    return {
      label: stmt.nodeId.toString(),
      implicitTxn: stmt.implicitTxn,
      stats: combineStatementStats(stmt.stats),
    };
  });
}

function fractionMatching(stats: ExecutionStatistics[], predicate: (stmt: ExecutionStatistics) => boolean): Fraction {
  let numerator = 0;
  let denominator = 0;

  stats.forEach(stmt => {
    const count = FixLong(stmt.stats.first_attempt_count).toInt();
    denominator += count;
    if (predicate(stmt)) {
      numerator += count;
    }
  });

  return { numerator, denominator };
}

function filterByRouterParamsPredicate(params: Params): (stat: ExecutionStatistics) => boolean {
  const statement = params[statementAttr];
  const implicitTxn = (params[implicitTxnAttr] === "true");
  let app = params[appAttr];

  if (!app) {
    return (stmt: ExecutionStatistics) => stmt.statement === statement && stmt.implicit_txn === implicitTxn;
  }

  if (app === "(unset)") {
    app = "";
  }
  return (stmt: ExecutionStatistics) => stmt.statement === statement && stmt.implicit_txn === implicitTxn && stmt.app === app;
}

export const selectStatement = createSelector(
  (state: StatementsState) => state.cachedData.statements.data && state.cachedData.statements.data.statements,
  (_state: StatementsState, props: { params: { [key: string]: string } }) => props,
  (statements, props) => {
    if (!statements) {
      return null;
    }

    const flattened = flattenStatementStats(statements);
    const results = _.filter(flattened, filterByRouterParamsPredicate(props.params));

    const statement = props.params[statementAttr];
    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(results.map(s => s.app)),
      distSQL: fractionMatching(results, s => s.distSQL),
      opt: fractionMatching(results, s => s.opt),
      implicit_txn: fractionMatching(results, s => s.implicit_txn),
      failed: fractionMatching(results, s => s.failed),
      node_id: _.uniq(results.map(s => s.node_id)),
    };
  },
);

const mapStateToProps = (state: AdminUIState, props: RouterState) => ({
  statement: selectStatement(state, props),
  statementsError: state.cachedData.statements.lastError,
  nodeNames: nodeDisplayNameByIDSelector(state),
});

const mapDispatchToProps = {
  refreshStatements,
};

// tslint:disable-next-line:variable-name
const StatementDetailsConnected = connect(
  mapStateToProps,
  mapDispatchToProps,
)(StatementDetails);

export default StatementDetailsConnected;
