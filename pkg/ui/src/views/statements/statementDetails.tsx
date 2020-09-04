// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Row, Tabs } from "antd";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps, match as Match, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { refreshStatementDiagnosticsRequests, refreshStatements } from "src/redux/apiReducers";
import { nodeDisplayNameByIDSelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import {
  combineStatementStats,
  ExecutionStatistics,
  flattenStatementStats,
  NumericStat,
  StatementStatistics,
  stdDev,
} from "src/util/appStats";
import { appAttr, implicitTxnAttr, statementAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import {Bytes, Duration} from "src/util/format";
import { intersperse } from "src/util/intersperse";
import { Pick } from "src/util/pick";
import Loading from "src/views/shared/components/loading";
import { SortSetting } from "src/views/shared/components/sortabletable";
import SqlBox from "src/views/shared/components/sql/box";
import { formatNumberForDisplay } from "src/views/shared/components/summaryBar";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { PlanView } from "src/views/statements/planView";
import { SummaryCard } from "../shared/components/summaryCard";
import { approximify, latencyBreakdown, genericBarChart, longToInt, rowsBreakdown } from "./barCharts";
import { AggregateStatistics, makeNodesColumns, StatementsSortedTable } from "./statementsTable";
import { getMatchParamByName } from "src/util/query";
import DiagnosticsView from "./diagnostics";
import classNames from "classnames/bind";
import {
  selectDiagnosticsReportsCountByStatementFingerprint,
} from "src/redux/statements/statementsSelectors";
import { Button } from "src/components/button";
import { BackIcon } from "src/components/icon";
import { trackSubnavSelection } from "src/util/analytics";
import styles from "./statementDetails.module.styl";
import sortableTableStyles from "src/views/shared/components/sortabletable/sortabletable.module.styl";
import summaryCardStyles from "src/views/shared/components/summaryCard/summaryCard.module.styl";
import d3 from "d3";

const { TabPane } = Tabs;

interface Fraction {
  numerator: number;
  denominator: number;
}

interface SingleStatementStatistics {
  statement: string;
  app: string[];
  distSQL: Fraction;
  vec: Fraction;
  opt: Fraction;
  implicit_txn: Fraction;
  failed: Fraction;
  node_id: number[];
  stats: StatementStatistics;
  byNode: AggregateStatistics[];
}

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

function AppLink(props: { app: string }) {
  if (!props.app) {
    return (
      <span className={cx("app-name", "app-name__unset")}>
        (unset)
      </span>
    );
  }

  return (
    <Link
      className={cx("app-name")}
      to={ `/statements/${encodeURIComponent(props.app)}` }
    >
      { props.app }
    </Link>
  );
}

interface StatementDetailsOwnProps {
  statement: SingleStatementStatistics;
  statementsError: Error | null;
  nodeNames: { [nodeId: string]: string };
  refreshStatements: typeof refreshStatements;
  refreshStatementDiagnosticsRequests: typeof refreshStatementDiagnosticsRequests;
  nodesSummary: NodesSummary;
  diagnosticsCount: number;
}

export type StatementDetailsProps = StatementDetailsOwnProps & RouteComponentProps;

interface StatementDetailsState {
  sortSetting: SortSetting;
  currentTab?: string;
}

interface NumericStatRow {
  name: string;
  value: NumericStat;
  bar?: () => ReactNode;
  summary?: boolean;
  // You can override the table's formatter on a per-row basis with this format
  // method.
  format?: (v: number) => string;
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
      <table className={classNames(sortableTableCx("sort-table"), cx("statements-table"))}>
        <thead>
          <tr className={sortableTableCx("sort-table__row", "sort-table__row--header")}>
            <th className={sortableTableCx("sort-table__cell", "sort-table__cell--header")}>
              {this.props.title}
            </th>
            <th className={sortableTableCx("sort-table__cell")}>
              Mean {this.props.measure}
            </th>
            <th className={sortableTableCx("sort-table__cell")}>
              Standard Deviation
            </th>
          </tr>
        </thead>
        <tbody>
          {
            rows.map((row: NumericStatRow) => {
              let { format } = this.props;
              if (row.format) {
                format = row.format;
              }
              const className = sortableTableCx(
                "sort-table__row",
                "sort-table__row--body",
                {
                  "sort-table__row--summary": row.summary,
                },
              );
              return (
                <tr className={className}>
                  <th
                    className={sortableTableCx("sort-table__cell", "sort-table__cell--header")}
                    style={{ textAlign: "left" }}
                  >
                    { row.name }
                  </th>
                  <td className={sortableTableCx("sort-table__cell")}>
                    { row.bar ? row.bar() : null }
                  </td>
                  <td className={sortableTableCx("sort-table__cell", "sort-table__cell--active")}>
                    { format(stdDev(row.value, this.props.count)) }
                  </td>
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
    const searchParams = new URLSearchParams(props.history.location.search);
    this.state = {
      sortSetting: {
        sortKey: 5,  // Latency
        ascending: false,
      },
      currentTab: searchParams.get("tab") || "overview",
    };
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  componentDidMount() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  }

  componentDidUpdate() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  }

  prevPage = () => this.props.history.goBack();

  onTabChange = (tabId: string) => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("tab", tabId);
    trackSubnavSelection(tabId);
    history.replace({
      ...history.location,
      search: searchParams.toString(),
    });
    this.setState({
      currentTab: tabId,
    });
  }

  render() {
    const app = getMatchParamByName(this.props.match, appAttr);
    return (
      <div>
        <Helmet title={`Details | ${(app ? `${app} App |` : "")} Statements`} />
        <div className={cx("section", "page--header")}>
          <Button
            onClick={this.prevPage}
            type="unstyled-link"
            size="small"
            icon={BackIcon}
            iconPosition="left"
          >
            Statements
          </Button>
          <h1 className={cx("base-heading", "page--header__title")}>
            Statement Details
          </h1>
        </div>
        <section className={cx("section", "section--container")}>
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
    const { diagnosticsCount } = this.props;
    const { currentTab } = this.state;

    if (!this.props.statement) {
      return null;
    }
    const { stats, statement, app, distSQL, vec, opt, failed, implicit_txn } = this.props.statement;

    if (!stats) {
      const sourceApp = getMatchParamByName(this.props.match, appAttr);
      const listUrl = "/statements" + (sourceApp ? "/" + sourceApp : "");

      return (
        <React.Fragment>
          <section className={cx("section")}>
            <SqlBox value={ statement } />
          </section>
          <section className={cx("section")}>
            <h3>Unable to find statement</h3>
            There are no execution statistics for this statement.{" "}
            <Link className={cx("back-link")} to={ listUrl }>
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
      <Tabs
        defaultActiveKey="1"
        className={cx("cockroach--tabs")}
        onChange={this.onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Overview" key="overview">
          <Row gutter={16}>
            <Col className="gutter-row" span={16}>
              <SqlBox value={ statement } />
            </Col>
            <Col className="gutter-row" span={8}>
              <SummaryCard>
                <Row>
                  <Col span={12}>
                    <div className={summaryCardStylesCx("summary--card__counting")}>
                      <h3 className={summaryCardStylesCx("summary--card__counting--value")}>
                        {formatNumberForDisplay(count * stats.service_lat.mean, duration)}
                      </h3>
                      <p className={summaryCardStylesCx("summary--card__counting--label")}>Total Time</p>
                    </div>
                  </Col>
                  <Col span={12}>
                    <div className={summaryCardStylesCx("summary--card__counting")}>
                      <h3 className={summaryCardStylesCx("summary--card__counting--value")}>
                        {formatNumberForDisplay(stats.service_lat.mean, duration)}
                      </h3>
                      <p className={summaryCardStylesCx("summary--card__counting--label")}>Mean Service Latency</p>
                    </div>
                  </Col>
                </Row>
                <p className={summaryCardStylesCx("summary--card__divider")} />
                <div
                  className={summaryCardStylesCx("summary--card__item")}
                  style={{ justifyContent: "flex-start" }}
                >
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>App:</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>
                    { intersperse<ReactNode>(app.map(a => <AppLink app={ a } key={ a } />), ", ") }
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Transaction Type</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ renderTransactionType(implicit_txn) }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Distributed execution?</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ renderBools(distSQL) }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Vectorized execution?</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ renderBools(vec) }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Used cost-based optimizer?</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ renderBools(opt) }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Failed?</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ renderBools(failed) }</p>
                </div>
              </SummaryCard>
              <SummaryCard>
                <h2
                  className={classNames(
                    cx("base-heading"),
                    summaryCardStylesCx("summary--card__title"),
                  )}
                >
                  Execution Count
                </h2>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>First Attempts</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ firstAttemptsBarChart }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Retries</h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red": retriesBarChart > 0,
                      },
                    )}
                  >
                    { retriesBarChart }
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Max Retries</h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red": maxRetriesBarChart > 0,
                      },
                    )}
                  >
                    { maxRetriesBarChart }
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Total</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ totalCountBarChart }</p>
                </div>
                <p className={summaryCardStylesCx("summary--card__divider")} />
                <h2
                  className={classNames(
                    cx("base-heading"),
                    summaryCardStylesCx("summary--card__title"),
                  )}
                >
                  Rows Affected
                </h2>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Mean Rows</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ rowsBarChart(true) }</p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4 className={summaryCardStylesCx("summary--card__item--label")}>Standard Deviation</h4>
                  <p className={summaryCardStylesCx("summary--card__item--value")}>{ rowsBarChart() }</p>
                </div>
              </SummaryCard>
            </Col>
          </Row>
        </TabPane>
        <TabPane tab={`Diagnostics ${diagnosticsCount > 0 ? `(${diagnosticsCount})` : ""}`} key="diagnostics">
          <DiagnosticsView statementFingerprint={statement} />
        </TabPane>
        <TabPane tab="Logical Plan" key="logical-plan">
          <SummaryCard>
            <PlanView
              title="Logical Plan"
              plan={logicalPlan}
            />
          </SummaryCard>
        </TabPane>
        <TabPane tab="Execution Stats" key="execution-stats">
          <SummaryCard>
            <h2
              className={classNames(
                cx("base-heading"),
                summaryCardStylesCx("summary--card__title"),
              )}
            >
              Execution Latency By Phase
              <div className={cx("numeric-stats-table__tooltip")}>
                <ToolTipWrapper text="The execution latency of this statement, broken down by phase.">
                  <div className={cx("numeric-stats-table__tooltip-hover-area")}>
                    <div className={cx("numeric-stats-table__info-icon")}>i</div>
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
            <h2
              className={classNames(
                cx("base-heading"),
                summaryCardStylesCx("summary--card__title"),
              )}
            >
              Other Execution Statistics
            </h2>
            <NumericStatTable
              title="Stat"
              measure="Quantity"
              count={ count }
              format={ d3.format(".2f") }
              rows={[
                { name: "Rows Read", value: stats.rows_read, bar: genericBarChart(stats.rows_read, stats.count)},
                { name: "Disk Bytes Read", value: stats.bytes_read, bar: genericBarChart(stats.bytes_read, stats.count, Bytes),
                  format: Bytes,
                },
              ].filter(r => r.value)}
            />
          </SummaryCard>
          <SummaryCard>
            <h2
              className={classNames(
                cx("base-heading"),
                summaryCardStylesCx("summary--card__title"),
              )}
            >
              Stats By Node
              <div className={cx("numeric-stats-table__tooltip")}>
                <ToolTipWrapper text="Execution statistics for this statement per gateway node.">
                  <div className={cx("numeric-stats-table__tooltip-hover-area")}>
                    <div className={cx("numeric-stats-table__info-icon")}>i</div>
                  </div>
                </ToolTipWrapper>
              </div>
            </h2>
            <StatementsSortedTable
              className={cx("statements-table")}
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

function filterByRouterParamsPredicate(match: Match<any>, internalAppNamePrefix: string): (stat: ExecutionStatistics) => boolean {
  const statement = getMatchParamByName(match, statementAttr);
  const implicitTxn = (getMatchParamByName(match, implicitTxnAttr) === "true");
  let app = getMatchParamByName(match, appAttr);

  const filterByStatementAndImplicitTxn = (stmt: ExecutionStatistics) =>
    stmt.statement === statement && stmt.implicit_txn === implicitTxn;

  if (!app) {
    return filterByStatementAndImplicitTxn;
  }

  if (app === "(unset)") {
    app = "";
  }

  if (app === "(internal)") {
    return (stmt: ExecutionStatistics) =>
      filterByStatementAndImplicitTxn(stmt) && stmt.app.startsWith(internalAppNamePrefix);
  }

  return (stmt: ExecutionStatistics) =>
    filterByStatementAndImplicitTxn(stmt) && stmt.app === app;
}

export const selectStatement = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (_state: StatementsState, props: RouteComponentProps) => props,
  (statementsState, props) => {
    const statements = statementsState.data?.statements;
    if (!statements) {
      return null;
    }

    const internalAppNamePrefix = statementsState.data?.internal_app_name_prefix;
    const flattened = flattenStatementStats(statements);
    const results = _.filter(flattened, filterByRouterParamsPredicate(props.match, internalAppNamePrefix));
    const statement = getMatchParamByName(props.match, statementAttr);
    return {
      statement,
      stats: combineStatementStats(results.map(s => s.stats)),
      byNode: coalesceNodeStats(results),
      app: _.uniq(results.map(s => s.app)),
      distSQL: fractionMatching(results, s => s.distSQL),
      vec: fractionMatching(results, s => s.vec),
      opt: fractionMatching(results, s => s.opt),
      implicit_txn: fractionMatching(results, s => s.implicit_txn),
      failed: fractionMatching(results, s => s.failed),
      node_id: _.uniq(results.map(s => s.node_id)),
    };
  },
);

const mapStateToProps = (state: AdminUIState, props: StatementDetailsProps) => {
  const statement = selectStatement(state, props);
  return {
    statement,
    statementsError: state.cachedData.statements.lastError,
    nodeNames: nodeDisplayNameByIDSelector(state),
    diagnosticsCount: selectDiagnosticsReportsCountByStatementFingerprint(state, statement?.statement),
  };
};

const mapDispatchToProps = {
  refreshStatements,
  refreshStatementDiagnosticsRequests,
};

// tslint:disable-next-line:variable-name
const StatementDetailsConnected = withRouter(connect(
  mapStateToProps,
  mapDispatchToProps,
)(StatementDetails));

export default StatementDetailsConnected;
