import { Col, Row, Tabs } from "antd";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { format as d3Format } from "d3-format";
import { ArrowLeft } from "@cockroachlabs/icons";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import {
  intersperse,
  Bytes,
  Duration,
  FixLong,
  appAttr,
  NumericStat,
  StatementStatistics,
  stdDev,
  getMatchParamByName,
  formatNumberForDisplay,
} from "src/util";
import { Loading } from "src/loading";
import { Button } from "src/button";
import { SqlBox } from "src/sql";
import { SortSetting } from "src/sortedtable";
import { Tooltip } from "src/tooltip";
import { PlanView } from "./planView";
import { SummaryCard } from "src/summaryCard";
import {
  approximify,
  latencyBreakdown,
  genericBarChart,
  longToInt,
  rowsBreakdown,
} from "src/barCharts";
import {
  AggregateStatistics,
  makeNodesColumns,
  StatementsSortedTable,
} from "src/statementsTable";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import styles from "./statementDetails.module.scss";
import { NodeSummaryStats } from "../nodes";
import { UIConfigState } from "../store/uiConfig";

const { TabPane } = Tabs;

export interface Fraction {
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

export type StatementDetailsProps = StatementDetailsOwnProps &
  RouteComponentProps;

export interface StatementDetailsState {
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

export type NodesSummary = {
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[];
  nodeIDs: string[];
  nodeStatusByID: Dictionary<cockroach.server.status.statuspb.INodeStatus>;
  nodeSums: NodeSummaryStats;
  nodeDisplayNameByID: Dictionary<string>;
  livenessStatusByNodeID: Dictionary<
    cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
  >;
  livenessByNodeID: Dictionary<
    cockroach.kv.kvserver.liveness.livenesspb.ILiveness
  >;
  storeIDsByNodeID: Dictionary<string[]>;
};

export interface StatementDetailsDispatchProps {
  refreshStatements: () => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshNodes: () => void;
  refreshNodesLiveness: () => void;
  createStatementDiagnosticsReport: (statementFingerprint: string) => void;
  dismissStatementDiagnosticsAlertMessage?: () => void;
  onTabChanged?: (tabName: string) => void;
  onDiagnosticBundleDownload?: (statementFingerprint: string) => void;
}

export interface StatementDetailsStateProps {
  statement: SingleStatementStatistics;
  statementsError: Error | null;
  nodeNames: { [nodeId: string]: string };
  diagnosticsReports: cockroach.server.serverpb.IStatementDiagnosticsReport[];
  uiConfig: UIConfigState["pages"]["statementDetails"];
}

export type StatementDetailsOwnProps = StatementDetailsDispatchProps &
  StatementDetailsStateProps;

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortedTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

function AppLink(props: { app: string }) {
  if (!props.app) {
    return <span className={cx("app-name", "app-name__unset")}>(unset)</span>;
  }

  return (
    <Link
      className={cx("app-name")}
      to={`/statements/${encodeURIComponent(props.app)}`}
    >
      {props.app}
    </Link>
  );
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
  const fraction =
    approximify(implicitTxn.numerator) +
    " of " +
    approximify(implicitTxn.denominator);
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
  return (
    approximify(fraction.numerator) + " of " + approximify(fraction.denominator)
  );
}

class NumericStatTable extends React.Component<NumericStatTableProps> {
  static defaultProps = {
    format: (v: number) => `${v}`,
  };

  render() {
    const { rows } = this.props;
    return (
      <table
        className={classNames(
          sortableTableCx("sort-table"),
          cx("statements-table"),
        )}
      >
        <thead>
          <tr
            className={sortableTableCx(
              "sort-table__row",
              "sort-table__row--header",
            )}
          >
            <th
              className={sortableTableCx(
                "sort-table__cell",
                "sort-table__cell--header",
              )}
            >
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
          {rows.map((row: NumericStatRow, idx) => {
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
              <tr className={className} key={idx}>
                <th
                  className={sortableTableCx(
                    "sort-table__cell",
                    "sort-table__cell--header",
                  )}
                  style={{ textAlign: "left" }}
                >
                  {row.name}
                </th>
                <td className={sortableTableCx("sort-table__cell")}>
                  {row.bar ? row.bar() : null}
                </td>
                <td
                  className={sortableTableCx(
                    "sort-table__cell",
                    "sort-table__cell--active",
                  )}
                >
                  {format(stdDev(row.value, this.props.count))}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  }
}

export class StatementDetails extends React.Component<
  StatementDetailsProps,
  StatementDetailsState
> {
  constructor(props: StatementDetailsProps) {
    super(props);
    const searchParams = new URLSearchParams(props.history.location.search);
    this.state = {
      sortSetting: {
        sortKey: 5, // Latency
        ascending: false,
      },
      currentTab: searchParams.get("tab") || "overview",
    };
  }

  static defaultProps: Partial<StatementDetailsProps> = {
    onDiagnosticBundleDownload: _.noop,
  };

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  };

  componentDidMount() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
    this.props.refreshNodes();
    this.props.refreshNodesLiveness();
  }

  componentDidUpdate() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
    this.props.refreshNodes();
    this.props.refreshNodesLiveness();
  }

  onTabChange = (tabId: string) => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("tab", tabId);
    history.replace({
      ...history.location,
      search: searchParams.toString(),
    });
    this.setState({
      currentTab: tabId,
    });
    this.props.onTabChanged && this.props.onTabChanged(tabId);
  };

  render() {
    const app = getMatchParamByName(this.props.match, appAttr);
    return (
      <div>
        <Helmet title={`Details | ${app ? `${app} App |` : ""} Statements`} />
        <div className={cx("section", "page--header")}>
          <Button
            onClick={() => this.props.history.push("/statements")}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
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
    const {
      createStatementDiagnosticsReport,
      diagnosticsReports,
      dismissStatementDiagnosticsAlertMessage,
      onDiagnosticBundleDownload,
    } = this.props;
    const { currentTab } = this.state;

    if (!this.props.statement) {
      return null;
    }
    const {
      stats,
      statement,
      app,
      distSQL,
      vec,
      opt,
      failed,
      implicit_txn,
    } = this.props.statement;

    if (!stats) {
      const sourceApp = getMatchParamByName(this.props.match, appAttr);
      const listUrl = "/statements" + (sourceApp ? "/" + sourceApp : "");

      return (
        <React.Fragment>
          <section className={cx("section")}>
            <SqlBox value={statement} />
          </section>
          <section className={cx("section")}>
            <h3>Unable to find statement</h3>
            There are no execution statistics for this statement.{" "}
            <Link className={cx("back-link")} to={listUrl}>
              Back to Statements
            </Link>
          </section>
        </React.Fragment>
      );
    }

    const count = FixLong(stats.count).toInt();

    const { rowsBarChart } = rowsBreakdown(this.props.statement);
    const {
      parseBarChart,
      planBarChart,
      runBarChart,
      overheadBarChart,
      overallBarChart,
    } = latencyBreakdown(this.props.statement);

    const totalCountBarChart = longToInt(this.props.statement.stats.count);
    const firstAttemptsBarChart = longToInt(
      this.props.statement.stats.first_attempt_count,
    );
    const retriesBarChart = totalCountBarChart - firstAttemptsBarChart;
    const maxRetriesBarChart = longToInt(
      this.props.statement.stats.max_retries,
    );

    const statsByNode = this.props.statement.byNode;
    const logicalPlan =
      stats.sensitive_info && stats.sensitive_info.most_recent_plan_description;
    const duration = (v: number) => Duration(v * 1e9);
    const hasDiagnosticReports = diagnosticsReports.length > 0;
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
              <SqlBox value={statement} />
            </Col>
            <Col className="gutter-row" span={8}>
              <SummaryCard>
                <Row>
                  <Col span={12}>
                    <div
                      className={summaryCardStylesCx("summary--card__counting")}
                    >
                      <h3
                        className={summaryCardStylesCx(
                          "summary--card__counting--value",
                        )}
                      >
                        {formatNumberForDisplay(
                          count * stats.service_lat.mean,
                          duration,
                        )}
                      </h3>
                      <p
                        className={summaryCardStylesCx(
                          "summary--card__counting--label",
                        )}
                      >
                        Total Time
                      </p>
                    </div>
                  </Col>
                  <Col span={12}>
                    <div
                      className={summaryCardStylesCx("summary--card__counting")}
                    >
                      <h3
                        className={summaryCardStylesCx(
                          "summary--card__counting--value",
                        )}
                      >
                        {formatNumberForDisplay(
                          stats.service_lat.mean,
                          duration,
                        )}
                      </h3>
                      <p
                        className={summaryCardStylesCx(
                          "summary--card__counting--label",
                        )}
                      >
                        Mean Service Latency
                      </p>
                    </div>
                  </Col>
                </Row>
                <p className={summaryCardStylesCx("summary--card__divider")} />
                <div
                  className={summaryCardStylesCx("summary--card__item")}
                  style={{ justifyContent: "flex-start" }}
                >
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    App:
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {intersperse<ReactNode>(
                      app.map(a => <AppLink app={a} key={a} />),
                      ", ",
                    )}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Transaction Type
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {renderTransactionType(implicit_txn)}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Distributed execution?
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {renderBools(distSQL)}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Vectorized execution?
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {renderBools(vec)}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Used cost-based optimizer?
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {renderBools(opt)}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Failed?
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {renderBools(failed)}
                  </p>
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
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    First Attempts
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {firstAttemptsBarChart}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Retries
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red": retriesBarChart > 0,
                      },
                    )}
                  >
                    {retriesBarChart}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Max Retries
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red":
                          maxRetriesBarChart > 0,
                      },
                    )}
                  >
                    {maxRetriesBarChart}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Total
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {totalCountBarChart}
                  </p>
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
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Mean Rows
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {rowsBarChart(true)}
                  </p>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <h4
                    className={summaryCardStylesCx(
                      "summary--card__item--label",
                    )}
                  >
                    Standard Deviation
                  </h4>
                  <p
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                    )}
                  >
                    {rowsBarChart()}
                  </p>
                </div>
              </SummaryCard>
            </Col>
          </Row>
        </TabPane>
        <TabPane
          tab={`Diagnostics ${
            hasDiagnosticReports ? `(${diagnosticsReports.length})` : ""
          }`}
          key="diagnostics"
        >
          <DiagnosticsView
            activate={createStatementDiagnosticsReport}
            diagnosticsReports={diagnosticsReports}
            dismissAlertMessage={dismissStatementDiagnosticsAlertMessage}
            hasData={hasDiagnosticReports}
            statementFingerprint={statement}
            onDownloadDiagnosticBundleClick={onDiagnosticBundleDownload}
            showDiagnosticsViewLink={
              this.props.uiConfig.showStatementDiagnosticsLink
            }
          />
        </TabPane>
        <TabPane tab="Logical Plan" key="logical-plan">
          <SummaryCard>
            <PlanView title="Logical Plan" plan={logicalPlan} />
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
                <Tooltip text="The execution latency of this statement, broken down by phase.">
                  <div
                    className={cx("numeric-stats-table__tooltip-hover-area")}
                  >
                    <div className={cx("numeric-stats-table__info-icon")}>
                      i
                    </div>
                  </div>
                </Tooltip>
              </div>
            </h2>
            <NumericStatTable
              title="Phase"
              measure="Latency"
              count={count}
              format={(v: number) => Duration(v * 1e9)}
              rows={[
                { name: "Parse", value: stats.parse_lat, bar: parseBarChart },
                { name: "Plan", value: stats.plan_lat, bar: planBarChart },
                { name: "Run", value: stats.run_lat, bar: runBarChart },
                {
                  name: "Overhead",
                  value: stats.overhead_lat,
                  bar: overheadBarChart,
                },
                {
                  name: "Overall",
                  summary: true,
                  value: stats.service_lat,
                  bar: overallBarChart,
                },
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
              count={count}
              format={d3Format(".2f")}
              rows={[
                {
                  name: "Rows Read",
                  value: stats.rows_read,
                  bar: genericBarChart(stats.rows_read, stats.count),
                },
                {
                  name: "Disk Bytes Read",
                  value: stats.bytes_read,
                  bar: genericBarChart(stats.bytes_read, stats.count, Bytes),
                  format: Bytes,
                },
                {
                  name: "Network Bytes Sent",
                  value: stats.bytes_sent_over_network,
                  bar: genericBarChart(
                    stats.bytes_sent_over_network,
                    stats.count,
                    Bytes,
                  ),
                  format: Bytes,
                },
              ].filter(function(r) {
                if (
                  r.name === "Network Bytes Sent" &&
                  r.value &&
                  r.value.mean === 0
                ) {
                  // Omit if empty.
                  return false;
                }
                return r.value;
              })}
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
                <Tooltip text="Execution statistics for this statement per gateway node.">
                  <div
                    className={cx("numeric-stats-table__tooltip-hover-area")}
                  >
                    <div className={cx("numeric-stats-table__info-icon")}>
                      i
                    </div>
                  </div>
                </Tooltip>
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
  };
}
