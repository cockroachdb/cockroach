// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Col, Row, Tabs } from "antd";
import { Text, Heading } from "@cockroachlabs/ui-components";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { format as d3Format } from "d3-format";
import { ArrowLeft } from "@cockroachlabs/icons";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import {
  intersperse,
  Bytes,
  Duration,
  FixLong,
  longToInt,
  appAttr,
  NumericStat,
  StatementStatistics,
  stdDev,
  formatNumberForDisplay,
  calculateTotalWorkload,
  unique,
  queryByName,
  aggregatedTsAttr,
  aggregationIntervalAttr,
} from "src/util";
import { Loading } from "src/loading";
import { Button } from "src/button";
import { SqlBox } from "src/sql";
import { SortSetting } from "src/sortedtable";
import { Tooltip } from "@cockroachlabs/ui-components";
import { PlanView } from "./planView";
import { SummaryCard } from "src/summaryCard";
import {
  approximify,
  latencyBreakdown,
  genericBarChart,
  formatTwoPlaces,
} from "src/barCharts";
import {
  AggregateStatistics,
  populateRegionNodeForStatements,
  makeNodesColumns,
  StatementsSortedTable,
} from "src/statementsTable";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import styles from "./statementDetails.module.scss";
import { commonStyles } from "src/common";
import { NodeSummaryStats } from "../nodes";
import { UIConfigState } from "../store";
import moment from "moment";
import { TimeScale, toDateRange } from "../timeScaleDropdown";
import { StatementsRequest } from "src/api/statementsApi";
import SQLActivityError from "../sqlActivity/errorComponent";
import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "../statementsDiagnostics";
type IDuration = google.protobuf.IDuration;
type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const { TabPane } = Tabs;

export interface Fraction {
  numerator: number;
  denominator: number;
}

interface SingleStatementStatistics {
  statement: string;
  app: string[];
  database: string;
  distSQL: Fraction;
  vec: Fraction;
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
  refreshStatements: (req?: StatementsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshUserSQLRoles: () => void;
  refreshNodes: () => void;
  refreshNodesLiveness: () => void;
  createStatementDiagnosticsReport: (
    statementFingerprint: string,
    minExecLatency: IDuration,
    expiresAfter: IDuration,
  ) => void;
  dismissStatementDiagnosticsAlertMessage?: () => void;
  onTabChanged?: (tabName: string) => void;
  onDiagnosticsModalOpen?: (statementFingerprint: string) => void;
  onDiagnosticBundleDownload?: (statementFingerprint?: string) => void;
  onDiagnosticCancelRequest?: (report: IStatementDiagnosticsReport) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onBackToStatementsClick?: () => void;
}

export interface StatementDetailsStateProps {
  statement: SingleStatementStatistics;
  statementsError: Error | null;
  timeScale: TimeScale;
  nodeNames: { [nodeId: string]: string };
  nodeRegions: { [nodeId: string]: string };
  diagnosticsReports: cockroach.server.serverpb.IStatementDiagnosticsReport[];
  uiConfig?: UIConfigState["pages"]["statementDetails"];
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
}

export type StatementDetailsOwnProps = StatementDetailsDispatchProps &
  StatementDetailsStateProps;

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortedTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

function statementsRequestFromProps(
  props: StatementDetailsProps,
): cockroach.server.serverpb.StatementsRequest {
  const [start, end] = toDateRange(props.timeScale);
  return new cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
  });
}

function AppLink(props: { app: string }) {
  if (!props.app) {
    return <Text className={cx("app-name", "app-name__unset")}>(unset)</Text>;
  }

  const searchParams = new URLSearchParams({ [appAttr]: props.app });

  return (
    <Link
      className={cx("app-name")}
      to={`/sql-activity?tab=Statements&${searchParams.toString()}`}
    >
      {props.app}
    </Link>
  );
}

function NodeLink(props: { node: string }) {
  return (
    <Link
      className={cx("app-name")}
      to={`/node/${encodeURIComponent(props.node)}`}
    >
      N{props.node}
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
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;
  constructor(props: StatementDetailsProps) {
    super(props);
    const searchParams = new URLSearchParams(props.history.location.search);
    this.state = {
      sortSetting: {
        // Latency
        ascending: false,
        columnTitle: "statementTime",
      },
      currentTab: searchParams.get("tab") || "overview",
    };
    this.activateDiagnosticsRef = React.createRef();
  }

  static defaultProps: Partial<StatementDetailsProps> = {
    onDiagnosticBundleDownload: _.noop,
    uiConfig: {
      showStatementDiagnosticsLink: true,
    },
    isTenant: false,
    hasViewActivityRedactedRole: false,
  };

  changeSortSetting = (ss: SortSetting): void => {
    this.setState({
      sortSetting: ss,
    });
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Stats By Node", ss.columnTitle, ss.ascending);
    }
  };

  refreshStatements = (): void => {
    const req = statementsRequestFromProps(this.props);
    this.props.refreshStatements(req);
  };

  componentDidMount(): void {
    this.refreshStatements();
    this.props.refreshUserSQLRoles();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      this.props.refreshNodesLiveness();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }
  }

  componentDidUpdate(): void {
    this.refreshStatements();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      this.props.refreshNodesLiveness();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }
  }

  onTabChange = (tabId: string): void => {
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

  backToStatementsClick = (): void => {
    this.props.history.push("/sql-activity?tab=Statements");
    if (this.props.onBackToStatementsClick) {
      this.props.onBackToStatementsClick();
    }
  };

  render(): React.ReactElement {
    const {
      refreshStatementDiagnosticsRequests,
      createStatementDiagnosticsReport,
      onDiagnosticsModalOpen,
    } = this.props;
    const app = queryByName(this.props.location, appAttr);
    return (
      <div className={cx("root")}>
        <Helmet title={`Details | ${app ? `${app} App |` : ""} Statements`} />
        <div className={cx("section", "page--header")}>
          <Button
            onClick={this.backToStatementsClick}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
            className="small-margin"
          >
            Statements
          </Button>
          <h3 className={commonStyles("base-heading", "no-margin-bottom")}>
            Statement Details
          </h3>
        </div>
        <section className={cx("section", "section--container")}>
          <Loading
            loading={_.isNil(this.props.statement)}
            page={"statement details"}
            error={this.props.statementsError}
            render={this.renderContent}
            renderError={() =>
              SQLActivityError({
                statsType: "statements",
              })
            }
          />
          <ActivateStatementDiagnosticsModal
            ref={this.activateDiagnosticsRef}
            activate={createStatementDiagnosticsReport}
            refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
            onOpenModal={onDiagnosticsModalOpen}
          />
        </section>
      </div>
    );
  }

  renderContent = (): React.ReactElement => {
    const {
      diagnosticsReports,
      dismissStatementDiagnosticsAlertMessage,
      onDiagnosticBundleDownload,
      onDiagnosticCancelRequest,
      nodeRegions,
      isTenant,
      hasViewActivityRedactedRole,
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
      failed,
      implicit_txn,
      database,
    } = this.props.statement;

    if (!stats) {
      const sourceApp = queryByName(this.props.location, appAttr);
      const listUrl =
        "/sql-activity?tab=Statements" +
        (sourceApp ? "&" + appAttr + "=" + sourceApp : "");

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
    const totalWorkload = calculateTotalWorkload(statsByNode);
    populateRegionNodeForStatements(statsByNode, nodeRegions, isTenant);
    const nodes: string[] = unique(
      (stats.nodes || []).map(node => node.toString()),
    ).sort();
    const regions = unique(
      (stats.nodes || []).map(node => nodeRegions[node.toString()]),
    ).sort();
    const explainPlan =
      stats.sensitive_info && stats.sensitive_info.most_recent_plan_description;
    const explainGlobalProps = { distribution: distSQL, vectorized: vec };
    const duration = (v: number) => Duration(v * 1e9);
    const hasDiagnosticReports = diagnosticsReports.length > 0;
    const lastExec =
      stats.last_exec_timestamp &&
      moment(stats.last_exec_timestamp.seconds.low * 1e3).format(
        "MMM DD, YYYY HH:MM",
      );
    const statementSampled = stats.exec_stats.count > Long.fromNumber(0);
    const unavailableTooltip = !statementSampled && (
      <Tooltip
        placement="bottom"
        style="default"
        content={
          <p>
            This metric is part of the statement execution and therefore will
            not be available until the statement is sampled via tracing.
          </p>
        }
      >
        <span className={cx("tooltip-info")}>unavailable</span>
      </Tooltip>
    );

    // If the aggregatedTs is unset, we are aggregating over the whole date range.
    const aggregatedTs = queryByName(this.props.location, aggregatedTsAttr);
    const aggregationInterval =
      queryByName(this.props.location, aggregationIntervalAttr) || 0;
    const [timeScaleStart, timeScaleEnd] = toDateRange(this.props.timeScale);
    const intervalStartTime = aggregatedTs
      ? moment.unix(parseInt(aggregatedTs)).utc()
      : timeScaleStart;
    const intervalEndTime =
      aggregatedTs && aggregationInterval
        ? moment
            .unix(parseInt(aggregatedTs) + parseInt(aggregationInterval))
            .utc()
        : timeScaleEnd;

    const db = database ? (
      <Text>{database}</Text>
    ) : (
      <Text className={cx("app-name", "app-name__unset")}>(unset)</Text>
    );

    return (
      <Tabs
        defaultActiveKey="1"
        className={commonStyles("cockroach--tabs")}
        onChange={this.onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Overview" key="overview">
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox value={statement} />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Row>
                  <Col>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Heading type="h5">Mean statement time</Heading>
                      <Text type="body-strong">
                        {formatNumberForDisplay(
                          stats.service_lat.mean,
                          duration,
                        )}
                      </Text>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Planning time</Text>
                      <Text>
                        {formatNumberForDisplay(stats.plan_lat.mean, duration)}
                      </Text>
                    </div>
                    <p
                      className={summaryCardStylesCx("summary--card__divider")}
                    />
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Execution time</Text>
                      <Text>
                        {formatNumberForDisplay(stats.run_lat.mean, duration)}
                      </Text>
                    </div>
                    <p
                      className={summaryCardStylesCx("summary--card__divider")}
                    />
                  </Col>
                </Row>
              </SummaryCard>
              <SummaryCard className={cx("summary-card")}>
                <Row>
                  <Col>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Heading type="h5">Resource usage</Heading>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Mean rows/bytes read</Text>
                      {statementSampled && (
                        <Text>
                          {formatNumberForDisplay(
                            stats.rows_read.mean,
                            formatTwoPlaces,
                          )}
                          {" / "}
                          {formatNumberForDisplay(stats.bytes_read.mean, Bytes)}
                        </Text>
                      )}
                      {unavailableTooltip}
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Mean rows written</Text>
                      <Text>
                        {formatNumberForDisplay(
                          stats.rows_written?.mean,
                          formatTwoPlaces,
                        )}
                      </Text>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Max memory usage</Text>
                      {statementSampled && (
                        <Text>
                          {formatNumberForDisplay(
                            stats.exec_stats.max_mem_usage.mean,
                            Bytes,
                          )}
                        </Text>
                      )}
                      {unavailableTooltip}
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Network usage</Text>
                      {statementSampled && (
                        <Text>
                          {formatNumberForDisplay(
                            stats.exec_stats.network_bytes.mean,
                            Bytes,
                          )}
                        </Text>
                      )}
                      {unavailableTooltip}
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Max scratch disk usage</Text>
                      {statementSampled && (
                        <Text>
                          {formatNumberForDisplay(
                            stats.exec_stats.max_disk_usage.mean,
                            Bytes,
                          )}
                        </Text>
                      )}
                      {unavailableTooltip}
                    </div>
                  </Col>
                </Row>
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Heading type="h5">Statement details</Heading>
                {!isTenant && (
                  <div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Nodes</Text>
                      <Text>
                        {intersperse<ReactNode>(
                          nodes.map(n => <NodeLink node={n} key={n} />),
                          ", ",
                        )}
                      </Text>
                    </div>
                    <div className={summaryCardStylesCx("summary--card__item")}>
                      <Text>Regions</Text>
                      <Text>{intersperse<ReactNode>(regions, ", ")}</Text>
                    </div>
                  </div>
                )}

                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Database</Text>
                  {db}
                </div>
                <p
                  className={summaryCardStylesCx(
                    "summary--card__divider--large",
                  )}
                />
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>App</Text>
                  <Text>
                    {intersperse<ReactNode>(
                      app.map(a => <AppLink app={a} key={a} />),
                      ", ",
                    )}
                  </Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Failed?</Text>
                  <Text>{renderBools(failed)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Distributed execution?</Text>
                  <Text>{renderBools(distSQL)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Vectorized execution?</Text>
                  <Text>{renderBools(vec)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Transaction type</Text>
                  <Text>{renderTransactionType(implicit_txn)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Last execution time</Text>
                  <Text>{lastExec}</Text>
                </div>
                <p
                  className={summaryCardStylesCx(
                    "summary--card__divider--large",
                  )}
                />
                <Heading type="h5">Execution counts</Heading>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>First attempts</Text>
                  <Text>{firstAttemptsBarChart}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Total executions</Text>
                  <Text>{totalCountBarChart}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Retries</Text>
                  <Text
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red": retriesBarChart > 0,
                      },
                    )}
                  >
                    {retriesBarChart}
                  </Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Max retries</Text>
                  <Text
                    className={summaryCardStylesCx(
                      "summary--card__item--value",
                      {
                        "summary--card__item--value-red":
                          maxRetriesBarChart > 0,
                      },
                    )}
                  >
                    {maxRetriesBarChart}
                  </Text>
                </div>
              </SummaryCard>
            </Col>
          </Row>
        </TabPane>
        {!isTenant && !hasViewActivityRedactedRole && (
          <TabPane
            tab={`Diagnostics ${
              hasDiagnosticReports ? `(${diagnosticsReports.length})` : ""
            }`}
            key="diagnostics"
          >
            <DiagnosticsView
              activateDiagnosticsRef={this.activateDiagnosticsRef}
              diagnosticsReports={diagnosticsReports}
              dismissAlertMessage={dismissStatementDiagnosticsAlertMessage}
              hasData={hasDiagnosticReports}
              statementFingerprint={statement}
              onDownloadDiagnosticBundleClick={onDiagnosticBundleDownload}
              onDiagnosticCancelRequestClick={onDiagnosticCancelRequest}
              showDiagnosticsViewLink={
                this.props.uiConfig.showStatementDiagnosticsLink
              }
              onSortingChange={this.props.onSortingChange}
            />
          </TabPane>
        )}
        <TabPane tab="Explain Plan" key="explain-plan">
          <SummaryCard>
            <PlanView
              title="Explain Plan"
              plan={explainPlan}
              globalProperties={explainGlobalProps}
            />
          </SummaryCard>
        </TabPane>
        <TabPane
          tab="Execution Stats"
          key="execution-stats"
          className={cx("fit-content-width")}
        >
          <SummaryCard>
            <h3
              className={classNames(
                commonStyles("base-heading"),
                summaryCardStylesCx("summary--card__title"),
              )}
            >
              Execution Latency By Phase
              <div className={cx("numeric-stats-table__tooltip")}>
                <Tooltip content="The execution latency of this statement, broken down by phase.">
                  <div
                    className={cx("numeric-stats-table__tooltip-hover-area")}
                  >
                    <div className={cx("numeric-stats-table__info-icon")}>
                      i
                    </div>
                  </div>
                </Tooltip>
              </div>
            </h3>
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
            <h3
              className={classNames(
                commonStyles("base-heading"),
                summaryCardStylesCx("summary--card__title"),
              )}
            >
              Other Execution Statistics
            </h3>
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
                  name: "Rows Written",
                  value: stats.rows_written,
                  bar: genericBarChart(stats.rows_written, stats.count),
                },
                {
                  name: "Network Bytes Sent",
                  value: stats.exec_stats.network_bytes,
                  bar: genericBarChart(
                    stats.exec_stats.network_bytes,
                    stats.exec_stats.count,
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
          {!isTenant && (
            <SummaryCard className={cx("fit-content-width")}>
              <h3
                className={classNames(
                  commonStyles("base-heading"),
                  summaryCardStylesCx("summary--card__title"),
                )}
              >
                Stats By Node
                <div className={cx("numeric-stats-table__tooltip")}>
                  <Tooltip content="Execution statistics for this statement per gateway node.">
                    <div
                      className={cx("numeric-stats-table__tooltip-hover-area")}
                    >
                      <div className={cx("numeric-stats-table__info-icon")}>
                        i
                      </div>
                    </div>
                  </Tooltip>
                </div>
              </h3>
              <StatementsSortedTable
                className={cx("statements-table")}
                data={statsByNode}
                columns={makeNodesColumns(
                  statsByNode,
                  this.props.nodeNames,
                  totalWorkload,
                  nodeRegions,
                )}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.changeSortSetting}
                firstCellBordered
              />
            </SummaryCard>
          )}
        </TabPane>
      </Tabs>
    );
  };
}
