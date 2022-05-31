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
import { Text, Heading, InlineAlert } from "@cockroachlabs/ui-components";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import _ from "lodash";
import React, { ReactNode } from "react";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { format as d3Format } from "d3-format";
import { ArrowLeft } from "@cockroachlabs/icons";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import { Location } from "history";

import {
  NumericStat,
  intersperse,
  Bytes,
  Duration,
  FixLong,
  longToInt,
  stdDev,
  formatNumberForDisplay,
  unique,
  queryByName,
  appAttr,
  appNamesAttr,
  RenderCount,
} from "src/util";
import { Loading } from "src/loading";
import { Button } from "src/button";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SortSetting } from "src/sortedtable";
import { Tooltip } from "@cockroachlabs/ui-components";
import { PlanDetails } from "./planDetails";
import { SummaryCard } from "src/summaryCard";
import {
  latencyBreakdown,
  genericBarChart,
  formatTwoPlaces,
} from "src/barCharts";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import styles from "./statementDetails.module.scss";
import { commonStyles } from "src/common";
import { NodeSummaryStats } from "../nodes";
import { UIConfigState } from "../store";
import moment from "moment";
import { StatementDetailsRequest } from "src/api/statementsApi";
import {
  TimeScale,
  TimeScaleDropdown,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import SQLActivityError from "../sqlActivity/errorComponent";
import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "../statementsDiagnostics";
type IDuration = google.protobuf.IDuration;
type StatementDetailsResponse = cockroach.server.serverpb.StatementDetailsResponse;
type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const { TabPane } = Tabs;

export interface Fraction {
  numerator: number;
  denominator: number;
}

export type StatementDetailsProps = StatementDetailsOwnProps &
  RouteComponentProps<{ implicitTxn: string; statement: string }>;

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
  refreshStatementDetails: (req: StatementDetailsRequest) => void;
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
  onTimeScaleChange: (ts: TimeScale) => void;
  onDiagnosticsModalOpen?: (statementFingerprint: string) => void;
  onDiagnosticBundleDownload?: (statementFingerprint?: string) => void;
  onDiagnosticCancelRequest?: (report: IStatementDiagnosticsReport) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onBackToStatementsClick?: () => void;
  onStatementDetailsQueryChange: (query: string) => void;
  onStatementDetailsFormattedQueryChange: (formattedQuery: string) => void;
}

export interface StatementDetailsStateProps {
  statementFingerprintID: string;
  statementDetails: StatementDetailsResponse;
  isLoading: boolean;
  latestQuery: string;
  latestFormattedQuery: string;
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

function getStatementDetailsRequest(
  timeScale: TimeScale,
  statementFingerprintID: string,
  location: Location,
): cockroach.server.serverpb.StatementDetailsRequest {
  const [start, end] = toRoundedDateRange(timeScale);
  return new cockroach.server.serverpb.StatementDetailsRequest({
    fingerprint_id: statementFingerprintID,
    app_names: queryByName(location, appNamesAttr)?.split(","),
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
      className={cx("text-link")}
      to={`/sql-activity?tab=Statements&${searchParams.toString()}`}
    >
      {props.app}
    </Link>
  );
}

function NodeLink(props: { node: string }) {
  return (
    <Link
      className={cx("text-link")}
      to={`/node/${encodeURIComponent(props.node)}`}
    >
      N{props.node}
    </Link>
  );
}

function renderTransactionType(implicitTxn: boolean) {
  if (implicitTxn) {
    return "Implicit";
  }
  return "Explicit";
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

  hasDiagnosticReports = (): boolean =>
    this.props.diagnosticsReports.length > 0;

  changeSortSetting = (ss: SortSetting): void => {
    this.setState({
      sortSetting: ss,
    });
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Stats By Node", ss.columnTitle, ss.ascending);
    }
  };

  refreshStatementDetails = (
    timeScale: TimeScale,
    statementFingerprintID: string,
    location: Location,
  ): void => {
    const req = getStatementDetailsRequest(
      timeScale,
      statementFingerprintID,
      location,
    );
    this.props.refreshStatementDetails(req);
  };

  componentDidMount(): void {
    this.refreshStatementDetails(
      this.props.timeScale,
      this.props.statementFingerprintID,
      this.props.location,
    );
    this.props.refreshUserSQLRoles();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      this.props.refreshNodesLiveness();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }
  }

  componentDidUpdate(prevProps: StatementDetailsProps): void {
    if (
      prevProps.timeScale != this.props.timeScale ||
      prevProps.statementFingerprintID != this.props.statementFingerprintID ||
      prevProps.location != this.props.location
    ) {
      this.refreshStatementDetails(
        this.props.timeScale,
        this.props.statementFingerprintID,
        this.props.location,
      );
    }

    if (!this.props.isTenant) {
      this.props.refreshNodes();
      this.props.refreshNodesLiveness();
      if (!this.props.hasViewActivityRedactedRole) {
        this.props.refreshStatementDiagnosticsRequests();
      }
    }

    // If new, non-empty-string query text is available (derived from the statement details response),
    // cache the query text.
    if (
      this.props.statementDetails &&
      this.props.statementDetails.statement.metadata.query != "" &&
      this.props.latestQuery !=
        this.props.statementDetails.statement.metadata.query
    ) {
      this.props.onStatementDetailsQueryChange(
        this.props.statementDetails.statement.metadata.query,
      );
    }

    // If a new, non-empty-string formatted query text is available (derived from the statement details response),
    // cache the query text.
    if (
      this.props.statementDetails &&
      this.props.statementDetails.statement.metadata.formatted_query != "" &&
      this.props.latestFormattedQuery !=
        this.props.statementDetails.statement.metadata.formatted_query
    ) {
      this.props.onStatementDetailsFormattedQueryChange(
        this.props.statementDetails.statement.metadata.formatted_query,
      );
    }

    // If the statementFingerprintID (derived from the URL) changes, invalidate the cached query texts.
    // This is necessary for when the new statementFingerprintID does not have data for the given time frame.
    // The new query text and the formatted query text would be an empty string, and we need to invalidate the old
    // query text and formatted query text.
    if (this.props.statementFingerprintID != prevProps.statementFingerprintID) {
      this.props.onStatementDetailsQueryChange("");
      this.props.onStatementDetailsFormattedQueryChange("");
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
    this.props.history.push("/sql-activity?tab=Statements&view=fingerprints");
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
            loading={this.props.isLoading}
            page={"statement details"}
            error={this.props.statementsError}
            render={this.renderTabs}
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

  renderTabs = (): React.ReactElement => {
    const { currentTab } = this.state;
    const { stats } = this.props.statementDetails.statement;

    const hasData = Number(stats.count) > 0;

    return (
      <Tabs
        defaultActiveKey="1"
        className={commonStyles("cockroach--tabs")}
        onChange={this.onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Overview" key="overview">
          {this.renderOverviewTabContent(hasData)}
        </TabPane>
        <TabPane tab="Explain Plans" key="explain-plan">
          {this.renderExplainPlanTabContent(hasData)}
        </TabPane>
        {!this.props.isTenant && !this.props.hasViewActivityRedactedRole && (
          <TabPane
            tab={`Diagnostics${
              this.hasDiagnosticReports()
                ? ` (${this.props.diagnosticsReports.length})`
                : ""
            }`}
            key="diagnostics"
          >
            {this.renderDiagnosticsTabContent(hasData)}
          </TabPane>
        )}
        <TabPane
          tab="Execution Stats"
          key="execution-stats"
          className={cx("fit-content-width")}
        >
          {this.renderExecutionStatsTabContent(hasData)}
        </TabPane>
      </Tabs>
    );
  };

  renderNoDataTabContent = (): React.ReactElement => (
    <section className={cx("section")}>
      <InlineAlert intent="info" title="No data available." />
    </section>
  );

  renderNoDataWithTimeScaleAndSqlBoxTabContent = (): React.ReactElement => (
    <>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            currentScale={this.props.timeScale}
            setTimeScale={this.props.onTimeScaleChange}
          />
        </PageConfigItem>
      </PageConfig>
      <section className={cx("section")}>
        {this.props.latestFormattedQuery && (
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox
                value={this.props.latestFormattedQuery}
                size={SqlBoxSize.small}
              />
            </Col>
          </Row>
        )}
        <InlineAlert
          intent="info"
          title="Data not available for this time frame. Select a different time frame."
        />
      </section>
    </>
  );

  renderOverviewTabContent = (hasData: boolean): React.ReactElement => {
    if (!hasData) {
      return this.renderNoDataWithTimeScaleAndSqlBoxTabContent();
    }
    const { nodeRegions, isTenant } = this.props;
    const { stats } = this.props.statementDetails.statement;
    const {
      app_names,
      databases,
      dist_sql_count,
      failed_count,
      full_scan_count,
      vec_count,
      total_count,
      implicit_txn,
    } = this.props.statementDetails.statement.metadata;

    const { statement } = this.props.statementDetails;

    const totalCountBarChart = longToInt(statement.stats.count);
    const firstAttemptsBarChart = longToInt(
      statement.stats.first_attempt_count,
    );
    const retriesBarChart = totalCountBarChart - firstAttemptsBarChart;
    const maxRetriesBarChart = longToInt(statement.stats.max_retries);

    const nodes: string[] = unique(
      (stats.nodes || []).map(node => node.toString()),
    ).sort();
    const regions = unique(
      (stats.nodes || []).map(node => nodeRegions[node.toString()]),
    ).sort();

    const duration = (v: number) => Duration(v * 1e9);
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

    const db = databases ? (
      <Text>{databases}</Text>
    ) : (
      <Text className={cx("app-name", "app-name__unset")}>(unset)</Text>
    );
    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              currentScale={this.props.timeScale}
              setTimeScale={this.props.onTimeScaleChange}
            />
          </PageConfigItem>
        </PageConfig>
        <section className={cx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox
                value={this.props.latestFormattedQuery}
                size={SqlBoxSize.small}
              />
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
                      app_names.map(a => <AppLink app={a} key={a} />),
                      ", ",
                    )}
                  </Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Failed?</Text>
                  <Text>{RenderCount(failed_count, total_count)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Distributed execution?</Text>
                  <Text>{RenderCount(dist_sql_count, total_count)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Full Scan?</Text>
                  <Text>{RenderCount(full_scan_count, total_count)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Vectorized execution?</Text>
                  <Text>{RenderCount(vec_count, total_count)}</Text>
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
        </section>
      </>
    );
  };

  renderExplainPlanTabContent = (hasData: boolean): React.ReactElement => {
    if (!hasData) {
      return this.renderNoDataWithTimeScaleAndSqlBoxTabContent();
    }
    const { statement_statistics_per_plan_hash } = this.props.statementDetails;
    const { formatted_query } = this.props.statementDetails.statement.metadata;
    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              currentScale={this.props.timeScale}
              setTimeScale={this.props.onTimeScaleChange}
            />
          </PageConfigItem>
        </PageConfig>
        <section className={cx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox value={formatted_query} size={SqlBoxSize.small} />
            </Col>
          </Row>
          <p className={summaryCardStylesCx("summary--card__divider")} />
          <PlanDetails plans={statement_statistics_per_plan_hash} />
        </section>
      </>
    );
  };

  renderDiagnosticsTabContent = (hasData: boolean): React.ReactElement => {
    if (!hasData && !this.props.latestQuery) {
      return this.renderNoDataTabContent();
    }

    return (
      <DiagnosticsView
        activateDiagnosticsRef={this.activateDiagnosticsRef}
        diagnosticsReports={this.props.diagnosticsReports}
        dismissAlertMessage={this.props.dismissStatementDiagnosticsAlertMessage}
        hasData={this.hasDiagnosticReports()}
        statementFingerprint={this.props.latestQuery}
        onDownloadDiagnosticBundleClick={this.props.onDiagnosticBundleDownload}
        onDiagnosticCancelRequestClick={this.props.onDiagnosticCancelRequest}
        showDiagnosticsViewLink={
          this.props.uiConfig.showStatementDiagnosticsLink
        }
        onSortingChange={this.props.onSortingChange}
      />
    );
  };

  renderExecutionStatsTabContent = (hasData: boolean): React.ReactElement => {
    if (!hasData) {
      return this.renderNoDataTabContent();
    }
    const { stats } = this.props.statementDetails.statement;

    const count = FixLong(stats.count).toInt();
    const { statement } = this.props.statementDetails;
    const {
      parseBarChart,
      planBarChart,
      runBarChart,
      overheadBarChart,
      overallBarChart,
    } = latencyBreakdown(statement);
    return (
      <>
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
                <div className={cx("numeric-stats-table__tooltip-hover-area")}>
                  <div className={cx("numeric-stats-table__info-icon")}>i</div>
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
      </>
    );
  };
}
