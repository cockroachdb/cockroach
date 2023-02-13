// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { ReactNode, useEffect } from "react";
import { Col, Row, Tabs } from "antd";
import { cockroach, google } from "@cockroachlabs/crdb-protobuf-client";
import { Text, InlineAlert } from "@cockroachlabs/ui-components";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Location } from "history";
import _, { isNil } from "lodash";
import Long from "long";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { BarGraphTimeSeries } from "../graphs/bargraph";
import { AxisUnits } from "../graphs";
import { AlignedData, Options } from "uplot";

import {
  intersperse,
  unique,
  queryByName,
  appAttr,
  appNamesAttr,
  FixFingerprintHexValue,
  RenderCount,
  TimestampToMoment,
  DATE_FORMAT_24_UTC,
} from "src/util";
import { getValidErrorsList, Loading } from "src/loading";
import { Button } from "src/button";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SortSetting } from "src/sortedtable";
import { PlanDetails } from "./planDetails";
import { SummaryCard } from "src/summaryCard";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import timeScaleStyles from "src/timeScaleDropdown/timeScale.module.scss";
import styles from "./statementDetails.module.scss";
import { commonStyles } from "src/common";
import { UIConfigState } from "../store";
import { StatementDetailsRequest } from "src/api/statementsApi";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  timeScaleToString,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import LoadingError from "../sqlActivity/errorComponent";
import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "../statementsDiagnostics";
import {
  generateExecCountTimeseries,
  generateExecRetriesTimeseries,
  generateExecuteAndPlanningTimeseries,
  generateRowsProcessedTimeseries,
  generateContentionTimeseries,
} from "./timeseriesUtils";
import { Delayed } from "../delayed";
import moment from "moment";
type IDuration = google.protobuf.IDuration;
type StatementDetailsResponse = cockroach.server.serverpb.StatementDetailsResponse;
type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const { TabPane } = Tabs;

export type StatementDetailsProps = StatementDetailsOwnProps &
  RouteComponentProps<{ implicitTxn: string; statement: string }>;

export interface StatementDetailsState {
  plansSortSetting: SortSetting;
  currentTab?: string;
  cardWidth: number;
}

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
const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

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

function renderTransactionType(implicitTxn: boolean) {
  if (implicitTxn) {
    return "Implicit";
  }
  return "Explicit";
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
      plansSortSetting: {
        ascending: false,
        columnTitle: "lastExecTime",
      },
      currentTab: searchParams.get("tab") || "overview",
      cardWidth: 700,
    };
    this.activateDiagnosticsRef = React.createRef();

    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.props.onTimeScaleChange(ts);
    }
  }

  hasDiagnosticReports = (): boolean =>
    this.props.diagnosticsReports.length > 0;

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

  handleResize = (): void => {
    // Use the same size as the summary card and remove a space for margin (22).
    const cardWidth = document.getElementById("first-card")
      ? document.getElementById("first-card")?.offsetWidth - 22
      : 700;
    if (cardWidth !== this.state.cardWidth) {
      this.setState({
        cardWidth: cardWidth,
      });
    }
  };

  componentDidMount(): void {
    window.addEventListener("resize", this.handleResize);
    this.handleResize();
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
    this.handleResize();
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
    this.props.history.push("/sql-activity?tab=Statements");
    if (this.props.onBackToStatementsClick) {
      this.props.onBackToStatementsClick();
    }
  };

  onChangePlansSortSetting = (ss: SortSetting): void => {
    this.setState({
      plansSortSetting: ss,
    });
  };

  render(): React.ReactElement {
    const {
      refreshStatementDiagnosticsRequests,
      createStatementDiagnosticsReport,
      onDiagnosticsModalOpen,
    } = this.props;
    const app = queryByName(this.props.location, appAttr);
    const longLoadingMessage = this.props.isLoading &&
      isNil(this.props.statementDetails) &&
      isNil(getValidErrorsList(this.props.statementsError)) && (
        <Delayed delay={moment.duration(2, "s")}>
          <InlineAlert
            intent="info"
            title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
          />
        </Delayed>
      );

    const hasTimeout = this.props.statementsError?.name
      ?.toLowerCase()
      .includes("timeout");
    const error = hasTimeout ? null : this.props.statementsError;

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
            Statement Fingerprint
          </h3>
        </div>
        <section className={cx("section", "section--container")}>
          <Loading
            loading={this.props.isLoading}
            page={"statement fingerprint"}
            error={error}
            render={this.renderTabs}
            renderError={() =>
              LoadingError({
                statsType: "statements",
              })
            }
          />
          {longLoadingMessage}
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
    const hasTimeout = this.props.statementsError?.name
      ?.toLowerCase()
      .includes("timeout");
    const hasData =
      Number(this.props.statementDetails?.statement?.stats?.count) > 0;
    const period = timeScaleToString(this.props.timeScale);

    return (
      <Tabs
        defaultActiveKey="1"
        className={commonStyles("cockroach--tabs")}
        onChange={this.onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Overview" key="overview">
          {this.renderOverviewTabContent(hasTimeout, hasData, period)}
        </TabPane>
        <TabPane tab="Explain Plans" key="explain-plan">
          {this.renderExplainPlanTabContent(hasTimeout, hasData, period)}
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
      </Tabs>
    );
  };

  renderNoDataTabContent = (): React.ReactElement => (
    <section className={cx("section")}>
      <InlineAlert intent="info" title="No data available." />
    </section>
  );

  renderNoDataWithTimeScaleAndSqlBoxTabContent = (
    hasTimeout: boolean,
  ): React.ReactElement => (
    <>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
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
        {hasTimeout && (
          <InlineAlert
            intent="danger"
            title={LoadingError({
              statsType: "statements",
              timeout: true,
            })}
          />
        )}
        {!hasTimeout && (
          <InlineAlert
            intent="info"
            title="Data not available for this time frame. Select a different time frame."
          />
        )}
      </section>
    </>
  );

  renderOverviewTabContent = (
    hasTimeout: boolean,
    hasData: boolean,
    period: string,
  ): React.ReactElement => {
    if (!hasData) {
      return this.renderNoDataWithTimeScaleAndSqlBoxTabContent(hasTimeout);
    }
    const { cardWidth } = this.state;
    const { nodeRegions, isTenant } = this.props;
    const { stats } = this.props.statementDetails.statement;
    const {
      app_names,
      databases,
      fingerprint_id,
      failed_count,
      full_scan_count,
      vec_count,
      total_count,
      implicit_txn,
    } = this.props.statementDetails.statement.metadata;
    const {
      statement_statistics_per_aggregated_ts,
    } = this.props.statementDetails;

    const nodes: string[] = unique(
      (stats.nodes || []).map(node => node.toString()),
    ).sort();
    const regions = unique(
      (stats.nodes || []).map(node => nodeRegions[node.toString()]),
    ).sort();

    const lastExec =
      stats.last_exec_timestamp &&
      TimestampToMoment(stats.last_exec_timestamp).format(DATE_FORMAT_24_UTC);

    const statementSampled = stats.exec_stats.count > Long.fromNumber(0);
    const unavailableTooltip = !statementSampled && (
      <div>
        This metric is part of the statement execution and therefore will not be
        available until the statement is sampled.
      </div>
    );
    const noSamples = statementSampled ? "" : " (no samples)";

    const db = databases ? (
      <Text>{databases}</Text>
    ) : (
      <Text className={cx("app-name", "app-name__unset")}>(unset)</Text>
    );

    const statsPerAggregatedTs = statement_statistics_per_aggregated_ts.sort(
      (a, b) =>
        a.aggregated_ts.seconds < b.aggregated_ts.seconds
          ? -1
          : a.aggregated_ts.seconds > b.aggregated_ts.seconds
          ? 1
          : 0,
    );

    const executionAndPlanningTimeseries: AlignedData = generateExecuteAndPlanningTimeseries(
      statsPerAggregatedTs,
    );
    const executionAndPlanningOps: Partial<Options> = {
      axes: [{}, { label: "Time Spent" }],
      series: [{}, { label: "Execution" }, { label: "Planning" }],
      width: cardWidth,
    };

    const rowsProcessedTimeseries: AlignedData = generateRowsProcessedTimeseries(
      statsPerAggregatedTs,
    );
    const rowsProcessedOps: Partial<Options> = {
      axes: [{}, { label: "Rows" }],
      series: [{}, { label: "Rows Read" }, { label: "Rows Written" }],
      width: cardWidth,
    };

    const execRetriesTimeseries: AlignedData = generateExecRetriesTimeseries(
      statsPerAggregatedTs,
    );
    const execRetriesOps: Partial<Options> = {
      axes: [{}, { label: "Retries" }],
      series: [{}, { label: "Retries" }],
      legend: { show: false },
      width: cardWidth,
    };

    const execCountTimeseries: AlignedData = generateExecCountTimeseries(
      statsPerAggregatedTs,
    );
    const execCountOps: Partial<Options> = {
      axes: [{}, { label: "Execution Counts" }],
      series: [{}, { label: "Execution Counts" }],
      legend: { show: false },
      width: cardWidth,
    };

    const contentionTimeseries: AlignedData = generateContentionTimeseries(
      statsPerAggregatedTs,
    );
    const contentionOps: Partial<Options> = {
      axes: [{}, { label: "Contention" }],
      series: [{}, { label: "Contention" }],
      legend: { show: false },
      width: cardWidth,
    };

    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={this.props.timeScale}
              setTimeScale={this.props.onTimeScaleChange}
            />
          </PageConfigItem>
        </PageConfig>
        <p className={timeScaleStylesCx("time-label", "label-margin")}>
          Showing aggregated stats from{" "}
          <span className={timeScaleStylesCx("bold")}>{period}</span>
        </p>
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
              <SummaryCard id="first-card" className={cx("summary-card")}>
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
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Application Name</Text>
                  <Text>
                    {intersperse<ReactNode>(
                      app_names.map(a => <AppLink app={a} key={a} />),
                      ", ",
                    )}
                  </Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Fingerprint ID</Text>
                  {FixFingerprintHexValue(fingerprint_id)}
                </div>
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Failed?</Text>
                  <Text>{RenderCount(failed_count, total_count)}</Text>
                </div>
                <div className={summaryCardStylesCx("summary--card__item")}>
                  <Text>Full scan?</Text>
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
              </SummaryCard>
            </Col>
          </Row>
          <p className={summaryCardStylesCx("summary--card__divider--large")} />
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Statement Execution and Planning Time"
                alignedData={executionAndPlanningTimeseries}
                uPlotOptions={executionAndPlanningOps}
                yAxisUnits={AxisUnits.Duration}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Rows Processed"
                alignedData={rowsProcessedTimeseries}
                uPlotOptions={rowsProcessedOps}
                yAxisUnits={AxisUnits.Count}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Execution Retries"
                alignedData={execRetriesTimeseries}
                uPlotOptions={execRetriesOps}
                yAxisUnits={AxisUnits.Count}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Execution Count"
                alignedData={execCountTimeseries}
                uPlotOptions={execCountOps}
                yAxisUnits={AxisUnits.Count}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title={`Contention${noSamples}`}
                alignedData={contentionTimeseries}
                uPlotOptions={contentionOps}
                tooltip={unavailableTooltip}
                yAxisUnits={AxisUnits.Duration}
              />
            </Col>
          </Row>
        </section>
      </>
    );
  };

  renderExplainPlanTabContent = (
    hasTimeout: boolean,
    hasData: boolean,
    period: string,
  ): React.ReactElement => {
    if (!hasData) {
      return this.renderNoDataWithTimeScaleAndSqlBoxTabContent(hasTimeout);
    }
    const { statement_statistics_per_plan_hash } = this.props.statementDetails;
    const { formatted_query } = this.props.statementDetails.statement.metadata;
    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={this.props.timeScale}
              setTimeScale={this.props.onTimeScaleChange}
            />
          </PageConfigItem>
        </PageConfig>
        <p className={timeScaleStylesCx("time-label", "label-margin")}>
          Showing explain plans from{" "}
          <span className={timeScaleStylesCx("bold")}>{period}</span>
        </p>
        <section className={cx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox value={formatted_query} size={SqlBoxSize.small} />
            </Col>
          </Row>
          <p className={summaryCardStylesCx("summary--card__divider")} />
          <PlanDetails
            plans={statement_statistics_per_plan_hash}
            sortSetting={this.state.plansSortSetting}
            onChangeSortSetting={this.onChangePlansSortSetting}
          />
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
          this.props.uiConfig?.showStatementDiagnosticsLink
        }
        onSortingChange={this.props.onSortingChange}
      />
    );
  };
}
