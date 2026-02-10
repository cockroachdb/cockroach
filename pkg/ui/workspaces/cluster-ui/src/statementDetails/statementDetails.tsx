// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import { InlineAlert, Text } from "@cockroachlabs/ui-components";
import { Col, Row, Tabs } from "antd";
import classNames from "classnames/bind";
import isNil from "lodash/isNil";
import Long from "long";
import moment from "moment-timezone";
import React, {
  ReactNode,
  useCallback,
  useContext,
  useState,
  useEffect,
  useRef,
} from "react";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";
import { AlignedData, Options } from "uplot";

import { Anchor } from "src/anchor";
import { StatementDetailsRequest } from "src/api/statementsApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { getValidErrorsList, Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import timeScaleStyles from "src/timeScaleDropdown/timeScale.module.scss";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import {
  appAttr,
  appNamesAttr,
  FixFingerprintHexValue,
  DATE_FORMAT_24_TZ,
  intersperse,
  queryByName,
  RenderCount,
  TimestampToMoment,
  unique,
  batchStatements,
  formatNumberForDisplay,
  Duration,
  Count,
  longToInt,
} from "src/util";

import {
  InsertStmtDiagnosticRequest,
  InsightRecommendation,
  StatementDiagnosticsReport,
  StmtInsightsReq,
} from "../api";
import { CockroachCloudContext } from "../contexts";
import { Delayed } from "../delayed";
import { AxisUnits } from "../graphs";
import { BarGraphTimeSeries, XScale } from "../graphs/bargraph";
import {
  getStmtInsightRecommendations,
  InsightType,
  StmtInsightEvent,
} from "../insights";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../insightsTable/insightsTable";
import insightTableStyles from "../insightsTable/insightsTable.module.scss";
import LoadingError from "../sqlActivity/errorComponent";
import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "../statementsDiagnostics";
import { UIConfigState } from "../store";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import { FormattedTimescale } from "../timeScaleDropdown/formattedTimeScale";
import { Timestamp } from "../timestamp";

import { filterByTimeScale } from "./diagnostics/diagnosticsUtils";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import { PlanDetails } from "./planDetails";
import styles from "./statementDetails.module.scss";
import {
  generateContentionTimeseries,
  generateExecCountTimeseries,
  generateExecRetriesTimeseries,
  generateExecuteAndPlanningTimeseries,
  generateRowsProcessedTimeseries,
  generateCPUTimeseries,
  generateClientWaitTimeseries,
  generatePlanDistributionTimeseries,
} from "./timeseriesUtils";

type StatementDetailsResponse =
  cockroach.server.serverpb.StatementDetailsResponse;

const { TabPane } = Tabs;

export type StatementDetailsProps = StatementDetailsOwnProps &
  RouteComponentProps<{ statement: string }>;

export interface StatementDetailsState {
  currentTab?: string;
  cardWidth: number;

  /**
   * The latest non-null query text associated with the statement fingerprint in the URL.
   * We save this to preserve this data when the time frame changes such that there is no
   * longer data for this statement fingerprint in the selected time frame.
   */
  query: string;

  /**
   * The latest non-null formatted query associated with the statement fingerprint in the URL.
   * We save this to preserve data when the time frame changes such that there is no longer
   * data for this statement fingerprint in the selected time frame.
   */
  formattedQuery: string;
}

export interface StatementDetailsDispatchProps {
  refreshStatementDetails: (req: StatementDetailsRequest) => void;
  refreshStatementDiagnosticsRequests: () => void;
  refreshUserSQLRoles: () => void;
  refreshNodes: () => void;
  refreshNodesLiveness: () => void;
  refreshStatementFingerprintInsights: (req: StmtInsightsReq) => void;
  createStatementDiagnosticsReport: (
    insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest,
  ) => void;
  dismissStatementDiagnosticsAlertMessage?: () => void;
  onTabChanged?: (tabName: string) => void;
  onTimeScaleChange: (ts: TimeScale) => void;
  onDiagnosticsModalOpen?: (statementFingerprint: string) => void;
  onDiagnosticBundleDownload?: (statementFingerprint?: string) => void;
  onDiagnosticCancelRequest?: (report: StatementDiagnosticsReport) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onBackToStatementsClick?: () => void;
  onRequestTimeChange: (t: moment.Moment) => void;
}

export interface StatementDetailsStateProps {
  statementFingerprintID: string;
  statementDetails: StatementDetailsResponse;
  isLoading: boolean;
  statementsError: Error | null;
  lastUpdated: moment.Moment | null;
  timeScale: TimeScale;
  nodeRegions: { [nodeId: string]: string };
  diagnosticsReports: StatementDiagnosticsReport[];
  uiConfig?: UIConfigState["pages"]["statementDetails"];
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  hasAdminRole?: UIConfigState["hasAdminRole"];
  statementFingerprintInsights?: StmtInsightEvent[];
  requestTime: moment.Moment;
}

export type StatementDetailsOwnProps = StatementDetailsDispatchProps &
  StatementDetailsStateProps;

const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);
const insightsTableCx = classNames.bind(insightTableStyles);

function getStatementDetailsRequestFromProps(
  props: StatementDetailsProps,
): cockroach.server.serverpb.StatementDetailsRequest {
  const [start, end] = toRoundedDateRange(props.timeScale);
  return new cockroach.server.serverpb.StatementDetailsRequest({
    fingerprint_id: props.statementFingerprintID,
    app_names: queryByName(props.location, appNamesAttr)?.split(","),
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

export function StatementDetails(
  props: StatementDetailsProps,
): React.ReactElement {
  const {
    history,
    location,
    statementFingerprintID,
    statementDetails,
    isLoading,
    statementsError,
    lastUpdated,
    timeScale,
    nodeRegions,
    diagnosticsReports,
    uiConfig,
    isTenant,
    hasViewActivityRedactedRole,
    hasAdminRole,
    statementFingerprintInsights,
    requestTime,
    refreshStatementDetails,
    refreshStatementDiagnosticsRequests,
    refreshUserSQLRoles,
    refreshNodes,
    refreshNodesLiveness,
    refreshStatementFingerprintInsights,
    createStatementDiagnosticsReport,
    dismissStatementDiagnosticsAlertMessage,
    onTabChanged,
    onTimeScaleChange,
    onDiagnosticsModalOpen,
    onDiagnosticBundleDownload,
    onDiagnosticCancelRequest,
    onSortingChange,
    onBackToStatementsClick,
    onRequestTimeChange,
  } = props;

  const activateDiagnosticsRef = useRef<ActivateDiagnosticsModalRef>(null);

  // Refs to hold latest values for mount effects, avoiding stale closures
  // while preserving "run once on mount" semantics.
  const timeScaleRef = useRef(timeScale);
  const lastUpdatedRef = useRef(lastUpdated);
  const refreshUserSQLRolesRef = useRef(refreshUserSQLRoles);
  const isTenantRef = useRef(isTenant);
  const refreshNodesRef = useRef(refreshNodes);
  const refreshNodesLivenessRef = useRef(refreshNodesLiveness);
  const hasViewActivityRedactedRoleRef = useRef(hasViewActivityRedactedRole);
  const refreshStatementDiagnosticsRequestsRef = useRef(
    refreshStatementDiagnosticsRequests,
  );

  // Keep refs up to date on each render
  timeScaleRef.current = timeScale;
  lastUpdatedRef.current = lastUpdated;
  refreshUserSQLRolesRef.current = refreshUserSQLRoles;
  isTenantRef.current = isTenant;
  refreshNodesRef.current = refreshNodes;
  refreshNodesLivenessRef.current = refreshNodesLiveness;
  hasViewActivityRedactedRoleRef.current = hasViewActivityRedactedRole;
  refreshStatementDiagnosticsRequestsRef.current =
    refreshStatementDiagnosticsRequests;

  // Context must be called at the top level of the component
  const isCockroachCloud = useContext(CockroachCloudContext);

  // Initialize state from URL and props
  const getInitialTab = (): string => {
    const searchParams = new URLSearchParams(history.location.search);
    return searchParams.get("tab") || "overview";
  };

  const [currentTab, setCurrentTab] = useState<string>(getInitialTab);
  const [cardWidth, setCardWidth] = useState<number>(700);
  const [query, setQuery] = useState<string>(
    statementDetails?.statement?.metadata?.query,
  );
  const [formattedQuery, setFormattedQuery] = useState<string>(
    statementDetails?.statement?.metadata?.formatted_query,
  );

  // Track previous values for componentDidUpdate logic
  const prevTimeScaleRef = useRef<TimeScale>(timeScale);
  const prevStatementFingerprintIDRef = useRef<string>(statementFingerprintID);
  const prevLocationRef = useRef(location);

  const hasDiagnosticReports = (): boolean => diagnosticsReports.length > 0;

  const changeTimeScale = useCallback(
    (ts: TimeScale): void => {
      if (onTimeScaleChange) {
        onTimeScaleChange(ts);
      }
      onRequestTimeChange(moment());
    },
    [onRequestTimeChange, onTimeScaleChange],
  );

  const refreshStatementInsights = useCallback((): void => {
    const [startTime, endTime] = toRoundedDateRange(timeScale);
    const id = BigInt(statementFingerprintID).toString(16);
    const req: StmtInsightsReq = {
      start: startTime,
      end: endTime,
      stmtFingerprintId: id,
    };
    refreshStatementFingerprintInsights(req);
  }, [refreshStatementFingerprintInsights, statementFingerprintID, timeScale]);

  const refreshStatementDetailsInternal = useCallback((): void => {
    const req = getStatementDetailsRequestFromProps(props);
    refreshStatementDetails(req);
    refreshStatementInsights();
  }, [props, refreshStatementDetails, refreshStatementInsights]);

  const handleResize = useCallback((): void => {
    // Use the same size as the summary card and remove a space for margin (22).
    const cardElement = document.getElementById("first-card");
    const newCardWidth = cardElement ? cardElement.offsetWidth - 22 : 700;
    if (newCardWidth !== cardWidth) {
      setCardWidth(newCardWidth);
    }
  }, [cardWidth]);

  // Refs for local functions used in mount effect
  const refreshStatementDetailsInternalRef = useRef(
    refreshStatementDetailsInternal,
  );
  const handleResizeRef = useRef(handleResize);
  refreshStatementDetailsInternalRef.current = refreshStatementDetailsInternal;
  handleResizeRef.current = handleResize;

  const onTabChange = (tabId: string): void => {
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("tab", tabId);
    history.replace({
      ...history.location,
      search: searchParams.toString(),
    });
    setCurrentTab(tabId);
    onTabChanged && onTabChanged(tabId);
  };

  const backToStatementsClick = (): void => {
    history.push("/sql-activity?tab=Statements&view=fingerprints");
    if (onBackToStatementsClick) {
      onBackToStatementsClick();
    }
  };

  // componentDidMount
  useEffect(() => {
    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(timeScaleRef.current, timeScale1hMinOptions);
    if (ts !== timeScaleRef.current) {
      changeTimeScale(ts);
    }

    refreshStatementDetailsInternalRef.current();

    const resizeHandler = handleResizeRef.current;
    window.addEventListener("resize", resizeHandler);
    handleResizeRef.current();

    // For the first data fetch for this page, we refresh if there are:
    // - Last updated is null (no statement details fetched previously)
    // - The time interval is not custom, i.e. we have a moving window
    // in which case we poll every 5 minutes. For the first fetch we will
    // calculate the next time to refresh based on when the data was last
    // updated.
    if (timeScaleRef.current.key !== "Custom" || !lastUpdatedRef.current) {
      const now = moment();
      const nextRefresh =
        lastUpdatedRef.current?.clone().add(5, "minutes") || now;
      setTimeout(
        () => refreshStatementDetailsInternalRef.current(),
        Math.max(0, nextRefresh.diff(now, "milliseconds")),
      );
    }
    refreshUserSQLRolesRef.current();
    if (!isTenantRef.current) {
      refreshNodesRef.current();
      refreshNodesLivenessRef.current();
    }
    if (!hasViewActivityRedactedRoleRef.current) {
      refreshStatementDiagnosticsRequestsRef.current();
    }

    return () => {
      window.removeEventListener("resize", resizeHandler);
    };
  }, [changeTimeScale]);

  // componentDidUpdate - handle resize and refresh data on prop changes
  useEffect(() => {
    handleResize();

    if (
      prevTimeScaleRef.current !== timeScale ||
      prevStatementFingerprintIDRef.current !== statementFingerprintID ||
      prevLocationRef.current !== location
    ) {
      refreshStatementDetailsInternal();
    }

    if (!isTenant) {
      refreshNodes();
      refreshNodesLiveness();
    }
    if (!hasViewActivityRedactedRole) {
      refreshStatementDiagnosticsRequests();
    }

    // Update refs
    prevTimeScaleRef.current = timeScale;
    prevStatementFingerprintIDRef.current = statementFingerprintID;
    prevLocationRef.current = location;
  }, [
    timeScale,
    statementFingerprintID,
    location,
    handleResize,
    refreshStatementDetailsInternal,
    isTenant,
    refreshNodes,
    refreshNodesLiveness,
    hasViewActivityRedactedRole,
    refreshStatementDiagnosticsRequests,
  ]);

  // Update query state when statementDetails changes
  useEffect(() => {
    const newQuery =
      statementDetails?.statement?.metadata?.query || query || null;
    const newFormattedQuery =
      statementDetails?.statement?.metadata?.formatted_query ||
      formattedQuery ||
      null;
    if (newQuery !== query || newFormattedQuery !== formattedQuery) {
      setQuery(newQuery);
      setFormattedQuery(newFormattedQuery);
    }
  }, [statementDetails, query, formattedQuery]);

  // Invalidate cached query texts when statementFingerprintID changes
  useEffect(() => {
    if (
      prevStatementFingerprintIDRef.current !== statementFingerprintID &&
      prevStatementFingerprintIDRef.current !== undefined
    ) {
      setQuery(null);
      setFormattedQuery(null);
    }
  }, [statementFingerprintID]);

  const renderNoDataTabContent = (): React.ReactElement => (
    <>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
            currentScale={timeScale}
            setTimeScale={changeTimeScale}
          />
        </PageConfigItem>
      </PageConfig>
      <section className={cx("section")}>
        <InlineAlert intent="info" title="No data available." />
      </section>
    </>
  );

  const renderNoDataWithTimeScaleAndSqlBoxTabContent = (
    hasTimeout: boolean,
  ): React.ReactElement => (
    <>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
            currentScale={timeScale}
            setTimeScale={changeTimeScale}
          />
        </PageConfigItem>
      </PageConfig>
      <section className={cx("section")}>
        {formattedQuery && (
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox
                value={formattedQuery}
                size={SqlBoxSize.CUSTOM}
                format={true}
              />
            </Col>
          </Row>
        )}
        {hasTimeout && (
          <InlineAlert
            intent="danger"
            title={LoadingError({
              statsType: "statements",
              error: statementsError,
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

  const renderOverviewTabContent = (
    hasTimeout: boolean,
    hasData: boolean,
  ): React.ReactElement => {
    if (!hasData) {
      return renderNoDataWithTimeScaleAndSqlBoxTabContent(hasTimeout);
    }
    const { stats } = statementDetails.statement;
    const {
      app_names: appNames,
      databases,
      fingerprint_id: fingerprintId,
      full_scan_count: fullScanCount,
      vec_count: vecCount,
      total_count: totalCount,
      implicit_txn: implicitTxn,
    } = statementDetails.statement.metadata;
    const statementStatisticsPerAggregatedTs =
      statementDetails.statement_statistics_per_aggregated_ts;

    const nodes: string[] = unique(
      (stats.nodes || []).map((node: Long) => node.toString()),
    ).sort();
    // TODO(yuzefovich): use kv_node_ids to show KV regions.
    const regions = unique(
      isTenant
        ? stats.regions || []
        : nodes.map(node => nodeRegions[node]).filter(r => r), // Remove undefined / unknown regions.
    ).sort();

    const lastExec = stats.last_exec_timestamp && (
      <Timestamp
        time={TimestampToMoment(stats.last_exec_timestamp)}
        format={DATE_FORMAT_24_TZ}
      />
    );

    const statementSampled = stats.exec_stats.count > Long.fromNumber(0);
    const failureCount = stats.failure_count;
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

    const statsPerAggregatedTs = statementStatisticsPerAggregatedTs.sort(
      (a, b) =>
        a.aggregated_ts.seconds < b.aggregated_ts.seconds
          ? -1
          : a.aggregated_ts.seconds > b.aggregated_ts.seconds
            ? 1
            : 0,
    );

    const executionAndPlanningTimeseries: AlignedData =
      generateExecuteAndPlanningTimeseries(statsPerAggregatedTs);
    const executionAndPlanningOps: Partial<Options> = {
      axes: [{}, { label: "Time Spent" }],
      series: [{}, { label: "Execution" }, { label: "Planning" }],
      width: cardWidth,
    };

    const rowsProcessedTimeseries: AlignedData =
      generateRowsProcessedTimeseries(statsPerAggregatedTs);
    const rowsProcessedOps: Partial<Options> = {
      axes: [{}, { label: "Rows" }],
      series: [{}, { label: "Rows Read" }, { label: "Rows Written" }],
      width: cardWidth,
    };

    const execRetriesTimeseries: AlignedData =
      generateExecRetriesTimeseries(statsPerAggregatedTs);
    const execRetriesOps: Partial<Options> = {
      axes: [{}, { label: "Retries" }],
      series: [{}, { label: "Retries" }],
      legend: { show: false },
      width: cardWidth,
    };

    const execCountTimeseries: AlignedData =
      generateExecCountTimeseries(statsPerAggregatedTs);
    const execCountOps: Partial<Options> = {
      axes: [{}, { label: "Execution Counts" }],
      series: [{}, { label: "Execution Counts" }],
      legend: { show: false },
      width: cardWidth,
    };

    const contentionTimeseries: AlignedData =
      generateContentionTimeseries(statsPerAggregatedTs);
    const contentionOps: Partial<Options> = {
      axes: [{}, { label: "Contention" }],
      series: [{}, { label: "Contention" }],
      legend: { show: false },
      width: cardWidth,
    };

    const cpuTimeseries: AlignedData =
      generateCPUTimeseries(statsPerAggregatedTs);
    const cpuOps: Partial<Options> = {
      axes: [{}, { label: "SQL CPU Time" }],
      series: [{}, { label: "SQL CPU Time" }],
      legend: { show: false },
      width: cardWidth,
    };

    const clientWaitTimeseries: AlignedData =
      generateClientWaitTimeseries(statsPerAggregatedTs);
    const clientWaitOps: Partial<Options> = {
      axes: [{}, { label: "Time Spent" }],
      series: [{}, { label: "Client Wait Time" }],
      legend: { show: false },
      width: cardWidth,
    };

    const insightsColumns = makeInsightsColumns(
      isCockroachCloud,
      hasAdminRole,
      true,
      true,
    );
    const tableData: InsightRecommendation[] = [];
    if (statementFingerprintInsights) {
      const tableDataTypes = new Set<InsightType>();
      statementFingerprintInsights.forEach(insight => {
        const rec = getStmtInsightRecommendations(insight);
        rec.forEach(entry => {
          if (!tableDataTypes.has(entry.type)) {
            tableData.push(entry);
            tableDataTypes.add(entry.type);
          }
        });
      });
    }

    const duration = (v: number) => Duration(v * 1e9);
    const [chartsStart, chartsEnd] = toRoundedDateRange(timeScale);
    const xScale = {
      graphTsStartMillis: chartsStart.valueOf(),
      graphTsEndMillis: chartsEnd.valueOf(),
    } as XScale;

    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={timeScale}
              setTimeScale={changeTimeScale}
            />
          </PageConfigItem>
        </PageConfig>
        <p className={timeScaleStylesCx("time-label", "label-margin")}>
          <TimeScaleLabel
            timeScale={timeScale}
            requestTime={moment(requestTime)}
          />
        </p>
        <section className={cx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox
                value={formattedQuery}
                size={SqlBoxSize.CUSTOM}
                format={true}
              />
            </Col>
          </Row>
          <Row gutter={24} className={cx("margin-left-neg")}>
            <Col className="gutter-row" span={12}>
              <SummaryCard id="first-card" className={cx("summary-card")}>
                {!isTenant && (
                  <SummaryCardItem
                    label="Nodes"
                    value={intersperse<ReactNode>(
                      nodes.map(n => <NodeLink node={n} key={n} />),
                      ", ",
                    )}
                  />
                )}
                <SummaryCardItem
                  label="Regions"
                  value={intersperse<ReactNode>(regions, ", ")}
                />
                <SummaryCardItem label="Database" value={db} />
                <SummaryCardItem
                  label="Application Name"
                  value={intersperse<ReactNode>(
                    appNames.map(a => <AppLink app={a} key={a} />),
                    ", ",
                  )}
                />
                <SummaryCardItem
                  label="Fingerprint ID"
                  value={FixFingerprintHexValue(fingerprintId)}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label="Failure Count"
                  value={Count(failureCount.toNumber())}
                />
                <SummaryCardItem
                  label="Full scan?"
                  value={RenderCount(fullScanCount, totalCount)}
                />
                <SummaryCardItem
                  label="Vectorized execution?"
                  value={RenderCount(vecCount, totalCount)}
                />
                <SummaryCardItem
                  label="Transaction type"
                  value={renderTransactionType(implicitTxn)}
                />
                <SummaryCardItem label="Last execution time" value={lastExec} />
              </SummaryCard>
            </Col>
          </Row>
          <Row gutter={24} className={cx("margin-left-neg")}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label="Statement Time"
                  value={`${formatNumberForDisplay(
                    stats?.service_lat.mean,
                    duration,
                  )}`}
                />
                <span className={summaryCardStylesCx("summary-small-info")}>
                  {`Execution: ${formatNumberForDisplay(
                    stats?.run_lat.mean,
                    duration,
                  )} /
                    Planning:
                    ${formatNumberForDisplay(stats?.plan_lat.mean, duration)}`}
                </span>
                <SummaryCardItem
                  label="Rows Processed"
                  value={`${Count(
                    Number(stats?.rows_read?.mean),
                  )} Reads / ${Count(
                    Number(stats?.rows_written?.mean),
                  )} Writes`}
                />
                <SummaryCardItem
                  label="Execution Retries"
                  value={Count(
                    longToInt(stats?.count) -
                      longToInt(stats?.first_attempt_count),
                  )}
                />
                <SummaryCardItem
                  label="Execution Count"
                  value={Count(longToInt(stats?.count))}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label="Contention Time"
                  value={formatNumberForDisplay(
                    stats?.exec_stats?.contention_time.mean,
                    duration,
                  )}
                />
                <SummaryCardItem
                  label="SQL CPU Time"
                  value={formatNumberForDisplay(
                    stats?.exec_stats?.cpu_sql_nanos.mean,
                    Duration,
                  )}
                />
                <SummaryCardItem
                  label="KV CPU Time"
                  value={formatNumberForDisplay(
                    stats?.kv_cpu_time_nanos?.mean,
                    Duration,
                  )}
                />
                <SummaryCardItem
                  label="Admission Wait Time"
                  value={formatNumberForDisplay(
                    stats?.exec_stats?.admission_wait_time?.mean,
                    Duration,
                  )}
                />
                <SummaryCardItem
                  label="Client Wait Time"
                  value={formatNumberForDisplay(stats?.idle_lat.mean, duration)}
                />
              </SummaryCard>
            </Col>
          </Row>
          {tableData?.length > 0 && (
            <>
              <p
                className={summaryCardStylesCx("summary--card__divider--large")}
              />
              <Row gutter={24}>
                <Col className="gutter-row" span={24}>
                  <InsightsSortedTable
                    columns={insightsColumns}
                    data={tableData}
                    tableWrapperClassName={insightsTableCx("sorted-table")}
                  />
                </Col>
              </Row>
            </>
          )}
          <p className={summaryCardStylesCx("summary--card__divider--large")} />
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Statement Times"
                alignedData={executionAndPlanningTimeseries}
                uPlotOptions={executionAndPlanningOps}
                yAxisUnits={AxisUnits.Duration}
                xScale={xScale}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Rows Processed"
                alignedData={rowsProcessedTimeseries}
                uPlotOptions={rowsProcessedOps}
                yAxisUnits={AxisUnits.Count}
                xScale={xScale}
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
                xScale={xScale}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Execution Count"
                alignedData={execCountTimeseries}
                uPlotOptions={execCountOps}
                yAxisUnits={AxisUnits.Count}
                xScale={xScale}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title={`Contention Time${noSamples}`}
                alignedData={contentionTimeseries}
                uPlotOptions={contentionOps}
                tooltip={unavailableTooltip}
                yAxisUnits={AxisUnits.Duration}
                xScale={xScale}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title={`SQL CPU Time${noSamples}`}
                alignedData={cpuTimeseries}
                uPlotOptions={cpuOps}
                tooltip={unavailableTooltip}
                yAxisUnits={AxisUnits.Duration}
                xScale={xScale}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <BarGraphTimeSeries
                title="Client Wait Time"
                tooltip={
                  <>
                    {"The wait time for this statement on the client. This time measures the time spent waiting " +
                      "for the client to send the statement while holding the transaction open. A high wait time " +
                      "indicates that you should revisit the entire transaction and "}
                    <Anchor
                      href={batchStatements}
                      className={cx("crl-anchor")}
                      target="_blank"
                    >
                      batch your statements
                    </Anchor>
                    {"."}
                  </>
                }
                alignedData={clientWaitTimeseries}
                uPlotOptions={clientWaitOps}
                yAxisUnits={AxisUnits.Duration}
                xScale={xScale}
              />
            </Col>
          </Row>
        </section>
      </>
    );
  };

  const renderExplainPlanTabContent = (
    hasTimeout: boolean,
    hasData: boolean,
  ): React.ReactElement => {
    if (!hasData) {
      return renderNoDataWithTimeScaleAndSqlBoxTabContent(hasTimeout);
    }
    const statementStatisticsPerPlanHash =
      statementDetails.statement_statistics_per_plan_hash;
    const statementStatisticsPerAggregatedTsAndPlanHash =
      statementDetails.statement_statistics_per_aggregated_ts_and_plan_hash;
    const formattedQueryValue =
      statementDetails.statement.metadata.formatted_query;

    // Generate plan distribution data for the chart
    const { alignedData: planDistData, planGists } =
      generatePlanDistributionTimeseries(
        statementStatisticsPerAggregatedTsAndPlanHash || [],
      );

    const planDistOps: Partial<Options> = {
      axes: [{}, { label: "Execution Count" }],
      series: [
        {},
        ...planGists.map(gist => ({
          label: `Plan ${gist}`,
          stroke: undefined, // Let the default color palette handle it
        })),
      ],
    };

    const [chartsStart, chartsEnd] = toRoundedDateRange(timeScale);
    const xScale = {
      graphTsStartMillis: chartsStart.valueOf(),
      graphTsEndMillis: chartsEnd.valueOf(),
    } as XScale;

    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={timeScale}
              setTimeScale={changeTimeScale}
            />
          </PageConfigItem>
        </PageConfig>
        <p className={timeScaleStylesCx("time-label", "label-margin")}>
          Showing explain plans from{" "}
          <span className={timeScaleStylesCx("bold")}>
            <FormattedTimescale
              ts={timeScale}
              requestTime={moment(requestTime)}
            />
          </span>
        </p>
        <section className={cx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox
                value={formattedQueryValue}
                size={SqlBoxSize.CUSTOM}
                format={true}
              />
            </Col>
          </Row>
          <p className={summaryCardStylesCx("summary--card__divider")} />
          {planGists.length > 0 && (
            <>
              <Row gutter={24}>
                <Col className="gutter-row" span={24}>
                  <BarGraphTimeSeries
                    title="Plan Distribution Over Time"
                    tooltip={
                      <>
                        Shows which execution plans were used during each time
                        period. Each color represents a different plan hash.
                        Stacked bars show the total execution count broken down
                        by plan.
                      </>
                    }
                    alignedData={planDistData}
                    uPlotOptions={planDistOps}
                    yAxisUnits={AxisUnits.Count}
                    xScale={xScale}
                  />
                </Col>
              </Row>
            </>
          )}
          <PlanDetails
            statementFingerprintID={statementFingerprintID}
            plans={statementStatisticsPerPlanHash}
            hasAdminRole={hasAdminRole}
          />
        </section>
      </>
    );
  };

  const renderDiagnosticsTabContent = (
    hasData: boolean,
  ): React.ReactElement => {
    if (!hasData && !query) {
      return renderNoDataTabContent();
    }

    const fingerprint =
      statementDetails?.statement?.metadata?.query.length === 0
        ? formattedQuery
        : statementDetails?.statement?.metadata?.query;
    return (
      <DiagnosticsView
        activateDiagnosticsRef={activateDiagnosticsRef}
        diagnosticsReports={diagnosticsReports}
        dismissAlertMessage={dismissStatementDiagnosticsAlertMessage}
        statementFingerprint={fingerprint}
        requestTime={moment(requestTime)}
        onDownloadDiagnosticBundleClick={onDiagnosticBundleDownload}
        onDiagnosticCancelRequestClick={report =>
          onDiagnosticCancelRequest(report)
        }
        showDiagnosticsViewLink={uiConfig?.showStatementDiagnosticsLink}
        onSortingChange={onSortingChange}
        currentScale={timeScale}
        onChangeTimeScale={changeTimeScale}
        planGists={statementDetails.statement.stats.plan_gists}
      />
    );
  };

  const renderTabs = (): React.ReactElement => {
    const hasTimeout = statementsError?.name?.toLowerCase().includes("timeout");
    const hasData = Number(statementDetails?.statement?.stats?.count) > 0;

    return (
      <Tabs
        defaultActiveKey="1"
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
      >
        <TabPane tab="Overview" key="overview">
          {renderOverviewTabContent(hasTimeout, hasData)}
        </TabPane>
        <TabPane tab="Explain Plans" key="explain-plan">
          {renderExplainPlanTabContent(hasTimeout, hasData)}
        </TabPane>
        {!hasViewActivityRedactedRole && (
          <TabPane
            tab={`Diagnostics${
              hasDiagnosticReports()
                ? ` (${
                    filterByTimeScale(diagnosticsReports, timeScale).length
                  })`
                : ""
            }`}
            key="diagnostics"
          >
            {renderDiagnosticsTabContent(hasData)}
          </TabPane>
        )}
      </Tabs>
    );
  };

  const app = queryByName(location, appAttr);
  const longLoadingMessage = isLoading &&
    isNil(statementDetails) &&
    isNil(getValidErrorsList(statementsError)) && (
      <Delayed delay={moment.duration(2, "s")}>
        <InlineAlert
          intent="info"
          title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
        />
      </Delayed>
    );

  const hasTimeout = statementsError?.name?.toLowerCase().includes("timeout");
  const error = hasTimeout ? null : statementsError;

  return (
    <div className={cx("root")}>
      <Helmet title={`Details | ${app ? `${app} App |` : ""} Statements`} />
      <div className={cx("section", "page--header")}>
        <Button
          onClick={backToStatementsClick}
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
          loading={isLoading}
          page={"statement fingerprint"}
          error={error}
          render={renderTabs}
          renderError={() =>
            LoadingError({
              statsType: "statements",
              error: error,
            })
          }
        />
        {longLoadingMessage}
        <ActivateStatementDiagnosticsModal
          ref={activateDiagnosticsRef}
          activate={createStatementDiagnosticsReport}
          refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
          onOpenModal={onDiagnosticsModalOpen}
        />
      </section>
    </div>
  );
}
