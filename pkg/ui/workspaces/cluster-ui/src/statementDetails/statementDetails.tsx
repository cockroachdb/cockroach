// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
  useMemo,
  useRef,
} from "react";
import { Helmet } from "react-helmet";
import { Link, RouteComponentProps } from "react-router-dom";

import { Anchor } from "src/anchor";
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
  DurationToNumber,
} from "src/util";

import { InsightRecommendation, StatementDiagnosticsReport } from "../api";
import { useNodes } from "../api/nodesApi";
import {
  useStatementDiagnostics,
  useCreateDiagnosticsReport,
  useCancelDiagnosticsReport,
} from "../api/statementDiagnosticsApi";
import { useStatementDetails } from "../api/statementsApi";
import { useStmtFingerprintInsights } from "../api/stmtInsightsApi";
import { useUserSQLRoles } from "../api/userApi";
import { CockroachCloudContext, ClusterDetailsContext } from "../contexts";
import { Delayed } from "../delayed";
import { AxisUnits } from "../graphs";
import { GroupedBarChart, XScale } from "../graphs/groupedBarChart";
import { getStmtInsightRecommendations, InsightType } from "../insights";
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
  generateCanaryVsStableTimeseries,
  generatePlanDistributionTimeseries,
  generateCanaryVsStablePlanDistribution,
} from "./timeseriesUtils";

const { TabPane } = Tabs;

export type StatementDetailsProps = StatementDetailsOwnProps &
  RouteComponentProps<{ statement: string }>;

export interface StatementDetailsState {
  currentTab?: string;

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
  dismissStatementDiagnosticsAlertMessage?: () => void;
  onTabChanged?: (tabName: string) => void;
  onTimeScaleChange: (ts: TimeScale) => void;
  onDiagnosticsModalOpen?: (statementFingerprint: string) => void;
  onDiagnosticBundleDownload?: (statementFingerprint?: string) => void;
  onActivateStatementDiagnosticsAnalytics?: (
    statementFingerprint: string,
  ) => void;
  onDiagnosticCancelRequestTracking?: (
    report: StatementDiagnosticsReport,
  ) => void;
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
  timeScale: TimeScale;
  requestTime: moment.Moment;
}

export type StatementDetailsOwnProps = StatementDetailsDispatchProps &
  StatementDetailsStateProps;

const cx = classNames.bind(styles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);
const insightsTableCx = classNames.bind(insightTableStyles);

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
    timeScale,
    requestTime,
    dismissStatementDiagnosticsAlertMessage,
    onTabChanged,
    onTimeScaleChange,
    onDiagnosticsModalOpen,
    onDiagnosticBundleDownload,
    onActivateStatementDiagnosticsAnalytics,
    onDiagnosticCancelRequestTracking,
    onSortingChange,
    onBackToStatementsClick,
    onRequestTimeChange,
  } = props;

  // SWR data hooks.
  const appNames = queryByName(location, appNamesAttr);
  const {
    data: statementDetails,
    error: statementsError,
    isLoading,
  } = useStatementDetails(statementFingerprintID, appNames, timeScale);

  const { data: diagnosticsData } = useStatementDiagnostics();
  const { createReport } = useCreateDiagnosticsReport();
  const { cancelReport } = useCancelDiagnosticsReport();

  const { nodeRegionsByID: nodeRegions } = useNodes();
  const { data: userRoles } = useUserSQLRoles();
  const { isTenant } = useContext(ClusterDetailsContext);

  const hasAdminRole = userRoles?.roles?.includes("ADMIN") ?? false;
  const hasViewActivityRedactedRole =
    userRoles?.roles?.includes("VIEWACTIVITYREDACTED") ?? false;

  const { data: insightsResp } = useStmtFingerprintInsights(
    statementFingerprintID,
    timeScale,
  );
  const statementFingerprintInsights = insightsResp?.results ?? [];

  const statementFingerprint = statementDetails?.statement?.metadata?.query;
  const diagnosticsReports = useMemo(() => {
    if (
      hasViewActivityRedactedRole ||
      !diagnosticsData ||
      !statementFingerprint
    ) {
      return [];
    }
    return diagnosticsData.filter(
      d => d.statement_fingerprint === statementFingerprint,
    );
  }, [diagnosticsData, statementFingerprint, hasViewActivityRedactedRole]);

  const activateDiagnosticsRef = useRef<ActivateDiagnosticsModalRef>(null);

  const isCockroachCloud = useContext(CockroachCloudContext);

  const getInitialTab = (): string => {
    const searchParams = new URLSearchParams(history.location.search);
    return searchParams.get("tab") || "overview";
  };

  const [currentTab, setCurrentTab] = useState<string>(getInitialTab);
  const [query, setQuery] = useState<string>(
    statementDetails?.statement?.metadata?.query,
  );
  const [formattedQuery, setFormattedQuery] = useState<string>(
    statementDetails?.statement?.metadata?.formatted_query,
  );

  const prevStatementFingerprintIDRef = useRef<string>(statementFingerprintID);

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

  // Validate time scale on mount.
  useEffect(() => {
    const ts = getValidOption(timeScale, timeScale1hMinOptions);
    if (ts !== timeScale) {
      changeTimeScale(ts);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

  // Invalidate cached query texts when statementFingerprintID changes.
  useEffect(() => {
    if (
      prevStatementFingerprintIDRef.current !== statementFingerprintID &&
      prevStatementFingerprintIDRef.current !== undefined
    ) {
      setQuery(null);
      setFormattedQuery(null);
    }
    prevStatementFingerprintIDRef.current = statementFingerprintID;
  }, [statementFingerprintID]);

  // Extract the aggregation interval from the cluster setting so bar
  // widths match it exactly.  The value lives on the summary-level
  // statement entry (same field exists on each per-ts row).
  // DurationToNumber returns seconds; convert to millis.
  const aggregationIntervalMillis = useMemo(() => {
    const dur = statementDetails?.statement?.aggregation_interval;
    const secs = DurationToNumber(dur);
    return secs > 0 ? secs * 1e3 : 0;
  }, [statementDetails]);

  // Shared xScale derived from the user's selected time range.
  const xScale: XScale = useMemo(() => {
    const [chartsStart, chartsEnd] = toRoundedDateRange(timeScale);
    return {
      graphTsStartMillis: chartsStart.valueOf(),
      graphTsEndMillis: chartsEnd.valueOf(),
    };
  }, [timeScale]);

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

    const executionAndPlanningTimeseries =
      generateExecuteAndPlanningTimeseries(statsPerAggregatedTs);
    const rowsProcessedTimeseries =
      generateRowsProcessedTimeseries(statsPerAggregatedTs);
    const execRetriesTimeseries =
      generateExecRetriesTimeseries(statsPerAggregatedTs);
    const execCountTimeseries =
      generateExecCountTimeseries(statsPerAggregatedTs);
    const contentionTimeseries =
      generateContentionTimeseries(statsPerAggregatedTs);
    const cpuTimeseries = generateCPUTimeseries(statsPerAggregatedTs);
    const clientWaitTimeseries =
      generateClientWaitTimeseries(statsPerAggregatedTs);

    const canaryVsStableData =
      generateCanaryVsStableTimeseries(statsPerAggregatedTs);
    const hasCanaryData = canaryVsStableData.length > 0;

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
              <GroupedBarChart
                data={executionAndPlanningTimeseries}
                yAxisUnits={AxisUnits.Duration}
                title="Statement Times"
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={rowsProcessedTimeseries}
                yAxisUnits={AxisUnits.Count}
                title="Rows Processed"
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
          </Row>
          {hasCanaryData && (
            <Row gutter={24}>
              <Col className="gutter-row" span={12}>
                <GroupedBarChart
                  data={canaryVsStableData}
                  yAxisUnits={AxisUnits.Duration}
                  title="Canary vs Stable Statement Times"
                  tooltip={
                    <>
                      Compares planning and execution latency between canary
                      (newest) and stable table statistics.
                    </>
                  }
                  xScale={xScale}
                  aggregationIntervalMillis={aggregationIntervalMillis}
                />
              </Col>
            </Row>
          )}
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={execRetriesTimeseries}
                yAxisUnits={AxisUnits.Count}
                title="Execution Retries"
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={execCountTimeseries}
                yAxisUnits={AxisUnits.Count}
                title="Execution Count"
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={contentionTimeseries}
                yAxisUnits={AxisUnits.Duration}
                title={`Contention Time${noSamples}`}
                tooltip={unavailableTooltip}
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={cpuTimeseries}
                yAxisUnits={AxisUnits.Duration}
                title={`SQL CPU Time${noSamples}`}
                tooltip={unavailableTooltip}
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
              />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <GroupedBarChart
                data={clientWaitTimeseries}
                yAxisUnits={AxisUnits.Duration}
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
                xScale={xScale}
                aggregationIntervalMillis={aggregationIntervalMillis}
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
    const { data: planDistData, planGists } =
      generatePlanDistributionTimeseries(
        statementStatisticsPerAggregatedTsAndPlanHash || [],
      );

    // Compute weighted-average execution latency per plan gist so
    // that the canary vs stable chart can color-code by latency.
    const latencyByGist = new Map<string, number>();
    const countByGist = new Map<string, number>();
    (statementStatisticsPerPlanHash || []).forEach(plan => {
      const gist = plan.stats?.plan_gists?.[0] || "unknown";
      const count = longToInt(plan.stats?.count);
      if (!count || count <= 0) return;
      const lat = plan.stats?.run_lat?.mean ?? 0;
      latencyByGist.set(gist, (latencyByGist.get(gist) || 0) + count * lat);
      countByGist.set(gist, (countByGist.get(gist) || 0) + count);
    });
    latencyByGist.forEach((totalLat, gist) => {
      latencyByGist.set(gist, totalLat / (countByGist.get(gist) || 1));
    });

    const { data: canaryPlanDistData, latencyRange: canaryLatencyRange } =
      generateCanaryVsStablePlanDistribution(
        statementStatisticsPerAggregatedTsAndPlanHash || [],
        latencyByGist,
      );
    const hasCanaryPlanData = canaryPlanDistData.length > 0;

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
            <Row gutter={24}>
              <Col className="gutter-row" span={24}>
                <GroupedBarChart
                  data={planDistData}
                  yAxisUnits={AxisUnits.Count}
                  title="Plan Distribution Over Time"
                  tooltip={
                    <>
                      Shows which execution plans were used during each time
                      period. Each color represents a different plan hash.
                      Stacked bars show the total execution count broken down by
                      plan.
                    </>
                  }
                  xScale={xScale}
                  aggregationIntervalMillis={aggregationIntervalMillis}
                />
              </Col>
            </Row>
          )}
          {hasCanaryPlanData && (
            <Row gutter={24}>
              <Col className="gutter-row" span={24}>
                <div style={{ display: "inline-flex", alignItems: "stretch" }}>
                  <div>
                    <GroupedBarChart
                      data={canaryPlanDistData}
                      yAxisUnits={AxisUnits.Count}
                      title="Canary vs Stable Plan Distribution"
                      tooltip={
                        <>
                          Compares plan distribution between canary (newest
                          table statistics) and stable (second-newest)
                          executions. Left bars show canary execution counts,
                          right bars show stable. Color intensity indicates
                          average execution latency: darker purple = higher
                          latency, lighter purple = lower latency.
                        </>
                      }
                      xScale={xScale}
                      aggregationIntervalMillis={aggregationIntervalMillis}
                    />
                  </div>
                  {canaryLatencyRange && (
                    <div className={cx("latency-legend")}>
                      <span>{Duration(canaryLatencyRange.max * 1e9)}</span>
                      <div className={cx("latency-legend__bar")} />
                      <span>{Duration(canaryLatencyRange.min * 1e9)}</span>
                      <span className={cx("latency-legend__label")}>
                        Avg Latency
                      </span>
                    </div>
                  )}
                </div>
              </Col>
            </Row>
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
        onDiagnosticCancelRequestClick={report => {
          cancelReport({ requestId: report.id }).catch(() => {});
          onDiagnosticCancelRequestTracking?.(report);
        }}
        onSortingChange={onSortingChange}
        currentScale={timeScale}
        onChangeTimeScale={changeTimeScale}
        planGists={statementDetails?.statement?.stats?.plan_gists}
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
          activate={req => {
            createReport(req)
              .then(() =>
                onActivateStatementDiagnosticsAnalytics?.(req.stmtFingerprint),
              )
              .catch(() => {});
          }}
          onOpenModal={onDiagnosticsModalOpen}
        />
      </section>
    </div>
  );
}
