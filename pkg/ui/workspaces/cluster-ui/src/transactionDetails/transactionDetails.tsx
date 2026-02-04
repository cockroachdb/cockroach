// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  InlineAlert,
  Tooltip,
  Text,
  Heading,
} from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import get from "lodash/get";
import Long from "long";
import moment from "moment-timezone";
import React, { useContext, useState, useEffect, useCallback } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { SqlStatsSortType } from "src/api/statementsApi";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  populateRegionNodeForStatements,
  makeStatementsColumns,
} from "src/statementsTable/statementsTable";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { Transaction } from "src/transactionsTable";
import {
  Bytes,
  calculateTotalWorkload,
  FixFingerprintHexValue,
  Duration,
  formatNumberForDisplay,
  queryByName,
  appNamesAttr,
  unset,
} from "src/util";

import {
  createCombinedStmtsRequest,
  InsightRecommendation,
  RequestState,
  SqlStatsResponse,
  StatementsRequest,
  TxnInsightsRequest,
} from "../api";
import { formatTwoPlaces } from "../barCharts";
import { Button } from "../button";
import { CockroachCloudContext } from "../contexts";
import {
  getTxnInsightRecommendations,
  InsightType,
  TxnInsightEvent,
} from "../insights";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../insightsTable/insightsTable";
import insightTableStyles from "../insightsTable/insightsTable.module.scss";
import { Loading } from "../loading";
import { Pagination } from "../pagination";
import {
  SortedTable,
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable";
import { SqlBox } from "../sql";
import LoadingError from "../sqlActivity/errorComponent";
import statementsStyles from "../statementsPage/statementsPage.module.scss";
import { UIConfigState } from "../store";
import { SummaryCard, SummaryCardItem } from "../summaryCard";
import summaryCardStyles from "../summaryCard/summaryCard.module.scss";
import { TableStatistics } from "../tableStatistics";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  timeScaleRangeToObj,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { aggregateStatements } from "../transactionsPage/utils";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";

import transactionDetailsStyles from "./transactionDetails.modules.scss";
import {
  getStatementsForTransaction,
  getTxnFromSqlStatsMemoized,
  getTxnQueryString,
} from "./transactionDetailsUtils";

const { containerClass } = tableClasses;
const cx = classNames.bind(statementsStyles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);
const insightsTableCx = classNames.bind(insightTableStyles);

type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const transactionDetailsStylesCx = classNames.bind(transactionDetailsStyles);

export interface TransactionDetailsStateProps {
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  isTenant: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  nodeRegions: { [nodeId: string]: string };
  transactionFingerprintId: string;
  transactionInsights: TxnInsightEvent[];
  hasAdminRole?: UIConfigState["hasAdminRole"];
  txnStatsResp: RequestState<SqlStatsResponse>;
  requestTime: moment.Moment;
}

export interface TransactionDetailsDispatchProps {
  refreshData: (req?: StatementsRequest) => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  refreshTransactionInsights: (req: TxnInsightsRequest) => void;
  onTimeScaleChange: (ts: TimeScale) => void;
  onRequestTimeChange: (t: moment.Moment) => void;
}

export type TransactionDetailsProps = TransactionDetailsStateProps &
  TransactionDetailsDispatchProps &
  RouteComponentProps;

export function TransactionDetails(
  props: TransactionDetailsProps,
): React.ReactElement {
  const {
    timeScale,
    limit,
    reqSortSetting,
    isTenant,
    hasViewActivityRedactedRole,
    nodeRegions,
    transactionFingerprintId,
    transactionInsights,
    hasAdminRole,
    txnStatsResp,
    requestTime,
    refreshData,
    refreshNodes,
    refreshUserSQLRoles,
    refreshTransactionInsights,
    onTimeScaleChange,
    onRequestTimeChange,
    location,
    match,
    history,
  } = props;

  const isCockroachCloud = useContext(CockroachCloudContext);

  // Initialize state from props
  const getInitialState = useCallback(() => {
    const appsAsStr = queryByName(location, appNamesAttr) || null;
    const txnDetails = getTxnFromSqlStatsMemoized(
      txnStatsResp?.data,
      match,
      appsAsStr,
    );
    const apps = appsAsStr?.split(",").map(s => s.trim());
    const stmts = getStatementsForTransaction(
      txnDetails?.stats_data?.transaction_fingerprint_id.toString(),
      apps,
      txnStatsResp?.data?.statements,
    );
    return {
      txnDetails,
      statements: stmts,
      latestTransactionText: getTxnQueryString(txnDetails, stmts),
      appsAsStr,
    };
  }, [location, txnStatsResp?.data, match]);

  const initialState = getInitialState();

  const [sortSetting, setSortSetting] = useState<SortSetting>({
    // Sort by statement latency as default column.
    ascending: false,
    columnTitle: "statementTime",
  });

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    pageSize: 10,
    current: 1,
  });

  const [latestTransactionText, setLatestTransactionText] = useState<string>(
    initialState.latestTransactionText,
  );
  const [txnDetails, setTxnDetails] = useState<Transaction | null>(
    initialState.txnDetails,
  );
  const [statements, setStatements] = useState<Statement[] | null>(
    initialState.statements,
  );

  const changeTimeScale = useCallback(
    (ts: TimeScale): void => {
      if (onTimeScaleChange) {
        onTimeScaleChange(ts);
      }
      onRequestTimeChange(moment());
    },
    [onTimeScaleChange, onRequestTimeChange],
  );

  const doRefreshData = useCallback((): void => {
    const insightsReq = timeScaleRangeToObj(timeScale);
    refreshTransactionInsights(insightsReq);
    const [start, end] = toRoundedDateRange(timeScale);
    const req = createCombinedStmtsRequest({
      start,
      end,
      limit,
      sort: reqSortSetting,
    });
    refreshData(req);
  }, [
    timeScale,
    limit,
    reqSortSetting,
    refreshData,
    refreshTransactionInsights,
  ]);

  // Update txnDetails when props change
  const updateTxnDetails = useCallback((): void => {
    const newAppsAsStr = queryByName(location, appNamesAttr) || null;
    const newTxnDetails = getTxnFromSqlStatsMemoized(
      txnStatsResp?.data,
      match,
      newAppsAsStr,
    );

    const newStatements = getStatementsForTransaction(
      newTxnDetails?.stats_data?.transaction_fingerprint_id.toString(),
      newAppsAsStr?.split(",").map(s => s.trim()),
      txnStatsResp?.data?.statements,
    );

    // Only overwrite the transaction text if it is non-null.
    const transactionText =
      getTxnQueryString(newTxnDetails, newStatements) ?? latestTransactionText;

    // If a new, non-empty-string transaction text is available, cache the text.
    if (
      transactionText !== latestTransactionText ||
      newTxnDetails !== txnDetails
    ) {
      setLatestTransactionText(transactionText);
      setTxnDetails(newTxnDetails);
      setStatements(newStatements);
    }
  }, [location, txnStatsResp?.data, match, latestTransactionText, txnDetails]);

  // componentDidMount equivalent
  useEffect(() => {
    if (!txnStatsResp?.data || !txnStatsResp?.valid) {
      doRefreshData();
    }
    refreshUserSQLRoles();
    if (!isTenant) {
      refreshNodes();
    }

    // Validate timeScale and update if needed
    const ts = getValidOption(timeScale, timeScale1hMinOptions);
    if (ts !== timeScale) {
      changeTimeScale(ts);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Track previous values for componentDidUpdate logic
  const prevTransactionFingerprintIdRef = React.useRef(
    transactionFingerprintId,
  );
  const prevTxnStatsRespRef = React.useRef(txnStatsResp);
  const prevLocationSearchRef = React.useRef(location?.search);
  const prevTimeScaleRef = React.useRef(timeScale);

  useEffect(() => {
    if (!isTenant) {
      refreshNodes();
    }

    // Check if we need to update txnDetails
    if (
      prevTransactionFingerprintIdRef.current !== transactionFingerprintId ||
      prevTxnStatsRespRef.current !== txnStatsResp ||
      prevLocationSearchRef.current !== location?.search
    ) {
      updateTxnDetails();
    }

    // Refresh data if time scale changes
    if (prevTimeScaleRef.current !== timeScale) {
      doRefreshData();
    }

    // Update refs
    prevTransactionFingerprintIdRef.current = transactionFingerprintId;
    prevTxnStatsRespRef.current = txnStatsResp;
    prevLocationSearchRef.current = location?.search;
    prevTimeScaleRef.current = timeScale;
  }, [
    isTenant,
    refreshNodes,
    transactionFingerprintId,
    txnStatsResp,
    location?.search,
    timeScale,
    updateTxnDetails,
    doRefreshData,
  ]);

  const onChangeSortSetting = useCallback((ss: SortSetting): void => {
    setSortSetting(ss);
  }, []);

  const onChangePage = useCallback(
    (current: number, pageSize: number): void => {
      setPagination(prev => ({ ...prev, current, pageSize }));
    },
    [],
  );

  const backToTransactionsClick = useCallback((): void => {
    history.push("/sql-activity?tab=Transactions&view=fingerprints");
  }, [history]);

  const transaction = txnDetails;
  const error = txnStatsResp?.error;
  const transactionStats = transaction?.stats_data?.stats;
  const visibleApps = Array.from(
    new Set(
      statements?.map(s => (s.key.key_data.app ? s.key.key_data.app : unset)) ??
        [],
    ),
  )?.join(", ");

  return (
    <div>
      <Helmet title={"Details | Transactions"} />
      <section className={baseHeadingClasses.wrapper}>
        <Button
          onClick={backToTransactionsClick}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className="small-margin"
        >
          Transactions
        </Button>
        <h3 className={baseHeadingClasses.tableName}>Transaction Details</h3>
      </section>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            options={timeScale1hMinOptions}
            currentScale={timeScale}
            setTimeScale={changeTimeScale}
          />
        </PageConfigItem>
      </PageConfig>
      <p className={timeScaleStylesCx("time-label", "label-no-margin-bottom")}>
        <TimeScaleLabel
          timeScale={timeScale}
          requestTime={moment(requestTime)}
        />
      </p>
      <Loading
        error={error}
        page={"transaction details"}
        loading={txnStatsResp?.inFlight}
        render={() => {
          if (!transaction) {
            return (
              <section className={containerClass}>
                {latestTransactionText && (
                  <Row
                    gutter={16}
                    className={transactionDetailsStylesCx("summary-columns")}
                  >
                    <Col span={16}>
                      <SqlBox
                        value={latestTransactionText}
                        className={transactionDetailsStylesCx("summary-card")}
                        format={true}
                      />
                    </Col>
                  </Row>
                )}
                <InlineAlert
                  intent="info"
                  title="Data not available for this time frame. Select a different time frame."
                />
              </section>
            );
          }

          const aggregatedStatements = aggregateStatements(statements);
          populateRegionNodeForStatements(aggregatedStatements, nodeRegions);
          const duration = (v: number) => Duration(v * 1e9);

          const transactionSampled =
            transactionStats.exec_stats.count > Long.fromNumber(0);
          const unavailableTooltip = !transactionSampled && (
            <Tooltip
              placement="bottom"
              style="default"
              content={
                <p>
                  This metric is part of the transaction execution and therefore
                  will not be available until it is sampled via tracing.
                </p>
              }
            >
              <span className={cx("tooltip-info")}>unavailable</span>
            </Tooltip>
          );
          const meanIdleLatency = (
            <Text>
              {formatNumberForDisplay(
                get(transactionStats, "idle_lat.mean", 0),
                duration,
              )}
            </Text>
          );
          const meanCommitLatency = (
            <Text>
              {formatNumberForDisplay(
                get(transactionStats, "commit_lat.mean", 0),
                duration,
              )}
            </Text>
          );
          const meansRows = `${formatNumberForDisplay(
            transactionStats.rows_read.mean,
            formatTwoPlaces,
          )} /
            ${formatNumberForDisplay(transactionStats.bytes_read.mean, Bytes)}`;
          const bytesRead = transactionSampled ? (
            <Text>
              {formatNumberForDisplay(
                transactionStats.exec_stats.network_bytes.mean,
                Bytes,
              )}
            </Text>
          ) : (
            unavailableTooltip
          );
          const maxMem = transactionSampled ? (
            <Text>
              {formatNumberForDisplay(
                transactionStats.exec_stats.max_mem_usage.mean,
                Bytes,
              )}
            </Text>
          ) : (
            unavailableTooltip
          );
          const maxDisc = transactionSampled ? (
            <Text>
              {formatNumberForDisplay(
                get(transactionStats, "exec_stats.max_disk_usage.mean", 0),
                Bytes,
              )}
            </Text>
          ) : (
            unavailableTooltip
          );

          const insightsColumns = makeInsightsColumns(
            isCockroachCloud,
            hasAdminRole,
            true,
            true,
          );
          const tableData: InsightRecommendation[] = [];
          if (transactionInsights) {
            const tableDataTypes = new Set<InsightType>();
            transactionInsights.forEach(txn => {
              const rec = getTxnInsightRecommendations(txn);
              rec.forEach(entry => {
                if (!tableDataTypes.has(entry.type)) {
                  tableData.push(entry);
                  tableDataTypes.add(entry.type);
                }
              });
            });
          }

          return (
            <React.Fragment>
              <section className={containerClass}>
                <Row
                  gutter={16}
                  className={transactionDetailsStylesCx("summary-columns")}
                >
                  <Col span={16}>
                    <SqlBox
                      value={latestTransactionText}
                      className={transactionDetailsStylesCx("summary-card")}
                      format={true}
                    />
                  </Col>
                  <Col span={8}>
                    <SummaryCard
                      className={transactionDetailsStylesCx("summary-card")}
                    >
                      <SummaryCardItem
                        label="Mean transaction time"
                        value={formatNumberForDisplay(
                          transactionStats?.service_lat.mean,
                          duration,
                        )}
                      />
                      <SummaryCardItem
                        label="Application name"
                        value={visibleApps}
                      />
                      <SummaryCardItem
                        label="Fingerprint ID"
                        value={FixFingerprintHexValue(
                          transaction?.stats_data.transaction_fingerprint_id.toString(
                            16,
                          ),
                        )}
                      />
                      <p
                        className={summaryCardStylesCx(
                          "summary--card__divider",
                        )}
                      />
                      <div
                        className={summaryCardStylesCx("summary--card__item")}
                      >
                        <Heading type="h5">Transaction resource usage</Heading>
                      </div>
                      <SummaryCardItem
                        label="Idle latency"
                        value={meanIdleLatency}
                      />
                      <SummaryCardItem
                        label="Commit latency"
                        value={meanCommitLatency}
                      />
                      <SummaryCardItem
                        label="Mean rows/bytes read"
                        value={meansRows}
                      />
                      <SummaryCardItem
                        label="Bytes read over network"
                        value={bytesRead}
                      />
                      <SummaryCardItem
                        label="Mean rows written"
                        value={formatNumberForDisplay(
                          transactionStats.rows_written?.mean,
                          formatTwoPlaces,
                        )}
                      />
                      <SummaryCardItem
                        label="Max memory usage"
                        value={maxMem}
                      />
                      <SummaryCardItem
                        label="Max scratch disk usage"
                        value={maxDisc}
                      />
                    </SummaryCard>
                  </Col>
                </Row>
                {tableData?.length > 0 && (
                  <>
                    <p
                      className={summaryCardStylesCx(
                        "summary--card__divider--large",
                      )}
                    />
                    <Row gutter={24}>
                      <Col className="gutter-row" span={24}>
                        <InsightsSortedTable
                          columns={insightsColumns}
                          data={tableData}
                          tableWrapperClassName={insightsTableCx(
                            "sorted-table",
                          )}
                        />
                      </Col>
                    </Row>
                  </>
                )}
                <p
                  className={summaryCardStylesCx(
                    "summary--card__divider--large",
                  )}
                />
                <TableStatistics
                  pagination={pagination}
                  totalCount={aggregatedStatements.length}
                  arrayItemName={"statement fingerprints for this transaction"}
                  activeFilters={0}
                />
                <div className={cx("table-area")}>
                  <SortedTable
                    data={aggregatedStatements}
                    columns={makeStatementsColumns(
                      aggregatedStatements,
                      [],
                      calculateTotalWorkload(aggregatedStatements),
                      "transactionDetails",
                      isTenant,
                      hasViewActivityRedactedRole,
                    ).filter(c => !(isTenant && c.hideIfTenant))}
                    className={cx("statements-table")}
                    sortSetting={sortSetting}
                    onChangeSortSetting={onChangeSortSetting}
                    pagination={pagination}
                  />
                </div>
              </section>
              <Pagination
                pageSize={pagination.pageSize}
                current={pagination.current}
                total={aggregatedStatements.length}
                onChange={onChangePage}
                onShowSizeChange={onChangePage}
              />
            </React.Fragment>
          );
        }}
        renderError={() =>
          LoadingError({
            statsType: "transactions",
            error: error,
          })
        }
      />
    </div>
  );
}
