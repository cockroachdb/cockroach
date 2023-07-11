// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import _ from "lodash";
import { RouteComponentProps } from "react-router-dom";
import { Helmet } from "react-helmet";
import moment from "moment-timezone";

import statementsStyles from "../statementsPage/statementsPage.module.scss";
import {
  SortedTable,
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { InlineAlert, Tooltip } from "@cockroachlabs/ui-components";
import { Pagination } from "../pagination";
import { TableStatistics } from "../tableStatistics";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { Button } from "../button";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";
import { SqlBox } from "../sql";
import { aggregateStatements } from "../transactionsPage/utils";
import { Loading } from "../loading";
import { SummaryCard, SummaryCardItem } from "../summaryCard";
import {
  Bytes,
  calculateTotalWorkload,
  FixFingerprintHexValue,
  Duration,
  formatNumberForDisplay,
  unset,
} from "src/util";
import { UIConfigState } from "../store";
import LoadingError from "../sqlActivity/errorComponent";

import summaryCardStyles from "../summaryCard/summaryCard.module.scss";
import transactionDetailsStyles from "./transactionDetails.modules.scss";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { Text, Heading } from "@cockroachlabs/ui-components";
import { formatTwoPlaces } from "../barCharts";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  populateRegionNodeForStatements,
  makeStatementsColumns,
} from "src/statementsTable/statementsTable";
import { Transaction } from "src/transactionsTable";
import Long from "long";
import {
  createCombinedStmtsRequest,
  InsightRecommendation,
  RequestState,
  SqlStatsResponse,
  StatementsRequest,
  TxnInsightsRequest,
} from "../api";
import {
  getTxnInsightRecommendations,
  InsightType,
  TxnInsightEvent,
} from "../insights";
import {
  getValidOption,
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  timeScaleRangeToObj,
  toRoundedDateRange,
} from "../timeScaleDropdown";

import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import insightTableStyles from "../insightsTable/insightsTable.module.scss";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../insightsTable/insightsTable";
import { CockroachCloudContext } from "../contexts";
import { SqlStatsSortType } from "src/api/statementsApi";
import { FormattedTimescale } from "../timeScaleDropdown/formattedTimeScale";
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

interface TState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
  latestTransactionText: string;
  txnDetails: Transaction | null;
  statements: Statement[] | null;
}

function statementsRequestFromProps(
  props: TransactionDetailsProps,
): StatementsRequest {
  const [start, end] = toRoundedDateRange(props.timeScale);
  return createCombinedStmtsRequest({
    start,
    end,
    limit: props.limit,
    sort: props.reqSortSetting,
  });
}

export class TransactionDetails extends React.Component<
  TransactionDetailsProps,
  TState
> {
  constructor(props: TransactionDetailsProps) {
    super(props);

    const txnDetails = getTxnFromSqlStatsMemoized(
      this.props.txnStatsResp?.data,
      this.props.match,
      this.props.location,
    );

    const stmts = getStatementsForTransaction(
      txnDetails,
      this.props.txnStatsResp?.data?.statements,
    );

    this.state = {
      sortSetting: {
        // Sort by statement latency as default column.
        ascending: false,
        columnTitle: "statementTime",
      },
      pagination: {
        pageSize: 10,
        current: 1,
      },
      latestTransactionText: getTxnQueryString(txnDetails, stmts),
      txnDetails,
      statements: stmts,
    };

    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.changeTimeScale(ts);
    }
  }

  setTxnDetails = (): void => {
    const txnDetails = getTxnFromSqlStatsMemoized(
      this.props.txnStatsResp?.data,
      this.props.match,
      this.props.location,
    );

    const statements = getStatementsForTransaction(
      txnDetails,
      this.props.txnStatsResp?.data?.statements,
    );

    // Only overwrite the transaction text if it is non-null.
    const transactionText =
      getTxnQueryString(txnDetails, statements) ??
      this.state.latestTransactionText;

    // If a new, non-empty-string transaction text is available (derived from the time-frame-specific endpoint
    // response), cache the text.
    if (
      transactionText !== this.state.latestTransactionText ||
      txnDetails !== this.state.txnDetails
    ) {
      this.setState({
        latestTransactionText: transactionText,
        txnDetails,
        statements,
      });
    }
  };

  changeTimeScale = (ts: TimeScale): void => {
    if (this.props.onTimeScaleChange) {
      this.props.onTimeScaleChange(ts);
    }
    this.props.onRequestTimeChange(moment());
  };

  refreshData = (): void => {
    const insightsReq = timeScaleRangeToObj(this.props.timeScale);
    this.props.refreshTransactionInsights(insightsReq);
    const req = statementsRequestFromProps(this.props);
    this.props.refreshData(req);
  };

  componentDidMount(): void {
    if (!this.props.txnStatsResp?.data || !this.props.txnStatsResp?.valid) {
      this.refreshData();
    }
    this.props.refreshUserSQLRoles();
    if (!this.props.isTenant) {
      this.props.refreshNodes();
    }
  }

  componentDidUpdate(prevProps: TransactionDetailsProps): void {
    if (!this.props.isTenant) {
      this.props.refreshNodes();
    }

    if (
      prevProps.transactionFingerprintId !==
        this.props.transactionFingerprintId ||
      prevProps.txnStatsResp !== this.props.txnStatsResp
    ) {
      this.setTxnDetails();
    }

    if (this.props.timeScale !== prevProps.timeScale) {
      // Refresh the data if the time range changes.
      this.refreshData();
    }
  }

  onChangeSortSetting = (ss: SortSetting): void => {
    this.setState({
      sortSetting: ss,
    });
  };

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  backToTransactionsClick = (): void => {
    this.props.history.push("/sql-activity?tab=Transactions&view=fingerprints");
  };

  render(): React.ReactElement {
    const { nodeRegions } = this.props;
    const transaction = this.state.txnDetails;
    const error = this.props.txnStatsResp?.error;
    const { latestTransactionText, statements } = this.state;
    const transactionStats = transaction?.stats_data?.stats;
    const period = (
      <FormattedTimescale
        ts={this.props.timeScale}
        requestTime={moment(this.props.requestTime)}
      />
    );

    return (
      <div>
        <Helmet title={"Details | Transactions"} />
        <section className={baseHeadingClasses.wrapper}>
          <Button
            onClick={this.backToTransactionsClick}
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
              currentScale={this.props.timeScale}
              setTimeScale={this.changeTimeScale}
            />
          </PageConfigItem>
        </PageConfig>
        <p
          className={timeScaleStylesCx("time-label", "label-no-margin-bottom")}
        >
          Showing aggregated stats from{" "}
          <span className={timeScaleStylesCx("bold")}>{period}</span>
        </p>
        <Loading
          error={error}
          page={"transaction details"}
          loading={this.props.txnStatsResp?.inFlight}
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

            const {
              isTenant,
              hasViewActivityRedactedRole,
              transactionInsights,
            } = this.props;
            const { sortSetting, pagination } = this.state;

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
                    This metric is part of the transaction execution and
                    therefore will not be available until it is sampled via
                    tracing.
                  </p>
                }
              >
                <span className={cx("tooltip-info")}>unavailable</span>
              </Tooltip>
            );
            const meanIdleLatency = transactionSampled ? (
              <Text>
                {formatNumberForDisplay(
                  _.get(transactionStats, "idle_lat.mean", 0),
                  duration,
                )}
              </Text>
            ) : (
              unavailableTooltip
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
                  _.get(transactionStats, "exec_stats.max_disk_usage.mean", 0),
                  Bytes,
                )}
              </Text>
            ) : (
              unavailableTooltip
            );

            const isCockroachCloud = useContext(CockroachCloudContext);
            const insightsColumns = makeInsightsColumns(
              isCockroachCloud,
              this.props.hasAdminRole,
              true,
              true,
            );
            const tableData: InsightRecommendation[] = [];
            if (transactionInsights) {
              const tableDataTypes = new Set<InsightType>();
              transactionInsights.forEach(transaction => {
                const rec = getTxnInsightRecommendations(transaction);
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
                          value={
                            transaction?.stats_data?.app?.length > 0
                              ? transaction?.stats_data?.app
                              : unset
                          }
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
                          <Heading type="h5">
                            Transaction resource usage
                          </Heading>
                        </div>
                        <SummaryCardItem
                          label="Idle latency"
                          value={meanIdleLatency}
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
                    arrayItemName={
                      "statement fingerprints for this transaction"
                    }
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
                      onChangeSortSetting={this.onChangeSortSetting}
                      pagination={pagination}
                    />
                  </div>
                </section>
                <Pagination
                  pageSize={pagination.pageSize}
                  current={pagination.current}
                  total={aggregatedStatements.length}
                  onChange={this.onChangePage}
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
}
