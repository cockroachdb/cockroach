// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import _ from "lodash";

import statementsStyles from "../statementsPage/statementsPage.module.scss";
import {
  SortedTable,
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable";
import { Tooltip } from "@cockroachlabs/ui-components";
import { Pagination } from "../pagination";
import { TableStatistics } from "../tableStatistics";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { Button } from "../button";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";
import { SqlBox } from "../sql";
import { aggregateStatements } from "../transactionsPage/utils";
import { Loading } from "../loading";
import { SummaryCard } from "../summaryCard";
import { Bytes, Duration, formatNumberForDisplay } from "src/util";
import { UIConfigState } from "../store";

import summaryCardStyles from "../summaryCard/summaryCard.module.scss";
import transactionDetailsStyles from "./transactionDetails.modules.scss";
import { Col, Row } from "antd";
import { Text, Heading } from "@cockroachlabs/ui-components";
import { formatTwoPlaces } from "../barCharts";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  populateRegionNodeForStatements,
  makeStatementFingerprintColumn,
} from "src/statementsTable/statementsTable";
import { TransactionInfo } from "src/transactionsTable";
import Long from "long";

const { containerClass } = tableClasses;
const cx = classNames.bind(statementsStyles);

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const transactionDetailsStylesCx = classNames.bind(transactionDetailsStyles);

interface TransactionDetailsProps {
  transactionText: string;
  statements?: Statement[];
  nodeRegions: { [nodeId: string]: string };
  transactionStats?: TransactionStats;
  lastReset?: string | Date;
  handleDetails: (txn?: TransactionInfo) => void;
  error?: Error | null;
  resetSQLStats: () => void;
  isTenant: UIConfigState["isTenant"];
}

interface TState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
}

export class TransactionDetails extends React.Component<
  TransactionDetailsProps,
  TState
> {
  state: TState = {
    sortSetting: {
      // Sort by statement latency as default column.
      ascending: false,
      columnTitle: "statementTime",
    },
    pagination: {
      pageSize: 10,
      current: 1,
    },
  };

  static defaultProps: Partial<TransactionDetailsProps> = {
    isTenant: false,
  };

  onChangeSortSetting = (ss: SortSetting): void => {
    this.setState({
      sortSetting: ss,
    });
  };

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  render(): React.ReactElement {
    const {
      transactionText,
      statements,
      transactionStats,
      handleDetails,
      error,
      nodeRegions,
    } = this.props;
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <Button
            onClick={() => handleDetails()}
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
        <Loading
          error={error}
          loading={!statements || !transactionStats}
          render={() => {
            const { statements, transactionStats, isTenant } = this.props;
            const { sortSetting, pagination } = this.state;
            const aggregatedStatements = aggregateStatements(statements);
            populateRegionNodeForStatements(
              aggregatedStatements,
              nodeRegions,
              isTenant,
            );
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

            return (
              <React.Fragment>
                <section className={containerClass}>
                  <Row
                    gutter={16}
                    className={transactionDetailsStylesCx("summary-columns")}
                  >
                    <Col span={16}>
                      <SqlBox
                        value={transactionText}
                        className={transactionDetailsStylesCx("summary-card")}
                      />
                    </Col>
                    <Col span={8}>
                      <SummaryCard
                        className={transactionDetailsStylesCx("summary-card")}
                      >
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Heading type="h5">Mean transaction time</Heading>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.service_lat.mean,
                              duration,
                            )}
                          </Text>
                        </div>
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
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Mean rows/bytes read</Text>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.rows_read.mean,
                              formatTwoPlaces,
                            )}
                            {" / "}
                            {formatNumberForDisplay(
                              transactionStats.bytes_read.mean,
                              Bytes,
                            )}
                          </Text>
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Bytes read over network</Text>
                          {transactionSampled && (
                            <Text>
                              {formatNumberForDisplay(
                                transactionStats.exec_stats.network_bytes.mean,
                                Bytes,
                              )}
                            </Text>
                          )}
                          {unavailableTooltip}
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Mean rows written</Text>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.rows_written?.mean,
                              formatTwoPlaces,
                            )}
                          </Text>
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Max memory usage</Text>
                          {transactionSampled && (
                            <Text>
                              {formatNumberForDisplay(
                                transactionStats.exec_stats.max_mem_usage.mean,
                                Bytes,
                              )}
                            </Text>
                          )}
                          {unavailableTooltip}
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Max scratch disk usage</Text>
                          {transactionSampled && (
                            <Text>
                              {formatNumberForDisplay(
                                _.get(
                                  transactionStats,
                                  "exec_stats.max_disk_usage.mean",
                                  0,
                                ),
                                Bytes,
                              )}
                            </Text>
                          )}
                          {unavailableTooltip}
                        </div>
                      </SummaryCard>
                    </Col>
                  </Row>
                  <TableStatistics
                    pagination={pagination}
                    totalCount={statements.length}
                    arrayItemName={
                      "statement fingerprints for this transaction"
                    }
                    activeFilters={0}
                  />
                  <div className={cx("table-area")}>
                    <SortedTable
                      data={aggregatedStatements}
                      columns={[
                        makeStatementFingerprintColumn(
                          "transactionDetails",
                          "",
                        ),
                      ]}
                      className={cx("statements-table")}
                      sortSetting={sortSetting}
                      onChangeSortSetting={this.onChangeSortSetting}
                    />
                  </div>
                </section>
                <Pagination
                  pageSize={pagination.pageSize}
                  current={pagination.current}
                  total={statements.length}
                  onChange={this.onChangePage}
                />
              </React.Fragment>
            );
          }}
        />
      </div>
    );
  }
}
