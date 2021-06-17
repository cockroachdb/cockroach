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
import { Pagination } from "../pagination";
import { TableStatistics } from "../tableStatistics";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { Button } from "../button";
import {
  collectStatementsText,
  generateRegionNode,
} from "src/transactionsPage/utils";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";
import { SqlBox } from "../sql";
import { aggregateStatements } from "../transactionsPage/utils";
import Long from "long";
import { Loading } from "../loading";
import { SummaryCard } from "../summaryCard";
import {
  Bytes,
  Duration,
  formatNumberForDisplay,
  calculateTotalWorkload,
} from "src/util";

import summaryCardStyles from "../summaryCard/summaryCard.module.scss";
import transactionDetailsStyles from "./transactionDetails.modules.scss";
import { Col, Row } from "antd";
import { Text, Heading } from "@cockroachlabs/ui-components";
import { formatTwoPlaces } from "../barCharts";
import { ArrowLeft } from "@cockroachlabs/icons";
import {
  populateRegionNodeForStatements,
  makeStatementsColumns,
} from "src/statementsTable/statementsTable";

const { containerClass } = tableClasses;
const cx = classNames.bind(statementsStyles);

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const transactionDetailsStylesCx = classNames.bind(transactionDetailsStyles);

interface TransactionDetailsProps {
  statements?: Statement[];
  nodeRegions: { [nodeId: string]: string };
  transactionStats?: TransactionStats;
  lastReset?: string | Date;
  handleDetails: (
    statementFingerprintIds: Long[] | null,
    transactionStats: TransactionStats | null,
  ) => void;
  error?: Error | null;
  resetSQLStats: () => void;
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

  onChangeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  };

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  render() {
    const {
      statements,
      transactionStats,
      handleDetails,
      error,
      resetSQLStats,
      nodeRegions,
    } = this.props;
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <Button
            onClick={() => handleDetails(null, null)}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
          >
            Transactions
          </Button>
          <h1 className={baseHeadingClasses.tableName}>Transaction Details</h1>
        </section>
        <Loading
          error={error}
          loading={!statements || !transactionStats}
          render={() => {
            const { statements, transactionStats, lastReset } = this.props;
            const { sortSetting, pagination } = this.state;
            const statementsSummary = collectStatementsText(statements);
            const aggregatedStatements = aggregateStatements(statements);
            const totalWorkload = calculateTotalWorkload(statements);
            populateRegionNodeForStatements(aggregatedStatements, nodeRegions);
            const duration = (v: number) => Duration(v * 1e9);
            return (
              <React.Fragment>
                <section className={containerClass}>
                  <Row
                    gutter={16}
                    className={transactionDetailsStylesCx("summary-columns")}
                  >
                    <Col span={16}>
                      <SqlBox
                        value={statementsSummary}
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
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.exec_stats.network_bytes.mean,
                              Bytes,
                            )}
                          </Text>
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Max memory usage</Text>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.exec_stats.max_mem_usage.mean,
                              Bytes,
                            )}
                          </Text>
                        </div>
                        <div
                          className={summaryCardStylesCx("summary--card__item")}
                        >
                          <Text>Max scratch disk usage</Text>
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
                        </div>
                      </SummaryCard>
                    </Col>
                  </Row>
                  <TableStatistics
                    pagination={pagination}
                    totalCount={statements.length}
                    lastReset={lastReset}
                    arrayItemName={"statements for this transaction"}
                    activeFilters={0}
                    resetSQLStats={resetSQLStats}
                  />
                  <div className={cx("table-area")}>
                    <SortedTable
                      data={aggregatedStatements}
                      columns={makeStatementsColumns(
                        aggregatedStatements,
                        "",
                        totalWorkload,
                        nodeRegions,
                        "transactionDetails",
                      )}
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
