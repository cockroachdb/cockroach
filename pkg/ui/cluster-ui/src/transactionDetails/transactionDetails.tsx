import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";

import { makeStatementsColumns } from "../statementsTable";
import {
  SortedTable,
  ISortedTablePagination,
  SortSetting,
} from "../sortedtable";
import { Pagination } from "../pagination";
import { TransactionsPageStatistic } from "../transactionsPage/transactionsPageStatistic";
import { baseHeadingClasses } from "../transactionsPage/transactionsPageClasses";
import { Button } from "../button";
import { collectStatementsText } from "src/transactionsPage/utils";
import { tableClasses } from "../transactionsTable/transactionsTableClasses";
import { BackIcon } from "../icon";
import { SqlBox } from "../sql";
import { aggregateStatements } from "../transactionsPage/utils";
import Long from "long";
import { Loading } from "../loading";
import { SummaryCard } from "../summaryCard";
import { Bytes, Duration, formatNumberForDisplay } from "src/util";

import summaryCardStyles from "../summaryCard/summaryCard.module.scss";
import transactionDetailsStyles from "./transactionDetails.modules.scss";
import { Col, Row } from "antd";
import { Text, Heading } from "@cockroachlabs/ui-components";

const { containerClass } = tableClasses;

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

const summaryCardStylesCx = classNames.bind(summaryCardStyles);
const transactionDetailsStylesCx = classNames.bind(transactionDetailsStyles);

interface TransactionDetailsProps {
  statements?: Statement[];
  transactionStats?: TransactionStats;
  lastReset?: string | Date;
  handleDetails: (
    statementIds: Long[] | null,
    transactionStats: TransactionStats | null,
  ) => void;
  error?: Error | null;
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
      sortKey: 2,
      ascending: false,
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
    const { statements, transactionStats, handleDetails, error } = this.props;
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <Button
            onClick={() => handleDetails(null, null)}
            type="unstyled-link"
            size="small"
            icon={BackIcon}
            iconPosition="left"
          >
            All transactions
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
                          <Text type="body-strong">Mean rows/bytes read</Text>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.rows_read.mean,
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
                          <Text type="body-strong">
                            Bytes read over network
                          </Text>
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
                          <Text type="body-strong">Max memory usage</Text>
                          <Text>
                            {formatNumberForDisplay(
                              transactionStats.exec_stats.max_mem_usage.mean,
                              Bytes,
                            )}
                          </Text>
                        </div>
                        {/* TODO(asubiotto): Add temporary disk usage */}
                      </SummaryCard>
                    </Col>
                  </Row>
                  <TransactionsPageStatistic
                    pagination={pagination}
                    totalCount={statements.length}
                    lastReset={lastReset}
                    arrayItemName={"statements for this transaction"}
                    activeFilters={0}
                  />
                  <SortedTable
                    data={aggregatedStatements}
                    columns={makeStatementsColumns(
                      aggregatedStatements,
                      "",
                      "",
                    )}
                    className="statements-table"
                    sortSetting={sortSetting}
                    onChangeSortSetting={this.onChangeSortSetting}
                  />
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
