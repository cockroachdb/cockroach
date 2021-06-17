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
import {
  SortedTable,
  ISortedTablePagination,
  longListWithTooltip,
} from "../sortedtable";
import {
  transactionsCountBarChart,
  transactionsRowsReadBarChart,
  transactionsBytesReadBarChart,
  transactionsLatencyBarChart,
  transactionsContentionBarChart,
  transactionsMaxMemUsageBarChart,
  transactionsNetworkBytesBarChart,
  transactionsRetryBarChart,
} from "./transactionsBarCharts";
import { statisticsTableTitles } from "../statsTableUtil/statsTableUtil";
import { tableClasses } from "./transactionsTableClasses";
import { textCell } from "./transactionsCells";
import { FixLong, longToInt } from "src/util";
import { SortSetting } from "../sortedtable";
import {
  getStatementsByFingerprintId,
  collectStatementsText,
} from "../transactionsPage/utils";
import Long from "long";
import classNames from "classnames/bind";
import statsTablePageStyles from "src/statementsTable/statementsTableContent.module.scss";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;
type CollectedTransactionStatistics = protos.cockroach.sql.ICollectedTransactionStatistics;
type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface TransactionsTable {
  transactions: TransactionInfo[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  handleDetails: (
    statementFingerprintIds: Long[] | null,
    transactionStats: TransactionStats,
  ) => void;
  pagination: ISortedTablePagination;
  statements: Statement[];
  nodeRegions: { [key: string]: string };
  search?: string;
  renderNoResult?: React.ReactNode;
}

export interface TransactionInfo extends Transaction {
  regionNodes: string[];
}

const { latencyClasses } = tableClasses;

const cx = classNames.bind(statsTablePageStyles);

export const TransactionsTable: React.FC<TransactionsTable> = props => {
  const defaultBarChartOptions = {
    classes: {
      root: cx("statements-table__col--bar-chart"),
      label: cx("statements-table__col--bar-chart__label"),
    },
  };
  const sampledExecStatsBarChartOptions = {
    classes: defaultBarChartOptions.classes,
    displayNoSamples: (d: TransactionInfo) => {
      return longToInt(d.stats_data.stats.exec_stats?.count) == 0;
    },
  };

  const { transactions, handleDetails, statements, search } = props;
  const countBar = transactionsCountBarChart(transactions);
  const rowsReadBar = transactionsRowsReadBarChart(
    transactions,
    defaultBarChartOptions,
  );
  const bytesReadBar = transactionsBytesReadBarChart(
    transactions,
    defaultBarChartOptions,
  );
  const latencyBar = transactionsLatencyBarChart(
    transactions,
    latencyClasses.barChart,
  );
  const contentionBar = transactionsContentionBarChart(
    transactions,
    sampledExecStatsBarChartOptions,
  );
  const maxMemUsageBar = transactionsMaxMemUsageBarChart(
    transactions,
    sampledExecStatsBarChartOptions,
  );
  const networkBytesBar = transactionsNetworkBytesBarChart(
    transactions,
    sampledExecStatsBarChartOptions,
  );
  const retryBar = transactionsRetryBarChart(transactions);
  const columns = [
    {
      name: "transactions",
      title: <>Transactions</>,
      cell: (item: TransactionInfo) =>
        textCell({
          transactionText: collectStatementsText(
            getStatementsByFingerprintId(
              item.stats_data.statement_fingerprint_ids,
              statements,
            ),
          ),
          transactionFingerprintIds: item.stats_data.statement_fingerprint_ids,
          transactionStats: item.stats_data.stats,
          handleDetails,
          search,
        }),
      sort: (item: TransactionInfo) =>
        collectStatementsText(
          getStatementsByFingerprintId(
            item.stats_data.statement_fingerprint_ids,
            statements,
          ),
        ),
    },
    {
      name: "execution count",
      title: statisticsTableTitles.executionCount("transaction"),
      cell: countBar,
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.count)),
    },
    {
      name: "rows read",
      title: statisticsTableTitles.rowsRead("transaction"),
      cell: rowsReadBar,
      className: cx("statements-table__col-rows-read"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.rows_read.mean)),
    },
    {
      name: "bytes read",
      title: statisticsTableTitles.bytesRead("transaction"),
      cell: bytesReadBar,
      className: cx("statements-table__col-bytes-read"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.bytes_read.mean)),
    },
    {
      name: "latency",
      title: statisticsTableTitles.time("transaction"),
      cell: latencyBar,
      className: latencyClasses.column,
      sort: (item: TransactionInfo) => item.stats_data.stats.service_lat.mean,
    },
    {
      name: "contention",
      title: statisticsTableTitles.contention("transaction"),
      cell: contentionBar,
      className: cx("statements-table__col-contention"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.contention_time?.mean)),
    },
    {
      name: "max memory",
      title: statisticsTableTitles.maxMemUsage("transaction"),
      cell: maxMemUsageBar,
      className: cx("statements-table__col-max-mem-usage"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.max_mem_usage?.mean)),
    },
    {
      name: "network",
      title: statisticsTableTitles.networkBytes("transaction"),
      cell: networkBytesBar,
      className: cx("statements-table__col-network-bytes"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.network_bytes?.mean)),
    },
    {
      name: "retries",
      title: statisticsTableTitles.retries("transaction"),
      cell: retryBar,
      sort: (item: TransactionInfo) =>
        longToInt(Number(item.stats_data.stats.max_retries)),
    },
    {
      name: "regionNodes",
      title: statisticsTableTitles.regionNodes("transaction"),
      className: cx("statements-table__col-regions"),
      cell: (item: TransactionInfo) => {
        return longListWithTooltip(item.regionNodes.sort().join(", "), 50);
      },
      sort: (item: TransactionInfo) => item.regionNodes.sort().join(", "),
    },
    {
      name: "statements",
      title: <>Statements</>,
      cell: (item: TransactionInfo) =>
        item.stats_data.statement_fingerprint_ids.length,
      sort: (item: TransactionInfo) =>
        item.stats_data.statement_fingerprint_ids.length,
    },
  ];

  return (
    <SortedTable
      data={transactions}
      columns={columns}
      className="statements-table"
      {...props}
    />
  );
};

TransactionsTable.defaultProps = {};
