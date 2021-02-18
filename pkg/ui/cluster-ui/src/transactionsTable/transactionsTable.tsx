import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { SortedTable, ISortedTablePagination } from "../sortedtable";
import {
  transactionsCountBarChart,
  transactionsRowsReadBarChart,
  transactionsBytesReadBarChart,
  transactionsLatencyBarChart,
  transactionsMaxMemUsageBarChart,
  transactionsNetworkBytesBarChart,
  transactionsRetryBarChart,
} from "./transactionsBarCharts";
import { StatementTableTitle } from "../statementsTable/statementsTableContent";
import { longToInt } from "./utils";
import { tableClasses } from "./transactionsTableClasses";
import { textCell } from "./transactionsCells";
import { FixLong } from "src/util";
import { SortSetting } from "../sortedtable";
import {
  getStatementsById,
  collectStatementsText,
} from "../transactionsPage/utils";
import Long from "long";
import classNames from "classnames/bind";
import statementsPageStyles from "src/statementsTable/statementsTableContent.module.scss";

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;
type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface TransactionsTable {
  transactions: Transaction[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  handleDetails: (
    statementIds: Long[] | null,
    transactionStats: TransactionStats,
  ) => void;
  pagination: ISortedTablePagination;
  statements: Statement[];
  search?: string;
  renderNoResult?: React.ReactNode;
}

const { latencyClasses } = tableClasses;

const cx = classNames.bind(statementsPageStyles);

export const TransactionsTable: React.FC<TransactionsTable> = props => {
  const barChartOptions = {
    classes: {
      root: cx("statements-table__col--bar-chart"),
      label: cx("statements-table__col--bar-chart__label"),
    },
  };

  const { transactions, handleDetails, statements, search } = props;
  const countBar = transactionsCountBarChart(transactions);
  const rowsReadBar = transactionsRowsReadBarChart(
    transactions,
    barChartOptions,
  );
  const bytesReadBar = transactionsBytesReadBarChart(
    transactions,
    barChartOptions,
  );
  const latencyBar = transactionsLatencyBarChart(
    transactions,
    latencyClasses.barChart,
  );
  const maxMemUsageBar = transactionsMaxMemUsageBarChart(
    transactions,
    barChartOptions,
  );
  const networkBytesBar = transactionsNetworkBytesBarChart(
    transactions,
    barChartOptions,
  );
  const retryBar = transactionsRetryBarChart(transactions);
  const columns = [
    {
      name: "transactions",
      title: <>transactions</>,
      cell: (item: Transaction) =>
        textCell({
          transactionText: collectStatementsText(
            getStatementsById(item.stats_data.statement_ids, statements),
          ),
          transactionIds: item.stats_data.statement_ids,
          transactionStats: item.stats_data.stats,
          handleDetails,
          search,
        }),
      sort: (item: Transaction) =>
        collectStatementsText(
          getStatementsById(item.stats_data.statement_ids, statements),
        ),
    },
    {
      name: "execution count",
      title: StatementTableTitle.executionCount,
      cell: countBar,
      sort: (item: Transaction) => FixLong(Number(item.stats_data.stats.count)),
    },
    {
      name: "rows read",
      title: StatementTableTitle.rowsRead,
      cell: rowsReadBar,
      className: cx("statements-table__col-rows-read"),
      sort: (item: Transaction) =>
        FixLong(Number(item.stats_data.stats.rows_read.mean)),
    },
    {
      name: "bytes read",
      title: StatementTableTitle.bytesRead,
      cell: bytesReadBar,
      className: cx("statements-table__col-bytes-read"),
      sort: (item: Transaction) =>
        FixLong(Number(item.stats_data.stats.bytes_read.mean)),
    },
    {
      name: "latency",
      title: StatementTableTitle.latency,
      cell: latencyBar,
      className: latencyClasses.column,
      sort: (item: Transaction) => item.stats_data.stats.service_lat.mean,
    },
    {
      name: "max memory",
      title: StatementTableTitle.maxMemUsage,
      cell: maxMemUsageBar,
      className: cx("statements-table__col-max-mem-usage"),
      sort: (item: Transaction) =>
        FixLong(Number(item.stats_data.stats.exec_stats.max_mem_usage?.mean)),
    },
    {
      name: "network",
      title: StatementTableTitle.networkBytes,
      cell: networkBytesBar,
      className: cx("statements-table__col-network-bytes"),
      sort: (item: Transaction) =>
        FixLong(Number(item.stats_data.stats.exec_stats.network_bytes?.mean)),
    },
    {
      name: "retries",
      title: StatementTableTitle.retries,
      cell: retryBar,
      sort: (item: Transaction) =>
        longToInt(Number(item.stats_data.stats.max_retries)),
    },
    {
      name: "statements",
      title: <>statements</>,
      cell: (item: Transaction) => item.stats_data.statement_ids.length,
      sort: (item: Transaction) => item.stats_data.statement_ids.length,
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
