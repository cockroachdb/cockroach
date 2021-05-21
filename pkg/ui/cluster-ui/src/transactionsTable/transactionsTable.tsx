import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { SortedTable, ISortedTablePagination } from "../sortedtable";
import {
  transactionsRetryBarChart,
  transactionsCountBarChart,
  transactionsLatencyBarChart,
  transactionsRowsBarChart,
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

type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface TransactionsTable {
  transactions: Transaction[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  handleDetails: (statementIds: Long[] | null) => void;
  pagination: ISortedTablePagination;
  statements: Statement[];
  search?: string;
  renderNoResult?: React.ReactNode;
}

const { latencyClasses, RowsAffectedClasses } = tableClasses;

export const TransactionsTable: React.FC<TransactionsTable> = props => {
  const { transactions, handleDetails, statements, search } = props;
  const retryBar = transactionsRetryBarChart(transactions);
  const countBar = transactionsCountBarChart(transactions);
  const latencyBar = transactionsLatencyBarChart(
    transactions,
    latencyClasses.barChart,
  );
  const rowsBar = transactionsRowsBarChart(
    transactions,
    RowsAffectedClasses.barChart,
  );
  const columns = [
    {
      name: "transactions",
      title: <>Transactions</>,
      cell: (item: Transaction) =>
        textCell({
          transactionText: collectStatementsText(
            getStatementsById(item.stats_data.statement_ids, statements),
          ),
          transactionIds: item.stats_data.statement_ids,
          handleDetails,
          search,
        }),
      sort: (item: Transaction) =>
        collectStatementsText(
          getStatementsById(item.stats_data.statement_ids, statements),
        ),
    },
    {
      name: "statements",
      title: <>Statements</>,
      cell: (item: Transaction) => item.stats_data.statement_ids.length,
      sort: (item: Transaction) => item.stats_data.statement_ids.length,
    },
    {
      name: "retries",
      title: StatementTableTitle.retries,
      cell: retryBar,
      sort: (item: Transaction) =>
        longToInt(Number(item.stats_data.stats.max_retries)),
    },
    {
      name: "execution count",
      title: StatementTableTitle.executionCount,
      cell: countBar,
      sort: (item: Transaction) => FixLong(Number(item.stats_data.stats.count)),
    },
    {
      name: "rows affected",
      title: StatementTableTitle.rowsAffected,
      cell: rowsBar,
      className: RowsAffectedClasses.column,
      sort: (item: Transaction) => item.stats_data.stats.num_rows.mean,
    },
    {
      name: "latency",
      title: StatementTableTitle.latency,
      cell: latencyBar,
      className: latencyClasses.column,
      sort: (item: Transaction) => item.stats_data.stats.service_lat.mean,
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
