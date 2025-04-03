// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import React from "react";

import statsTablePageStyles from "src/statementsTable/statementsTableContent.module.scss";
import {
  FixFingerprintHexValue,
  Count,
  FixLong,
  longToInt,
  unset,
  appNamesAttr,
  propsToQueryString,
} from "src/util";

import { BarChartOptions } from "../barCharts/barChartFactory";
import {
  SortedTable,
  ISortedTablePagination,
  longListWithTooltip,
  ColumnDescriptor,
  SortSetting,
} from "../sortedtable";
import { statisticsTableTitles } from "../statsTableUtil/statsTableUtil";
import {
  getStatementsByFingerprintId,
  collectStatementsText,
  statementFingerprintIdsToText,
  statementFingerprintIdsToSummarizedText,
} from "../transactionsPage/utils";

import {
  transactionsCountBarChart,
  transactionsBytesReadBarChart,
  transactionsServiceLatencyBarChart,
  transactionsContentionBarChart,
  transactionsCPUBarChart,
  transactionsMaxMemUsageBarChart,
  transactionsNetworkBytesBarChart,
  transactionsRetryBarChart,
  transactionsCommitLatencyBarChart,
} from "./transactionsBarCharts";
import { transactionLink } from "./transactionsCells";
import { tableClasses } from "./transactionsTableClasses";

export type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

interface TransactionsTable {
  transactions: TransactionInfo[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  columns: ColumnDescriptor<TransactionInfo>[];
}

export interface TransactionInfo extends Transaction {
  regions: string[];
  regionNodes: string[];
}

const { latencyClasses } = tableClasses;

const cx = classNames.bind(statsTablePageStyles);

interface TransactionLinkTargetProps {
  transactionFingerprintId: string;
  application?: string;
}

// TransactionLinkTarget returns the link to the relevant transaction page, given
// the input transaction details.
export const TransactionLinkTarget = (
  props: TransactionLinkTargetProps,
): string => {
  let searchParams = "";
  if (props.application != null) {
    searchParams = propsToQueryString({
      [appNamesAttr]: [props.application],
    });
  }

  return `/transaction/${props.transactionFingerprintId}?${searchParams}`;
};

export function makeTransactionsColumns(
  transactions: TransactionInfo[],
  statements: Statement[],
  isTenant: boolean,
  search?: string,
): ColumnDescriptor<TransactionInfo>[] {
  const defaultBarChartOptions = {
    classes: {
      root: cx("statements-table__col--bar-chart"),
      label: cx("statements-table__col--bar-chart__label"),
    },
  };
  const sampledExecStatsBarChartOptions: BarChartOptions<TransactionInfo> = {
    classes: defaultBarChartOptions.classes,
    displayNoSamples: (d: TransactionInfo) => {
      return longToInt(d.stats_data.stats.exec_stats?.count) === 0;
    },
  };

  const countBar = transactionsCountBarChart(transactions);
  const bytesReadBar = transactionsBytesReadBarChart(
    transactions,
    defaultBarChartOptions,
  );
  const serviceLatencyBar = transactionsServiceLatencyBarChart(
    transactions,
    latencyClasses.barChart,
  );
  const commitLatencyBar = transactionsCommitLatencyBarChart(
    transactions,
    latencyClasses.barChart,
  );
  const contentionBar = transactionsContentionBarChart(
    transactions,
    sampledExecStatsBarChartOptions,
  );
  const cpuBar = transactionsCPUBarChart(
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

  const statType = "transaction";
  return [
    {
      name: "transactions",
      title: statisticsTableTitles.transactions(statType),
      cell: (item: TransactionInfo) =>
        transactionLink({
          transactionText:
            statementFingerprintIdsToText(
              item.stats_data.statement_fingerprint_ids,
              statements,
            ) || "Transaction query unavailable.",
          transactionSummary:
            statementFingerprintIdsToSummarizedText(
              item.stats_data.statement_fingerprint_ids,
              statements,
            ) || "Transaction query unavailable.",
          appName: item.stats_data.app ? item.stats_data.app : unset,
          transactionFingerprintId:
            item.stats_data.transaction_fingerprint_id.toString(),
          search,
        }),
      sort: (item: TransactionInfo) =>
        collectStatementsText(
          getStatementsByFingerprintId(
            item.stats_data.statement_fingerprint_ids,
            statements,
          ),
        ),
      alwaysShow: true,
    },
    {
      name: "executionCount",
      title: statisticsTableTitles.executionCount(statType),
      cell: countBar,
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.count)),
    },
    {
      name: "applicationName",
      title: statisticsTableTitles.applicationName(statType),
      className: cx("statements-table__col-app-name"),
      cell: (item: TransactionInfo) =>
        item.stats_data?.app?.length ? item.stats_data.app : unset,
      sort: (item: TransactionInfo) => item.stats_data?.app,
    },
    {
      name: "rowsProcessed",
      title: statisticsTableTitles.rowsProcessed(statType),
      cell: (item: TransactionInfo) =>
        `${Count(Number(item.stats_data.stats.rows_read.mean))} Reads / ${Count(
          Number(item.stats_data.stats.rows_written?.mean),
        )} Writes`,
      className: cx("statements-table__col-rows-read"),
      sort: (item: TransactionInfo) =>
        FixLong(
          Number(item.stats_data.stats.rows_read.mean) +
            Number(item.stats_data.stats.rows_written?.mean),
        ),
    },
    {
      name: "bytesRead",
      title: statisticsTableTitles.bytesRead(statType),
      cell: bytesReadBar,
      className: cx("statements-table__col-bytes-read"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.bytes_read.mean)),
    },
    {
      name: "time",
      title: statisticsTableTitles.time(statType),
      cell: serviceLatencyBar,
      className: latencyClasses.column,
      sort: (item: TransactionInfo) => item.stats_data.stats.service_lat.mean,
    },
    {
      name: "commitLatency",
      title: statisticsTableTitles.commitLatency(statType),
      cell: commitLatencyBar,
      className: latencyClasses.column,
      sort: (item: TransactionInfo) => item.stats_data.stats.commit_lat.mean,
    },
    {
      name: "contention",
      title: statisticsTableTitles.contention(statType),
      cell: contentionBar,
      className: cx("statements-table__col-contention"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.contention_time?.mean)),
    },
    {
      name: "cpu",
      title: statisticsTableTitles.cpu(statType),
      cell: cpuBar,
      className: cx("statements-table__col-cpu"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.cpu_sql_nanos?.mean)),
    },
    {
      name: "maxMemUsage",
      title: statisticsTableTitles.maxMemUsage(statType),
      cell: maxMemUsageBar,
      className: cx("statements-table__col-max-mem-usage"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.max_mem_usage?.mean)),
    },
    {
      name: "networkBytes",
      title: statisticsTableTitles.networkBytes(statType),
      cell: networkBytesBar,
      className: cx("statements-table__col-network-bytes"),
      sort: (item: TransactionInfo) =>
        FixLong(Number(item.stats_data.stats.exec_stats.network_bytes?.mean)),
    },
    {
      name: "retries",
      title: statisticsTableTitles.retries(statType),
      cell: retryBar,
      sort: (item: TransactionInfo) =>
        longToInt(Number(item.stats_data.stats.max_retries)),
    },
    makeRegionsColumn(isTenant),
    {
      name: "statementsCount",
      title: statisticsTableTitles.statementsCount(statType),
      cell: (item: TransactionInfo) =>
        item.stats_data.statement_fingerprint_ids.length,
      sort: (item: TransactionInfo) =>
        item.stats_data.statement_fingerprint_ids.length,
    },
    {
      name: "transactionFingerprintId",
      title: statisticsTableTitles.transactionFingerprintId(statType),
      cell: (item: TransactionInfo) =>
        FixFingerprintHexValue(
          item.stats_data?.transaction_fingerprint_id.toString(16),
        ),
      sort: (item: TransactionInfo) =>
        item.stats_data?.transaction_fingerprint_id.toString(16),
      showByDefault: false,
    },
  ];
}

function makeRegionsColumn(
  isTenant: boolean,
): ColumnDescriptor<TransactionInfo> {
  if (isTenant) {
    return {
      name: "regions",
      title: statisticsTableTitles.regions("transaction"),
      className: cx("statements-table__col-regions"),
      cell: (item: TransactionInfo) => {
        return longListWithTooltip(item.regions.sort().join(", "), 50);
      },
      sort: (item: TransactionInfo) => item.regions.sort().join(", "),
    };
  } else {
    return {
      name: "regionNodes",
      title: statisticsTableTitles.regionNodes("transaction"),
      className: cx("statements-table__col-regions"),
      cell: (item: TransactionInfo) => {
        return longListWithTooltip(item.regionNodes.sort().join(", "), 50);
      },
      sort: (item: TransactionInfo) => item.regionNodes.sort().join(", "),
    };
  }
}

export const TransactionsTable: React.FC<TransactionsTable> = props => {
  const { transactions, columns } = props;

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
