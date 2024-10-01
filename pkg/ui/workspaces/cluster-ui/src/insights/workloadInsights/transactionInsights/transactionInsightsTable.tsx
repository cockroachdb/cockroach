// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link } from "react-router-dom";

import { Badge } from "src/badge";
import {
  InsightExecEnum,
  TransactionStatus,
  TxnInsightEvent,
} from "src/insights";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import {
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ,
  Duration,
} from "src/util";

import { TimeScale } from "../../../timeScaleDropdown";
import { Timestamp } from "../../../timestamp";
import {
  InsightCell,
  insightsTableTitles,
  QueriesCell,
  TransactionDetailsLink,
} from "../util";

function txnStatusToString(status: TransactionStatus) {
  switch (status) {
    case TransactionStatus.COMPLETED:
      return "success";
    case TransactionStatus.FAILED:
      return "danger";
    case TransactionStatus.CANCELLED:
      return "info";
  }
}

interface TransactionInsightsTable {
  data: TxnInsightEvent[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  setTimeScale: (ts: TimeScale) => void;
}

export function makeTransactionInsightsColumns(): ColumnDescriptor<TxnInsightEvent>[] {
  const execType = InsightExecEnum.TRANSACTION;
  return [
    {
      name: "latestExecutionID",
      title: insightsTableTitles.latestExecutionID(execType),
      cell: item => (
        <Link to={`/insights/transaction/${item.transactionExecutionID}`}>
          {String(item.transactionExecutionID)}
        </Link>
      ),
      sort: item => item.transactionExecutionID,
    },
    {
      name: "fingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: item =>
        TransactionDetailsLink(item.transactionFingerprintID, item.application),
      sort: item => item.transactionFingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: item => QueriesCell([item.query], 50),
      sort: item => item.query,
    },
    {
      name: "status",
      title: insightsTableTitles.status(execType),
      cell: item => (
        <Badge text={item.status} status={txnStatusToString(item.status)} />
      ),
      sort: item => item.status,
      showByDefault: true,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: item => item.insights.map(insight => InsightCell(insight)),
      sort: item =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: item =>
        item.startTime ? (
          <Timestamp
            time={item.startTime}
            format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
          />
        ) : (
          <>N/A</>
        ),
      sort: item => item.startTime?.unix() || 0,
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: item =>
        Duration((item.contentionTime?.asMilliseconds() ?? 0) * 1e6),
      sort: item => item.contentionTime?.asMilliseconds() ?? 0,
    },
    {
      name: "cpu",
      title: insightsTableTitles.cpu(execType),
      cell: item => Duration(item.cpuSQLNanos),
      sort: item => item.cpuSQLNanos,
      showByDefault: false,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: item => item.application,
      sort: item => item.application,
    },
  ];
}

export const TransactionInsightsTable: React.FC<
  TransactionInsightsTable
> = props => {
  const columns = makeTransactionInsightsColumns();
  return (
    <SortedTable columns={columns} className="statements-table" {...props} />
  );
};

TransactionInsightsTable.defaultProps = {};
