// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import { DATE_FORMAT, Duration } from "src/util";
import { InsightExecEnum, TransactionInsightEvent } from "src/insights";
import {
  InsightCell,
  insightsTableTitles,
  QueriesCell,
  TransactionDetailsLink,
} from "../util";
import { Link } from "react-router-dom";
import { TimeScale } from "../../../timeScaleDropdown";

interface TransactionInsightsTable {
  data: TransactionInsightEvent[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
  setTimeScale: (ts: TimeScale) => void;
}

export function makeTransactionInsightsColumns(
  setTimeScale: (ts: TimeScale) => void,
): ColumnDescriptor<TransactionInsightEvent>[] {
  const execType = InsightExecEnum.TRANSACTION;
  return [
    {
      name: "latestExecutionID",
      title: insightsTableTitles.latestExecutionID(execType),
      cell: (item: TransactionInsightEvent) => (
        <Link to={`/insights/transaction/${item.transactionID}`}>
          {String(item.transactionID)}
        </Link>
      ),
      sort: (item: TransactionInsightEvent) => item.transactionID,
    },
    {
      name: "fingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: TransactionInsightEvent) =>
        TransactionDetailsLink(
          item.fingerprintID,
          item.startTime,
          setTimeScale,
        ),
      sort: (item: TransactionInsightEvent) => item.fingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: TransactionInsightEvent) => QueriesCell(item.queries, 50),
      sort: (item: TransactionInsightEvent) => item.queries.length,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: TransactionInsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: TransactionInsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: TransactionInsightEvent) =>
        item.startTime.format(DATE_FORMAT),
      sort: (item: TransactionInsightEvent) => item.startTime.unix(),
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: TransactionInsightEvent) =>
        Duration(item.contentionDuration.asMilliseconds() * 1e6),
      sort: (item: TransactionInsightEvent) =>
        item.contentionDuration.asMilliseconds(),
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: TransactionInsightEvent) => item.application,
      sort: (item: TransactionInsightEvent) => item.application,
    },
  ];
}

export const TransactionInsightsTable: React.FC<
  TransactionInsightsTable
> = props => {
  const columns = makeTransactionInsightsColumns(props.setTimeScale);
  return (
    <SortedTable columns={columns} className="statements-table" {...props} />
  );
};

TransactionInsightsTable.defaultProps = {};
