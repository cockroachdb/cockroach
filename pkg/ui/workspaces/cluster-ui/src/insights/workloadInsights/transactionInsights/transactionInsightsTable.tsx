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
import { InsightEvent, InsightExecEnum } from "src/insights";
import { InsightCell, insightsTableTitles, QueriesCell } from "../util";
import { Link } from "react-router-dom";

interface TransactionInsightsTable {
  data: InsightEvent[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
}

export function makeTransactionInsightsColumns(): ColumnDescriptor<InsightEvent>[] {
  const execType = InsightExecEnum.TRANSACTION;
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(execType),
      cell: (item: InsightEvent) => (
        <Link to={`/insights/${item.transactionID}`}>
          {String(item.transactionID)}
        </Link>
      ),
      sort: (item: InsightEvent) => item.transactionID,
    },
    {
      name: "fingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: InsightEvent) => String(item.fingerprintID),
      sort: (item: InsightEvent) => item.fingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: InsightEvent) =>
        QueriesCell({ transactionQueries: item.queries, textLimit: 50 }),
      sort: (item: InsightEvent) => item.queries.length,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: InsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: InsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: InsightEvent) => item.startTime.format(DATE_FORMAT),
      sort: (item: InsightEvent) => item.startTime.unix(),
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: InsightEvent) => Duration(item.elapsedTimeMillis * 1e6),
      sort: (item: InsightEvent) => item.elapsedTimeMillis,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: InsightEvent) => item.application,
      sort: (item: InsightEvent) => item.application,
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
