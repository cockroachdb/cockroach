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
  SortedTable,
  ISortedTablePagination,
  ColumnDescriptor,
  SortSetting,
} from "src/sortedtable";
import { DATE_FORMAT } from "src/util";
import { InsightExecEnum, TransactionInsight } from "../../types";

import { Link } from "react-router-dom";
import { InsightCell, insightsTableTitles } from "../sqlInsightsTable";

interface TransactionInsightsTable {
  data: TransactionInsight[];
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
}

export function makeTransactionInsightsColumns(): ColumnDescriptor<TransactionInsight>[] {
  const execType = InsightExecEnum.TRANSACTION;
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(execType),
      cell: (item: TransactionInsight) => (
        <Link to={`/insights/${item.executionID}`}>
          {String(item.executionID)}
        </Link>
      ),
      sort: (item: TransactionInsight) => String(item.executionID),
      alwaysShow: true,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: TransactionInsight) => item.query,
      sort: (item: TransactionInsight) => item.query,
      alwaysShow: true,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: TransactionInsight) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: TransactionInsight) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: TransactionInsight) => item.startTime.format(DATE_FORMAT),
      sort: (item: TransactionInsight) => item.startTime.unix(),
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: TransactionInsight) => `${item.elapsedTime} ms`,
      sort: (item: TransactionInsight) => item.elapsedTime,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: TransactionInsight) => item.application,
      sort: (item: TransactionInsight) => item.application,
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
