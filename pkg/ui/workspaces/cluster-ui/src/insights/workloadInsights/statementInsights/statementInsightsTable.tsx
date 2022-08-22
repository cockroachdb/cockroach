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
import { InsightExecEnum, StatementInsightEvent } from "src/insights";
import { InsightCell, insightsTableTitles, QueriesCell } from "../util";
import { StatementInsights } from "../../../api";

interface StatementInsightsTable {
  data: StatementInsights;
  sortSetting: SortSetting;
  onChangeSortSetting: (ss: SortSetting) => void;
  pagination: ISortedTablePagination;
  renderNoResult?: React.ReactNode;
}

export function makeStatementInsightsColumns(): ColumnDescriptor<StatementInsightEvent>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(execType),
      cell: (item: StatementInsightEvent) => String(item.transactionID),
      sort: (item: StatementInsightEvent) => String(item.transactionID),
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: StatementInsightEvent) =>
        QueriesCell({ transactionQueries: [item.query], textLimit: 50 }),
      sort: (item: StatementInsightEvent) => item.query,
    },
    {
      name: "insights",
      title: insightsTableTitles.insights(execType),
      cell: (item: StatementInsightEvent) =>
        item.insights ? item.insights.map(insight => InsightCell(insight)) : "",
      sort: (item: StatementInsightEvent) =>
        item.insights
          ? item.insights.map(insight => insight.label).toString()
          : "",
    },
    {
      name: "startTime",
      title: insightsTableTitles.startTime(execType),
      cell: (item: StatementInsightEvent) => item.startTime.format(DATE_FORMAT),
      sort: (item: StatementInsightEvent) => item.startTime.unix(),
    },
    {
      name: "elapsedTime",
      title: insightsTableTitles.elapsedTime(execType),
      cell: (item: StatementInsightEvent) =>
        Duration(item.elapsedTimeMillis * 1e6),
      sort: (item: StatementInsightEvent) => item.elapsedTimeMillis,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: StatementInsightEvent) => item.application,
      sort: (item: StatementInsightEvent) => item.application,
    },
  ];
}

export const StatementInsightsTable: React.FC<
  StatementInsightsTable
> = props => {
  const columns = makeStatementInsightsColumns();
  return (
    <SortedTable columns={columns} className="statements-table" {...props} />
  );
};

StatementInsightsTable.defaultProps = {};
