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
import { DATE_FORMAT, Duration, limitText } from "src/util";
import { InsightExecEnum, StatementInsightEvent } from "src/insights";
import { InsightCell, insightsTableTitles } from "../util";
import { StatementInsights } from "../../../api";
import { Tooltip } from "@cockroachlabs/ui-components";
import { Link } from "react-router-dom";

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
      cell: (item: StatementInsightEvent) => (
        <Link to={`/insights/statement/${item.statementID}`}>
          {String(item.statementID)}
        </Link>
      ),
      sort: (item: StatementInsightEvent) => item.statementID,
    },
    {
      name: "statementFingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: StatementInsightEvent) => item.statementFingerprintID,
      sort: (item: StatementInsightEvent) => item.statementFingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: StatementInsightEvent) => (
        <Tooltip placement="bottom" content={item.query}>
          {limitText(item.query, 50)}
        </Tooltip>
      ),
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
      name: "userName",
      title: insightsTableTitles.username(execType),
      cell: (item: StatementInsightEvent) => item.username,
      sort: (item: StatementInsightEvent) => item.username,
    },
    {
      name: "applicationName",
      title: insightsTableTitles.applicationName(execType),
      cell: (item: StatementInsightEvent) => item.application,
      sort: (item: StatementInsightEvent) => item.application,
    },
    {
      name: "retries",
      title: insightsTableTitles.numRetries(execType),
      cell: (item: StatementInsightEvent) => item.retries,
      sort: (item: StatementInsightEvent) => item.retries,
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
