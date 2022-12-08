// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { ColumnDescriptor, SortedTable } from "src/sortedtable";
import { StatementInsightEvent, TxnInsightDetails } from "../types";
import { InsightCell } from "../workloadInsights/util/insightCell";
import { getStmtInsightsApi } from "src/api/stmtInsightsApi";
import {
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT,
  Duration,
  limitText,
} from "src/util";

const stmtColumns: ColumnDescriptor<StatementInsightEvent>[] = [
  {
    name: "executionID",
    title: "Execution ID",
    cell: (item: StatementInsightEvent) =>
      item.insights?.length ? (
        <Link to={`/insights/statement/${item.statementExecutionID}`}>
          {item.statementExecutionID}
        </Link>
      ) : (
        item.statementExecutionID
      ),
    sort: (item: StatementInsightEvent) => item.statementExecutionID,
    alwaysShow: true,
  },
  {
    name: "query",
    title: "Statement Execution",
    cell: (item: StatementInsightEvent) => limitText(item.query, 50),
    sort: (item: StatementInsightEvent) => item.query,
  },
  {
    name: "insights",
    title: "Insights",
    cell: (item: StatementInsightEvent) =>
      item.insights?.map(i => InsightCell(i)) ?? "None",
    sort: (item: StatementInsightEvent) =>
      item.insights?.reduce((str, i) => (str += i.label), ""),
  },
  {
    name: "startTime",
    title: "Start Time (UTC)",
    cell: (item: StatementInsightEvent) =>
      item.startTime?.format(DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT),
    sort: (item: StatementInsightEvent) => item.startTime.unix(),
  },
  {
    name: "endTime",
    title: "End Time (UTC)",
    cell: (item: StatementInsightEvent) =>
      item.endTime?.format(DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT),
    sort: (item: StatementInsightEvent) => item.endTime.unix(),
  },
  {
    name: "executionTime",
    title: "Execution Time",
    cell: (item: StatementInsightEvent) =>
      Duration(item.elapsedTimeMillis * 1e6),
    sort: (item: StatementInsightEvent) => item.elapsedTimeMillis,
  },
  {
    name: "waitTime",
    title: "Time Spent Waiting",
    cell: (item: StatementInsightEvent) =>
      Duration((item.contentionTime?.asMilliseconds() ?? 0) * 1e6),
    sort: (item: StatementInsightEvent) => item.elapsedTimeMillis,
  },
];

type Props = {
  insightDetails: TxnInsightDetails;
};

export const TransactionInsightsDetailsStmtsTab: React.FC<Props> = ({
  insightDetails,
}) => {
  return <SortedTable columns={stmtColumns} data={insightDetails.statements} />;
};
