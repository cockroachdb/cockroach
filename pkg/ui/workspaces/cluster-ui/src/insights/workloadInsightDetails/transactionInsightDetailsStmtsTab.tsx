// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Link } from "react-router-dom";

import { Loading } from "src/loading";
import { ColumnDescriptor, SortedTable } from "src/sortedtable";
import {
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT,
  Duration,
  limitText,
} from "src/util";

import { Timestamp, Timezone } from "../../timestamp";
import { InsightsError } from "../insightsErrorComponent";
import { StmtInsightEvent } from "../types";
import { InsightCell } from "../workloadInsights/util/insightCell";

const stmtColumns: ColumnDescriptor<StmtInsightEvent>[] = [
  {
    name: "executionID",
    title: "Execution ID",
    cell: (item: StmtInsightEvent) =>
      item.insights?.length ? (
        <Link to={`/insights/statement/${item.statementExecutionID}`}>
          {item.statementExecutionID}
        </Link>
      ) : (
        item.statementExecutionID
      ),
    sort: (item: StmtInsightEvent) => item.statementExecutionID,
    alwaysShow: true,
  },
  {
    name: "query",
    title: "Statement Execution",
    cell: (item: StmtInsightEvent) => limitText(item.query, 50),
    sort: (item: StmtInsightEvent) => item.query,
  },
  {
    name: "insights",
    title: "Insights",
    cell: (item: StmtInsightEvent) =>
      item.insights?.map(i => InsightCell(i)) ?? "None",
    sort: (item: StmtInsightEvent) =>
      item.insights?.reduce((str, i) => (str += i.label), ""),
  },
  {
    name: "startTime",
    title: (
      <>
        Start Time <Timezone />
      </>
    ),
    cell: (item: StmtInsightEvent) =>
      item.startTime ? (
        <Timestamp
          time={item.startTime}
          format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT}
        />
      ) : (
        <>N/A</>
      ),
    sort: (item: StmtInsightEvent) => item.startTime.unix(),
  },
  {
    name: "endTime",
    title: (
      <>
        End Time <Timezone />
      </>
    ),
    cell: (item: StmtInsightEvent) =>
      item.endTime ? (
        <Timestamp
          time={item.endTime}
          format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT}
        />
      ) : (
        <>N/A</>
      ),
    sort: (item: StmtInsightEvent) => item.endTime.unix(),
  },
  {
    name: "executionTime",
    title: "Execution Time",
    cell: (item: StmtInsightEvent) => Duration(item.elapsedTimeMillis * 1e6),
    sort: (item: StmtInsightEvent) => item.elapsedTimeMillis,
  },
  {
    name: "waitTime",
    title: "Time Spent Waiting",
    cell: (item: StmtInsightEvent) =>
      Duration((item.contentionTime?.asMilliseconds() ?? 0) * 1e6),
    sort: (item: StmtInsightEvent) => item.elapsedTimeMillis,
  },
];

type Props = {
  isLoading: boolean;
  statements: StmtInsightEvent[] | null;
  error: Error;
};

export const TransactionInsightsDetailsStmtsTab: React.FC<Props> = ({
  isLoading,
  error,
  statements,
}) => {
  return (
    <Loading
      loading={isLoading}
      page="Transaction Details | Statements"
      error={error}
      renderError={() => InsightsError(error?.message)}
    >
      <SortedTable columns={stmtColumns} data={statements ?? []} />
    </Loading>
  );
};
