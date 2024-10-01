// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";

import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import { DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT, Duration } from "src/util";

import { TimeScale } from "../../timeScaleDropdown";
import { Timestamp } from "../../timestamp";
import { ContentionDetails, ContentionEvent, InsightExecEnum } from "../types";
import {
  insightsTableTitles,
  QueriesCell,
  StatementDetailsLink,
  TransactionDetailsLink,
} from "../workloadInsights/util";

interface InsightDetailsTableProps {
  data: ContentionEvent[];
  execType: InsightExecEnum;
  setTimeScale?: (tw: TimeScale) => void;
}

export function makeInsightDetailsColumns(
  execType: InsightExecEnum,
): ColumnDescriptor<ContentionEvent>[] {
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(execType),
      cell: (item: ContentionEvent) => String(item.executionID),
      sort: (item: ContentionEvent) => item.executionID,
    },
    {
      name: "fingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: ContentionEvent) =>
        TransactionDetailsLink(item.fingerprintID),
      sort: (item: ContentionEvent) => item.fingerprintID,
    },
    {
      name: "waitingStmtId",
      title: insightsTableTitles.waitingID(InsightExecEnum.STATEMENT),
      cell: (item: ContentionEvent) => String(item.waitingStmtID),
      sort: (item: ContentionEvent) => item.waitingStmtID,
    },
    {
      name: "waitingStmtFingerprintID",
      title: insightsTableTitles.waitingFingerprintID(
        InsightExecEnum.STATEMENT,
      ),
      cell: (item: ContentionEvent) =>
        item.stmtInsightEvent
          ? StatementDetailsLink(item.stmtInsightEvent)
          : item.waitingStmtFingerprintID,
      sort: (item: ContentionEvent) => item.waitingStmtFingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: ContentionEvent) => QueriesCell(item?.queries, 50),
      sort: (item: ContentionEvent) => item.queries?.length,
    },
    {
      name: "contentionStartTime",
      title: insightsTableTitles.contentionStartTime(execType),
      cell: (item: ContentionEvent) => (
        <Timestamp
          time={item.startTime}
          format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT}
          fallback={"N/A"}
        />
      ),
      sort: (item: ContentionEvent) => item.startTime.unix(),
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: ContentionEvent) => Duration(item.contentionTimeMs * 1e6),
      sort: (item: ContentionEvent) => item.contentionTimeMs,
    },
    {
      name: "schemaName",
      title: insightsTableTitles.schemaName(execType),
      cell: (item: ContentionEvent) => item.schemaName,
      sort: (item: ContentionEvent) => item.schemaName,
    },
    {
      name: "databaseName",
      title: insightsTableTitles.databaseName(execType),
      cell: (item: ContentionEvent) => item.databaseName,
      sort: (item: ContentionEvent) => item.databaseName,
    },
    {
      name: "tableName",
      title: insightsTableTitles.tableName(execType),
      cell: (item: ContentionEvent) => item.tableName,
      sort: (item: ContentionEvent) => item.tableName,
    },
    {
      name: "indexName",
      title: insightsTableTitles.indexName(execType),
      cell: (item: ContentionEvent) => item.indexName,
      sort: (item: ContentionEvent) => item.indexName,
    },
  ];
}

export const WaitTimeDetailsTable: React.FC<
  InsightDetailsTableProps
> = props => {
  const columns = makeInsightDetailsColumns(props.execType);
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "contention",
  });
  return (
    <SortedTable
      className="statements-table"
      columns={columns}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
      {...props}
    />
  );
};

export function makeInsightStatementContentionColumns(): ColumnDescriptor<ContentionDetails>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(InsightExecEnum.TRANSACTION),
      cell: (item: ContentionDetails) => item.blockingExecutionID,
      sort: (item: ContentionDetails) => item.blockingExecutionID,
    },
    {
      name: "fingerprintId",
      title: insightsTableTitles.fingerprintID(InsightExecEnum.TRANSACTION),
      cell: (item: ContentionDetails) =>
        TransactionDetailsLink(item.blockingTxnFingerprintID),
      sort: (item: ContentionDetails) => item.blockingTxnFingerprintID,
    },
    {
      name: "duration",
      title: insightsTableTitles.contention(execType),
      cell: (item: ContentionDetails) => Duration(item.contentionTimeMs * 1e6),
      sort: (item: ContentionDetails) => item.contentionTimeMs,
    },
    {
      name: "databaseName",
      title: insightsTableTitles.databaseName(execType),
      cell: (item: ContentionDetails) => item.databaseName,
      sort: (item: ContentionDetails) => item.databaseName,
    },
    {
      name: "schemaName",
      title: insightsTableTitles.schemaName(execType),
      cell: (item: ContentionDetails) => item.schemaName,
      sort: (item: ContentionDetails) => item.schemaName,
    },
    {
      name: "tableName",
      title: insightsTableTitles.tableName(execType),
      cell: (item: ContentionDetails) => item.tableName,
      sort: (item: ContentionDetails) => item.tableName,
    },
    {
      name: "indexName",
      title: insightsTableTitles.indexName(execType),
      cell: (item: ContentionDetails) => item.indexName,
      sort: (item: ContentionDetails) => item.indexName,
    },
  ];
}

interface InsightContentionTableProps {
  data: ContentionDetails[];
  sortSetting?: SortSetting;
  onChangeSortSetting?: (ss: SortSetting) => void;
}

export const ContentionStatementDetailsTable: React.FC<
  InsightContentionTableProps
> = props => {
  const columns = makeInsightStatementContentionColumns();
  return (
    <SortedTable
      className="statements-table"
      columns={columns}
      sortSetting={props.sortSetting}
      onChangeSortSetting={props.onChangeSortSetting}
      {...props}
    />
  );
};
