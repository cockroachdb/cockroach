// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import { DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT, Duration } from "src/util";
import {
  BlockedStatementContentionDetails,
  ContentionEvent,
  InsightExecEnum,
} from "../types";
import {
  insightsTableTitles,
  QueriesCell,
  TransactionDetailsLink,
} from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";

interface InsightDetailsTableProps {
  data: ContentionEvent[];
  execType: InsightExecEnum;
  setTimeScale?: (tw: TimeScale) => void;
}

export function makeInsightDetailsColumns(
  execType: InsightExecEnum,
  setTimeScale: (tw: TimeScale) => void,
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
        TransactionDetailsLink(
          item.fingerprintID,
          item.startTime,
          setTimeScale,
        ),
      sort: (item: ContentionEvent) => item.fingerprintID,
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
      cell: (item: ContentionEvent) =>
        item.startTime?.format(DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT),
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
  const columns = makeInsightDetailsColumns(props.execType, props.setTimeScale);
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

export function makeInsightStatementContentionColumns(): ColumnDescriptor<BlockedStatementContentionDetails>[] {
  const execType = InsightExecEnum.STATEMENT;
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(InsightExecEnum.TRANSACTION),
      cell: (item: BlockedStatementContentionDetails) => item.blockingTxnID,
      sort: (item: BlockedStatementContentionDetails) => item.blockingTxnID,
    },
    {
      name: "duration",
      title: insightsTableTitles.contention(execType),
      cell: (item: BlockedStatementContentionDetails) =>
        Duration(item.durationInMs * 1e6),
      sort: (item: BlockedStatementContentionDetails) => item.durationInMs,
    },
    {
      name: "schemaName",
      title: insightsTableTitles.schemaName(execType),
      cell: (item: BlockedStatementContentionDetails) => item.schemaName,
      sort: (item: BlockedStatementContentionDetails) => item.schemaName,
    },
    {
      name: "databaseName",
      title: insightsTableTitles.databaseName(execType),
      cell: (item: BlockedStatementContentionDetails) => item.databaseName,
      sort: (item: BlockedStatementContentionDetails) => item.databaseName,
    },
    {
      name: "tableName",
      title: insightsTableTitles.tableName(execType),
      cell: (item: BlockedStatementContentionDetails) => item.tableName,
      sort: (item: BlockedStatementContentionDetails) => item.tableName,
    },
    {
      name: "indexName",
      title: insightsTableTitles.indexName(execType),
      cell: (item: BlockedStatementContentionDetails) => item.indexName,
      sort: (item: BlockedStatementContentionDetails) => item.indexName,
    },
  ];
}

interface InsightContentionTableProps {
  data: BlockedStatementContentionDetails[];
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
