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
import { ColumnDescriptor, SortedTable } from "src/sortedtable";
import { DATE_FORMAT, Duration } from "src/util";
import { ContentionEvent, InsightExecEnum } from "../types";
import { insightsTableTitles, QueriesCell } from "../workloadInsights/util";

interface InsightDetailsTableProps {
  data: ContentionEvent[];
  execType: InsightExecEnum;
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
      cell: (item: ContentionEvent) => String(item.fingerprintID),
      sort: (item: ContentionEvent) => item.fingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: ContentionEvent) => QueriesCell(item.queries, 50),
      sort: (item: ContentionEvent) => item.queries.length,
    },
    {
      name: "contentionStartTime",
      title: insightsTableTitles.contentionStartTime(execType),
      cell: (item: ContentionEvent) => item.startTime.format(DATE_FORMAT),
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
  return (
    <SortedTable className="statements-table" columns={columns} {...props} />
  );
};
