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
import { EventExecution, InsightExecEnum } from "../types";
import { insightsTableTitles, QueriesCell } from "../workloadInsights/util";

interface InsightDetailsTableProps {
  data: EventExecution[];
  execType: InsightExecEnum;
}

export function makeInsightDetailsColumns(
  execType: InsightExecEnum,
): ColumnDescriptor<EventExecution>[] {
  return [
    {
      name: "executionID",
      title: insightsTableTitles.executionID(execType),
      cell: (item: EventExecution) => String(item.executionID),
      sort: (item: EventExecution) => item.executionID,
    },
    {
      name: "fingerprintID",
      title: insightsTableTitles.fingerprintID(execType),
      cell: (item: EventExecution) => String(item.fingerprintID),
      sort: (item: EventExecution) => item.fingerprintID,
    },
    {
      name: "query",
      title: insightsTableTitles.query(execType),
      cell: (item: EventExecution) => QueriesCell(item.queries, 50),
      sort: (item: EventExecution) => item.queries.length,
    },
    {
      name: "contentionStartTime",
      title: insightsTableTitles.contentionStartTime(execType),
      cell: (item: EventExecution) => item.startTime.format(DATE_FORMAT),
      sort: (item: EventExecution) => item.startTime.unix(),
    },
    {
      name: "contention",
      title: insightsTableTitles.contention(execType),
      cell: (item: EventExecution) => Duration(item.contentionTimeMs * 1e6),
      sort: (item: EventExecution) => item.contentionTimeMs,
    },
    {
      name: "schemaName",
      title: insightsTableTitles.schemaName(execType),
      cell: (item: EventExecution) => item.schemaName,
      sort: (item: EventExecution) => item.schemaName,
    },
    {
      name: "databaseName",
      title: insightsTableTitles.databaseName(execType),
      cell: (item: EventExecution) => item.databaseName,
      sort: (item: EventExecution) => item.databaseName,
    },
    {
      name: "tableName",
      title: insightsTableTitles.tableName(execType),
      cell: (item: EventExecution) => item.tableName,
      sort: (item: EventExecution) => item.tableName,
    },
    {
      name: "indexName",
      title: insightsTableTitles.indexName(execType),
      cell: (item: EventExecution) => item.indexName,
      sort: (item: EventExecution) => item.indexName,
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
