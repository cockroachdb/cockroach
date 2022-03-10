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
import { Tooltip } from "@cockroachlabs/ui-components";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  Duration,
  formatNumberForDisplay,
  longToInt,
  TimestampToMoment,
} from "../../util";

export type PlanHashStats = cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByPlanHash;
export class PlansSortedTable extends SortedTable<PlanHashStats> {}

const planDetailsColumnLabels = {
  planID: "Plan ID",
  last_exec_time: "Last Execution Time",
  avg_exec_time: "Average Execution Time",
  exec_count: "Execution Count",
  avg_rows_read: "Average Rows Read",
};
export type PlanDetailsTableColumnKeys = keyof typeof planDetailsColumnLabels;

type PlanDetailsTableTitleType = {
  [key in PlanDetailsTableColumnKeys]: () => JSX.Element;
};

export const planDetailsTableTitles: PlanDetailsTableTitleType = {
  planID: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The ID of the Plan."}
      >
        {planDetailsColumnLabels.planID}
      </Tooltip>
    );
  },
  last_exec_time: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The last time this Plan was executed."}
      >
        {planDetailsColumnLabels.last_exec_time}
      </Tooltip>
    );
  },
  avg_exec_time: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The average execution time for this Plan."}
      >
        {planDetailsColumnLabels.avg_exec_time}
      </Tooltip>
    );
  },
  exec_count: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The execution count for this Plan."}
      >
        {planDetailsColumnLabels.exec_count}
      </Tooltip>
    );
  },
  avg_rows_read: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The average of rows read by this Plan."}
      >
        {planDetailsColumnLabels.avg_rows_read}
      </Tooltip>
    );
  },
};

export function makeExplainPlanColumns(
  handleDetails: (plan: PlanHashStats) => void,
): ColumnDescriptor<PlanHashStats>[] {
  const duration = (v: number) => Duration(v * 1e9);
  return [
    {
      name: "planID",
      title: planDetailsTableTitles.planID(),
      cell: (item: PlanHashStats) => (
        <a onClick={() => handleDetails(item)}>{longToInt(item.plan_hash)}</a>
      ),
      sort: (item: PlanHashStats) => longToInt(item.plan_hash),
      alwaysShow: true,
    },
    {
      name: "last_exec_time",
      title: planDetailsTableTitles.last_exec_time(),
      cell: (item: PlanHashStats) =>
        TimestampToMoment(item.stats.last_exec_timestamp).format(
          "MMM DD, YYYY HH:MM",
        ),
      sort: (item: PlanHashStats) =>
        TimestampToMoment(item.stats.last_exec_timestamp).unix(),
    },
    {
      name: "avg_exec_time",
      title: planDetailsTableTitles.avg_exec_time(),
      cell: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.run_lat.mean, duration),
      sort: (item: PlanHashStats) => item.stats.run_lat.mean,
    },
    {
      name: "exec_count",
      title: planDetailsTableTitles.exec_count(),
      cell: (item: PlanHashStats) => longToInt(item.stats.count),
      sort: (item: PlanHashStats) => longToInt(item.stats.count),
    },
    {
      name: "avg_rows_read",
      title: planDetailsTableTitles.avg_rows_read(),
      cell: (item: PlanHashStats) => longToInt(item.stats.rows_read.mean),
      sort: (item: PlanHashStats) => longToInt(item.stats.rows_read.mean),
    },
  ];
}
