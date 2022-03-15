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
  FormatBoolCount,
} from "../../util";

export type PlanHashStats = cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByPlanHash;
export class PlansSortedTable extends SortedTable<PlanHashStats> {}

const planDetailsColumnLabels = {
  planID: "Plan ID",
  lastExecTime: "Last Execution Time",
  avgExecTime: "Average Execution Time",
  execCount: "Execution Count",
  avgRowsRead: "Average Rows Read",
  fullScan: "Full Scan",
  distSQL: "Distributed",
  vectorized: "Vectorized",
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
  lastExecTime: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The last time this Plan was executed."}
      >
        {planDetailsColumnLabels.lastExecTime}
      </Tooltip>
    );
  },
  avgExecTime: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The average execution time for this Plan."}
      >
        {planDetailsColumnLabels.avgExecTime}
      </Tooltip>
    );
  },
  execCount: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The execution count for this Plan."}
      >
        {planDetailsColumnLabels.execCount}
      </Tooltip>
    );
  },
  avgRowsRead: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The average of rows read by this Plan."}
      >
        {planDetailsColumnLabels.avgRowsRead}
      </Tooltip>
    );
  },
  fullScan: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"If the Plan executed a Full Scan."}
      >
        {planDetailsColumnLabels.fullScan}
      </Tooltip>
    );
  },
  distSQL: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"If the Plan was distributed."}
      >
        {planDetailsColumnLabels.distSQL}
      </Tooltip>
    );
  },
  vectorized: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"If the Plan was vectorized."}
      >
        {planDetailsColumnLabels.vectorized}
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
      name: "lastExecTime",
      title: planDetailsTableTitles.lastExecTime(),
      cell: (item: PlanHashStats) =>
        TimestampToMoment(item.stats.last_exec_timestamp).format(
          "MMM DD, YYYY HH:MM",
        ),
      sort: (item: PlanHashStats) =>
        TimestampToMoment(item.stats.last_exec_timestamp).unix(),
    },
    {
      name: "avgExecTime",
      title: planDetailsTableTitles.avgExecTime(),
      cell: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.run_lat.mean, duration),
      sort: (item: PlanHashStats) => item.stats.run_lat.mean,
    },
    {
      name: "execCount",
      title: planDetailsTableTitles.execCount(),
      cell: (item: PlanHashStats) => longToInt(item.stats.count),
      sort: (item: PlanHashStats) => longToInt(item.stats.count),
    },
    {
      name: "avgRowsRead",
      title: planDetailsTableTitles.avgRowsRead(),
      cell: (item: PlanHashStats) => longToInt(item.stats.rows_read.mean),
      sort: (item: PlanHashStats) => longToInt(item.stats.rows_read.mean),
    },
    {
      name: "fullScan",
      title: planDetailsTableTitles.fullScan(),
      cell: (item: PlanHashStats) => FormatBoolCount(item.key_data.full_scan),
      sort: (item: PlanHashStats) => FormatBoolCount(item.key_data.full_scan),
    },
    {
      name: "distSQL",
      title: planDetailsTableTitles.distSQL(),
      cell: (item: PlanHashStats) => FormatBoolCount(item.key_data.distSQL),
      sort: (item: PlanHashStats) => FormatBoolCount(item.key_data.distSQL),
    },
    {
      name: "vectorized",
      title: planDetailsTableTitles.vectorized(),
      cell: (item: PlanHashStats) => FormatBoolCount(item.key_data.vec),
      sort: (item: PlanHashStats) => FormatBoolCount(item.key_data.vec),
    },
  ];
}
