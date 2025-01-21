// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { ReactNode } from "react";
import { Link } from "react-router-dom";

import { ColumnDescriptor, SortedTable } from "src/sortedtable";

import { Anchor } from "../../anchor";
import { Timestamp, Timezone } from "../../timestamp";
import {
  Duration,
  formatNumberForDisplay,
  longToInt,
  TimestampToMoment,
  RenderCount,
  DATE_FORMAT,
  explainPlan,
  limitText,
  Count,
  intersperse,
  EncodeDatabaseTableIndexUri,
} from "../../util";

import styles from "./plansTable.module.scss";

export type PlanHashStats =
  cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByPlanHash;
export class PlansSortedTable extends SortedTable<PlanHashStats> {}

const cx = classNames.bind(styles);

const planDetailsColumnLabels = {
  avgExecTime: "Average Execution Time",
  avgRowsRead: "Average Rows Read",
  distSQL: "Distributed",
  execCount: "Execution Count",
  fullScan: "Full Scan",
  insights: "Insights",
  indexes: "Used Indexes",
  lastExecTime: "Last Execution Time",
  latencyMax: "Max Latency",
  latencyMin: "Min Latency",
  planGist: "Plan Gist",
  vectorized: "Vectorized",
};
export type PlanDetailsTableColumnKeys = keyof typeof planDetailsColumnLabels;

type PlanDetailsTableTitleType = {
  [key in PlanDetailsTableColumnKeys]: () => JSX.Element;
};

export const planDetailsTableTitles: PlanDetailsTableTitleType = {
  planGist: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          <p>
            The Gist of the{" "}
            <Anchor href={explainPlan} target="_blank">
              Explain Plan.
            </Anchor>
          </p>
        }
      >
        {planDetailsColumnLabels.planGist}
      </Tooltip>
    );
  },
  lastExecTime: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The last time this Explain Plan was executed."}
      >
        <>
          {planDetailsColumnLabels.lastExecTime} <Timezone />
        </>
      </Tooltip>
    );
  },
  avgExecTime: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The average execution time for this Explain Plan."}
      >
        {planDetailsColumnLabels.avgExecTime}
      </Tooltip>
    );
  },
  latencyMin: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The lowest latency value for all statement executions with this Explain Plan."
        }
      >
        {planDetailsColumnLabels.latencyMin}
      </Tooltip>
    );
  },
  latencyMax: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The highest latency value for all statement executions with this Explain Plan."
        }
      >
        {planDetailsColumnLabels.latencyMax}
      </Tooltip>
    );
  },
  execCount: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The execution count for this Explain Plan."}
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
        content={"The average of rows read by this Explain Plan."}
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
        content={"If the Explain Plan executed a full scan."}
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
        content={"If the Explain Plan was distributed."}
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
        content={"If the Explain Plan was vectorized."}
      >
        {planDetailsColumnLabels.vectorized}
      </Tooltip>
    );
  },
  insights: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The amount of insights for the Explain Plan."}
      >
        {planDetailsColumnLabels.insights}
      </Tooltip>
    );
  },
  indexes: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"Indexes used by the Explain Plan."}
      >
        {planDetailsColumnLabels.indexes}
      </Tooltip>
    );
  },
};

function formatInsights(recommendations: string[]): string {
  if (!recommendations || recommendations.length === 0) {
    return "None";
  }
  if (recommendations.length === 1) {
    return "1 Insight";
  }
  return `${recommendations.length} Insights`;
}

export function formatIndexes(indexes: string[], database: string): ReactNode {
  if (indexes.length === 0) {
    return <></>;
  }
  const indexMap: Map<string, Array<string>> = new Map<string, Array<string>>();
  let droppedCount = 0;
  let tableName;
  let idxName;
  let indexInfo;
  for (let i = 0; i < indexes.length; i++) {
    if (indexes[i] === "dropped") {
      droppedCount++;
      continue;
    }
    if (!indexes[i].includes("@")) {
      continue;
    }
    indexInfo = indexes[i].split("@");
    tableName = indexInfo[0];
    idxName = indexInfo[1];
    if (indexMap.has(tableName)) {
      indexMap.set(tableName, indexMap.get(tableName).concat(idxName));
    } else {
      indexMap.set(tableName, [idxName]);
    }
  }

  let newLine;
  const list = Array.from(indexMap).map((value, i) => {
    const table = value[0];
    newLine = i > 0 ? <br /> : "";
    const indexesList = intersperse<ReactNode>(
      value[1].map(idx => {
        return (
          <Link
            className={cx("regular-link")}
            to={EncodeDatabaseTableIndexUri(database, table, idx)}
            key={`${table}${idx}`}
          >
            {idx}
          </Link>
        );
      }),
      ", ",
    );
    return (
      <span key={table}>
        {newLine}
        {table}: {indexesList}
      </span>
    );
  });
  newLine = list.length > 0 ? <br /> : "";
  if (droppedCount === 1) {
    list.push(<span key={`dropped`}>{newLine}[dropped index]</span>);
  } else if (droppedCount > 1) {
    list.push(
      <span key={`dropped`}>
        {newLine}[{droppedCount} dropped indexes]
      </span>,
    );
  }

  return intersperse<ReactNode>(list, ",");
}

export function makeExplainPlanColumns(
  handleDetails: (plan: PlanHashStats) => void,
): ColumnDescriptor<PlanHashStats>[] {
  const duration = (v: number) => Duration(v * 1e9);
  const count = (v: number) => v.toFixed(1);
  return [
    {
      name: "planGist",
      title: planDetailsTableTitles.planGist(),
      cell: (item: PlanHashStats) => (
        <Tooltip placement="bottom" content={item.stats.plan_gists[0]}>
          <a onClick={() => handleDetails(item)}>
            {limitText(item.stats.plan_gists[0], 25)}
          </a>
        </Tooltip>
      ),
      sort: (item: PlanHashStats) => item.stats.plan_gists[0],
      alwaysShow: true,
    },
    {
      name: "indexes",
      title: planDetailsTableTitles.indexes(),
      cell: (item: PlanHashStats) =>
        formatIndexes(item.stats.indexes, item.metadata.databases[0]),
      sort: (item: PlanHashStats) => item.stats.indexes?.join(""),
    },
    {
      name: "insights",
      title: planDetailsTableTitles.insights(),
      cell: (item: PlanHashStats) =>
        formatInsights(item.stats.index_recommendations),
      sort: (item: PlanHashStats) => item.stats.index_recommendations?.length,
    },
    {
      name: "lastExecTime",
      title: planDetailsTableTitles.lastExecTime(),
      cell: (item: PlanHashStats) => (
        <Timestamp
          time={TimestampToMoment(item.stats.last_exec_timestamp)}
          format={DATE_FORMAT}
        />
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
      cell: (item: PlanHashStats) => Count(longToInt(item.stats.count)),
      sort: (item: PlanHashStats) => longToInt(item.stats.count),
    },
    {
      name: "avgRowsRead",
      title: planDetailsTableTitles.avgRowsRead(),
      cell: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.rows_read.mean, count),
      sort: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.rows_read.mean, count),
    },
    {
      name: "fullScan",
      title: planDetailsTableTitles.fullScan(),
      cell: (item: PlanHashStats) =>
        RenderCount(item.metadata.full_scan_count, item.metadata.total_count),
      sort: (item: PlanHashStats) =>
        RenderCount(item.metadata.full_scan_count, item.metadata.total_count),
    },
    {
      name: "latencyMin",
      title: planDetailsTableTitles.latencyMin(),
      cell: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.latency_info.min, duration),
      sort: (item: PlanHashStats) => item.stats.latency_info.min,
    },
    {
      name: "latencyMax",
      title: planDetailsTableTitles.latencyMax(),
      cell: (item: PlanHashStats) =>
        formatNumberForDisplay(item.stats.latency_info.max, duration),
      sort: (item: PlanHashStats) => item.stats.latency_info.max,
    },
    {
      name: "distSQL",
      title: planDetailsTableTitles.distSQL(),
      cell: (item: PlanHashStats) =>
        RenderCount(item.metadata.dist_sql_count, item.metadata.total_count),
      sort: (item: PlanHashStats) =>
        RenderCount(item.metadata.dist_sql_count, item.metadata.total_count),
    },
    {
      name: "vectorized",
      title: planDetailsTableTitles.vectorized(),
      cell: (item: PlanHashStats) =>
        RenderCount(item.metadata.vec_count, item.metadata.total_count),
      sort: (item: PlanHashStats) =>
        RenderCount(item.metadata.vec_count, item.metadata.total_count),
    },
  ];
}
