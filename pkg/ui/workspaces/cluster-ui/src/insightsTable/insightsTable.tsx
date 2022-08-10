// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { ColumnDescriptor, SortedTable } from "../sortedtable";
import classNames from "classnames/bind";
import styles from "./insightsTable.module.scss";

const cx = classNames.bind(styles);

export type InsightType = "DROP_INDEX" | "CREATE_INDEX" | "REPLACE_INDEX";

export interface InsightRecommendation {
  type: InsightType;
  database: string;
  table: string;
  index_id: number;
  query: string;
  exec_stmt: string;
  exec_id: string;
}

export class InsightsSortedTable extends SortedTable<InsightRecommendation> {}

const insightColumnLabels = {
  insights: "Insights",
  details: "Details",
};
export type InsightsTableColumnKeys = keyof typeof insightColumnLabels;

type InsightsTableTitleType = {
  [key in InsightsTableColumnKeys]: () => JSX.Element;
};

export const insightsTableTitles: InsightsTableTitleType = {
  insights: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The insight type."}
      >
        {insightColumnLabels.insights}
      </Tooltip>
    );
  },
  details: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"Details about the insight."}
      >
        {insightColumnLabels.details}
      </Tooltip>
    );
  },
};

function insightType(type: InsightType): string {
  switch (type) {
    case "CREATE_INDEX":
      return "Create New Index";
    case "DROP_INDEX":
      return "Drop Unused Index";
    case "REPLACE_INDEX":
      return "Replace Index";
    default:
      return "Insight";
  }
}

function typeCell(value: string): React.ReactElement {
  return <div className={cx("insight-type")}>{value}</div>;
}

function descriptionCell(
  insightRec: InsightRecommendation,
): React.ReactElement {
  switch (insightRec.type) {
    case "CREATE_INDEX":
    case "REPLACE_INDEX":
      return (
        <>
          <div>
            <span className={cx("label-bold")}>Statement Execution: </span>{" "}
            {insightRec.exec_stmt}
          </div>
          <div>
            <span className={cx("label-bold")}>Recommendation: </span>{" "}
            {insightRec.query}
          </div>
        </>
      );
    case "DROP_INDEX":
      return <>{`Index ${insightRec.index_id}`}</>;
    default:
      return <>{insightRec.query}</>;
  }
}

export function makeInsightsColumns(): ColumnDescriptor<InsightRecommendation>[] {
  return [
    {
      name: "insights",
      title: insightsTableTitles.insights(),
      cell: (item: InsightRecommendation) => typeCell(insightType(item.type)),
      sort: (item: InsightRecommendation) => item.type,
    },
    {
      name: "details",
      title: insightsTableTitles.details(),
      cell: (item: InsightRecommendation) => descriptionCell(item),
      sort: (item: InsightRecommendation) => item.type,
    },
  ];
}
