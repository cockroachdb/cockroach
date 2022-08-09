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
import styles from "./indexRecommendationsTable.module.scss";

const cx = classNames.bind(styles);

export type IdxRecommendationType = "DROP" | "CREATE" | "REPLACE";

export interface IdxRecommendation {
  type: IdxRecommendationType;
  database: string;
  table: string;
  index_id: number;
  query: string;
  exec_stmt: string;
  exec_id: string;
}

export class IdxInsightsSortedTable extends SortedTable<IdxRecommendation> {}

const idxRecColumnLabels = {
  insights: "Insights",
  details: "Details",
};
export type IdxRecTableColumnKeys = keyof typeof idxRecColumnLabels;

type IdxRecTableTitleType = {
  [key in IdxRecTableColumnKeys]: () => JSX.Element;
};

export const idxRecTableTitles: IdxRecTableTitleType = {
  insights: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The insight type."}
      >
        {idxRecColumnLabels.insights}
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
        {idxRecColumnLabels.details}
      </Tooltip>
    );
  },
};

function insightType(type: IdxRecommendationType): string {
  switch (type) {
    case "CREATE":
      return "Create New Index";
    case "DROP":
      return "Drop Index";
    case "REPLACE":
      return "Replace Index";
    default:
      return "Insight";
  }
}

function typeCell(value: string): React.ReactElement {
  return <div className={cx("insight-type")}>{value}</div>;
}

function descriptionCell(idxRec: IdxRecommendation): React.ReactElement {
  switch (idxRec.type) {
    case "CREATE":
    case "REPLACE":
      return (
        <>
          <div>
            <span className={cx("label-bold")}>Statement Execution: </span>{" "}
            {idxRec.exec_stmt}
          </div>
          <div>
            <span className={cx("label-bold")}>Recommendation: </span>{" "}
            {idxRec.query}
          </div>
        </>
      );
    case "DROP":
      return <>{`Index ${idxRec.index_id}`}</>;
    default:
      return <>{idxRec.query}</>;
  }
}

export function makeIdxRecColumns(): ColumnDescriptor<IdxRecommendation>[] {
  return [
    {
      name: "insights",
      title: idxRecTableTitles.insights(),
      cell: (item: IdxRecommendation) => typeCell(insightType(item.type)),
      sort: (item: IdxRecommendation) => item.type,
    },
    {
      name: "details",
      title: idxRecTableTitles.details(),
      cell: (item: IdxRecommendation) => descriptionCell(item),
      sort: (item: IdxRecommendation) => item.type,
    },
  ];
}
