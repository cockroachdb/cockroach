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
import { StatementLink } from "../statementsTable";
import { Duration } from "../util";
import IdxRecAction from "../insights/indexActionBtn";
const cx = classNames.bind(styles);

export type InsightType =
  | "DROP_INDEX"
  | "CREATE_INDEX"
  | "REPLACE_INDEX"
  | "HIGH_WAIT_TIME";

export interface InsightRecommendation {
  type: InsightType;
  database?: string;
  table?: string;
  index_id?: number;
  query?: string;
  execution?: executionDetails;
  details?: insightDetails;
}

export interface executionDetails {
  statement: string;
  summary: string;
  fingerprintID: string;
  implicit: boolean;
}

export interface insightDetails {
  duration: number;
  description: string;
}

export class InsightsSortedTable extends SortedTable<InsightRecommendation> {}

const insightColumnLabels = {
  insights: "Insights",
  details: "Details",
  actions: "",
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
  actions: () => {
    return <></>;
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
    case "HIGH_WAIT_TIME":
      return "High Wait Time";
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
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Statement Fingerprint: </span>{" "}
            <StatementLink
              statementFingerprintID={insightRec.execution.fingerprintID}
              statement={insightRec.execution.statement}
              statementSummary={insightRec.execution.summary}
              implicitTxn={insightRec.execution.implicit}
              className={"inline"}
            />
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Recommendation: </span>{" "}
            {insightRec.query}
          </div>
        </>
      );
    case "DROP_INDEX":
      return <>{`Index ${insightRec.index_id}`}</>;
    case "HIGH_WAIT_TIME":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Time Spent Waiting: </span>{" "}
            {Duration(insightRec.details.duration * 1e6)}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description}
          </div>
        </>
      );
    default:
      return <>{insightRec.query}</>;
  }
}

function actionCell(
  insightRec: InsightRecommendation,
  isCockroachCloud: boolean,
): React.ReactElement {
  if (isCockroachCloud) {
    return <></>;
  }
  switch (insightRec.type) {
    case "CREATE_INDEX":
    case "REPLACE_INDEX":
    case "DROP_INDEX":
      return (
        <IdxRecAction
          actionQuery={insightRec.query}
          actionType={insightRec.type}
          database={insightRec.database}
        />
      );
  }
  return <></>;
}

export function makeInsightsColumns(
  isCockroachCloud: boolean,
): ColumnDescriptor<InsightRecommendation>[] {
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
    {
      name: "action",
      title: insightsTableTitles.actions(),
      cell: (item: InsightRecommendation) => actionCell(item, isCockroachCloud),
    },
  ];
}
