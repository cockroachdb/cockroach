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
import {Link} from "react-router-dom";
import {Search as IndexIcon} from "@cockroachlabs/icons";
import {Anchor} from "../anchor";
import {performanceTuningRecipes} from "../util";

const cx = classNames.bind(styles);

export type InsightType = "DROP_INDEX" | "CREATE_INDEX" | "REPLACE_INDEX";

export interface InsightRecommendation {
  type: InsightType;
  database: string;
  query?: string;
  lastUsed?: string;
  indexDetails?: indexDetails;
  execution?: executionDetails;
}

export interface indexDetails {
  table: string;
  indexID: number;
  indexName: string;
}

export interface executionDetails {
  statement: string;
  summary: string;
  fingerprintID: string;
  implicit: boolean;
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
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Index: </span>{" "}
            <Link
              to={`database/${insightRec.database}/table/${insightRec.indexDetails.table}/index/${insightRec.indexDetails.indexName}`}
              className={cx("table-link")}
            >
              {insightRec.indexDetails.indexName}
            </Link>
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.lastUsed}{" Learn more about "}
            <Anchor href={performanceTuningRecipes} target="_blank" className={cx("table-link")}>
              unused indexes
            </Anchor>{"."}
          </div>
        </>
      );
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
