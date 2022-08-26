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
import IdxRecAction from "../insights/indexActionBtn";
import { Duration, statementsRetries } from "../util";
import { Anchor } from "../anchor";
import { Link } from "react-router-dom";
import { performanceTuningRecipes } from "../util";
import { InsightRecommendation, insightType } from "../insights";

const cx = classNames.bind(styles);

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

function typeCell(value: string): React.ReactElement {
  return <div className={cx("insight-type")}>{value}</div>;
}

function descriptionCell(
  insightRec: InsightRecommendation,
): React.ReactElement {
  let query = "";
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
            {insightRec.indexDetails?.lastUsed}
            {" Learn more about "}
            <Anchor
              href={performanceTuningRecipes}
              target="_blank"
              className={cx("table-link")}
            >
              unused indexes
            </Anchor>
            {"."}
          </div>
        </>
      );
    case "HighContentionTime":
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
    case "HighRetryCount":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Retries: </span>{" "}
            {insightRec.execution.retries}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description}
            {" Learn more about "}
            <Anchor href={statementsRetries} target="_blank">
              retries
            </Anchor>
          </div>
        </>
      );
    case "SuboptimalPlan":
      query = insightRec.execution.indexRecommendations
        .map(rec => rec.split(" : ")[1])
        .join(" ");

      return (
        <IdxRecAction
          actionQuery={query}
          actionType={
            query.toLowerCase().includes("drop ")
              ? "REPLACE_INDEX"
              : "CREATE_INDEX"
          }
          database={insightRec.database}
        />
      );
    case "PlanRegression":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description}
          </div>
        </>
      );
    case "FAILED":
      return (
        <>
          <div className={cx("description-item")}>
            This execution has failed.
          </div>
        </>
      );
    case "Unknown":
      return (
        <>
          <div className={cx("description-item")}>
            Unable to identify specific reasons why this execution was slow.
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
