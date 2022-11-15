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
import {
  clusterSettings,
  computeOrUseStmtSummary,
  Duration,
  performanceBestPractices,
  statementsRetries,
} from "../util";
import { Anchor } from "../anchor";
import { Link } from "react-router-dom";
import { performanceTuningRecipes } from "../util";
import { InsightRecommendation, insightType } from "../insights";

const cx = classNames.bind(styles);

export class InsightsSortedTable extends SortedTable<InsightRecommendation> {}

const insightColumnLabels = {
  insights: "Insights",
  details: "Details",
  query: "Statement",
  actions: "",
};
export type InsightsTableColumnKeys = keyof typeof insightColumnLabels;

type InsightsTableTitleType = {
  [key in InsightsTableColumnKeys]: () => React.ReactElement;
};

export const insightsTableTitles: InsightsTableTitleType = {
  query: () => <span>{insightColumnLabels.query}</span>,
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
  const clusterSettingsLink = (
    <>
      {"This threshold can be configured in "}
      <Anchor href={clusterSettings} target="_blank">
        cluster settings
      </Anchor>
      {"."}
    </>
  );
  switch (insightRec.type) {
    case "CreateIndex":
    case "ReplaceIndex":
    case "AlterIndex":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Recommendation: </span>{" "}
            {insightRec.query}
          </div>
        </>
      );
    case "DropIndex":
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
    case "HighContention":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Time Spent Waiting: </span>{" "}
            {Duration(insightRec.details.duration * 1e6)}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description} {clusterSettingsLink}
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
            {insightRec.details.description} {clusterSettingsLink}
            {" Learn more about "}
            <Anchor href={statementsRetries} target="_blank">
              retries
            </Anchor>
          </div>
        </>
      );
    case "SuboptimalPlan":
      return (
        <>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Recommendation: </span>{" "}
            {insightRec.execution.indexRecommendations
              .map(rec => rec.split(" : ")[1])
              .join(" ")}
          </div>
        </>
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
    case "FailedExecution":
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
            <span className={cx("label-bold")}>Elapsed Time: </span>
            {Duration(insightRec.details.duration * 1e6)}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description} {clusterSettingsLink}
          </div>
          <div className={cx("description-item")}>
            {"Learn about "}
            <Anchor href={performanceBestPractices} target="_blank">
              SQL performance best practices
            </Anchor>
            {" to optimize slow queries."}
          </div>
        </>
      );
    default:
      return <>{insightRec.query}</>;
  }
}

function actionCell(
  insightRec: InsightRecommendation,
  hideAction: boolean,
): React.ReactElement {
  if (hideAction) {
    return <></>;
  }
  let query = "";
  switch (insightRec.type) {
    case "CreateIndex":
    case "ReplaceIndex":
    case "DropIndex":
    case "AlterIndex":
      return (
        <IdxRecAction
          actionQuery={insightRec.query}
          actionType={insightRec.type}
          database={insightRec.database}
        />
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
              ? "ReplaceIndex"
              : query.toLowerCase().includes("alter ")
              ? "AlterIndex"
              : "CreateIndex"
          }
          database={insightRec.database}
        />
      );
  }
  return <></>;
}

const isIndexRec = (rec: InsightRecommendation) => {
  switch (rec.type) {
    case "AlterIndex":
    case "CreateIndex":
    case "DropIndex":
      return true;
    default:
      return false;
  }
};

const StatementExecution = ({
  rec,
  disableLink,
}: {
  rec: InsightRecommendation;
  disableLink: boolean;
}) => (
  <div className={cx("description-item")}>
    <span className={cx("label-bold")}>Statement: </span>{" "}
    {disableLink ? (
      <div className={cx("inline")}>
        <Tooltip placement="bottom" content={rec.execution.statement}>
          {computeOrUseStmtSummary(
            rec.execution?.statement,
            rec.execution?.summary,
          )}
        </Tooltip>
      </div>
    ) : (
      <StatementLink
        statementFingerprintID={rec.execution.fingerprintID}
        statement={rec.execution.statement}
        statementSummary={rec.execution.summary}
        implicitTxn={rec.execution.implicit}
        className="inline"
      />
    )}
  </div>
);

export function makeInsightsColumns(
  hideAction: boolean,
  showQuery?: boolean,
  disableStmtLink?: boolean,
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
      cell: (item: InsightRecommendation) => (
        <>
          {(showQuery || isIndexRec(item)) && (
            <StatementExecution rec={item} disableLink={disableStmtLink} />
          )}
          {descriptionCell(item)}
        </>
      ),
      sort: (item: InsightRecommendation) => item.type,
    },
    {
      name: "action",
      title: insightsTableTitles.actions(),
      cell: (item: InsightRecommendation) => actionCell(item, hideAction),
    },
  ];
}
