// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";
import { Link } from "react-router-dom";

import { Anchor } from "../anchor";
import IdxRecAction from "../insights/indexActionBtn";
import {
  InsightExecEnum,
  InsightRecommendation,
  InsightType,
} from "../insights/types";
import { insightType } from "../insights/utils";
import { ColumnDescriptor, SortedTable } from "../sortedtable";
import { StatementLink } from "../statementsTable";
import {
  clusterSettings,
  computeOrUseStmtSummary,
  Duration,
  EncodeDatabasesToIndexUri,
  EncodeDatabaseTableIndexUri,
  performanceBestPractices,
  performanceTuningRecipes,
  statementsRetries,
  stmtPerformanceRules,
} from "../util";

import styles from "./insightsTable.module.scss";

const cx = classNames.bind(styles);

export class InsightsSortedTable extends SortedTable<InsightRecommendation> {}

const insightColumnLabels = {
  insights: "Insights",
  details: "Details",
  query: "Statement",
  latestExecution: "Latest Execution ID",
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
  latestExecution: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The latest execution ID of the statement fingerprint with an insight."
        }
      >
        {insightColumnLabels.latestExecution}
      </Tooltip>
    );
  },
  actions: () => {
    return <></>;
  },
};

function TypeCell(value: InsightType): React.ReactElement {
  const className =
    insightType(value) === "FailedExecution"
      ? "insight-type-failed"
      : "insight-type";
  return <div className={cx(className)}>{insightType(value)}</div>;
}

const StatementExecution = ({
  rec,
  disableLink,
}: {
  rec: InsightRecommendation;
  disableLink: boolean;
}) => {
  if (!rec.execution.statement) return null;
  return (
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
          appNames={[rec.execution.application]}
          statementFingerprintID={rec.execution.fingerprintID}
          statement={rec.execution.statement}
          statementSummary={rec.execution.summary}
          implicitTxn={rec.execution.implicit}
          className="inline"
        />
      )}
    </div>
  );
};

function descriptionCell(
  insightRec: InsightRecommendation,
  disableStmtLink: boolean,
  isCockroachCloud: boolean,
  isFingerprint: boolean,
): React.ReactElement {
  const stmtLink = isIndexRec(insightRec) ? (
    <StatementExecution
      rec={insightRec}
      disableLink={disableStmtLink || isFingerprint}
    />
  ) : null;

  const clusterSettingsLink = (
    <>
      {"This threshold can be configured in "}
      <Anchor href={clusterSettings} target="_blank">
        cluster settings
      </Anchor>
      {"."}
    </>
  );

  const learnMoreSuboptimalPlan = (
    <Anchor href={stmtPerformanceRules} target="_blank">
      Learn more
    </Anchor>
  );

  const indexLink = isCockroachCloud
    ? EncodeDatabasesToIndexUri(
        insightRec.database,
        insightRec.indexDetails?.schema,
        insightRec.indexDetails?.table,
        insightRec.indexDetails?.indexName,
      )
    : EncodeDatabaseTableIndexUri(
        insightRec.database,
        insightRec.indexDetails?.table,
        insightRec.indexDetails?.indexName,
      );

  switch (insightRec.type) {
    case "CreateIndex":
    case "ReplaceIndex":
    case "AlterIndex":
      return (
        <>
          {stmtLink}
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
            <Link to={indexLink} className={cx("table-link")}>
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
          {!isFingerprint && (
            <div className={cx("description-item")}>
              <span className={cx("label-bold")}>Time Spent Waiting: </span>{" "}
              {Duration(insightRec.details.duration * 1e6)}
            </div>
          )}
          {stmtLink}
          <div className={cx("description-item")}>
            {!isFingerprint && (
              <span className={cx("label-bold")}>Description: </span>
            )}
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
          {stmtLink}
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}>Description: </span>{" "}
            {insightRec.details.description} {learnMoreSuboptimalPlan}
          </div>
          {insightRec.execution.indexRecommendations && (
            <div className={cx("description-item")}>
              <span className={cx("label-bold")}>Recommendation: </span>{" "}
              {insightRec.execution.indexRecommendations
                .map(rec => rec.split(" : ")[1])
                .join(" ")}
            </div>
          )}
        </>
      );
    case "PlanRegression":
      return (
        <>
          {stmtLink}
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
            <span className={cx("label-bold")}>Error Code: </span>{" "}
            {insightRec.execution.errorCode}
          </div>
          <div className={cx("description-item")}>
            <span className={cx("label-bold")}> Error Message: </span>{" "}
            {insightRec.execution.errorMsg}
          </div>
        </>
      );
    case "Unknown":
      return (
        <>
          {!isFingerprint && (
            <div className={cx("description-item")}>
              <span className={cx("label-bold")}>Elapsed Time: </span>
              {Duration(insightRec.details.duration * 1e6)}
            </div>
          )}
          {stmtLink}
          <div className={cx("description-item")}>
            {!isFingerprint && (
              <span className={cx("label-bold")}>Description: </span>
            )}
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

function linkCell(insightRec: InsightRecommendation): React.ReactElement {
  switch (insightRec.execution?.execType) {
    case InsightExecEnum.STATEMENT:
      return (
        <>
          <Link
            to={`/insights/statement/${insightRec.execution.statementExecutionID}`}
          >
            {String(insightRec.execution.statementExecutionID)}
          </Link>
        </>
      );
    case InsightExecEnum.TRANSACTION:
      return (
        <>
          <Link
            to={`/insights/transaction/${insightRec.execution.transactionExecutionID}`}
          >
            {String(insightRec.execution.transactionExecutionID)}
          </Link>
        </>
      );
    default:
      return <>No execution ID found</>;
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
      if (!insightRec.query) {
        return <></>;
      }
      return (
        <IdxRecAction
          actionQuery={insightRec.query}
          actionType={insightRec.type}
          database={insightRec.database}
        />
      );
    case "SuboptimalPlan":
      query = insightRec.execution.indexRecommendations
        ?.map(rec => rec.split(" : ")[1])
        .join(" ");

      if (!query) {
        return <></>;
      }

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
    case "ReplaceIndex":
      return true;
    default:
      return false;
  }
};

export function makeInsightsColumns(
  isCockroachCloud: boolean,
  hasAdminRole: boolean,
  disableStmtLink?: boolean,
  isFingerprint?: boolean,
): ColumnDescriptor<InsightRecommendation>[] {
  const columns: ColumnDescriptor<InsightRecommendation>[] = [
    {
      name: "insights",
      title: insightsTableTitles.insights(),
      cell: (item: InsightRecommendation) => TypeCell(item.type),
      sort: (item: InsightRecommendation) => item.type,
    },
    {
      name: "details",
      title: insightsTableTitles.details(),
      cell: (item: InsightRecommendation) =>
        descriptionCell(item, disableStmtLink, isCockroachCloud, isFingerprint),
      sort: (item: InsightRecommendation) => item.type,
    },
    {
      name: "action",
      title: insightsTableTitles.actions(),
      cell: (item: InsightRecommendation) =>
        actionCell(item, isCockroachCloud || !hasAdminRole || isFingerprint),
    },
  ];
  if (isFingerprint) {
    columns.push({
      name: "latestExecution",
      title: insightsTableTitles.latestExecution(),
      cell: (item: InsightRecommendation) => linkCell(item),
      sort: (item: InsightRecommendation) => item.type,
    });
  }
  return columns;
}
