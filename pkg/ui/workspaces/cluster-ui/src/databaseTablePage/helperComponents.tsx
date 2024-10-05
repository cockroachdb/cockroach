// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  DATE_FORMAT,
  DATE_FORMAT_24_TZ,
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
  EncodeUriName,
  minDate,
  performanceTuningRecipes,
} from "../util";
import { Timestamp } from "../timestamp";
import React, { useContext } from "react";
import { Moment } from "moment-timezone";
import { DatabaseTablePageDataDetails, IndexStat } from "./databaseTablePage";
import { CircleFilled } from "../icon";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import { Anchor } from "../anchor";
import classNames from "classnames/bind";
import styles from "./databaseTablePage.module.scss";
import { QuoteIdentifier } from "../api/safesql";
import IdxRecAction from "../insights/indexActionBtn";
import * as format from "../util/format";
import { Link } from "react-router-dom";
import { Search as IndexIcon } from "@cockroachlabs/icons";
import { Breadcrumbs } from "../breadcrumbs";
import { CaretRight } from "../icon/caretRight";
import { CockroachCloudContext } from "../contexts";
import { sqlApiErrorMessage } from "../api";
const cx = classNames.bind(styles);

export const NameCell = ({
  indexStat,
  showIndexRecommendations,
  tableName,
}: {
  indexStat: IndexStat;
  showIndexRecommendations: boolean;
  tableName: string;
}): JSX.Element => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  if (showIndexRecommendations) {
    const linkURL = isCockroachCloud
      ? `${location.pathname}/${indexStat.indexName}`
      : `${tableName}/index/${EncodeUriName(indexStat.indexName)}`;
    return (
      <Link to={linkURL} className={cx("icon__container")}>
        <IndexIcon className={cx("icon--s", "icon--primary")} />
        {indexStat.indexName}
      </Link>
    );
  }
  return (
    <>
      <IndexIcon className={cx("icon--s", "icon--primary")} />
      {indexStat.indexName}
    </>
  );
};

export const DbTablesBreadcrumbs = ({
  tableName,
  schemaName,
  databaseName,
}: {
  tableName: string;
  schemaName: string;
  databaseName: string;
}): JSX.Element => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  return (
    <Breadcrumbs
      items={[
        { link: "/databases", name: "Databases" },
        {
          link: isCockroachCloud
            ? `/databases/${EncodeUriName(databaseName)}`
            : EncodeDatabaseUri(databaseName),
          name: "Tables",
        },
        {
          link: isCockroachCloud
            ? `/databases/${EncodeUriName(databaseName)}/${EncodeUriName(
                schemaName,
              )}/${EncodeUriName(tableName)}`
            : EncodeDatabaseTableUri(databaseName, tableName),
          name: `Table: ${tableName}`,
        },
      ]}
      divider={<CaretRight className={cx("icon--xxs", "icon--primary")} />}
    />
  );
};

export const LastReset = ({
  lastReset,
}: {
  lastReset: Moment;
}): JSX.Element => {
  return (
    <span>
      Last reset:{" "}
      {lastReset.isSame(minDate) ? (
        "Never"
      ) : (
        <Timestamp time={lastReset} format={DATE_FORMAT_24_TZ} />
      )}
    </span>
  );
};

interface IndexStatProps {
  indexStat: IndexStat;
}

export const LastUsed = ({ indexStat }: IndexStatProps): JSX.Element => {
  // This case only occurs when we have no reads, resets, or creation time on
  // the index.
  if (indexStat.lastUsed.isSame(minDate)) {
    return <>Never</>;
  }
  return (
    <>
      Last {indexStat.lastUsedType}:{" "}
      <Timestamp time={indexStat.lastUsed} format={DATE_FORMAT} />
    </>
  );
};

export const IndexRecCell = ({ indexStat }: IndexStatProps): JSX.Element => {
  const classname =
    indexStat.indexRecommendations.length > 0
      ? "index-recommendations-icon__exist"
      : "index-recommendations-icon__none";

  if (indexStat.indexRecommendations.length === 0) {
    return (
      <div>
        <CircleFilled className={cx(classname)} />
        <span>None</span>
      </div>
    );
  }
  // Render only the first recommendation for an index.
  const recommendation = indexStat.indexRecommendations[0];
  let text: string;
  switch (recommendation.type) {
    case "DROP_UNUSED":
      text = "Drop unused index";
  }
  return (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("index-recommendations-text__tooltip-anchor")}>
          {recommendation.reason}{" "}
          <Anchor href={performanceTuningRecipes} target="_blank">
            Learn more
          </Anchor>
        </div>
      }
    >
      <CircleFilled className={cx(classname)} />
      <span className={cx("index-recommendations-text__border")}>{text}</span>
    </Tooltip>
  );
};

export const ActionCell = ({
  indexStat,
  tableName,
  databaseName,
}: {
  indexStat: IndexStat;
  tableName: string;
  databaseName: string;
}): JSX.Element => {
  const query = indexStat.indexRecommendations.map(recommendation => {
    switch (recommendation.type) {
      case "DROP_UNUSED":
        // Here, `tableName` is a fully qualified name whose identifiers have already been quoted.
        // See the QuoteIdentifier unit tests for more details.
        return `DROP INDEX ${tableName}@${QuoteIdentifier(
          indexStat.indexName,
        )};`;
    }
  });
  if (query.length === 0) {
    return <></>;
  }

  return (
    <IdxRecAction
      actionQuery={query.join(" ")}
      actionType={"DropIndex"}
      database={databaseName}
    />
  );
};

export const FormatMVCCInfo = ({
  details,
}: {
  details: DatabaseTablePageDataDetails;
}): JSX.Element => {
  return (
    <>
      {format.Percentage(details.spanStats?.live_percentage, 1, 1)}
      {" ("}
      <span className={cx("bold")}>
        {format.Bytes(details.spanStats?.live_bytes)}
      </span>{" "}
      live data /{" "}
      <span className={cx("bold")}>
        {format.Bytes(details.spanStats?.total_bytes)}
      </span>
      {" total)"}
    </>
  );
};

export const getCreateStmt = ({
  createStatement,
}: DatabaseTablePageDataDetails): string => {
  return createStatement?.create_statement
    ? createStatement?.create_statement
    : "(unavailable)\n" +
        sqlApiErrorMessage(createStatement?.error?.message || "");
};
