// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Caution } from "@cockroachlabs/icons";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import React, { useContext } from "react";
import { Link } from "react-router-dom";

import { CockroachCloudContext } from "../contexts";
import {
  LoadingCell,
  getNetworkErrorMessage,
  getQueryErrorMessage,
} from "../databases";
import { CircleFilled } from "../icon";
import { StackIcon } from "../icon/stackIcon";
import { EncodeDatabaseUri } from "../util";
import * as format from "../util/format";

import { DatabasesPageDataDatabase } from "./databasesPage";
import styles from "./databasesPage.module.scss";

const cx = classNames.bind(styles);

interface CellProps {
  database: DatabasesPageDataDatabase;
}

export const DiskSizeCell = ({ database }: CellProps) => (
  <LoadingCell
    requestError={database.spanStatsRequestError}
    queryError={database.spanStats?.error}
    loading={database.spanStatsLoading}
    errorClassName={cx("databases-table__cell-error")}
  >
    {database.spanStats?.approximate_disk_bytes
      ? format.Bytes(database.spanStats?.approximate_disk_bytes)
      : null}
  </LoadingCell>
);

export const IndexRecCell = ({ database }: CellProps): JSX.Element => {
  const text =
    database.numIndexRecommendations > 0
      ? `${database.numIndexRecommendations} index recommendation(s)`
      : "None";
  const classname =
    database.numIndexRecommendations > 0
      ? "index-recommendations-icon__exist"
      : "index-recommendations-icon__none";
  return (
    <div>
      <CircleFilled className={cx(classname)} />
      <span>{text}</span>
    </div>
  );
};

export const DatabaseNameCell = ({ database }: CellProps): JSX.Element => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  const linkURL = isCockroachCloud
    ? `${location.pathname}/${database.name}`
    : EncodeDatabaseUri(database.name);
  let icon = <StackIcon className={cx("icon--s", "icon--primary")} />;

  const needsWarning =
    database.detailsRequestError ||
    database.spanStatsRequestError ||
    database.detailsQueryError ||
    database.spanStatsQueryError;

  if (needsWarning) {
    const titleList = [];
    if (database.detailsRequestError) {
      titleList.push(getNetworkErrorMessage(database.detailsRequestError));
    }
    if (database.spanStatsRequestError) {
      titleList.push(getNetworkErrorMessage(database.spanStatsRequestError));
    }
    if (database.detailsQueryError) {
      titleList.push(database.detailsQueryError.message);
    }
    if (database.spanStatsQueryError) {
      titleList.push(getQueryErrorMessage(database.spanStatsQueryError));
    }

    icon = (
      <Tooltip
        overlayStyle={{ whiteSpace: "pre-line" }}
        placement="bottom"
        title={titleList.join("\n")}
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
    );
  }
  return (
    <>
      <Link to={linkURL} className={cx("icon__container")}>
        {icon}
        {database.name}
      </Link>
    </>
  );
};
