// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import { CircleFilled } from "../icon";
import { DatabasesPageDataDatabase } from "./databasesPage";
import classNames from "classnames/bind";
import styles from "./databasesPage.module.scss";
import { EncodeDatabaseUri } from "../util";
import { Link } from "react-router-dom";
import { StackIcon } from "../icon/stackIcon";
import { CockroachCloudContext } from "../contexts";

const cx = classNames.bind(styles);

interface CellProps {
  database: DatabasesPageDataDatabase;
}

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
  return (
    <Link to={linkURL} className={cx("icon__container")}>
      <StackIcon className={cx("icon--s", "icon--primary")} />
      {database.name}
    </Link>
  );
};
