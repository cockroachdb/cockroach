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
import { CockroachCloudContext } from "../contexts";
import { EncodeDatabaseUri } from "../util";
import { Link } from "react-router-dom";
import { StackIcon } from "../icon/stackIcon";

const cx = classNames.bind(styles);

export const indexRecCell = (
  database: DatabasesPageDataDatabase,
): React.ReactNode => {
  const text =
    database.numIndexRecommendations > 0
      ? `${database.numIndexRecommendations} index ${
          database.numIndexRecommendations > 1
            ? "recommendations"
            : "recommendation"
        }`
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

export const databaseNameCell = (
  database: DatabasesPageDataDatabase,
  isCockroachCloud: boolean,
): React.ReactNode => {
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
