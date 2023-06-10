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
import {
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
  EncodeUriName,
  getMatchParamByName,
  schemaNameAttr,
} from "../util";
import { Link } from "react-router-dom";
import { DatabaseIcon } from "../icon/databaseIcon";
import {
  DatabaseDetailsPageDataTable,
  DatabaseDetailsPageDataTableDetails,
  DatabaseDetailsPageProps,
  ViewMode,
} from "./databaseDetailsPage";
import classNames from "classnames/bind";
import styles from "./databaseDetailsPage.module.scss";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import { Caution } from "@cockroachlabs/icons";
import * as format from "../util/format";
import { Breadcrumbs } from "../breadcrumbs";
import { CaretRight } from "../icon/caretRight";
import { CockroachCloudContext } from "../contexts";

const cx = classNames.bind(styles);

export const TableNameCell = ({
  table,
  dbDetails,
}: {
  table: DatabaseDetailsPageDataTable;
  dbDetails: DatabaseDetailsPageProps;
}): JSX.Element => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  let linkURL = "";
  if (isCockroachCloud) {
    linkURL = `${location.pathname}/${EncodeUriName(
      getMatchParamByName(dbDetails.match, schemaNameAttr),
    )}/${EncodeUriName(table.name)}`;
    if (dbDetails.viewMode === ViewMode.Grants) {
      linkURL += `?viewMode=${ViewMode.Grants}`;
    }
  } else {
    linkURL = EncodeDatabaseTableUri(dbDetails.name, table.name);
    if (dbDetails.viewMode === ViewMode.Grants) {
      linkURL += `?tab=grants`;
    }
  }
  return (
    <Link to={linkURL} className={cx("icon__container")}>
      <DatabaseIcon className={cx("icon--s", "icon--primary")} />
      {table.name}
    </Link>
  );
};

export const IndexRecWithIconCell = ({
  table,
}: {
  table: DatabaseDetailsPageDataTable;
}): JSX.Element => {
  return (
    <div className={cx("icon__container")}>
      <Tooltip
        placement="bottom"
        title="This table has index recommendations. Click the table name to see more details."
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
      {table.details.indexCount}
    </div>
  );
};

export const MVCCInfoCell = ({
  details,
}: {
  details: DatabaseDetailsPageDataTableDetails;
}): JSX.Element => {
  return (
    <>
      <p className={cx("multiple-lines-info")}>
        {format.Percentage(details.livePercentage, 1, 1)}
      </p>
      <p className={cx("multiple-lines-info")}>
        <span className={cx("bold")}>{format.Bytes(details.liveBytes)}</span>{" "}
        live data /{" "}
        <span className={cx("bold")}>{format.Bytes(details.totalBytes)}</span>
        {" total"}
      </p>
    </>
  );
};

export const DbDetailsBreadcrumbs = ({ dbName }: { dbName: string }) => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  return (
    <Breadcrumbs
      items={[
        { link: "/databases", name: "Databases" },
        {
          link: isCockroachCloud
            ? `/databases/${EncodeUriName(dbName)}`
            : EncodeDatabaseUri(dbName),
          name: "Tables",
        },
      ]}
      divider={<CaretRight className={cx("icon--xxs", "icon--primary")} />}
    />
  );
};
