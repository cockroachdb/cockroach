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
import { checkInfoAvailable, getNetworkErrorMessage } from "../databases";

const cx = classNames.bind(styles);

export const DiskSizeCell = ({
  table,
}: {
  table: DatabaseDetailsPageDataTable;
}): JSX.Element => {
  return (
    <>
      {checkInfoAvailable(
        table.requestError,
        table.details?.spanStats?.error,
        table.details?.spanStats?.approximate_disk_bytes
          ? format.Bytes(table.details?.spanStats?.approximate_disk_bytes)
          : null,
      )}
    </>
  );
};

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
  let icon = <DatabaseIcon className={cx("icon--s", "icon--primary")} />;
  if (table.requestError || table.queryError) {
    icon = (
      <Tooltip
        overlayStyle={{ whiteSpace: "pre-line" }}
        placement="bottom"
        title={
          table.requestError
            ? getNetworkErrorMessage(table.requestError)
            : table.queryError.message
        }
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
    );
  }
  return (
    <Link to={linkURL} className={cx("icon__container")}>
      {icon}
      {table.name}
    </Link>
  );
};

export const IndexesCell = ({
  table,
  showIndexRecommendations,
}: {
  table: DatabaseDetailsPageDataTable;
  showIndexRecommendations: boolean;
}): JSX.Element => {
  const elem = (
    <>
      {checkInfoAvailable(
        table.requestError,
        table.details?.schemaDetails?.error,
        table.details?.schemaDetails?.indexes?.length,
      )}
    </>
  );
  // If index recommendations are not enabled or we don't have any index recommendations,
  // just return the number of indexes.
  if (
    !table.details.indexStatRecs?.has_index_recommendations ||
    !showIndexRecommendations
  ) {
    return elem;
  }
  // Display an icon indicating we have index recommendations next to the number of indexes.
  return (
    <div className={cx("icon__container")}>
      <Tooltip
        placement="bottom"
        title="This table has index recommendations. Click the table name to see more details."
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
      {elem}
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
        {format.Percentage(details?.spanStats?.live_percentage, 1, 1)}
      </p>
      <p className={cx("multiple-lines-info")}>
        <span className={cx("bold")}>
          {format.Bytes(details?.spanStats?.live_bytes)}
        </span>{" "}
        live data /{" "}
        <span className={cx("bold")}>
          {format.Bytes(details?.spanStats?.total_bytes)}
        </span>
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
