// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  DATE_FORMAT,
  DATE_FORMAT_24_TZ,
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
  EncodeUriName,
  minDate,
  performanceTuningRecipes,
} from "../util";
import { Timestamp, Timezone } from "../timestamp";
import React, { useContext } from "react";
import { Moment } from "moment-timezone";
import {
  DatabaseTablePageDataDetails,
  Grant,
  IndexStat,
} from "./databaseTablePage";
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
import { createSelector } from "@reduxjs/toolkit";
import { ColumnDescriptor, SortedTable } from "../sortedtable";
const cx = classNames.bind(styles);

export class DatabaseTableGrantsTable extends SortedTable<Grant> {}

export class IndexUsageStatsTable extends SortedTable<IndexStat> {}

const NameCell = ({
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
        return `DROP INDEX ${QuoteIdentifier(tableName)}@${QuoteIdentifier(
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
      {format.Percentage(details.livePercentage, 1, 1)}
      {" ("}
      <span className={cx("bold")}>{format.Bytes(details.liveBytes)}</span> live
      data /{" "}
      <span className={cx("bold")}>{format.Bytes(details.totalBytes)}</span>
      {" total)"}
    </>
  );
};

interface TableIndexStatsColumnsProps {
  showIndexRecommendations: boolean;
  isCockroachCloud: boolean;
  dbName: string;
  tableName: string;
}

export const getMemoizedTableIndexStatsColumns = createSelector(
  (state: TableIndexStatsColumnsProps) => state.showIndexRecommendations,
  (state: TableIndexStatsColumnsProps) => state.isCockroachCloud,
  (state: TableIndexStatsColumnsProps) => state.dbName,
  (state: TableIndexStatsColumnsProps) => state.tableName,
  (
    showIndexRecommendations,
    isCockroachCloud,
    dbName,
    tableName,
  ): ColumnDescriptor<IndexStat>[] => {
    return getTableIndexStatsColumns(
      showIndexRecommendations,
      isCockroachCloud,
      dbName,
      tableName,
    );
  },
);

const getTableIndexStatsColumns = (
  showIndexRecommendations: boolean,
  isCockroachCloud: boolean,
  dbName: string,
  tableName: string,
): ColumnDescriptor<IndexStat>[] => {
  const indexStatsColumns: ColumnDescriptor<IndexStat>[] = [
    {
      name: "indexes",
      title: "Indexes",
      hideTitleUnderline: true,
      className: cx("index-stats-table__col-indexes"),
      cell: indexStat => (
        <NameCell
          indexStat={indexStat}
          tableName={tableName}
          showIndexRecommendations={showIndexRecommendations}
        />
      ),
      sort: indexStat => indexStat.indexName,
    },
    {
      name: "total reads",
      title: "Total Reads",
      hideTitleUnderline: true,
      cell: indexStat => format.Count(indexStat.totalReads),
      sort: indexStat => indexStat.totalReads,
    },
    {
      name: "last used",
      title: (
        <>
          Last Used <Timezone />
        </>
      ),
      hideTitleUnderline: true,
      className: cx("index-stats-table__col-last-used"),
      cell: indexStat => <LastUsed indexStat={indexStat} />,
      sort: indexStat => indexStat.lastUsed,
    },
  ];
  if (showIndexRecommendations) {
    indexStatsColumns.push({
      name: "index recommendations",
      title: (
        <Tooltip
          placement="bottom"
          title="Index recommendations will appear if the system detects improper index usage, such as the occurrence of unused indexes. Following index recommendations may help improve query performance."
        >
          Index Recommendations
        </Tooltip>
      ),
      cell: indexStat => <IndexRecCell indexStat={indexStat} />,
      sort: indexStat => indexStat.indexRecommendations.length,
    });
    if (!isCockroachCloud) {
      indexStatsColumns.push({
        name: "action",
        title: "",
        cell: indexStat => (
          <ActionCell
            indexStat={indexStat}
            databaseName={dbName}
            tableName={tableName}
          />
        ),
      });
    }
  }
  return indexStatsColumns;
};

export const grantsColumns: ColumnDescriptor<Grant>[] = [
  {
    name: "username",
    title: (
      <Tooltip placement="bottom" title="The user name.">
        User Name
      </Tooltip>
    ),
    cell: grant => grant.user,
    sort: grant => grant.user,
  },
  {
    name: "privilege",
    title: (
      <Tooltip placement="bottom" title="The list of grants for the user.">
        Grants
      </Tooltip>
    ),
    cell: grant => grant.privileges.join(", "),
    sort: grant => grant.privileges.join(", "),
  },
];
