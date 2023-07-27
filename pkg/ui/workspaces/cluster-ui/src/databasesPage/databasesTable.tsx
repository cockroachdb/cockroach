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
import { ColumnDescriptor, SortedTable } from "../sortedtable";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import * as format from "../util/format";
import { createSelector } from "@reduxjs/toolkit";
const cx = classNames.bind(styles);

export class DatabasesSortedTable extends SortedTable<DatabasesPageDataDatabase> {}

interface CellProps {
  database: DatabasesPageDataDatabase;
}

const IndexRecCell = ({ database }: CellProps): JSX.Element => {
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

const DatabaseNameCell = ({ database }: CellProps): JSX.Element => {
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

interface DatabaseTableColumnsProps {
  indexRecommendationsEnabled: boolean;
  showNodeRegionsColumn: boolean;
  isTenant: boolean;
}

export const getMemoizedDatabaseColumns = createSelector(
  (state: DatabaseTableColumnsProps) => state.indexRecommendationsEnabled,
  (state: DatabaseTableColumnsProps) => state.showNodeRegionsColumn,
  (state: DatabaseTableColumnsProps) => state.isTenant,
  (
    indexRecommendationsEnabled,
    showNodeRegionsColumn,
    isTenant,
  ): ColumnDescriptor<DatabasesPageDataDatabase>[] => {
    return getDatabaseColumns(
      indexRecommendationsEnabled,
      showNodeRegionsColumn,
      isTenant,
    );
  },
);

const getDatabaseColumns = (
  indexRecommendationsEnabled: boolean,
  showNodeRegionsColumn: boolean,
  isTenant: boolean,
): ColumnDescriptor<DatabasesPageDataDatabase>[] => {
  const columns: ColumnDescriptor<DatabasesPageDataDatabase>[] = [
    {
      title: (
        <Tooltip placement="bottom" title="The name of the database.">
          Databases
        </Tooltip>
      ),
      cell: database => <DatabaseNameCell database={database} />,
      sort: database => database.name,
      className: cx("databases-table__col-name"),
      name: "name",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The approximate total disk size across all table replicas in the database."
        >
          Size
        </Tooltip>
      ),
      cell: database =>
        checkInfoAvailable(database, format.Bytes(database.sizeInBytes)),
      sort: database => database.sizeInBytes,
      className: cx("databases-table__col-size"),
      name: "size",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The total number of tables in the database."
        >
          Tables
        </Tooltip>
      ),
      cell: database => checkInfoAvailable(database, database.tableCount),
      sort: database => database.tableCount,
      className: cx("databases-table__col-table-count"),
      name: "tableCount",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The total number of ranges across all tables in the database."
        >
          Range Count
        </Tooltip>
      ),
      cell: database => checkInfoAvailable(database, database.rangeCount),
      sort: database => database.rangeCount,
      className: cx("databases-table__col-range-count"),
      name: "rangeCount",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="Regions/Nodes on which the database tables are located."
        >
          {isTenant ? "Regions" : "Regions/Nodes"}
        </Tooltip>
      ),
      cell: database =>
        checkInfoAvailable(database, database.nodesByRegionString || "None"),
      sort: database => database.nodesByRegionString,
      className: cx("databases-table__col-node-regions"),
      name: "nodeRegions",
      hideIfTenant: true,
      showByDefault: showNodeRegionsColumn,
    },
  ];
  if (indexRecommendationsEnabled) {
    columns.push({
      title: (
        <Tooltip
          placement="bottom"
          title="Index recommendations will appear if the system detects improper index usage, such as the
        occurrence of unused indexes. Following index recommendations may help improve query performance."
        >
          Index Recommendations
        </Tooltip>
      ),
      cell: database => <IndexRecCell database={database} />,
      sort: database => database.numIndexRecommendations,
      className: cx("databases-table__col-idx-rec"),
      name: "numIndexRecommendations",
    });
  }
  return columns;
};

const checkInfoAvailable = (
  database: DatabasesPageDataDatabase,
  cell: React.ReactNode,
): React.ReactNode => {
  if (
    database.lastError &&
    database.lastError.name !== "GetDatabaseInfoError"
  ) {
    return "(unavailable)";
  }
  return cell;
};
