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
  DATE_FORMAT,
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
  EncodeUriName,
  getMatchParamByName,
  mvccGarbage,
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
import { ColumnDescriptor, SortedTable } from "../sortedtable";
import { Anchor } from "../anchor";
import { Timestamp, Timezone } from "../timestamp";

const cx = classNames.bind(styles);

export class DatabaseDetailsSortedTable extends SortedTable<DatabaseDetailsPageDataTable> {}

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

export const getDatabaseDetailsColumns = (
  dbDetails: DatabaseDetailsPageProps,
): ColumnDescriptor<DatabaseDetailsPageDataTable>[] => {
  switch (dbDetails.viewMode) {
    case ViewMode.Tables:
      return columnsForTablesViewMode(dbDetails);
    case ViewMode.Grants:
      return columnsForGrantsViewMode(dbDetails.name);
    default:
      throw new Error(`Unknown view mode ${dbDetails.viewMode}`);
  }
};

const columnsForTablesViewMode = (
  dbDetails: DatabaseDetailsPageProps,
): ColumnDescriptor<DatabaseDetailsPageDataTable>[] => {
  return (
    [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => <TableNameCell table={table} dbDetails={dbDetails} />,
        sort: table => table.name,
        className: cx("database-table__col-name"),
        name: "name",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The approximate compressed total disk size across all replicas of the table."
          >
            Replication Size
          </Tooltip>
        ),
        cell: table =>
          checkInfoAvailable(
            table.lastError,
            format.Bytes(table.details.replicationSizeInBytes),
          ),
        sort: table => table.details.replicationSizeInBytes,
        className: cx("database-table__col-size"),
        name: "replicationSize",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of ranges in the table."
          >
            Ranges
          </Tooltip>
        ),
        cell: table =>
          checkInfoAvailable(table.lastError, table.details.rangeCount),
        sort: table => table.details.rangeCount,
        className: cx("database-table__col-range-count"),
        name: "rangeCount",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The number of columns in the table."
          >
            Columns
          </Tooltip>
        ),
        cell: table =>
          checkInfoAvailable(table.lastError, table.details.columnCount),
        sort: table => table.details.columnCount,
        className: cx("database-table__col-column-count"),
        name: "columnCount",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The number of indexes in the table."
          >
            Indexes
          </Tooltip>
        ),
        cell: table => {
          return table.details.hasIndexRecommendations &&
            dbDetails.showIndexRecommendations
            ? checkInfoAvailable(
                table.lastError,
                <IndexRecWithIconCell table={table} />,
              )
            : checkInfoAvailable(table.lastError, table.details.indexCount);
        },
        sort: table => table.details.indexCount,
        className: cx("database-table__col-index-count"),
        name: "indexCount",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="Regions/Nodes on which the table data is stored."
          >
            Regions
          </Tooltip>
        ),
        cell: table =>
          checkInfoAvailable(
            table.lastError,
            table.details.nodesByRegionString || "None",
          ),
        sort: table => table.details.nodesByRegionString,
        className: cx("database-table__col--regions"),
        name: "regions",
        showByDefault: dbDetails.showNodeRegionsColumn,
        hideIfTenant: true,
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title={
              <div className={cx("tooltip__table--title")}>
                {"% of total uncompressed logical data that has not been modified (updated or deleted). " +
                  "A low percentage can cause statements to scan more data ("}
                <Anchor href={mvccGarbage} target="_blank">
                  MVCC values
                </Anchor>
                {") than required, which can reduce performance."}
              </div>
            }
          >
            % of Live Data
          </Tooltip>
        ),
        cell: table =>
          checkInfoAvailable(
            table.lastError,
            <MVCCInfoCell details={table.details} />,
          ),
        sort: table => table.details.livePercentage,
        className: cx("database-table__col-column-count"),
        name: "livePercentage",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The last time table statistics were created or updated."
          >
            Table Stats Last Updated <Timezone />
          </Tooltip>
        ),
        cell: table => (
          <Timestamp
            time={table.details.statsLastUpdated}
            format={DATE_FORMAT}
            fallback={"No table statistics found"}
          />
        ),
        sort: table => table.details.statsLastUpdated,
        className: cx("database-table__col--table-stats"),
        name: "tableStatsUpdated",
      },
    ] as ColumnDescriptor<DatabaseDetailsPageDataTable>[]
  ).filter(c => c.showByDefault !== false);
};

const columnsForGrantsViewMode = (
  dbName: string,
): ColumnDescriptor<DatabaseDetailsPageDataTable>[] => {
  return [
    {
      title: (
        <Tooltip placement="bottom" title="The name of the table.">
          Tables
        </Tooltip>
      ),
      cell: table => (
        <Link
          to={EncodeDatabaseTableUri(dbName, table.name) + `?tab=grants`}
          className={cx("icon__container")}
        >
          <DatabaseIcon className={cx("icon--s")} />
          {table.name}
        </Link>
      ),
      sort: table => table.name,
      className: cx("database-table__col-name"),
      name: "name",
    },
    {
      title: (
        <Tooltip placement="bottom" title="The number of users of the table.">
          Users
        </Tooltip>
      ),
      cell: table =>
        checkInfoAvailable(table.lastError, table.details.userCount),
      sort: table => table.details.userCount,
      className: cx("database-table__col-user-count"),
      name: "userCount",
    },
    {
      title: (
        <Tooltip placement="bottom" title="The list of roles of the table.">
          Roles
        </Tooltip>
      ),
      cell: table =>
        checkInfoAvailable(table.lastError, table.details.roles.join(", ")),
      sort: table => table.details.roles.join(", "),
      className: cx("database-table__col-roles"),
      name: "roles",
    },
    {
      title: (
        <Tooltip placement="bottom" title="The list of grants of the table.">
          Grants
        </Tooltip>
      ),
      cell: table =>
        checkInfoAvailable(table.lastError, table.details.grants.join(", ")),
      sort: table => table.details.grants.join(", "),
      className: cx("database-table__col-grants"),
      name: "grants",
    },
  ];
};

const checkInfoAvailable = (
  error: Error,
  cell: React.ReactNode,
): React.ReactNode => {
  if (error) {
    return "(unavailable)";
  }
  return cell;
};
