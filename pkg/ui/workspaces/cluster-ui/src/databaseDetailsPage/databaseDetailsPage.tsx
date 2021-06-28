// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import _ from "lodash";

import { Breadcrumbs } from "src/breadcrumbs";
import { Dropdown, DropdownOption } from "src/dropdown";
import { CaretRight } from "src/icon/caretRight";
import { DatabaseIcon } from "src/icon/databaseIcon";
import { StackIcon } from "src/icon/stackIcon";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortSetting,
  SortedTable,
} from "src/sortedtable";
import * as format from "src/util/format";

import styles from "./databaseDetailsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "src/transactionsPage/transactionsPageClasses";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

// We break out separate interfaces for some of the nested objects in our data
// both so that they can be available as SortedTable rows and for making
// (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface DatabaseDetailsPageData {
//     loading: boolean;
//     loaded: boolean;
//     name: string;
//     tables: { // DatabaseDetailsPageDataTable[]
//       name: string;
//       details: { // DatabaseDetailsPageDataTableDetails
//         loading: boolean;
//         loaded: boolean;
//         columnCount: number;
//         indexCount: number;
//         userCount: number;
//         roles: string[];
//         grants: string[];
//       };
//       stats: {  // DatabaseDetailsPageDataTableStats
//         loading: boolean;
//         loaded: boolean;
//         replicationSizeInBytes: number;
//         rangeCount: number;
//       };
//     }[];
//   }
export interface DatabaseDetailsPageData {
  loading: boolean;
  loaded: boolean;
  name: string;
  tables: DatabaseDetailsPageDataTable[];
}

export interface DatabaseDetailsPageDataTable {
  name: string;
  details: DatabaseDetailsPageDataTableDetails;
  stats: DatabaseDetailsPageDataTableStats;
}

export interface DatabaseDetailsPageDataTableDetails {
  loading: boolean;
  loaded: boolean;
  columnCount: number;
  indexCount: number;
  userCount: number;
  roles: string[];
  grants: string[];
}

export interface DatabaseDetailsPageDataTableStats {
  loading: boolean;
  loaded: boolean;
  replicationSizeInBytes: number;
  rangeCount: number;
}

export interface DatabaseDetailsPageActions {
  refreshDatabaseDetails: (database: string) => void;
  refreshTableDetails: (database: string, table: string) => void;
  refreshTableStats: (database: string, table: string) => void;
}

export type DatabaseDetailsPageProps = DatabaseDetailsPageData &
  DatabaseDetailsPageActions;

enum ViewMode {
  Tables = "Tables",
  Grants = "Grants",
}

interface DatabaseDetailsPageState {
  pagination: ISortedTablePagination;
  sortSetting: SortSetting;
  viewMode: ViewMode;
}

class DatabaseSortedTable extends SortedTable<DatabaseDetailsPageDataTable> {}

export class DatabaseDetailsPage extends React.Component<
  DatabaseDetailsPageProps,
  DatabaseDetailsPageState
> {
  constructor(props: DatabaseDetailsPageProps) {
    super(props);

    this.state = {
      pagination: {
        current: 1,
        pageSize: 20,
      },
      sortSetting: {
        ascending: true,
      },
      viewMode: ViewMode.Tables,
    };
  }

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate() {
    this.refresh();
  }

  private refresh() {
    if (!this.props.loaded && !this.props.loading) {
      return this.props.refreshDatabaseDetails(this.props.name);
    }

    _.forEach(this.props.tables, table => {
      if (!table.details.loaded && !table.details.loading) {
        return this.props.refreshTableDetails(this.props.name, table.name);
      }

      if (!table.stats.loaded && !table.stats.loading) {
        return this.props.refreshTableStats(this.props.name, table.name);
      }
    });
  }

  private changePage(current: number) {
    this.setState({ pagination: { ...this.state.pagination, current } });
  }

  private changeSortSetting(sortSetting: SortSetting) {
    this.setState({ sortSetting });
  }

  private changeViewMode(viewMode: ViewMode) {
    this.setState({ viewMode });
  }

  private columns(): ColumnDescriptor<DatabaseDetailsPageDataTable>[] {
    switch (this.state.viewMode) {
      case ViewMode.Tables:
        return this.columnsForTablesViewMode();
      case ViewMode.Grants:
        return this.columnsForGrantsViewMode();
      default:
        throw new Error(`Unknown view mode ${this.state.viewMode}`);
    }
  }

  private columnsForTablesViewMode(): ColumnDescriptor<
    DatabaseDetailsPageDataTable
  >[] {
    return [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => (
          <Link
            to={`/database/${this.props.name}/table/${table.name}`}
            className={cx("icon__container")}
          >
            <DatabaseIcon className={cx("icon--s", "icon--primary")} />
            {table.name}
          </Link>
        ),
        sort: table => table.name,
        className: cx("database-table__col-name"),
        name: "name",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The approximate total disk size across all replicas of the table."
          >
            Replication Size
          </Tooltip>
        ),
        cell: table => format.Bytes(table.stats.replicationSizeInBytes),
        sort: table => table.stats.replicationSizeInBytes,
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
        cell: table => table.stats.rangeCount,
        sort: table => table.stats.rangeCount,
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
        cell: table => table.details.columnCount,
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
        cell: table => table.details.indexCount,
        sort: table => table.details.indexCount,
        className: cx("database-table__col-index-count"),
        name: "indexCount",
      },
    ];
  }

  private columnsForGrantsViewMode(): ColumnDescriptor<
    DatabaseDetailsPageDataTable
  >[] {
    return [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => (
          <Link
            to={`/database/${this.props.name}/table/${table.name}`}
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
        title: "Users",
        cell: table => table.details.userCount,
        sort: table => table.details.userCount,
        className: cx("database-table__col-user-count"),
        name: "userCount",
      },
      {
        title: "Roles",
        cell: table => _.join(table.details.roles, ", "),
        sort: table => _.join(table.details.roles, ", "),
        className: cx("database-table__col-roles"),
        name: "roles",
      },
      {
        title: "Grants",
        cell: table => _.join(table.details.grants, ", "),
        sort: table => _.join(table.details.grants, ", "),
        className: cx("database-table__col-grants"),
        name: "grants",
      },
    ];
  }

  private viewOptions(): DropdownOption<ViewMode>[] {
    return [
      {
        name: "Tables",
        value: ViewMode.Tables,
      },
      {
        name: "Grants",
        value: ViewMode.Grants,
      },
    ];
  }

  render() {
    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <Breadcrumbs
            items={[
              { link: "/databases", name: "Databases" },
              { link: `/database/${this.props.name}`, name: "Tables" },
            ]}
            divider={
              <CaretRight className={cx("icon--xxs", "icon--primary")} />
            }
          />

          <h1
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <StackIcon className={cx("icon--md", "icon--title")} />
            {this.props.name}
          </h1>
        </section>

        <PageConfig>
          <PageConfigItem>
            <Dropdown
              items={this.viewOptions()}
              onChange={this.changeViewMode.bind(this)}
            >
              View: {this.state.viewMode}
            </Dropdown>
          </PageConfigItem>
        </PageConfig>

        <section className={sortableTableCx("cl-table-container")}>
          <div className={statisticsClasses.statistic}>
            <h4 className={statisticsClasses.countTitle}>
              <ResultsPerPageLabel
                pagination={{
                  ...this.state.pagination,
                  total: this.props.tables.length,
                }}
                pageName={this.props.tables.length == 1 ? "table" : "tables"}
              />
            </h4>
          </div>

          <DatabaseSortedTable
            className={cx("database-table")}
            data={this.props.tables}
            columns={this.columns()}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting.bind(this)}
            pagination={this.state.pagination}
            loading={this.props.loading}
            renderNoResult={
              <div
                className={cx("database-table__no-result", "icon__container")}
              >
                <DatabaseIcon className={cx("icon--s")} />
                This database has no tables.
              </div>
            }
          />
        </section>

        <Pagination
          pageSize={this.state.pagination.pageSize}
          current={this.state.pagination.current}
          total={this.props.tables.length}
          onChange={this.changePage.bind(this)}
        />
      </div>
    );
  }
}
