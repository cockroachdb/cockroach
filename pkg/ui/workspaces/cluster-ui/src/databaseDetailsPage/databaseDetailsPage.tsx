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
import { Link, RouteComponentProps } from "react-router-dom";
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
import { syncHistory } from "../util";

import styles from "./databaseDetailsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "src/transactionsPage/transactionsPageClasses";
import { Moment } from "moment";
import { formatDate } from "antd/es/date-picker/utils";

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
//     sortSettingTables: SortSetting;
//     sortSettingGrants: SortSetting;
//     viewMode: ViewMode;
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
//         nodesByRegionString: string;
//       };
//     }[];
//   }
export interface DatabaseDetailsPageData {
  loading: boolean;
  loaded: boolean;
  name: string;
  tables: DatabaseDetailsPageDataTable[];
  sortSettingTables: SortSetting;
  sortSettingGrants: SortSetting;
  viewMode: ViewMode;
  showNodeRegionsColumn?: boolean;
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
  statsLastUpdated?: Moment;
}

export interface DatabaseDetailsPageDataTableStats {
  loading: boolean;
  loaded: boolean;
  replicationSizeInBytes: number;
  rangeCount: number;
  nodesByRegionString?: string;
}

export interface DatabaseDetailsPageActions {
  refreshDatabaseDetails: (database: string) => void;
  refreshTableDetails: (database: string, table: string) => void;
  refreshTableStats: (database: string, table: string) => void;
  onSortingTablesChange?: (columnTitle: string, ascending: boolean) => void;
  onSortingGrantsChange?: (columnTitle: string, ascending: boolean) => void;
  onViewModeChange?: (viewMode: ViewMode) => void;
}

export type DatabaseDetailsPageProps = DatabaseDetailsPageData &
  DatabaseDetailsPageActions &
  RouteComponentProps<unknown>;

export enum ViewMode {
  Tables = "Tables",
  Grants = "Grants",
}

interface DatabaseDetailsPageState {
  pagination: ISortedTablePagination;
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
    };

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // View Mode.
    const view = searchParams.get("viewMode") || undefined;
    let viewMode = ViewMode.Tables;
    if (view == ViewMode.Grants.toString()) {
      viewMode = ViewMode.Grants;
    }
    if (
      this.props.onViewModeChange &&
      view &&
      viewMode != this.props.viewMode
    ) {
      this.props.onViewModeChange(viewMode);
    }

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting =
      viewMode == ViewMode.Tables
        ? this.props.sortSettingTables
        : this.props.sortSettingGrants;
    const onSortingChange =
      viewMode == ViewMode.Tables
        ? this.props.onSortingTablesChange
        : this.props.onSortingGrantsChange;

    if (
      onSortingChange &&
      columnTitle &&
      (sortSetting.columnTitle != columnTitle ||
        sortSetting.ascending != ascending)
    ) {
      onSortingChange(columnTitle, ascending);
    }
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  private refresh(): void {
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

  changeSortSetting = (ss: SortSetting): void => {
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
    const onSortingChange =
      this.props.viewMode == ViewMode.Tables
        ? this.props.onSortingTablesChange
        : this.props.onSortingGrantsChange;

    if (onSortingChange) {
      onSortingChange(ss.columnTitle, ss.ascending);
    }
  };

  private changeViewMode(viewMode: ViewMode) {
    syncHistory(
      {
        viewMode: viewMode.toString(),
      },
      this.props.history,
    );
    if (this.props.onViewModeChange) {
      this.props.onViewModeChange(viewMode);
    }
  }

  private columns(): ColumnDescriptor<DatabaseDetailsPageDataTable>[] {
    switch (this.props.viewMode) {
      case ViewMode.Tables:
        return this.columnsForTablesViewMode();
      case ViewMode.Grants:
        return this.columnsForGrantsViewMode();
      default:
        throw new Error(`Unknown view mode ${this.props.viewMode}`);
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
      {
        title: (
          <Tooltip
            placement="bottom"
            title="Regions/nodes on which the table data is stored."
          >
            Regions
          </Tooltip>
        ),
        cell: table => table.stats.nodesByRegionString || "None",
        sort: table => table.stats.nodesByRegionString,
        className: cx("database-table__col--regions"),
        name: "regions",
        showByDefault: this.props.showNodeRegionsColumn,
        hideIfTenant: true,
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The last time table statistics were created or updated."
          >
            Table Stats Last Updated (UTC)
          </Tooltip>
        ),
        cell: table =>
          !table.details.statsLastUpdated
            ? "No table statistics found"
            : formatDate(
                table.details.statsLastUpdated,
                "MMM DD, YYYY [at] h:mm A",
              ),
        sort: table => table.details.statsLastUpdated,
        className: cx("database-table__col--table-stats"),
        name: "tableStatsUpdated",
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
            to={`/database/${this.props.name}/table/${table.name}?tab=grants`}
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
        cell: table => table.details.userCount,
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
        cell: table => _.join(table.details.roles, ", "),
        sort: table => _.join(table.details.roles, ", "),
        className: cx("database-table__col-roles"),
        name: "roles",
      },
      {
        title: (
          <Tooltip placement="bottom" title="The list of grants of the table.">
            Grants
          </Tooltip>
        ),
        cell: table => _.join(table.details.grants, ", "),
        sort: table => _.join(table.details.grants, ", "),
        className: cx("database-table__col-grants"),
        name: "grants",
      },
    ];
  }

  private static viewOptions(): DropdownOption<ViewMode>[] {
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

  render(): React.ReactElement {
    const sortSetting =
      this.props.viewMode == ViewMode.Tables
        ? this.props.sortSettingTables
        : this.props.sortSettingGrants;

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

          <h3
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <StackIcon className={cx("icon--md", "icon--title")} />
            {this.props.name}
          </h3>
        </section>

        <PageConfig>
          <PageConfigItem>
            <Dropdown
              items={DatabaseDetailsPage.viewOptions()}
              onChange={this.changeViewMode.bind(this)}
            >
              View: {this.props.viewMode}
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
            sortSetting={sortSetting}
            onChangeSortSetting={this.changeSortSetting}
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
