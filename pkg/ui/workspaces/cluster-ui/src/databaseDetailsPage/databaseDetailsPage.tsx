// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import { Link, RouteComponentProps } from "react-router-dom";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import classNames from "classnames/bind";
import { Breadcrumbs } from "src/breadcrumbs";
import { Dropdown, DropdownOption } from "src/dropdown";
import { CaretRight } from "src/icon/caretRight";
import { DatabaseIcon } from "src/icon/databaseIcon";
import { StackIcon } from "src/icon/stackIcon";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination } from "src/pagination";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import * as format from "src/util/format";
import {
  DATE_FORMAT,
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
} from "src/util/format";
import { mvccGarbage, syncHistory, unique } from "../util";

import styles from "./databaseDetailsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { Moment } from "moment-timezone";
import { Caution } from "@cockroachlabs/icons";
import { Anchor } from "../anchor";
import LoadingError from "../sqlActivity/errorComponent";
import { Loading } from "../loading";
import { Search } from "../search";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
} from "src/queryFilter";
import { UIConfigState } from "src/store";
import { TableStatistics } from "src/tableStatistics";
import { Timestamp, Timezone } from "../timestamp";

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
//     lastError: Error;
//     name: string;
//     sortSettingTables: SortSetting;
//     sortSettingGrants: SortSetting;
//     search: string;
//     filters: Filters;
//     nodeRegions: { [nodeId: string]: string };
//     isTenant: boolean;
//     viewMode: ViewMode;
//     tables: { // DatabaseDetailsPageDataTable[]
//       name: string;
//       details: { // DatabaseDetailsPageDataTableDetails
//         loading: boolean;
//         loaded: boolean;
//         lastError: Error;
//         columnCount: number;
//         indexCount: number;
//         userCount: number;
//         roles: string[];
//         grants: string[];
//         replicationSizeInBytes: number;
//         rangeCount: number;
//         nodes: number[];
//         nodesByRegionString: string;
//       };
//     }[];
//   }
export interface DatabaseDetailsPageData {
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  name: string;
  tables: DatabaseDetailsPageDataTable[];
  sortSettingTables: SortSetting;
  sortSettingGrants: SortSetting;
  search: string;
  filters: Filters;
  nodeRegions: { [nodeId: string]: string };
  isTenant?: UIConfigState["isTenant"];
  viewMode: ViewMode;
  showNodeRegionsColumn?: boolean;
}

export interface DatabaseDetailsPageDataTable {
  name: string;
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  details: DatabaseDetailsPageDataTableDetails;
}

export interface DatabaseDetailsPageDataTableDetails {
  columnCount: number;
  indexCount: number;
  userCount: number;
  roles: string[];
  grants: string[];
  statsLastUpdated?: Moment;
  hasIndexRecommendations: boolean;
  totalBytes: number;
  liveBytes: number;
  livePercentage: number;
  replicationSizeInBytes: number;
  rangeCount: number;
  // Array of node IDs used to unambiguously filter by node and region.
  nodes?: number[];
  // String of nodes grouped by region in alphabetical order, e.g.
  // regionA(n1,n2), regionB(n3). Used for display in the table's
  // "Regions/Nodes" column.
  nodesByRegionString?: string;
}

export interface DatabaseDetailsPageActions {
  refreshDatabaseDetails: (database: string) => void;
  refreshTableDetails: (database: string, table: string) => void;
  onFilterChange?: (value: Filters) => void;
  onSearchComplete?: (query: string) => void;
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
  filters?: Filters;
  activeFilters?: number;
  lastDetailsError: Error;
}

class DatabaseSortedTable extends SortedTable<DatabaseDetailsPageDataTable> {}

// filterBySearchQuery returns true if the search query matches the database name.
function filterBySearchQuery(
  table: DatabaseDetailsPageDataTable,
  search: string,
): boolean {
  const matchString = table.name.toLowerCase();

  if (search.startsWith('"') && search.endsWith('"')) {
    search = search.substring(1, search.length - 1);

    return matchString.includes(search);
  }

  const res = search
    .toLowerCase()
    .split(" ")
    .every(val => matchString.includes(val));

  return res;
}

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
      lastDetailsError: null,
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
    if (!this.props.loaded && !this.props.loading && !this.props.lastError) {
      return this.props.refreshDatabaseDetails(this.props.name);
    }

    let lastDetailsError: Error;
    this.props.tables.forEach(table => {
      if (table.lastError !== undefined) {
        lastDetailsError = table.lastError;
      }
      if (
        lastDetailsError &&
        this.state.lastDetailsError?.name != lastDetailsError?.name
      ) {
        this.setState({ lastDetailsError: lastDetailsError });
      }

      if (!table.loaded && !table.loading && table.lastError === undefined) {
        return this.props.refreshTableDetails(this.props.name, table.name);
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

  onClearSearchField = (): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete("");
    }

    syncHistory(
      {
        q: undefined,
      },
      this.props.history,
    );
  };

  onClearFilters = (): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(defaultFilters);
    }

    this.setState({
      filters: defaultFilters,
      activeFilters: 0,
    });

    this.resetPagination();
    syncHistory(
      {
        regions: undefined,
        nodes: undefined,
      },
      this.props.history,
    );
  };

  onSubmitSearchField = (search: string): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete(search);
    }

    this.resetPagination();
    syncHistory(
      {
        q: search,
      },
      this.props.history,
    );
  };

  onSubmitFilters = (filters: Filters): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(filters);
    }

    this.setState({
      filters: filters,
      activeFilters: calculateActiveFilters(filters),
    });

    this.resetPagination();
    syncHistory(
      {
        regions: filters.regions,
        nodes: filters.nodes,
      },
      this.props.history,
    );
  };

  resetPagination = (): void => {
    this.setState(prevState => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  // Returns a list of database tables to the display based on input from the
  // search box and the applied filters.
  filteredDatabaseTables = (): DatabaseDetailsPageDataTable[] => {
    const { search, tables, filters, nodeRegions } = this.props;

    const regionsSelected =
      filters.regions?.length > 0 ? filters.regions.split(",") : [];
    const nodesSelected =
      filters.nodes?.length > 0 ? filters.nodes.split(",") : [];

    return tables
      .filter(table => (search ? filterBySearchQuery(table, search) : true))
      .filter(table => {
        if (regionsSelected.length == 0 && nodesSelected.length == 0)
          return true;

        let foundRegion = regionsSelected.length == 0;
        let foundNode = nodesSelected.length == 0;

        table.details.nodes?.forEach(node => {
          const n = node?.toString() || "";
          if (foundRegion || regionsSelected.includes(nodeRegions[n])) {
            foundRegion = true;
          }
          if (foundNode || nodesSelected.includes("n" + n)) {
            foundNode = true;
          }
          if (foundNode && foundRegion) return true;
        });

        return foundRegion && foundNode;
      });
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

  formatMVCCInfo = (
    details: DatabaseDetailsPageDataTableDetails,
  ): React.ReactElement => {
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

  checkInfoAvailable = (
    error: Error,
    cell: React.ReactNode,
  ): React.ReactNode => {
    if (error) {
      return "(unavailable)";
    }
    return cell;
  };

  private columnsForTablesViewMode(): ColumnDescriptor<DatabaseDetailsPageDataTable>[] {
    return [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => (
          <Link
            to={EncodeDatabaseTableUri(this.props.name, table.name)}
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
            title="The approximate compressed total disk size across all replicas of the table."
          >
            Replication Size
          </Tooltip>
        ),
        cell: table =>
          this.checkInfoAvailable(
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
          this.checkInfoAvailable(table.lastError, table.details.rangeCount),
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
          this.checkInfoAvailable(table.lastError, table.details.columnCount),
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
          let cell;
          if (table.details.hasIndexRecommendations) {
            cell = (
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
          } else {
            cell = table.details.indexCount;
          }
          return this.checkInfoAvailable(table.lastError, cell);
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
          this.checkInfoAvailable(
            table.lastError,
            table.details.nodesByRegionString || "None",
          ),
        sort: table => table.details.nodesByRegionString,
        className: cx("database-table__col--regions"),
        name: "regions",
        showByDefault: this.props.showNodeRegionsColumn,
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
          this.checkInfoAvailable(
            table.lastError,
            this.formatMVCCInfo(table.details),
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
    ];
  }

  private columnsForGrantsViewMode(): ColumnDescriptor<DatabaseDetailsPageDataTable>[] {
    return [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => (
          <Link
            to={
              EncodeDatabaseTableUri(this.props.name, table.name) +
              `?tab=grants`
            }
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
          this.checkInfoAvailable(table.lastError, table.details.userCount),
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
          this.checkInfoAvailable(
            table.lastError,
            table.details.roles.join(", "),
          ),
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
          this.checkInfoAvailable(
            table.lastError,
            table.details.grants.join(", "),
          ),
        sort: table => table.details.grants.join(", "),
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
    const { search, filters, isTenant, nodeRegions } = this.props;

    const tablesToDisplay = this.filteredDatabaseTables();
    const activeFilters = calculateActiveFilters(filters);

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(Object.values(nodeRegions));

    const sortSetting =
      this.props.viewMode == ViewMode.Tables
        ? this.props.sortSettingTables
        : this.props.sortSettingGrants;

    const showNodes = !isTenant && nodes.length > 1;
    const showRegions = regions.length > 1;

    // Only show the filter component when the viewMode is Tables and if at
    // least one of drop-down is shown.
    const filterComponent =
      this.props.viewMode == ViewMode.Tables && (showNodes || showRegions) ? (
        <PageConfigItem>
          <Filter
            hideAppNames={true}
            regions={regions}
            hideTimeLabel={true}
            nodes={nodes.map(n => "n" + n?.toString())}
            activeFilters={activeFilters}
            filters={defaultFilters}
            onSubmitFilters={this.onSubmitFilters}
            showNodes={showNodes}
            showRegions={showRegions}
          />
        </PageConfigItem>
      ) : (
        <></>
      );

    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <Breadcrumbs
            items={[
              { link: "/databases", name: "Databases" },
              {
                link: EncodeDatabaseUri(this.props.name),
                name: "Tables",
              },
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
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField}
              onClear={this.onClearSearchField}
              defaultValue={search}
              placeholder={"Search Tables"}
            />
          </PageConfigItem>
          {filterComponent}
        </PageConfig>

        <section className={sortableTableCx("cl-table-container")}>
          <TableStatistics
            pagination={this.state.pagination}
            totalCount={tablesToDisplay.length}
            arrayItemName="tables"
            activeFilters={activeFilters}
            onClearFilters={this.onClearFilters}
          />
          <Loading
            loading={this.props.loading}
            page={"databases"}
            error={this.props.lastError}
            render={() => (
              <DatabaseSortedTable
                className={cx("database-table")}
                tableWrapperClassName={cx("sorted-table")}
                data={tablesToDisplay}
                columns={this.columns()}
                sortSetting={sortSetting}
                onChangeSortSetting={this.changeSortSetting}
                pagination={this.state.pagination}
                loading={this.props.loading}
                renderNoResult={
                  <div
                    className={cx(
                      "database-table__no-result",
                      "icon__container",
                    )}
                  >
                    <DatabaseIcon className={cx("icon--s")} />
                    This database has no tables.
                  </div>
                }
              />
            )}
            renderError={() =>
              LoadingError({
                statsType: "databases",
                timeout: this.props.lastError?.name
                  ?.toLowerCase()
                  .includes("timeout"),
              })
            }
          />
          {!this.props.loading && (
            <Loading
              loading={this.props.loading}
              page={"database_details"}
              error={this.state.lastDetailsError}
              render={() => <></>}
              renderError={() =>
                LoadingError({
                  statsType: "part of the information",
                  timeout: this.state.lastDetailsError?.name
                    ?.toLowerCase()
                    .includes("timeout"),
                })
              }
            />
          )}
        </section>

        <Pagination
          pageSize={this.state.pagination.pageSize}
          current={this.state.pagination.current}
          total={tablesToDisplay.length}
          onChange={this.changePage.bind(this)}
        />
      </div>
    );
  }
}
