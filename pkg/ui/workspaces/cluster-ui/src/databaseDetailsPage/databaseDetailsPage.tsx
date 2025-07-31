// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import React from "react";
import { Link, RouteComponentProps } from "react-router-dom";

import { Dropdown, DropdownOption } from "src/dropdown";
import { DatabaseIcon } from "src/icon/databaseIcon";
import { StackIcon } from "src/icon/stackIcon";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination } from "src/pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
} from "src/queryFilter";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { UIConfigState } from "src/store";
import { TableStatistics } from "src/tableStatistics";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { DATE_FORMAT, EncodeDatabaseTableUri } from "src/util/format";

import { Anchor } from "../anchor";
import {
  isMaxSizeError,
  SqlApiQueryResponse,
  SqlExecutionErrorMessage,
  TableHeuristicDetailsRow,
  TableIndexUsageStats,
  TableNameParts,
  TableSchemaDetailsRow,
  TableSpanStatsRow,
} from "../api";
import { LoadingCell } from "../databases";
import { Loading } from "../loading";
import { Search } from "../search";
import LoadingError from "../sqlActivity/errorComponent";
import { Timestamp, Timezone } from "../timestamp";
import { mvccGarbage, syncHistory, unique } from "../util";

import styles from "./databaseDetailsPage.module.scss";
import {
  DbDetailsBreadcrumbs,
  DiskSizeCell,
  IndexesCell,
  MVCCInfoCell,
  TableNameCell,
} from "./tableCells";
import { ViewMode } from "./types";

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
  // Request error when getting table names.
  requestError?: Error;
  // Query error when getting table names.
  queryError?: SqlExecutionErrorMessage;
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
  showIndexRecommendations: boolean;
  csIndexUnusedDuration: string;
}

export interface DatabaseDetailsPageDataTable {
  name: TableNameParts;
  // Display name containing unquoted, unescaped schema and table name parts.
  qualifiedDisplayName: string;
  loading: boolean;
  loaded: boolean;
  // Request error when getting table details.
  requestError?: Error;
  // Query error when getting table details.
  queryError?: SqlExecutionErrorMessage;
  details: DatabaseDetailsPageDataTableDetails;
}

interface GrantsData {
  roles: string[];
  privileges: string[];
}

export interface DatabaseDetailsPageDataTableDetails {
  schemaDetails?: SqlApiQueryResponse<TableSchemaDetailsRow>;
  grants: SqlApiQueryResponse<GrantsData>;
  statsLastUpdated?: SqlApiQueryResponse<TableHeuristicDetailsRow>;
  indexStatRecs?: SqlApiQueryResponse<TableIndexUsageStats>;
  spanStats?: SqlApiQueryResponse<TableSpanStatsRow>;
  // Array of node IDs used to unambiguously filter by node and region.
  nodes?: number[];
  // String of nodes grouped by region in alphabetical order, e.g.
  // regionA(n1,n2), regionB(n3). Used for display in the table's
  // "Regions/Nodes" column.
  nodesByRegionString?: string;
}

export interface DatabaseDetailsPageActions {
  refreshDatabaseDetails: (
    database: string,
    csIndexUnusedDuration: string,
  ) => void;
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => void;
  onFilterChange?: (value: Filters) => void;
  onSearchComplete?: (query: string) => void;
  onSortingTablesChange?: (columnTitle: string, ascending: boolean) => void;
  onSortingGrantsChange?: (columnTitle: string, ascending: boolean) => void;
  onViewModeChange?: (viewMode: ViewMode) => void;
  refreshNodes: () => void;
}

export type DatabaseDetailsPageProps = DatabaseDetailsPageData &
  DatabaseDetailsPageActions &
  RouteComponentProps;

interface DatabaseDetailsPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
}

const tablePageSize = 20;
const disableTableSortSize = tablePageSize * 2;

class DatabaseSortedTable extends SortedTable<DatabaseDetailsPageDataTable> {}

// filterBySearchQuery returns true if the search query matches the database name.
function filterBySearchQuery(
  table: DatabaseDetailsPageDataTable,
  search: string,
): boolean {
  const matchString = table.qualifiedDisplayName.toLowerCase();

  if (search.startsWith('"') && search.endsWith('"')) {
    search = search.substring(1, search.length - 1);

    return matchString.includes(search);
  }

  return search
    .toLowerCase()
    .split(" ")
    .every(val => matchString.includes(val));
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
    };

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // View Mode.
    const view = searchParams.get("viewMode") || undefined;
    let viewMode = ViewMode.Tables;
    if (view === ViewMode.Grants.toString()) {
      viewMode = ViewMode.Grants;
    }
    if (
      this.props.onViewModeChange &&
      view &&
      viewMode !== this.props.viewMode
    ) {
      this.props.onViewModeChange(viewMode);
    }

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting =
      viewMode === ViewMode.Tables
        ? this.props.sortSettingTables
        : this.props.sortSettingGrants;
    const onSortingChange =
      viewMode === ViewMode.Tables
        ? this.props.onSortingTablesChange
        : this.props.onSortingGrantsChange;

    if (
      onSortingChange &&
      columnTitle &&
      (sortSetting.columnTitle !== columnTitle ||
        sortSetting.ascending !== ascending)
    ) {
      onSortingChange(columnTitle, ascending);
    }
  }

  componentDidMount(): void {
    if (!this.props.isTenant) {
      this.props.refreshNodes();
    }
    if (!this.props.loaded && !this.props.loading && !this.props.requestError) {
      this.props.refreshDatabaseDetails(
        this.props.name,
        this.props.csIndexUnusedDuration,
      );
    } else {
      // If the props are already loaded then componentDidUpdate
      // will not get called so call refresh to make sure details
      // are loaded
      this.refresh();
    }
  }

  componentDidUpdate(
    prevProps: Readonly<DatabaseDetailsPageProps>,
    prevState: Readonly<DatabaseDetailsPageState>,
  ): void {
    if (this.shouldRefreshTableInformation(prevState, prevProps)) {
      this.refresh();
    }
  }

  private shouldRefreshTableInformation(
    prevState: Readonly<DatabaseDetailsPageState>,
    prevProps: Readonly<DatabaseDetailsPageProps>,
  ): boolean {
    // No new tables to update
    if (
      !this.props.tables ||
      this.props.tables.length === 0 ||
      this.props.tables.every(x => x.loaded || x.loading)
    ) {
      return false;
    }

    if (this.state.pagination.current !== prevState.pagination.current) {
      return true;
    }

    if (prevProps && this.props.search !== prevProps.search) {
      return true;
    }

    const filteredTables = this.filteredDatabaseTables();
    for (
      let i = 0;
      i < filteredTables.length && i < disableTableSortSize;
      i++
    ) {
      const table = filteredTables[i];
      if (!table.loaded && !table.loading && table.requestError == null) {
        return true;
      }
    }

    return false;
  }

  private refresh(): void {
    // Load everything by default
    let filteredTables = this.props.tables;

    // Loading only the first page. If more than 2 page of tables
    // exists the sort will be disabled.
    if (this.props.tables.length > disableTableSortSize) {
      const startIndex =
        this.state.pagination.pageSize * (this.state.pagination.current - 1);
      // Result maybe filtered so get db names from filtered results
      if (this.props.search && this.props.search.length > 0) {
        filteredTables = this.filteredDatabaseTables();
      }

      if (!filteredTables || filteredTables.length === 0) {
        return;
      }

      // Only load the first page
      filteredTables = filteredTables.slice(
        startIndex,
        startIndex + disableTableSortSize,
      );
    }

    filteredTables.forEach(table => {
      if (!table.loaded && !table.loading && table.requestError === undefined) {
        this.props.refreshTableDetails(
          this.props.name,
          table.name.qualifiedNameWithSchemaAndTable,
          this.props.csIndexUnusedDuration,
        );
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
      this.props.viewMode === ViewMode.Tables
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

    // Avoid the loop if no filters/search are applied
    if (
      (!search || search.length === 0) &&
      regionsSelected.length === 0 &&
      nodesSelected.length === 0
    ) {
      return tables;
    }

    return tables
      .filter(table => (search ? filterBySearchQuery(table, search) : true))
      .filter(table => {
        if (regionsSelected.length === 0 && nodesSelected.length === 0)
          return true;

        let foundRegion = regionsSelected.length === 0;
        let foundNode = nodesSelected.length === 0;

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

  private columnsForTablesViewMode(): ColumnDescriptor<DatabaseDetailsPageDataTable>[] {
    return (
      [
        {
          title: (
            <Tooltip placement="bottom" title="The name of the table.">
              Tables
            </Tooltip>
          ),
          cell: table => <TableNameCell table={table} dbDetails={this.props} />,
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
          cell: table => <DiskSizeCell table={table} />,
          sort: table => table.details.spanStats?.approximate_disk_bytes,
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
          cell: table => (
            <LoadingCell
              requestError={table.requestError}
              queryError={table.details.spanStats?.error}
              loading={table.loading}
              errorClassName={cx("database-table__cell-error")}
            >
              {table.details.spanStats?.range_count}
            </LoadingCell>
          ),
          sort: table => table.details.spanStats?.range_count,
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
          cell: table => (
            <LoadingCell
              requestError={table.requestError}
              queryError={table.details.schemaDetails?.error}
              loading={table.loading}
              errorClassName={cx("database-table__cell-error")}
            >
              {table.details.schemaDetails?.columns?.length}
            </LoadingCell>
          ),
          sort: table => table.details.schemaDetails?.columns?.length,
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
          cell: table => (
            <IndexesCell
              table={table}
              showIndexRecommendations={this.props.showIndexRecommendations}
            />
          ),
          sort: table => table.details.schemaDetails?.indexes?.length,
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
          cell: table => (
            <LoadingCell
              requestError={table.requestError}
              queryError={null}
              loading={table.loading}
              errorClassName={cx("database-table__cell-error")}
            >
              {table.details.nodesByRegionString &&
              table.details.nodesByRegionString.length > 0
                ? table.details.nodesByRegionString
                : null}
            </LoadingCell>
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
          cell: table => (
            <LoadingCell
              requestError={table.requestError}
              queryError={table.details.spanStats?.error}
              loading={table.loading}
              errorClassName={cx("database-table__cell-error")}
            >
              {table.details.spanStats ? (
                <MVCCInfoCell details={table.details} />
              ) : null}
            </LoadingCell>
          ),
          sort: table => table.details.spanStats?.live_percentage,
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
            <LoadingCell
              requestError={table.requestError}
              queryError={table.details.statsLastUpdated?.error}
              loading={table.loading}
              errorClassName={cx("database-table__cell-error")}
            >
              <Timestamp
                time={table.details.statsLastUpdated?.stats_last_created_at}
                format={DATE_FORMAT}
                fallback={"No table statistics found"}
              />
            </LoadingCell>
          ),
          sort: table => table.details.statsLastUpdated,
          className: cx("database-table__col--table-stats"),
          name: "tableStatsUpdated",
        },
      ] as ColumnDescriptor<DatabaseDetailsPageDataTable>[]
    ).filter(c => c.showByDefault !== false);
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
              EncodeDatabaseTableUri(
                this.props.name,
                table.name.qualifiedNameWithSchemaAndTable,
              ) + `?tab=grants`
            }
            className={cx("icon__container")}
          >
            <DatabaseIcon className={cx("icon--s")} />
            <span className={cx("schema-name")}>{table.name.schema}.</span>
            <span>{table.name.table}</span>
          </Link>
        ),
        sort: table => table.qualifiedDisplayName,
        className: cx("database-table__col-name"),
        name: "name",
      },
      {
        title: (
          <Tooltip placement="bottom" title="The number of users of the table.">
            Users
          </Tooltip>
        ),
        cell: table => (
          <LoadingCell
            requestError={table.requestError}
            queryError={table.details.grants?.error}
            loading={table.loading}
            errorClassName={cx("database-table__cell-error")}
          >
            {table.details.grants?.roles.length}
          </LoadingCell>
        ),
        sort: table => table.details.grants?.roles.length,
        className: cx("database-table__col-user-count"),
        name: "userCount",
      },
      {
        title: (
          <Tooltip placement="bottom" title="The list of roles of the table.">
            Roles
          </Tooltip>
        ),
        cell: table => (
          <LoadingCell
            requestError={table.requestError}
            queryError={table.details.grants?.error}
            loading={table.loading}
            errorClassName={cx("database-table__cell-error")}
          >
            {table.details.grants?.roles.join(", ")}
          </LoadingCell>
        ),
        sort: table => table.details.grants?.roles.join(", "),
        className: cx("database-table__col-roles"),
        name: "roles",
      },
      {
        title: (
          <Tooltip placement="bottom" title="The list of grants of the table.">
            Grants
          </Tooltip>
        ),
        cell: table => (
          <LoadingCell
            requestError={table.requestError}
            queryError={table.details.grants?.error}
            loading={table.loading}
            errorClassName={cx("database-table__cell-error")}
          >
            {table.details.grants?.privileges.join(", ")}
          </LoadingCell>
        ),
        sort: table => table.details.grants?.privileges?.join(", "),
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
      this.props.viewMode === ViewMode.Tables
        ? this.props.sortSettingTables
        : this.props.sortSettingGrants;

    const showNodes = !isTenant && nodes.length > 1;
    const showRegions = regions.length > 1;

    // Only show the filter component when the viewMode is Tables and if at
    // least one of drop-down is shown.
    const filterComponent =
      this.props.viewMode === ViewMode.Tables && (showNodes || showRegions) ? (
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
          <DbDetailsBreadcrumbs dbName={this.props.name} />
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
            error={this.props.requestError}
            renderError={() =>
              LoadingError({
                statsType: "databases",
                error: this.props.requestError,
              })
            }
          >
            {isMaxSizeError(this.props.queryError?.message) && (
              <InlineAlert
                intent="info"
                title={
                  <>
                    Not all tables are displayed because the maximum number of
                    tables was reached in the console.&nbsp;
                  </>
                }
              />
            )}
            <DatabaseSortedTable
              className={cx("database-table")}
              tableWrapperClassName={cx("sorted-table")}
              data={tablesToDisplay}
              columns={this.columns()}
              sortSetting={sortSetting}
              onChangeSortSetting={this.changeSortSetting}
              pagination={this.state.pagination}
              loading={this.props.loading}
              disableSortSizeLimit={disableTableSortSize}
              renderNoResult={
                <div
                  className={cx("database-table__no-result", "icon__container")}
                >
                  <DatabaseIcon className={cx("icon--s")} />
                  This database has no tables.
                </div>
              }
            />
          </Loading>
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
