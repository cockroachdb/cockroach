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
import { RouteComponentProps } from "react-router-dom";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import classNames from "classnames/bind";
import classnames from "classnames/bind";

import { Anchor } from "src/anchor";
import { StackIcon } from "src/icon/stackIcon";
import { Pagination } from "src/pagination";
import { BooleanSetting } from "src/settings/booleanSetting";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  ColumnDescriptor,
  handleSortSettingFromQueryString,
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import * as format from "src/util/format";
import styles from "./databasesPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { syncHistory, tableStatsClusterSetting, unique } from "src/util";
import booleanSettingStyles from "../settings/booleanSetting.module.scss";
import LoadingError from "../sqlActivity/errorComponent";
import { Loading } from "../loading";
import { Search } from "../search";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
  handleFiltersFromQueryString,
} from "../queryFilter";
import { merge } from "lodash";
import { UIConfigState } from "src/store";
import { TableStatistics } from "../tableStatistics";
import { DatabaseNameCell, IndexRecCell } from "./helperComponents";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const booleanSettingCx = classnames.bind(booleanSettingStyles);

// We break out separate interfaces for some of the nested objects in our data
// both so that they can be available as SortedTable rows and for making
// (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface DatabasesPageData {
//     loading: boolean;
//     loaded: boolean;
//     lastError: Error;
//     sortSetting: SortSetting;
//     search: string;
//     filters: Filters;
//     nodeRegions: { [nodeId: string]: string };
//     isTenant: boolean;
//     databases: { // DatabasesPageDataDatabase[]
//       loading: boolean;
//       loaded: boolean;
//       name: string;
//       sizeInBytes: number;
//       tableCount: number;
//       rangeCount: number;
//       nodes: number[];
//       nodesByRegionString: string;
//     }[];
//   }
export interface DatabasesPageData {
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  databases: DatabasesPageDataDatabase[];
  sortSetting: SortSetting;
  search: string;
  filters: Filters;
  nodeRegions: { [nodeId: string]: string };
  isTenant?: UIConfigState["isTenant"];
  automaticStatsCollectionEnabled?: boolean;
  indexRecommendationsEnabled: boolean;
  showNodeRegionsColumn?: boolean;
}

export interface DatabasesPageDataDatabase {
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  name: string;
  sizeInBytes: number;
  tableCount: number;
  rangeCount: number;
  // Array of node IDs used to unambiguously filter by node and region.
  nodes?: number[];
  // String of nodes grouped by region in alphabetical order, e.g.
  // regionA(n1,n2), regionB(n3). Used for display in the table's
  // "Regions/Nodes" column.
  nodesByRegionString?: string;
  numIndexRecommendations: number;
}

export interface DatabasesPageActions {
  refreshDatabases: () => void;
  refreshDatabaseDetails: (database: string) => void;
  refreshSettings: () => void;
  refreshNodes?: () => void;
  onFilterChange?: (value: Filters) => void;
  onSearchComplete?: (query: string) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
}

export type DatabasesPageProps = DatabasesPageData &
  DatabasesPageActions &
  RouteComponentProps<unknown>;

interface DatabasesPageState {
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
  lastDetailsError: Error;
  columns: ColumnDescriptor<DatabasesPageDataDatabase>[];
}

class DatabasesSortedTable extends SortedTable<DatabasesPageDataDatabase> {}

// filterBySearchQuery returns true if the search query matches the database name.
function filterBySearchQuery(
  database: DatabasesPageDataDatabase,
  search: string,
): boolean {
  const matchString = database.name.toLowerCase();

  if (search.startsWith('"') && search.endsWith('"')) {
    search = search.substring(1, search.length - 1);

    return matchString.includes(search);
  }

  return search
    .toLowerCase()
    .split(" ")
    .every(val => matchString.includes(val));
}

const tablePageSize = 20;
const disableTableSortSize = tablePageSize * 2;

export class DatabasesPage extends React.Component<
  DatabasesPageProps,
  DatabasesPageState
> {
  constructor(props: DatabasesPageProps) {
    super(props);

    this.state = {
      filters: defaultFilters,
      pagination: {
        current: 1,
        pageSize: tablePageSize,
      },
      lastDetailsError: null,
      columns: this.columns(),
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(this.state, stateFromHistory);

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sortSetting;

    if (
      this.props.onSortingChange &&
      columnTitle &&
      (sortSetting.columnTitle != columnTitle ||
        sortSetting.ascending != ascending)
    ) {
      this.props.onSortingChange("Databases", columnTitle, ascending);
    }
  }

  getStateFromHistory = (): Partial<DatabasesPageState> => {
    const {
      history,
      search,
      sortSetting,
      filters,
      onFilterChange,
      onSearchComplete,
      onSortingChange,
    } = this.props;

    const searchParams = new URLSearchParams(history.location.search);

    const searchQuery = searchParams.get("q") || undefined;
    if (onSearchComplete && searchQuery && search != searchQuery) {
      onSearchComplete(searchQuery);
    }

    handleSortSettingFromQueryString(
      "Databases",
      history.location.search,
      sortSetting,
      onSortingChange,
    );

    const latestFilter = handleFiltersFromQueryString(
      history,
      filters,
      onFilterChange,
    );

    return {
      filters: latestFilter,
      activeFilters: calculateActiveFilters(latestFilter),
    };
  };

  componentDidMount(): void {
    if (this.props.refreshNodes != null) {
      this.props.refreshNodes();
    }

    if (this.props.refreshSettings != null) {
      this.props.refreshSettings();
    }

    if (
      !this.props.loaded &&
      !this.props.loading &&
      this.props.lastError === undefined
    ) {
      return this.props.refreshDatabases();
    } else {
      // If the props are already loaded then componentDidUpdate
      // will not get called so call refresh to make sure details
      // are loaded
      this.refresh();
    }
  }

  updateQueryParams(): void {
    const { history, search } = this.props;

    // Search
    const searchParams = new URLSearchParams(history.location.search);
    const searchQueryString = searchParams.get("q") || "";
    if (search && search != searchQueryString) {
      syncHistory(
        {
          q: search,
        },
        history,
      );
    }
  }

  componentDidUpdate(
    prevProps: Readonly<DatabasesPageProps>,
    prevState: Readonly<DatabasesPageState>,
  ): void {
    if (this.shouldRefreshDatabaseInformation(prevState, prevProps)) {
      this.updateQueryParams();
      this.refresh();
    }
    if (
      prevProps.indexRecommendationsEnabled !==
        this.props.indexRecommendationsEnabled ||
      prevProps.showNodeRegionsColumn !== this.props.showNodeRegionsColumn
    ) {
      this.setState({ columns: this.columns() });
    }
  }

  private refresh(): void {
    let lastDetailsError: Error;

    // load everything by default
    let filteredDbs = this.props.databases;

    // Loading only the first page if there are more than
    // 40 dbs. If there is more than 40 dbs sort will be disabled.
    if (this.props.databases.length > disableTableSortSize) {
      const startIndex =
        this.state.pagination.pageSize * (this.state.pagination.current - 1);
      // Result maybe filtered so get db names from filtered results
      if (this.props.search && this.props.search.length > 0) {
        filteredDbs = this.filteredDatabasesData();
      }

      if (!filteredDbs || filteredDbs.length === 0) {
        return;
      }

      // Only load the first page
      filteredDbs = filteredDbs.slice(
        startIndex,
        startIndex + this.state.pagination.pageSize,
      );
    }

    filteredDbs.forEach(database => {
      if (database.lastError) {
        lastDetailsError = database.lastError;
      }

      if (
        lastDetailsError &&
        this.state.lastDetailsError?.name != lastDetailsError?.name
      ) {
        this.setState({ lastDetailsError: lastDetailsError });
      }

      if (
        !database.loaded &&
        !database.loading &&
        database.lastError === undefined
      ) {
        this.props.refreshDatabaseDetails(database.name);
      }
    });
  }

  changePage = (current: number): void => {
    this.setState({ pagination: { ...this.state.pagination, current } });
  };

  changeSortSetting = (ss: SortSetting): void => {
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Databases", ss.columnTitle, ss.ascending);
    }
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

  // Returns a list of databases to the display based on input from the search
  // box and the applied filters.
  filteredDatabasesData = (): DatabasesPageDataDatabase[] => {
    const { search, databases, filters, nodeRegions } = this.props;

    // The regions and nodes selected from the filter dropdown.
    const regionsSelected =
      filters.regions?.length > 0 ? filters.regions.split(",") : [];
    const nodesSelected =
      filters.nodes?.length > 0 ? filters.nodes.split(",") : [];

    return databases
      .filter(db => (search ? filterBySearchQuery(db, search) : true))
      .filter(db => {
        if (regionsSelected.length == 0 && nodesSelected.length == 0)
          return true;

        let foundRegion = regionsSelected.length == 0;
        let foundNode = nodesSelected.length == 0;

        db.nodes?.forEach(node => {
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

  private shouldRefreshDatabaseInformation(
    prevState: Readonly<DatabasesPageState>,
    prevProps: Readonly<DatabasesPageProps>,
  ): boolean {
    // No new dbs to update
    if (
      !this.props.databases ||
      this.props.databases.length == 0 ||
      this.props.databases.every(x => x.loaded || x.loading)
    ) {
      return false;
    }

    if (this.state.pagination.current != prevState.pagination.current) {
      return true;
    }

    if (prevProps && this.props.search != prevProps.search) {
      return true;
    }

    const filteredDatabases = this.filteredDatabasesData();
    for (
      let i = 0;
      i < filteredDatabases.length && i < disableTableSortSize;
      i++
    ) {
      const db = filteredDatabases[i];
      if (db.loaded || db.loading || db.lastError != undefined) {
        continue;
      }
      // Info is not loaded for a visible database.
      return true;
    }

    return false;
  }

  checkInfoAvailable = (
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

  private columns(): ColumnDescriptor<DatabasesPageDataDatabase>[] {
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
          this.checkInfoAvailable(database, format.Bytes(database.sizeInBytes)),
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
        cell: database =>
          this.checkInfoAvailable(database, database.tableCount),
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
        cell: database =>
          this.checkInfoAvailable(database, database.rangeCount),
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
            {this.props.isTenant ? "Regions" : "Regions/Nodes"}
          </Tooltip>
        ),
        cell: database =>
          this.checkInfoAvailable(
            database,
            database.nodesByRegionString || "None",
          ),
        sort: database => database.nodesByRegionString,
        className: cx("databases-table__col-node-regions"),
        name: "nodeRegions",
        hideIfTenant: true,
        showByDefault: this.props.showNodeRegionsColumn,
      },
    ];
    if (this.props.indexRecommendationsEnabled) {
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
  }

  render(): React.ReactElement {
    const displayColumns = this.state.columns.filter(
      col => col.showByDefault !== false,
    );

    const { filters, search, nodeRegions, isTenant } = this.props;
    const { pagination } = this.state;

    const databasesToDisplay = this.filteredDatabasesData();
    const activeFilters = calculateActiveFilters(filters);

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(Object.values(nodeRegions));
    const showNodes = !isTenant && nodes.length > 1;
    const showRegions = regions.length > 1;

    // Only show the databases filter if at least one drop-down is shown.
    const databasesFilter =
      showNodes || showRegions ? (
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
      <div>
        <div className={baseHeadingClasses.wrapper}>
          <h3 className={baseHeadingClasses.tableName}>Databases</h3>
          {this.props.automaticStatsCollectionEnabled != null && (
            <BooleanSetting
              text={"Auto stats collection"}
              enabled={this.props.automaticStatsCollectionEnabled}
              tooltipText={
                <span>
                  {" "}
                  Automatic statistics can help improve query performance. Learn
                  how to{" "}
                  <Anchor
                    href={tableStatsClusterSetting}
                    target="_blank"
                    className={booleanSettingCx("crl-hover-text__link-text")}
                  >
                    manage statistics collection
                  </Anchor>
                  .
                </span>
              }
            />
          )}
        </div>
        <section className={sortableTableCx("cl-table-container")}>
          <PageConfig>
            <PageConfigItem>
              <Search
                onSubmit={this.onSubmitSearchField}
                onClear={this.onClearSearchField}
                defaultValue={search}
                placeholder={"Search Databases"}
              />
            </PageConfigItem>
            {databasesFilter}
          </PageConfig>
          <TableStatistics
            pagination={pagination}
            totalCount={databasesToDisplay.length}
            arrayItemName="databases"
            activeFilters={activeFilters}
            onClearFilters={this.onClearFilters}
          />
          <Loading
            loading={this.props.loading}
            page={"databases"}
            error={this.props.lastError}
            render={() => (
              <DatabasesSortedTable
                className={cx("databases-table")}
                tableWrapperClassName={cx("sorted-table")}
                data={databasesToDisplay}
                columns={displayColumns}
                sortSetting={this.props.sortSetting}
                onChangeSortSetting={this.changeSortSetting}
                pagination={this.state.pagination}
                loading={this.props.loading}
                disableSortSizeLimit={disableTableSortSize}
                renderNoResult={
                  <div
                    className={cx(
                      "databases-table__no-result",
                      "icon__container",
                    )}
                  >
                    <StackIcon className={cx("icon--s")} />
                    This cluster has no databases.
                  </div>
                }
              />
            )}
            renderError={() =>
              LoadingError({
                statsType: "databases",
                error: this.props.lastError,
              })
            }
          />
          {!this.props.loading && (
            <Loading
              loading={this.props.loading}
              page={"databases"}
              error={this.state.lastDetailsError}
              render={() => <></>}
              renderError={() =>
                LoadingError({
                  statsType: "part of the information",
                  error: this.state.lastDetailsError,
                })
              }
            />
          )}
        </section>

        <Pagination
          pageSize={this.state.pagination.pageSize}
          current={this.state.pagination.current}
          total={databasesToDisplay.length}
          onChange={this.changePage}
        />
      </div>
    );
  }
}
